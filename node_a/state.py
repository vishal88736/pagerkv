"""Bounded active window (L1) for Node A and all of its state transitions.

The active-context view may page a block to its stub only after Node B has
acknowledged the exact version. Full values remain in the durable primary
backing store independently of the active prompt.

A dirty block may still leave the *prompt* while Node B is unreachable: it is
"spilled" to Node A's local disk (the fsync'd JSON state file, which already
holds its full text) and shown as a ``[SPILLED ...]`` stub.  Its full text is
never dropped; once Node B acks that version it becomes an ordinary paged block.
With ``spill_dirty=False`` dirty blocks stay resident and the window may go over
budget instead.  Safety always wins over the token cap.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from common.summarizer import build_stub
from common.tokens import count_tokens

if TYPE_CHECKING:  # pragma: no cover - typing only
    from node_a.replicator import ReplicationResult, Replicator

logger = logging.getLogger("pagerkv.node_a.state")

RESIDENT_FULL = "RESIDENT_FULL"
PAGED_TO_NODE_B = "PAGED_TO_NODE_B"
SPILLED_LOCAL = "SPILLED_LOCAL"

PENDING_REPLICATION = "PENDING_REPLICATION"
SYNCED_ARCHIVED = "SYNCED_ARCHIVED"
DIRTY_UNREPLICATED = "DIRTY_UNREPLICATED"

_RESIDENCES = {RESIDENT_FULL, PAGED_TO_NODE_B, SPILLED_LOCAL}
_STATUSES = {PENDING_REPLICATION, SYNCED_ARCHIVED, DIRTY_UNREPLICATED}
_KEY_SUFFIX = re.compile(r"^(.*?)(\d+)$")


class KeyNotFound(Exception):
    """The key is not present in Node A."""


class HydrateUnavailable(Exception):
    """Node B could not provide the full text."""

    def __init__(self, error: str, stub: str | None) -> None:
        super().__init__(error)
        self.error = error
        self.stub = stub


class HydrateVersionMismatch(Exception):
    """Node B returned a different version than the one Node A expects."""


def compress_key_ranges(keys: list[str]) -> str:
    """Compress a list of keys into ranges where possible.

    Keys sharing a prefix with consecutive integer suffixes collapse to
    ``prefix{start}–prefix{end}``.  Non-numeric keys and gaps are preserved.
    """
    groups: dict[str, list[int]] = {}
    others: list[str] = []
    for key in keys:
        match = _KEY_SUFFIX.match(key)
        if match:
            prefix, number = match.group(1), int(match.group(2))
            groups.setdefault(prefix, []).append(number)
        else:
            others.append(key)

    parts: list[str] = []
    for prefix in sorted(groups):
        numbers = sorted(set(groups[prefix]))
        runs: list[str] = []
        start = prev = numbers[0]
        tail: list[int | None] = [*numbers[1:], None]
        for nxt in tail:
            if nxt is not None and nxt == prev + 1:
                prev = nxt
                continue
            if start == prev:
                runs.append(f"{prefix}{start}")
            else:
                runs.append(f"{prefix}{start}–{prefix}{prev}")
            if nxt is not None:
                start = prev = nxt
        parts.extend(runs)
    parts.extend(sorted(others))
    return ", ".join(parts)


@dataclass
class KeyEntry:
    key: str
    version: int
    full_text: str | None
    full_tokens: int
    stub: str | None
    residence: str
    consistency_status: str
    acked_version: int
    replication_error: str | None
    created_at: float
    last_access: float
    # Token count of the version Node B actually holds.  It is deliberately
    # separate from ``full_tokens``: a new version can be dirty while Node B
    # still holds the previous one.
    acked_full_tokens: int | None = None
    pinned_until_compaction: bool = False
    # Caller-controlled eviction policy: pinned keys stay resident; among
    # evictable keys the lowest priority goes first, then least recently used.
    pinned: bool = False
    priority: int = 0

    @property
    def active_tokens(self) -> int:
        """Tokens this key currently contributes to the window."""
        if self.residence == RESIDENT_FULL:
            return self.full_tokens
        return count_tokens(self.stub or "")

    @property
    def eviction_allowed(self) -> bool:
        """True only when Node B holds this exact version and a stub exists."""
        return (
            self.residence == RESIDENT_FULL
            and self.consistency_status == SYNCED_ARCHIVED
            and self.acked_version == self.version
            and self.stub is not None
        )

    @property
    def spill_allowed(self) -> bool:
        """Dirty, resident, unpinned blocks may move to the local spill."""
        return (
            self.residence == RESIDENT_FULL
            and self.consistency_status == DIRTY_UNREPLICATED
            and self.full_text is not None
            and not self.pinned
        )

    @property
    def eviction_key(self) -> tuple[int, float]:
        return (self.priority, self.last_access)


class NodeAState:
    """All Node A state plus the compaction / hydration algorithms."""

    def __init__(
        self,
        max_window_tokens: int = 2000,
        state_file: str = "node_a_state.json",
        index_budget_tokens: int = 150,
        spill_dirty: bool = True,
    ) -> None:
        self.max_window_tokens = max_window_tokens
        self.index_budget_tokens = index_budget_tokens
        self.spill_dirty = spill_dirty
        self.state_file = state_file
        self.entries: dict[str, KeyEntry] = {}
        self.full_values: dict[str, str] = {}
        # key -> highest version deleted locally whose Node B copy may remain.
        self.tombstones: dict[str, int] = {}
        self.page_faults = 0
        self.over_budget = False
        self.replication_attempts = 0
        self.replication_successes = 0
        self.replication_failures = 0
        self.last_replication_error: str | None = None
        self._render_cache: tuple[tuple[object, ...], str, int, int] | None = None
        self._lock = asyncio.Lock()

        self._load()
        self.full_values = {k: v for k, v in self.full_values.items() if k in self.entries}
        for key, entry in self.entries.items():
            if entry.full_text is not None:
                self.full_values.setdefault(key, entry.full_text)
        self.compact()
        self._persist()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _persist(self) -> None:
        """Atomically replace the JSON state file (tmp file + os.replace)."""
        directory = os.path.dirname(os.path.abspath(self.state_file))
        try:
            os.makedirs(directory, exist_ok=True)
            tmp_path = f"{self.state_file}.tmp"
            payload = {
                "full_values": self.full_values,
                "page_faults": self.page_faults,
                "replication_attempts": self.replication_attempts,
                "replication_successes": self.replication_successes,
                "replication_failures": self.replication_failures,
                "last_replication_error": self.last_replication_error,
                "tombstones": self.tombstones,
                "keys": {key: asdict(entry) for key, entry in self.entries.items()},
            }
            with open(tmp_path, "w", encoding="utf-8") as handle:
                json.dump(
                    payload,
                    handle,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self.state_file)
            # Make the rename itself durable where the platform supports it.
            try:
                directory_fd = os.open(directory, os.O_RDONLY)
            except OSError:
                directory_fd = -1
            if directory_fd >= 0:
                try:
                    os.fsync(directory_fd)
                except OSError:
                    pass
                finally:
                    os.close(directory_fd)
        except OSError:
            logger.exception("could not persist Node A state to %s", self.state_file)
            raise

    def _load(self) -> None:
        if not os.path.exists(self.state_file):
            return
        try:
            with open(self.state_file, encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, ValueError):
            logger.exception("could not load Node A state from %s", self.state_file)
            return
        if not isinstance(payload, dict):
            logger.error("state file %s is not a JSON object", self.state_file)
            return

        try:
            self.page_faults = int(payload.get("page_faults", 0))
        except (TypeError, ValueError):
            self.page_faults = 0

        try:
            self.replication_attempts = int(payload.get("replication_attempts", 0))
            self.replication_successes = int(payload.get("replication_successes", 0))
            self.replication_failures = int(payload.get("replication_failures", 0))
        except (TypeError, ValueError):
            self.replication_attempts = 0
            self.replication_successes = 0
            self.replication_failures = 0
        last_error = payload.get("last_replication_error")
        self.last_replication_error = str(last_error) if last_error is not None else None

        raw_tombstones = payload.get("tombstones")
        if isinstance(raw_tombstones, dict):
            for key, version in raw_tombstones.items():
                if isinstance(key, str) and isinstance(version, int):
                    self.tombstones[key] = version

        values = payload.get("full_values", {})
        if isinstance(values, dict):
            self.full_values = {
                k: v for k, v in values.items() if isinstance(k, str) and isinstance(v, str)
            }
        raw_keys = payload.get("keys")
        if not isinstance(raw_keys, dict):
            return

        now = time.time()
        for key, raw in raw_keys.items():
            if not isinstance(key, str) or not isinstance(raw, dict):
                continue
            try:
                version = max(1, int(raw.get("version", 1)))
                full_text = raw.get("full_text")
                if full_text is not None and not isinstance(full_text, str):
                    full_text = None
                stub = raw.get("stub")
                if stub is not None and not isinstance(stub, str):
                    stub = None
                full_tokens = int(
                    raw.get(
                        "full_tokens",
                        count_tokens(full_text) if full_text is not None else 0,
                    )
                )
                acked_version = int(raw.get("acked_version", 0))
                raw_acked_tokens = raw.get("acked_full_tokens")
                acked_full_tokens = (
                    int(raw_acked_tokens) if isinstance(raw_acked_tokens, int) else None
                )
                created_at = float(raw.get("created_at", now))
                last_access = float(raw.get("last_access", created_at))
                residence = str(raw.get("residence", RESIDENT_FULL))
                consistency = str(raw.get("consistency_status", PENDING_REPLICATION))
                replication_error = raw.get("replication_error")
                if replication_error is not None:
                    replication_error = str(replication_error)
                pinned = bool(raw.get("pinned", False))
                priority = int(raw.get("priority", 0))
            except (TypeError, ValueError):
                logger.warning("skipping malformed state entry %r", key)
                continue

            if residence not in _RESIDENCES:
                residence = PAGED_TO_NODE_B if full_text is None and stub else RESIDENT_FULL
            if consistency not in _STATUSES:
                consistency = DIRTY_UNREPLICATED
                replication_error = replication_error or "invalid stored status"

            if residence == SPILLED_LOCAL and full_text is None:
                # A spill always keeps the full text; without it the entry can
                # only be what Node B holds.
                if stub is not None and acked_version == version:
                    residence = PAGED_TO_NODE_B
                else:
                    logger.warning("skipping spilled entry without text %r", key)
                    continue

            if full_text is None and residence == RESIDENT_FULL:
                if stub is not None:
                    residence = PAGED_TO_NODE_B
                else:
                    logger.warning("skipping unrecoverable state entry %r", key)
                    continue

            if residence == PAGED_TO_NODE_B and stub is None:
                if full_text is not None:
                    residence = RESIDENT_FULL
                else:
                    logger.warning("skipping paged entry without stub %r", key)
                    continue

            if consistency == PENDING_REPLICATION:
                if acked_version == version:
                    consistency = SYNCED_ARCHIVED
                else:
                    consistency = DIRTY_UNREPLICATED
                    replication_error = (
                        replication_error or "Node A restarted while replication was pending"
                    )

            acked_version = max(0, min(acked_version, version))
            if acked_full_tokens is None and acked_version == version:
                # State files written by older versions did not record this.
                acked_full_tokens = full_tokens
            if acked_version == 0:
                acked_full_tokens = None

            self.entries[key] = KeyEntry(
                key=key,
                version=version,
                full_text=full_text,
                full_tokens=full_tokens,
                stub=stub,
                residence=residence,
                consistency_status=consistency,
                acked_version=acked_version,
                acked_full_tokens=acked_full_tokens,
                replication_error=replication_error,
                created_at=created_at,
                last_access=last_access,
                pinned_until_compaction=False,
                pinned=pinned,
                priority=priority,
            )

    # ------------------------------------------------------------------
    # Window accounting
    # ------------------------------------------------------------------

    def _ordered_entries(self) -> list[KeyEntry]:
        return sorted(self.entries.values(), key=lambda entry: (entry.created_at, entry.key))

    def _block_text(self, entry: KeyEntry) -> str:
        if entry.residence == RESIDENT_FULL and entry.full_text is not None:
            body = entry.full_text
        else:
            body = entry.stub or ""
        return f"### {entry.key} (v{entry.version})\n{body}"

    def build_prompt(self, trim_to_budget: bool = False) -> str:
        """Raw or trimmed prompt text (see render_prompt for index metadata)."""
        if trim_to_budget:
            return self.render_prompt()[0]
        ordered = self._ordered_entries()
        blocks = [self._block_text(e) for e in ordered]
        return "\n\n".join(blocks)

    def render_prompt(self) -> tuple[str, int, int]:
        """Render the prompt, returning (text, omitted_count, index_tokens).

        When the full block list fits, no index is rendered.  Otherwise the
        oldest non-resident (paged or spilled) blocks are omitted and a single ``archive_index``
        block is rendered first, listing them (newest omitted first) up to
        ``index_budget_tokens``.  Resident full blocks are never omitted; dirty
        residents can therefore still push the prompt over the cap, which is
        reported as ``over_budget``.

        The result is cached until a prompt-visible field changes, so status,
        metrics and dashboard polls do not re-tokenize the whole window.
        """
        ordered = self._ordered_entries()
        signature = tuple(
            (
                entry.key,
                entry.version,
                entry.residence,
                entry.stub,
                entry.full_text is not None,
            )
            for entry in ordered
        )
        if self._render_cache is not None and self._render_cache[0] == signature:
            _, text, omitted, index_tokens = self._render_cache
            return text, omitted, index_tokens

        blocks = [self._block_text(e) for e in ordered]
        full_prompt = "\n\n".join(blocks)
        if count_tokens(full_prompt) <= self.max_window_tokens:
            return self._cache_render(signature, full_prompt, 0, 0)

        paged_indices = [i for i, e in enumerate(ordered) if e.residence != RESIDENT_FULL]
        if not paged_indices:
            return self._cache_render(signature, full_prompt, 0, 0)

        # Pick how many cold blocks to fold into archive_index.  The index is
        # capped at index_budget_tokens, so reserving that budget while
        # comparing block totals avoids rebuilding the index for every
        # candidate; the exact prompt is then counted once and adjusted.
        block_tokens = [count_tokens(block) for block in blocks]
        total_block_tokens = sum(block_tokens)
        prefix_tokens = [0]
        for idx in paged_indices:
            prefix_tokens.append(prefix_tokens[-1] + block_tokens[idx])
        total = len(blocks)

        available = self.max_window_tokens - self.index_budget_tokens - 2
        remove_count = len(paged_indices)
        for candidate in range(1, len(paged_indices) + 1):
            remaining_count = total - candidate
            remaining_tokens = (
                total_block_tokens - prefix_tokens[candidate] + 2 * max(0, remaining_count - 1)
            )
            if remaining_tokens <= available:
                remove_count = candidate
                break

        removed = set(paged_indices[:remove_count])
        prompt = self._prompt_with_index(blocks, ordered, paged_indices, removed)
        # Exact correction: boundaries can merge tokens, and the real index
        # may be larger than expected in pathological key sets.
        while count_tokens(prompt) > self.max_window_tokens and remove_count < len(paged_indices):
            remove_count += 1
            removed = set(paged_indices[:remove_count])
            prompt = self._prompt_with_index(blocks, ordered, paged_indices, removed)

        # The reservation usually over-folds by a block or so; reveal newer
        # cold blocks again while the exact prompt still fits.
        while remove_count > 1:
            trial_removed = set(paged_indices[: remove_count - 1])
            trial = self._prompt_with_index(blocks, ordered, paged_indices, trial_removed)
            if count_tokens(trial) > self.max_window_tokens:
                break
            remove_count -= 1
            removed = trial_removed
            prompt = trial

        index_block, index_tok = self._index_block_for(ordered, paged_indices, removed)
        return self._cache_render(signature, prompt, remove_count, index_tok)

    def _index_block_for(
        self,
        ordered: list[KeyEntry],
        paged_indices: list[int],
        removed: set[int],
    ) -> tuple[str, int]:
        omitted_keys = [ordered[i].key for i in reversed(paged_indices) if i in removed]
        omitted_tokens = sum(ordered[i].full_tokens for i in removed)
        index_block = self._build_index_block(len(removed), omitted_keys, omitted_tokens)
        return index_block, count_tokens(index_block)

    def _prompt_with_index(
        self,
        blocks: list[str],
        ordered: list[KeyEntry],
        paged_indices: list[int],
        removed: set[int],
    ) -> str:
        if not removed:
            return "\n\n".join(blocks)
        index_block, _ = self._index_block_for(ordered, paged_indices, removed)
        remaining = "\n\n".join(block for i, block in enumerate(blocks) if i not in removed)
        return f"{index_block}\n\n{remaining}" if remaining else index_block

    def _cache_render(
        self,
        signature: tuple[object, ...],
        prompt: str,
        omitted: int,
        index_tokens: int,
    ) -> tuple[str, int, int]:
        self._render_cache = (signature, prompt, omitted, index_tokens)
        return prompt, omitted, index_tokens

    def _build_index_block(
        self, omitted_count: int, omitted_keys_newest_first: list[str], omitted_full_tokens: int
    ) -> str:
        header = (
            f"{omitted_count} older keys archived "
            f"({omitted_full_tokens} tokens on Node B), not shown below."
        )
        find_line = "Find: GET /context/search?q=<words>   Load: GET /context/{key}?hydrate=true"

        chosen: list[str] = []
        more = omitted_count
        for key in omitted_keys_newest_first:
            trial = chosen + [key]
            trial_str = compress_key_ranges(trial)
            more_trial = omitted_count - len(trial)
            keys_line = f"Keys: {trial_str}" + (f", +{more_trial} more" if more_trial > 0 else "")
            candidate = f"### archive_index\n{header}\n{keys_line}\n{find_line}"
            if count_tokens(candidate) <= self.index_budget_tokens:
                chosen = trial
                more = more_trial
            else:
                break

        if chosen:
            keys_str = compress_key_ranges(chosen)
            keys_line = f"Keys: {keys_str}" + (f", +{more} more" if more > 0 else "")
        else:
            keys_line = f"Keys: +{omitted_count} more"
        return f"### archive_index\n{header}\n{keys_line}\n{find_line}"

    def window_tokens(self) -> int:
        return count_tokens(self.render_prompt()[0])

    def raw_window_tokens(self) -> int:
        ordered = self._ordered_entries()
        blocks = [self._block_text(e) for e in ordered]
        return count_tokens("\n\n".join(blocks))

    def tokens_spilled_locally(self) -> int:
        saved = 0
        for entry in self.entries.values():
            if entry.residence == SPILLED_LOCAL:
                saved += entry.full_tokens - count_tokens(entry.stub or "")
        return saved

    def total_active_tokens(self) -> int:
        """Sum of every key's current contribution (full text or stub)."""
        return sum(entry.active_tokens for entry in self.entries.values())

    def tokens_saved_by_replication(self) -> int:
        saved = 0
        for entry in self.entries.values():
            if entry.residence == PAGED_TO_NODE_B:
                saved += entry.full_tokens - count_tokens(entry.stub or "")
        return saved

    # ------------------------------------------------------------------
    # Compaction
    # ------------------------------------------------------------------

    def compact(self) -> None:
        """Bring the window under budget without ever dropping unacked data.

        The caller MUST hold ``self._lock``.  Two passes, both lowest
        ``priority`` first, then least recently used; pinned keys are skipped:

        1. Page out blocks Node B has acked (full text dropped from Node A).
        2. If still over budget and ``spill_dirty`` is on, spill dirty blocks
           to the local disk: they leave the prompt but keep their full text.

        Pending blocks (a replication call is in flight) and pinned blocks are
        never moved, so the window can still end up over budget.  If stubs
        alone cannot fit, render_prompt drops the oldest cold stubs.
        """
        # The rendered prompt, not the uncapped raw block list, is the budget.
        # Once cold stubs are folded into archive_index, the rendered view can
        # already fit; evicting more would just raise the page-fault rate.
        while True:
            window = self.window_tokens()
            if window <= self.max_window_tokens:
                break
            candidates = [
                entry
                for entry in self.entries.values()
                if entry.eviction_allowed
                and not entry.pinned
                and not entry.pinned_until_compaction
                # A stub longer than the text would make the prompt bigger.
                and count_tokens(entry.stub or "") < entry.full_tokens
            ]
            if not candidates:
                break
            # Evict a batch whose full blocks cover the excess.  Savings are
            # optimistic (folding into archive_index costs almost nothing), so
            # the exact render above runs once more and corrects the tail
            # instead of re-tokenizing the window after every single eviction.
            excess = window - self.max_window_tokens
            saved = 0
            for victim in sorted(candidates, key=lambda entry: entry.eviction_key):
                saved += count_tokens(self._block_text(victim))
                victim.full_text = None
                victim.residence = PAGED_TO_NODE_B
                logger.info("paged out %s v%s", victim.key, victim.version)
                if saved >= excess:
                    break

        while self.spill_dirty and self.window_tokens() > self.max_window_tokens:
            spill_candidates: list[tuple[KeyEntry, str]] = []
            for entry in self.entries.values():
                if not entry.spill_allowed or entry.pinned_until_compaction:
                    continue
                spill_stub = build_stub(
                    entry.key,
                    entry.version,
                    entry.full_text or "",
                    entry.full_tokens,
                    label="SPILLED",
                )
                if count_tokens(spill_stub) < entry.full_tokens:
                    spill_candidates.append((entry, spill_stub))
            if not spill_candidates:
                break
            victim, spill_stub = min(spill_candidates, key=lambda item: item[0].eviction_key)
            victim.stub = spill_stub
            victim.residence = SPILLED_LOCAL
            logger.info("spilled dirty %s v%s to local disk", victim.key, victim.version)

        for entry in self.entries.values():
            entry.pinned_until_compaction = False

        self.over_budget = self.window_tokens() > self.max_window_tokens

    # ------------------------------------------------------------------
    # PUT
    # ------------------------------------------------------------------

    async def put(
        self,
        key: str,
        text: str,
        replicator: Replicator,
        pinned: bool | None = None,
        priority: int | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        async with self._lock:
            entry = self.entries.get(key)
            if entry is None:
                entry = KeyEntry(
                    key=key,
                    # Continue after a deleted version so a pending delete on
                    # Node B can never remove the re-created key.
                    version=self.tombstones.get(key, 0),
                    full_text=None,
                    full_tokens=0,
                    stub=None,
                    residence=RESIDENT_FULL,
                    consistency_status=PENDING_REPLICATION,
                    acked_version=0,
                    replication_error=None,
                    created_at=now,
                    last_access=now,
                )
                self.entries[key] = entry

            entry.version += 1
            entry.full_text = text
            self.full_values[key] = text
            entry.full_tokens = count_tokens(text)
            entry.residence = RESIDENT_FULL
            entry.stub = None
            entry.consistency_status = PENDING_REPLICATION
            entry.replication_error = None
            entry.last_access = now
            entry.pinned_until_compaction = False
            if pinned is not None:
                entry.pinned = pinned
            if priority is not None:
                entry.priority = priority
            version = entry.version
            self._persist()

        # At most two attempts: the second only after reconciling with a newer
        # version Node B already holds (e.g. Node A lost its state file).
        for _attempt in range(2):
            # No lock is held across the network call.
            result = await replicator.replicate(key, version, text)
            async with self._lock:
                entry = self.entries.get(key)
                if entry is None:
                    raise KeyNotFound(key)
                if entry.version != version:
                    logger.info(
                        "ignoring replication result for %s v%s; current version is v%s",
                        key,
                        version,
                        entry.version,
                    )
                    break
                self._apply_result(entry, version, result)
                if entry.version == version:  # not reconciled: done
                    break
                version = entry.version
                text = entry.full_text or text
                self._persist()

        async with self._lock:
            entry = self.entries.get(key)
            if entry is None:
                raise KeyNotFound(key)
            self.compact()
            self._persist()
            return self._summary(entry)

    # ------------------------------------------------------------------
    # GET / hydrate
    # ------------------------------------------------------------------

    async def read_value(self, key: str, replicator: Replicator) -> dict[str, Any]:
        """Read the durable primary value without hydrating or changing the prompt."""
        async with self._lock:
            entry = self.entries.get(key)
            if entry is None:
                raise KeyNotFound(key)
            version = entry.version
            if key in self.full_values:
                return {
                    "key": key,
                    "value": self.full_values[key],
                    "version": version,
                    "node": "A",
                    "source": "LOCAL_STORE",
                }
        # Recover legacy checkpoints whose paged values existed only on Node B.
        fetched = await replicator.fetch(key)
        if not fetched.ok or fetched.full_text is None:
            raise HydrateUnavailable(
                "Legacy local value unavailable; Node B is needed to recover it", entry.stub
            )
        async with self._lock:
            current = self.entries.get(key)
            if current is None:
                raise KeyNotFound(key)
            if current.version != version or fetched.version != version:
                raise HydrateVersionMismatch("Version changed while recovering legacy local value")
            self.full_values[key] = fetched.full_text
            self._persist()
            return {
                "key": key,
                "value": fetched.full_text,
                "version": version,
                "node": "A",
                "source": "LOCAL_STORE",
            }

    async def recover_local_values(self, replicator: Replicator) -> None:
        for key in list(self.entries):
            if key not in self.full_values:
                try:
                    await self.read_value(key, replicator)
                except (KeyNotFound, HydrateUnavailable, HydrateVersionMismatch):
                    logger.warning("Could not yet recover legacy local value for %s", key)

    async def get_view(self, key: str, touch: bool = True) -> dict[str, Any] | None:
        """Current content of ``key``; ``touch=False`` leaves LRU order alone."""
        async with self._lock:
            entry = self.entries.get(key)
            if entry is None:
                return None
            if touch:
                # LRU order is a cache heuristic; do not fsync the whole state
                # file on every read.  Mutations persist their own changes.
                entry.last_access = time.time()
            return self._view(entry)

    async def hydrate(self, key: str, replicator: Replicator) -> dict[str, Any]:
        async with self._lock:
            entry = self.entries.get(key)
            if entry is None:
                raise KeyNotFound(key)
            if entry.residence == RESIDENT_FULL and entry.full_text is not None:
                entry.last_access = time.time()
                return {
                    "key": entry.key,
                    "version": entry.version,
                    "full_text": entry.full_text,
                    "full_tokens": entry.full_tokens,
                    "source": "L1",
                }
            if entry.residence == SPILLED_LOCAL and entry.full_text is not None:
                # Local page fault: the spill holds the only full copy.
                entry.residence = RESIDENT_FULL
                entry.last_access = time.time()
                entry.pinned_until_compaction = True
                self.page_faults += 1
                self.compact()
                self._persist()
                return {
                    "key": entry.key,
                    "version": entry.version,
                    "full_text": entry.full_text,
                    "full_tokens": entry.full_tokens,
                    "source": "L1_SPILL",
                }
            version = entry.version
            stub = entry.stub

        fetched = await replicator.fetch(key)

        if not fetched.ok:
            raise HydrateUnavailable(fetched.error or "Node B unavailable", stub)
        if fetched.version != version:
            raise HydrateVersionMismatch(
                f"archive version mismatch: expected v{version}, got v{fetched.version}"
            )

        async with self._lock:
            entry = self.entries.get(key)
            if entry is None:
                raise KeyNotFound(key)
            if entry.version != version:
                raise HydrateVersionMismatch("archive version mismatch: key changed during hydrate")
            entry.full_text = fetched.full_text
            entry.full_tokens = (
                fetched.full_tokens if isinstance(fetched.full_tokens, int) else entry.full_tokens
            )
            entry.residence = RESIDENT_FULL
            entry.last_access = time.time()
            entry.pinned_until_compaction = True
            self.page_faults += 1
            self.compact()
            self._persist()
            return {
                "key": entry.key,
                "version": entry.version,
                "full_text": fetched.full_text,
                "full_tokens": entry.full_tokens,
                "source": "L2_PAGE_FAULT",
            }

    # ------------------------------------------------------------------
    # Retry loop support
    # ------------------------------------------------------------------

    async def retry_dirty(self, replicator: Replicator) -> None:
        async with self._lock:
            pending = [
                (entry.key, entry.version, entry.full_text)
                for entry in self.entries.values()
                if entry.consistency_status == DIRTY_UNREPLICATED and entry.full_text is not None
            ]

        for key, version, text in pending:
            if text is None:  # pragma: no cover - defensive
                continue
            result = await replicator.replicate(key, version, text)
            async with self._lock:
                entry = self.entries.get(key)
                if entry is None or entry.version != version:
                    continue
                self._apply_result(entry, version, result)

        async with self._lock:
            tombstones = list(self.tombstones.items())
        for key, version in tombstones:
            await self._settle_delete(key, version, replicator)

        async with self._lock:
            self.compact()
            self._persist()

    def _apply_result(self, entry: KeyEntry, version: int, result: ReplicationResult) -> None:
        """Apply Node B's answer for ``version`` (caller checked it is current)."""
        self.replication_attempts += 1
        if result.ok:
            self.replication_successes += 1
            entry.stub = result.stub
            entry.acked_version = version
            entry.acked_full_tokens = entry.full_tokens
            entry.consistency_status = SYNCED_ARCHIVED
            entry.replication_error = None
            if entry.residence == SPILLED_LOCAL:
                # Node B now holds this exact version: the spill can be dropped.
                entry.residence = PAGED_TO_NODE_B
                entry.full_text = None
            return
        self.replication_failures += 1
        self.last_replication_error = result.error or "replication failed"
        entry.consistency_status = DIRTY_UNREPLICATED
        if result.stale and result.remote_version is not None and result.remote_version >= version:
            # Node B is ahead (Node A lost its state, or a delete is pending
            # there).  Our text is the newest write, so jump past Node B's
            # version; the retry will then be accepted.
            entry.version = result.remote_version + 1
            entry.replication_error = (
                f"reconciled: Node B held v{result.remote_version}, retrying as v{entry.version}"
            )
            if entry.residence == SPILLED_LOCAL and entry.full_text is not None:
                entry.stub = build_stub(
                    entry.key,
                    entry.version,
                    entry.full_text,
                    entry.full_tokens,
                    label="SPILLED",
                )
            return
        entry.replication_error = result.error or "replication failed"

    # ------------------------------------------------------------------
    # DELETE / status / prompt / summaries
    # ------------------------------------------------------------------

    async def delete(self, key: str, replicator: Replicator | None = None) -> bool:
        """Remove ``key`` locally and (best effort, retried) from Node B."""
        async with self._lock:
            entry = self.entries.pop(key, None)
            self.full_values.pop(key, None)
            if entry is None:
                return False
            version = max(entry.version, self.tombstones.get(key, 0))
            self.tombstones[key] = version
            self.compact()
            self._persist()
        if replicator is not None:
            await self._settle_delete(key, version, replicator)
        return True

    async def _settle_delete(self, key: str, version: int, replicator: Replicator) -> None:
        if not await replicator.delete(key, version):
            return  # Node B unreachable; the retry loop tries again
        async with self._lock:
            if self.tombstones.get(key) == version:
                del self.tombstones[key]
                self._persist()

    async def set_policy(
        self, key: str, pinned: bool | None, priority: int | None
    ) -> dict[str, Any]:
        """Change a key's pin / eviction priority, then re-compact."""
        async with self._lock:
            entry = self.entries.get(key)
            if entry is None:
                raise KeyNotFound(key)
            if pinned is not None:
                entry.pinned = pinned
            if priority is not None:
                entry.priority = priority
            self.compact()
            self._persist()
            return self._summary(entry)

    async def status(self, include_text: bool = True) -> dict[str, Any]:
        async with self._lock:
            keys: dict[str, Any] = {}
            dirty = 0
            pending = 0
            for entry in self.entries.values():
                if entry.consistency_status == DIRTY_UNREPLICATED:
                    dirty += 1
                elif entry.consistency_status == PENDING_REPLICATION:
                    pending += 1
                prompt_view = None
                if include_text:
                    prompt_view = (
                        entry.full_text if entry.residence == RESIDENT_FULL else (entry.stub or "")
                    )
                keys[entry.key] = {
                    "version": entry.version,
                    "residence": entry.residence,
                    "consistency_status": entry.consistency_status,
                    "active_tokens_in_node_a": entry.active_tokens,
                    "full_tokens_on_node_b": (
                        entry.acked_full_tokens if entry.acked_version > 0 else None
                    ),
                    "eviction_allowed": entry.eviction_allowed,
                    "replication_error": entry.replication_error,
                    "pinned": entry.pinned,
                    "priority": entry.priority,
                    "acked_version": entry.acked_version,
                    "prompt_view": prompt_view,
                }
            prompt, omitted, index_tokens = self.render_prompt()
            window = count_tokens(prompt)
            consistent = dirty == 0 and pending == 0 and not self.tombstones
            return {
                "local_value_count": len(self.full_values),
                "active_window_tokens": window,
                "max_window_tokens": self.max_window_tokens,
                "over_budget": window > self.max_window_tokens,
                "tokens_saved_by_replication": self.tokens_saved_by_replication(),
                "tokens_spilled_locally": self.tokens_spilled_locally(),
                "page_faults": self.page_faults,
                "pending_deletes": len(self.tombstones),
                "omitted_from_prompt": omitted,
                "index_tokens": index_tokens,
                "consistency": "consistent" if consistent else "degraded",
                "replication_attempts": self.replication_attempts,
                "replication_successes": self.replication_successes,
                "replication_failures": self.replication_failures,
                "last_replication_error": self.last_replication_error,
                "keys": keys,
            }

    async def prompt(self) -> dict[str, Any]:
        async with self._lock:
            prompt, _omitted, _index = self.render_prompt()
            return {"prompt": prompt, "tokens": count_tokens(prompt)}

    async def local_search(self, q: str, include_text: bool = False) -> list[dict[str, Any]]:
        """Key-name matches; with ``include_text``, also local full texts.

        Text matching is the fallback while Node B (and its full-text index)
        is unreachable: resident and spilled blocks are still searchable.
        """
        async with self._lock:
            needle = (q or "").lower().strip()
            results: list[dict[str, Any]] = []
            if not needle:
                return results
            words = needle.split()
            for entry in self.entries.values():
                score = 0.0
                if needle in entry.key.lower():
                    score = 1.0
                elif include_text and entry.full_text:
                    text = entry.full_text.lower()
                    hits = sum(1 for w in words if w in text)
                    score = 0.9 * hits / len(words)
                if score <= 0:
                    continue
                stub = entry.stub
                if stub is None:
                    stub = (entry.full_text or "")[:120]
                results.append(
                    {
                        "key": entry.key,
                        "version": entry.version,
                        "stub": stub,
                        "score": score,
                    }
                )
            results.sort(key=lambda r: (-r["score"], r["key"]))
            return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _summary(self, entry: KeyEntry) -> dict[str, Any]:
        prompt, _omitted, _index = self.render_prompt()
        window = count_tokens(prompt)
        return {
            "key": entry.key,
            "version": entry.version,
            "consistency_status": entry.consistency_status,
            "residence": entry.residence,
            "active_tokens": entry.active_tokens,
            "full_tokens": entry.full_tokens,
            "replication_error": entry.replication_error,
            "active_window_tokens": window,
            "over_budget": window > self.max_window_tokens,
        }

    @staticmethod
    def _view(entry: KeyEntry) -> dict[str, Any]:
        is_stub = entry.residence != RESIDENT_FULL
        return {
            "key": entry.key,
            "version": entry.version,
            "residence": entry.residence,
            "consistency_status": entry.consistency_status,
            "acked_version": entry.acked_version,
            "active_tokens": entry.active_tokens,
            "full_tokens": entry.full_tokens,
            "content": entry.stub if is_stub else (entry.full_text or ""),
            "is_stub": is_stub,
            "replication_error": entry.replication_error,
        }
