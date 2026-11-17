"""Bounded active window (L1) for Node A and all of its state transitions.

The central rule is the dirty-bit rule: a block's full text may be dropped from
Node A only after Node B has acknowledged the *exact same version*.

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
        match = re.match(r"^(.*?)(\d+)$", key)
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
        # key -> highest version deleted locally whose Node B copy may remain.
        self.tombstones: dict[str, int] = {}
        self.page_faults = 0
        self.over_budget = False
        self._lock = asyncio.Lock()

        self._load()
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
                "page_faults": self.page_faults,
                "tombstones": self.tombstones,
                "keys": {key: asdict(entry) for key, entry in self.entries.items()},
            }
            with open(tmp_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self.state_file)
        except OSError:
            logger.exception("could not persist Node A state to %s", self.state_file)

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

        raw_tombstones = payload.get("tombstones")
        if isinstance(raw_tombstones, dict):
            for key, version in raw_tombstones.items():
                if isinstance(key, str) and isinstance(version, int):
                    self.tombstones[key] = version

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
                created_at = float(raw.get("created_at", now))
                last_access = float(raw.get("last_access", created_at))
                residence = str(raw.get("residence", RESIDENT_FULL))
                consistency = str(
                    raw.get("consistency_status", PENDING_REPLICATION)
                )
                replication_error = raw.get("replication_error")
                if replication_error is not None:
                    replication_error = str(replication_error)
                pinned = bool(raw.get("pinned", False))
                priority = int(raw.get("priority", 0))
            except (TypeError, ValueError):
                logger.warning("skipping malformed state entry %r", key)
                continue

            if residence not in _RESIDENCES:
                residence = (
                    PAGED_TO_NODE_B if full_text is None and stub else RESIDENT_FULL
                )
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
                        replication_error
                        or "Node A restarted while replication was pending"
                    )

            acked_version = max(0, min(acked_version, version))

            self.entries[key] = KeyEntry(
                key=key,
                version=version,
                full_text=full_text,
                full_tokens=full_tokens,
                stub=stub,
                residence=residence,
                consistency_status=consistency,
                acked_version=acked_version,
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
        """
        ordered = self._ordered_entries()
        blocks = [self._block_text(e) for e in ordered]

        full_prompt = "\n\n".join(blocks)
        if count_tokens(full_prompt) <= self.max_window_tokens:
            return full_prompt, 0, 0

        paged_indices = [
            i for i, e in enumerate(ordered) if e.residence != RESIDENT_FULL
        ]
        if not paged_indices:
            return full_prompt, 0, 0

        removed: set[int] = set()
        for idx in paged_indices:  # oldest first
            removed.add(idx)
            omitted_keys = [
                ordered[i].key for i in reversed(paged_indices) if i in removed
            ]
            omitted_tokens = sum(ordered[i].full_tokens for i in removed)
            index_block = self._build_index_block(
                len(removed), omitted_keys, omitted_tokens
            )
            index_tok = count_tokens(index_block)
            remaining = "\n\n".join(
                b for i, b in enumerate(blocks) if i not in removed
            )
            remaining_tok = count_tokens(remaining)
            sep = 2 if remaining else 0
            if index_tok + sep + remaining_tok <= self.max_window_tokens:
                break
            if idx == paged_indices[-1]:
                break

        if not removed:
            return full_prompt, 0, 0

        omitted_keys = [
            ordered[i].key for i in reversed(paged_indices) if i in removed
        ]
        omitted_tokens = sum(ordered[i].full_tokens for i in removed)
        index_block = self._build_index_block(
            len(removed), omitted_keys, omitted_tokens
        )
        index_tok = count_tokens(index_block)
        remaining = "\n\n".join(
            b for i, b in enumerate(blocks) if i not in removed
        )
        if remaining:
            prompt = index_block + "\n\n" + remaining
        else:
            prompt = index_block
        return prompt, len(removed), index_tok

    def _build_index_block(
        self, omitted_count: int, omitted_keys_newest_first: list[str], omitted_full_tokens: int
    ) -> str:
        header = (
            f"{omitted_count} older keys archived "
            f"({omitted_full_tokens} tokens on Node B), not shown below."
        )
        find_line = (
            "Find: GET /context/search?q=<words>   "
            "Load: GET /context/{key}?hydrate=true"
        )

        chosen: list[str] = []
        more = omitted_count
        for key in omitted_keys_newest_first:
            trial = chosen + [key]
            trial_str = compress_key_ranges(trial)
            more_trial = omitted_count - len(trial)
            keys_line = f"Keys: {trial_str}" + (
                f", +{more_trial} more" if more_trial > 0 else ""
            )
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
        while self.raw_window_tokens() > self.max_window_tokens:
            candidates = [
                entry
                for entry in self.entries.values()
                if entry.eviction_allowed
                and not entry.pinned
                and not entry.pinned_until_compaction
            ]
            if not candidates:
                break
            victim = min(candidates, key=lambda entry: entry.eviction_key)
            victim.full_text = None
            victim.residence = PAGED_TO_NODE_B
            logger.info("paged out %s v%s", victim.key, victim.version)

        while self.spill_dirty and self.raw_window_tokens() > self.max_window_tokens:
            candidates = [
                entry
                for entry in self.entries.values()
                if entry.spill_allowed and not entry.pinned_until_compaction
            ]
            if not candidates:
                break
            victim = min(candidates, key=lambda entry: entry.eviction_key)
            victim.stub = build_stub(
                victim.key,
                victim.version,
                victim.full_text or "",
                victim.full_tokens,
                label="SPILLED",
            )
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

    async def get_view(self, key: str) -> dict[str, Any] | None:
        async with self._lock:
            entry = self.entries.get(key)
            if entry is None:
                return None
            entry.last_access = time.time()
            self._persist()
            return self._view(entry)

    async def hydrate(self, key: str, replicator: Replicator) -> dict[str, Any]:
        async with self._lock:
            entry = self.entries.get(key)
            if entry is None:
                raise KeyNotFound(key)
            if entry.residence == RESIDENT_FULL and entry.full_text is not None:
                entry.last_access = time.time()
                self._persist()
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
            raise HydrateUnavailable(
                fetched.error or "Node B unavailable", stub
            )
        if fetched.version != version:
            raise HydrateVersionMismatch(
                f"archive version mismatch: expected v{version}, got v{fetched.version}"
            )

        async with self._lock:
            entry = self.entries.get(key)
            if entry is None:
                raise KeyNotFound(key)
            if entry.version != version:
                raise HydrateVersionMismatch(
                    "archive version mismatch: key changed during hydrate"
                )
            entry.full_text = fetched.full_text
            entry.full_tokens = (
                fetched.full_tokens
                if isinstance(fetched.full_tokens, int)
                else entry.full_tokens
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
                if entry.consistency_status == DIRTY_UNREPLICATED
                and entry.full_text is not None
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

    @staticmethod
    def _apply_result(
        entry: KeyEntry, version: int, result: ReplicationResult
    ) -> None:
        """Apply Node B's answer for ``version`` (caller checked it is current)."""
        if result.ok:
            entry.stub = result.stub
            entry.acked_version = version
            entry.consistency_status = SYNCED_ARCHIVED
            entry.replication_error = None
            if entry.residence == SPILLED_LOCAL:
                # Node B now holds this exact version: the spill can be dropped.
                entry.residence = PAGED_TO_NODE_B
                entry.full_text = None
            return
        entry.consistency_status = DIRTY_UNREPLICATED
        if (
            result.stale
            and result.remote_version is not None
            and result.remote_version >= version
        ):
            # Node B is ahead (Node A lost its state, or a delete is pending
            # there).  Our text is the newest write, so jump past Node B's
            # version; the retry will then be accepted.
            entry.version = result.remote_version + 1
            entry.replication_error = (
                f"reconciled: Node B held v{result.remote_version}, "
                f"retrying as v{entry.version}"
            )
            if entry.residence == SPILLED_LOCAL and entry.full_text is not None:
                entry.stub = build_stub(
                    entry.key, entry.version, entry.full_text, entry.full_tokens,
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
            if entry is None:
                return False
            version = max(entry.version, self.tombstones.get(key, 0))
            self.tombstones[key] = version
            self.compact()
            self._persist()
        if replicator is not None:
            await self._settle_delete(key, version, replicator)
        return True

    async def _settle_delete(
        self, key: str, version: int, replicator: Replicator
    ) -> None:
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
            for entry in self.entries.values():
                prompt_view = None
                if include_text:
                    prompt_view = (
                        entry.full_text
                        if entry.residence == RESIDENT_FULL
                        else (entry.stub or "")
                    )
                keys[entry.key] = {
                    "version": entry.version,
                    "residence": entry.residence,
                    "consistency_status": entry.consistency_status,
                    "active_tokens_in_node_a": entry.active_tokens,
                    "full_tokens_on_node_b": (
                        entry.full_tokens if entry.acked_version > 0 else None
                    ),
                    "eviction_allowed": entry.eviction_allowed,
                    "replication_error": entry.replication_error,
                    "pinned": entry.pinned,
                    "priority": entry.priority,
                    "prompt_view": prompt_view,
                }
            prompt, omitted, index_tokens = self.render_prompt()
            window = count_tokens(prompt)
            return {
                "active_window_tokens": window,
                "max_window_tokens": self.max_window_tokens,
                "over_budget": window > self.max_window_tokens,
                "tokens_saved_by_replication": self.tokens_saved_by_replication(),
                "tokens_spilled_locally": self.tokens_spilled_locally(),
                "page_faults": self.page_faults,
                "pending_deletes": len(self.tombstones),
                "omitted_from_prompt": omitted,
                "index_tokens": index_tokens,
                "keys": keys,
            }

    async def prompt(self) -> dict[str, Any]:
        async with self._lock:
            prompt, _omitted, _index = self.render_prompt()
            return {"prompt": prompt, "tokens": count_tokens(prompt)}

    async def local_search(
        self, q: str, include_text: bool = False
    ) -> list[dict[str, Any]]:
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
