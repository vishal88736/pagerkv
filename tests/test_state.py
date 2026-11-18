"""Unit tests for window accounting that do not need HTTP servers."""

from __future__ import annotations

import asyncio
from typing import Any

from common.summarizer import build_stub
from common.tokens import count_tokens
from node_a.replicator import ReplicationResult
from node_a.state import NodeAState


class OkReplicator:
    """Acknowledges every version with the same stub Node B would build."""

    async def replicate(self, key: str, version: int, text: str) -> ReplicationResult:
        return ReplicationResult(
            ok=True,
            version=version,
            stub=build_stub(key, version, text, count_tokens(text)),
            full_tokens=count_tokens(text),
        )


class DownReplicator:
    async def replicate(self, key: str, version: int, text: str) -> ReplicationResult:
        return ReplicationResult(ok=False, error="Node B down")


def test_compaction_uses_rendered_prompt_and_keeps_hot_keys(tmp_path) -> None:
    """Cold stubs fold into archive_index; compaction must not page every key.

    The old code compared the *raw* block list against the budget.  With many
    small keys that pushed the raw list over budget but the rendered prompt
    (index + newest blocks) still fit, it paged out every key, leaving zero
    resident blocks and a needless page fault on the next read.
    """

    async def scenario() -> None:
        state = NodeAState(
            max_window_tokens=2000,
            state_file=str(tmp_path / "state.json"),
        )
        replicator = OkReplicator()
        text = "observation data " * 60  # ~121 tokens; the stub is ~45

        for i in range(150):
            await state.put(f"k{i:03d}", text, replicator)

        assert state.raw_window_tokens() > 2000
        assert state.window_tokens() <= 2000

        residents = [
            entry.key for entry in state.entries.values() if entry.residence == "RESIDENT_FULL"
        ]
        assert residents, "every key was paged despite the rendered prompt fitting"
        assert "k149" in residents, "the newest (hottest) key was paged out"

        prompt, omitted, index_tokens = state.render_prompt()
        assert omitted > 0
        assert index_tokens > 0
        assert "### archive_index" in prompt
        assert count_tokens(prompt) <= 2000

    asyncio.run(scenario())


def test_full_tokens_on_node_b_reports_the_acked_version(tmp_path) -> None:
    """A dirty v2 must not make status claim Node B holds v2's token count."""

    class ToggleReplicator(OkReplicator):
        def __init__(self) -> None:
            self.up = True

        async def replicate(self, key: str, version: int, text: str) -> ReplicationResult:
            if not self.up:
                return ReplicationResult(ok=False, error="Node B down")
            return await super().replicate(key, version, text)

    async def scenario() -> None:
        state = NodeAState(state_file=str(tmp_path / "state.json"))
        replicator = ToggleReplicator()

        v1 = "first version data " * 25
        v2 = "second version data " * 100
        await state.put("k", v1, replicator)

        row = (await state.status())["keys"]["k"]
        assert row["version"] == 1
        assert row["acked_version"] == 1
        assert row["full_tokens_on_node_b"] == count_tokens(v1)

        replicator.up = False
        await state.put("k", v2, replicator)

        row = (await state.status())["keys"]["k"]
        assert row["version"] == 2
        assert row["consistency_status"] == "DIRTY_UNREPLICATED"
        assert row["acked_version"] == 1
        assert row["full_tokens_on_node_b"] == count_tokens(v1)
        assert row["full_tokens_on_node_b"] != count_tokens(v2)

        # A key Node B has never seen reports no archived token count at all.
        await state.put("fresh", "never replicated", DownReplicator())
        fresh = (await state.status())["keys"]["fresh"]
        assert fresh["acked_version"] == 0
        assert fresh["full_tokens_on_node_b"] is None

    asyncio.run(scenario())


def test_stub_that_is_longer_than_text_is_never_used(tmp_path) -> None:
    """Spilling a tiny block would grow the prompt; safety keeps it resident."""

    async def scenario() -> None:
        state = NodeAState(
            max_window_tokens=1,
            state_file=str(tmp_path / "state.json"),
        )
        await state.put("tiny", "ok", DownReplicator())

        entry = state.entries["tiny"]
        assert entry.residence == "RESIDENT_FULL"
        assert entry.full_text == "ok"
        assert state.tokens_spilled_locally() == 0
        assert state.over_budget is True

    asyncio.run(scenario())


def test_status_counts_and_consistency(tmp_path) -> None:
    async def scenario() -> None:
        state = NodeAState(state_file=str(tmp_path / "state.json"))
        replicator = OkReplicator()

        await state.put("a", "alpha", replicator)
        status: dict[str, Any] = await state.status()
        assert status["consistency"] == "consistent"
        assert status["replication_attempts"] == 1
        assert status["replication_successes"] == 1
        assert status["replication_failures"] == 0
        assert status["last_replication_error"] is None

        class FailingReplicator(DownReplicator):
            pass

        await state.put("b", "beta", FailingReplicator())
        status = await state.status()
        assert status["consistency"] == "degraded"
        assert status["replication_attempts"] == 2
        assert status["replication_failures"] == 1
        assert status["last_replication_error"]

    asyncio.run(scenario())
