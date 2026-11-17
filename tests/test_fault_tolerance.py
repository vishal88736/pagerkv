"""Fault-tolerance tests: the dirty-bit invariants under real failures."""

from __future__ import annotations

import asyncio
import hashlib
import threading
import time

import httpx

from common.tokens import count_tokens
from node_a.replicator import ReplicationResult
from node_a.state import NodeAState


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _set_fault(b_client, mode: str, delay_s: float = 0.0) -> None:
    response = b_client.post(
        "/admin/fault", json={"mode": mode, "delay_s": delay_s}
    )
    assert response.status_code == 200, response.text


def _status(a_client) -> dict:
    response = a_client.get("/context/status")
    assert response.status_code == 200, response.text
    return response.json()


def _put(a_client, key: str, text: str) -> dict:
    response = a_client.put(f"/context/{key}", json={"text": text})
    assert response.status_code == 200, response.text
    return response.json()


def _wait_until(predicate, timeout: float = 10.0, interval: float = 0.1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


def test_dirty_keys_spill_locally_never_paged(
    a_client, b_client, key_factory, text_factory
):
    """Node B down: dirty keys leave the prompt via the local spill, not Node B.

    Their full text stays on Node A (never PAGED_TO_NODE_B), the window stays
    within budget, and hydrate serves the exact text from the spill.
    """
    _set_fault(b_client, "down")

    keys = [key_factory(f"dirty_{i}") for i in range(4)]
    texts = {}
    for i, key in enumerate(keys):
        texts[key] = text_factory(850, seed=1000 + i)
        ack = _put(a_client, key, texts[key])
        assert ack["consistency_status"] == "DIRTY_UNREPLICATED"

    status = _status(a_client)
    assert status["over_budget"] is False
    assert status["active_window_tokens"] <= 2000
    residences = set()
    for key in keys:
        row = status["keys"][key]
        assert row["consistency_status"] == "DIRTY_UNREPLICATED"
        assert row["residence"] in ("RESIDENT_FULL", "SPILLED_LOCAL")
        assert row["eviction_allowed"] is False
        assert row["replication_error"]
        residences.add(row["residence"])
    assert "SPILLED_LOCAL" in residences
    assert status["tokens_spilled_locally"] > 0

    spilled = [k for k in keys if status["keys"][k]["residence"] == "SPILLED_LOCAL"]
    view = a_client.get(f"/context/{spilled[0]}").json()
    assert view["is_stub"] is True
    assert view["content"].startswith("[SPILLED v1")

    # Node B is still down, yet every key is recoverable byte-for-byte.
    for key in keys:
        response = a_client.get(f"/context/{key}?hydrate=true")
        assert response.status_code == 200, response.text
        assert _sha(response.json()["full_text"]) == _sha(texts[key])
        assert response.json()["source"] in ("L1", "L1_SPILL")

    _set_fault(b_client, "none")


def test_spill_disabled_keeps_dirty_resident_over_budget(tmp_path):
    """With spill_dirty=False the original rule holds: over budget, nothing moved."""

    class DownReplicator:
        async def replicate(self, key, version, text):
            return ReplicationResult(ok=False, error="Node B HTTP 503")

    async def scenario() -> None:
        state = NodeAState(
            max_window_tokens=200,
            state_file=str(tmp_path / "state.json"),
            spill_dirty=False,
        )
        for i in range(3):
            await state.put(f"k{i}", "dirty text " * 40, DownReplicator())
        assert state.over_budget is True
        assert all(e.residence == "RESIDENT_FULL" for e in state.entries.values())

    asyncio.run(scenario())


def test_stale_write_reconciles_version(tmp_path):
    """Node B holds v5 (e.g. Node A lost its state): the PUT becomes v6 and syncs."""

    class AheadReplicator:
        def __init__(self) -> None:
            self.remote_version = 5
            self.calls: list[int] = []

        async def replicate(self, key, version, text):
            self.calls.append(version)
            if version <= self.remote_version:
                return ReplicationResult(
                    ok=False,
                    error="stale",
                    stale=True,
                    remote_version=self.remote_version,
                )
            return ReplicationResult(
                ok=True, version=version, stub=f"[STUB v{version} | fake]: x", full_tokens=2
            )

    async def scenario() -> None:
        state = NodeAState(state_file=str(tmp_path / "state.json"))
        replicator = AheadReplicator()
        ack = await state.put("k", "newest text", replicator)
        assert replicator.calls == [1, 6]
        assert ack["version"] == 6
        assert ack["consistency_status"] == "SYNCED_ARCHIVED"

    asyncio.run(scenario())


def test_delete_propagates_and_retries(tmp_path):
    """DELETE leaves a tombstone until Node B confirms; re-PUT continues the version."""

    class FlakyDeleter:
        def __init__(self) -> None:
            self.up = False
            self.deletes: list[tuple[str, int]] = []

        async def replicate(self, key, version, text):
            return ReplicationResult(
                ok=True, version=version, stub=f"[STUB v{version} | fake]: x", full_tokens=2
            )

        async def delete(self, key, max_version):
            self.deletes.append((key, max_version))
            return self.up

    async def scenario() -> None:
        state = NodeAState(state_file=str(tmp_path / "state.json"))
        rep = FlakyDeleter()
        await state.put("k", "one", rep)
        await state.put("k", "two", rep)
        assert await state.delete("k", rep) is True
        assert state.tombstones == {"k": 2}          # Node B unreachable

        # Re-created key starts after the deleted version.
        ack = await state.put("k", "three", rep)
        assert ack["version"] == 3

        rep.up = True
        await state.retry_dirty(rep)
        assert state.tombstones == {}
        assert rep.deletes[-1] == ("k", 2)            # never deletes v3

        reloaded = NodeAState(state_file=str(tmp_path / "state.json"))
        assert reloaded.tombstones == {}
        assert reloaded.entries["k"].version == 3

    asyncio.run(scenario())


def test_pinned_and_priority_eviction(tmp_path):
    """Pinned keys are never paged; low priority goes before LRU order."""

    class OkReplicator:
        async def replicate(self, key, version, text):
            return ReplicationResult(
                ok=True, version=version, stub=f"[STUB v{version} | fake]: {key}", full_tokens=2
            )

    async def scenario() -> None:
        state = NodeAState(max_window_tokens=300, state_file=str(tmp_path / "s.json"))
        rep = OkReplicator()
        body = "observation text " * 40           # ~80 tokens each
        await state.put("oldest_pinned", body, rep, pinned=True)
        await state.put("low", body, rep, priority=-5)
        await state.put("normal", body, rep)
        await state.put("newest", body, rep)
        await state.put("trigger", body, rep)
        res = {k: e.residence for k, e in state.entries.items()}
        assert res["oldest_pinned"] == "RESIDENT_FULL"
        assert res["low"] == "PAGED_TO_NODE_B"      # evicted before older/LRU keys
        assert res["trigger"] == "RESIDENT_FULL"

        # Once unpinned it is the least recently used key, so it goes next.
        await state.set_policy("oldest_pinned", pinned=False, priority=None)
        await state.put("more", body, rep)
        assert state.entries["oldest_pinned"].residence == "PAGED_TO_NODE_B"

    asyncio.run(scenario())


def test_recovery_after_node_b_returns(
    a_client, b_client, key_factory, text_factory
):
    _set_fault(b_client, "down")
    keys = [key_factory(f"recover_{i}") for i in range(4)]
    texts = {}
    for i, key in enumerate(keys):
        texts[key] = text_factory(850, seed=1100 + i)
        _put(a_client, key, texts[key])

    _set_fault(b_client, "none")

    def all_synced():
        status = _status(a_client)
        rows = [status["keys"].get(key) for key in keys]
        if all(row and row["consistency_status"] == "SYNCED_ARCHIVED" for row in rows):
            return status
        return None

    status = _wait_until(all_synced, timeout=10.0)
    assert status is not None, _status(a_client)
    assert status["over_budget"] is False
    assert status["active_window_tokens"] <= 2000
    assert any(
        status["keys"][key]["residence"] == "PAGED_TO_NODE_B" for key in keys
    )

    for key, text in texts.items():
        response = a_client.get(f"/context/{key}?hydrate=true")
        assert response.status_code == 200, response.text
        assert _sha(response.json()["full_text"]) == _sha(text)


def test_slow_node_b_does_not_block(
    a_client, b_client, key_factory, text_factory
):
    _set_fault(b_client, "slow", 5.0)
    key = key_factory("slow")
    text = text_factory(850, seed=1200)

    start = time.time()
    ack = _put(a_client, key, text)
    elapsed = time.time() - start

    assert elapsed < 3.5, f"PUT took {elapsed:.2f}s; timeout was not enforced"
    assert ack["consistency_status"] == "DIRTY_UNREPLICATED"
    assert ack["residence"] == "RESIDENT_FULL"
    row = _status(a_client)["keys"][key]
    assert row["replication_error"]
    assert "timeout" in row["replication_error"].lower()

    _set_fault(b_client, "none")


def test_hydrate_when_node_b_down_returns_503_with_stub(
    a_client, b_client, key_factory, text_factory
):
    keys = [key_factory(f"downhyd_{i}") for i in range(3)]
    for i, key in enumerate(keys):
        _put(a_client, key, text_factory(850, seed=1300 + i))

    status = _status(a_client)
    paged_key = next(
        key
        for key, row in status["keys"].items()
        if row["residence"] == "PAGED_TO_NODE_B"
    )
    stub = status["keys"][paged_key]["prompt_view"]

    _set_fault(b_client, "down")
    response = a_client.get(f"/context/{paged_key}?hydrate=true")
    assert response.status_code == 503
    body = response.json()
    assert body["error"]
    assert body["stub"] == stub
    _set_fault(b_client, "none")


def test_stale_ack_ignored(
    a_client, b_client, base_urls, key_factory, text_factory
):
    """v1 is in flight when v2 is PUT; v1's late ack must not win."""
    _set_fault(b_client, "slow", 1.5)
    key = key_factory("race")
    v1 = text_factory(850, seed=1400)
    v2 = text_factory(850, seed=1401)
    results: dict[str, int] = {}

    def worker(label: str, text: str) -> None:
        with httpx.Client(base_url=base_urls["a"], timeout=20.0) as client:
            results[label] = client.put(
                f"/context/{key}", json={"text": text}
            ).status_code

    t1 = threading.Thread(target=worker, args=("v1", v1))
    t1.start()
    time.sleep(0.3)
    t2 = threading.Thread(target=worker, args=("v2", v2))
    t2.start()

    bad_stub = None
    start = time.time()
    while time.time() - start < 3.0:
        try:
            view = a_client.get(f"/context/{key}").json()
        except Exception:
            time.sleep(0.02)
            continue
        if (
            view["version"] == 2
            and view["consistency_status"] == "SYNCED_ARCHIVED"
            and view["content"].startswith("[STUB v")
            and not view["content"].startswith("[STUB v2")
        ):
            bad_stub = view["content"]
        time.sleep(0.02)

    t1.join(timeout=20)
    t2.join(timeout=20)
    assert results == {"v1": 200, "v2": 200}

    _set_fault(b_client, "none")
    assert bad_stub is None, f"an old ack marked v2 as synced: {bad_stub}"

    view = a_client.get(f"/context/{key}").json()
    assert view["version"] == 2
    assert view["acked_version"] == 2
    assert view["consistency_status"] == "SYNCED_ARCHIVED"
    assert view["content"] == v2

    # Force the key to page out and make sure the stored stub is the v2 stub.
    for i in range(3):
        filler = key_factory(f"race_filler_{i}")
        _put(a_client, filler, text_factory(850, seed=1500 + i))
    status = _status(a_client)
    row = status["keys"][key]
    assert row["residence"] == "PAGED_TO_NODE_B"
    assert row["prompt_view"].startswith("[STUB v2")


def test_stale_ack_out_of_order_is_ignored(tmp_path):
    """Deterministic out-of-order acks: v2 ack arrives before v1 ack."""

    class GateReplicator:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int, str]] = []
            self.waiters: dict[int, asyncio.Future] = {}

        async def replicate(self, key: str, version: int, text: str):
            future = asyncio.get_running_loop().create_future()
            self.calls.append((key, version, text))
            self.waiters[version] = future
            return await future

    async def wait_for_calls(replicator: GateReplicator, count: int) -> None:
        for _ in range(2000):
            if len(replicator.calls) >= count:
                return
            await asyncio.sleep(0.001)
        raise AssertionError(f"replicator saw {len(replicator.calls)} calls")

    async def scenario() -> None:
        state = NodeAState(
            max_window_tokens=2000,
            state_file=str(tmp_path / "state.json"),
        )
        replicator = GateReplicator()

        first = asyncio.create_task(state.put("x", "version one text", replicator))
        await wait_for_calls(replicator, 1)
        second = asyncio.create_task(state.put("x", "version two text", replicator))
        await wait_for_calls(replicator, 2)

        replicator.waiters[2].set_result(
            ReplicationResult(
                ok=True,
                version=2,
                stub="[STUB v2 | fake]: version two",
                full_tokens=3,
            )
        )
        await second

        replicator.waiters[1].set_result(
            ReplicationResult(
                ok=True,
                version=1,
                stub="[STUB v1 | fake]: version one",
                full_tokens=3,
            )
        )
        await first

        entry = state.entries["x"]
        assert entry.version == 2
        assert entry.acked_version == 2
        assert entry.consistency_status == "SYNCED_ARCHIVED"
        assert entry.stub is not None and entry.stub.startswith("[STUB v2")

    asyncio.run(scenario())


def test_node_a_restart_preserves_state(
    servers, a_client, base_urls, key_factory, text_factory
):
    key = key_factory("restart")
    text = text_factory(600, seed=1600)
    ack = _put(a_client, key, text)
    assert ack["consistency_status"] == "SYNCED_ARCHIVED"

    before = _status(a_client)["keys"][key]
    servers.restart_a()

    with httpx.Client(base_url=base_urls["a"], timeout=15.0) as fresh:
        health = fresh.get("/health")
        assert health.status_code == 200
        status = fresh.get("/context/status")
        assert status.status_code == 200
        after = status.json()["keys"][key]
        assert after["version"] == before["version"]
        assert after["consistency_status"] == before["consistency_status"]
        assert after["residence"] == before["residence"]
        view = fresh.get(f"/context/{key}")
        assert view.status_code == 200
        assert view.json()["content"] == text
        assert _sha(view.json()["content"]) == _sha(text)
