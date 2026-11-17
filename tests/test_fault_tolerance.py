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


def test_dirty_keys_never_evicted(
    a_client, b_client, key_factory, text_factory
):
    _set_fault(b_client, "down")

    keys = [key_factory(f"dirty_{i}") for i in range(4)]
    texts = {}
    for i, key in enumerate(keys):
        texts[key] = text_factory(850, seed=1000 + i)
        ack = _put(a_client, key, texts[key])
        assert ack["consistency_status"] == "DIRTY_UNREPLICATED"
        assert ack["residence"] == "RESIDENT_FULL"

    status = _status(a_client)
    assert status["over_budget"] is True
    assert status["active_window_tokens"] > 2000
    for key in keys:
        row = status["keys"][key]
        assert row["consistency_status"] == "DIRTY_UNREPLICATED"
        assert row["residence"] == "RESIDENT_FULL"
        assert row["eviction_allowed"] is False
        assert row["replication_error"]
        view = a_client.get(f"/context/{key}")
        assert view.status_code == 200
        assert view.json()["content"] == texts[key]
        assert view.json()["is_stub"] is False

    _set_fault(b_client, "none")


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
