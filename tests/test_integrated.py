"""REST replication and bounded context are two views of the same stored values."""

import asyncio
import hashlib
import json
import time

import httpx

from node_a.replicator import Replicator


def wait_synced(a_client):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        status = a_client.get("/replication/status").json()
        if status["comparison_status"] == "in_sync":
            return status
        time.sleep(0.1)
    raise AssertionError(status)


def seed_paged(a_client, text_factory):
    values = {}
    for i in range(5):
        key = f"context_{i}"
        values[key] = text_factory(700, seed=100 + i)
        response = a_client.put("/key", json={"key": key, "value": values[key]})
        assert response.status_code == 200 and response.json()["replicated"]
    status = a_client.get("/context/status?include_text=false").json()
    paged = next(
        key for key, row in status["keys"].items() if row["residence"] == "PAGED_TO_NODE_B"
    )
    assert status["active_window_tokens"] <= status["max_window_tokens"]
    return values, paged


def test_assignment_put_and_context_share_data(a_client, b_client):
    response = a_client.put("/key", json={"key": "city", "value": "Bhilai"})
    assert response.status_code == 200 and response.json()["replicated"]
    assert a_client.get("/key/city").json()["value"] == "Bhilai"
    assert b_client.get("/key/city").json()["value"] == "Bhilai"
    assert a_client.get("/context/city").json()["content"] == "Bhilai"
    a_client.put("/context/city", json={"text": "Raipur"}).raise_for_status()
    assert a_client.get("/key/city").json()["value"] == "Raipur"
    assert a_client.get("/keys").json()["keys"]["city"]["version"] == 2
    assert wait_synced(a_client)["local_value_count"] == 1
    assert a_client.delete("/key/city").status_code == 204
    assert a_client.get("/key/city").status_code == 404
    assert b_client.get("/key/city").status_code == 404


def test_paging_retains_full_primary_value_and_local_get_does_not_hydrate(a_client, text_factory):
    values, key = seed_paged(a_client, text_factory)
    before = a_client.get("/context/prompt").json()
    a_client.post("/admin/node_b/fault", json={"mode": "down", "delay_s": 0}).raise_for_status()
    try:
        result = a_client.get("/key/" + key)
        assert result.status_code == 200 and result.json()["value"] == values[key]
        assert result.json()["source"] == "LOCAL_STORE"
        assert a_client.get("/context/prompt").json() == before
        assert a_client.get("/context/" + key + "?hydrate=true").status_code == 503
        assert a_client.get("/replication/status").json()["comparison_status"] == "unknown"
    finally:
        a_client.post("/admin/node_b/fault", json={"mode": "none", "delay_s": 0})


def test_local_full_values_survive_restart_with_archive_offline(a_client, servers, text_factory):
    values, key = seed_paged(a_client, text_factory)
    servers.stop("b")
    servers.restart_a()
    assert a_client.get("/key/" + key).json()["value"] == values[key]
    assert a_client.get("/context/status").json()["local_value_count"] == len(values)
    servers.start_b()
    servers.wait_health(servers.b_url)
    wait_synced(a_client)


def test_default_safety_mode_keeps_dirty_context_over_budget(a_client, servers, text_factory):
    servers.env["SPILL_DIRTY"] = "0"
    servers.restart_a()
    a_client.post("/admin/node_b/fault", json={"mode": "down", "delay_s": 0}).raise_for_status()
    try:
        for i in range(4):
            value = text_factory(800, seed=200 + i)
            response = a_client.put("/context/dirty_" + str(i), json={"text": value})
            assert response.status_code == 200
        status = a_client.get("/context/status").json()
        assert status["over_budget"] is True
        assert all(row["residence"] == "RESIDENT_FULL" for row in status["keys"].values())
        assert status["local_value_count"] == 4
    finally:
        a_client.post("/admin/node_b/fault", json={"mode": "none", "delay_s": 0})
    wait_synced(a_client)
    assert a_client.get("/context/status").json()["over_budget"] is False


def test_live_comparison_reports_out_of_band_divergence(a_client, b_client):
    a_client.put("/key/sample", json={"value": "primary"}).raise_for_status()
    b_client.post(
        "/replicate", json={"key": "sample", "version": 9, "full_text": "external"}
    ).raise_for_status()
    comparison = a_client.get("/replication/status").json()
    assert comparison["comparison_status"] == "diverged"
    assert comparison["consistency"] == "degraded" and comparison["differing_keys"] == ["sample"]
    a_client.put("/key/sample", json={"value": "new primary"}).raise_for_status()
    assert wait_synced(a_client)["differing_keys"] == []


def test_legacy_checkpoint_recovers_local_values_without_treating_stubs_as_values(
    a_client, servers, text_factory
):
    values, key = seed_paged(a_client, text_factory)
    servers.stop("a")
    checkpoint = servers.tmp_dir / "node_a_state.json"
    data = json.loads(checkpoint.read_text())
    data.pop("full_values")
    checkpoint.write_text(json.dumps(data))
    servers.start_a()
    servers.wait_health(servers.a_url)
    assert a_client.get("/key/" + key).json()["value"] == values[key]
    servers.stop("b")
    assert a_client.get("/key/" + key).json()["value"] == values[key]
    servers.start_b()
    servers.wait_health(servers.b_url)


def test_ack_requires_matching_key_and_digest():
    async def run():
        for key, digest in [
            ("wrong", hashlib.sha256(b"correct").hexdigest()),
            ("sample", "wrong-digest"),
        ]:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(
                        200,
                        json={
                            "key": key,
                            "version": 1,
                            "sha256": digest,
                            "stub": "stub",
                            "full_tokens": 1,
                        },
                    )
                )
            ) as client:
                result = await Replicator(client, "http://node-b", 2).replicate(
                    "sample", 1, "correct"
                )
                assert not result.ok and "mismatch" in result.error

    asyncio.run(run())
