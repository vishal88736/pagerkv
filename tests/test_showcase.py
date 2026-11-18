"""Real HTTP evidence used by the assignment-focused dashboard."""

import time


def test_replication_result_reports_actual_http_status_timing_and_retry(a_client, servers):
    result = a_client.put("/key", json={"key": "evidence", "value": "original"})
    assert result.status_code == 200 and result.json()["replicated"]
    record = a_client.get("/context/status?include_text=false").json()["last_replication"]
    assert record["key"] == "evidence" and record["version"] == 1
    assert record["http_status"] == 200 and record["ok"] is True
    assert record["elapsed_ms"] >= 0
    assert record["method"] == "POST" and record["path"] == "/replicate"
    assert record["source"] == "write" and record["received_at"] > 0

    a_client.post("/admin/node_b/fault", json={"mode": "down", "delay_s": 0}).raise_for_status()
    failed = a_client.put("/key", json={"key": "evidence", "value": "updated"})
    assert failed.status_code == 200 and not failed.json()["replicated"]
    record = a_client.get("/replication/status").json()["last_replication"]
    assert record["http_status"] == 503 and record["ok"] is False and record["error"]
    assert record["version"] == 2 and record["elapsed_ms"] >= 0
    a_client.post("/admin/node_b/fault", json={"mode": "none", "delay_s": 0}).raise_for_status()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        record = a_client.get("/context/status?include_text=false").json()["last_replication"]
        if record["ok"] and record["source"] == "background_retry":
            break
        time.sleep(0.1)
    assert record["ok"] and record["source"] == "background_retry"
    assert record["version"] == 2 and record["http_status"] == 200
    servers.restart_a()
    assert a_client.get("/context/status?include_text=false").json()["last_replication"] == record


def test_key_comparison_has_full_values_separate_from_context_and_handles_outage(
    a_client, text_factory
):
    expected = {}
    for i in range(5):
        key = f"pair_{i}"
        expected[key] = text_factory(700, seed=800 + i)
        a_client.put("/key", json={"key": key, "value": expected[key]}).raise_for_status()
    context = a_client.get("/context/status?include_text=false").json()
    key = next(k for k, row in context["keys"].items() if row["residence"] == "PAGED_TO_NODE_B")
    before = a_client.get("/context/prompt").json()
    pair = a_client.get("/replication/key/" + key).json()
    assert pair["node_a"]["value"] == pair["node_b"]["value"] == expected[key]
    assert pair["node_a"]["version"] == pair["node_b"]["version"] == 1
    assert pair["node_a"]["residence"] == "PAGED_TO_NODE_B"
    assert pair["comparison_status"] == "in_sync"
    assert a_client.get("/context/prompt").json() == before
    a_client.post("/admin/node_b/fault", json={"mode": "down", "delay_s": 0}).raise_for_status()
    try:
        pair = a_client.get("/replication/key/" + key).json()
        assert pair["node_a"]["value"] == expected[key]
        assert pair["node_b"] is None and pair["node_b_state"] == "unavailable"
        assert pair["comparison_status"] == "unknown"
    finally:
        a_client.post("/admin/node_b/fault", json={"mode": "none", "delay_s": 0})


def test_key_comparison_detects_actual_replica_version_and_missing_copy(a_client, b_client):
    a_client.put("/key/item", json={"value": "primary"}).raise_for_status()
    b_client.post(
        "/replicate", json={"key": "item", "version": 9, "full_text": "external"}
    ).raise_for_status()
    pair = a_client.get("/replication/key/item").json()
    assert pair["node_a"]["version"] == 1 and pair["node_b"]["version"] == 9
    assert pair["node_b"]["value"] == "external" and pair["comparison_status"] == "diverged"
    pair = a_client.get("/replication/key/missing").json()
    assert pair["node_a"] is None and pair["node_b"] is None
    assert pair["node_b_state"] == "missing"
