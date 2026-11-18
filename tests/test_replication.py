"""Assignment requirements exercised through real HTTP calls between two nodes."""

import time
from concurrent.futures import ThreadPoolExecutor


def fault(a_client, mode):
    response = a_client.post("/admin/node_b/fault", json={"mode": mode, "delay_s": 3})
    assert response.status_code == 200


def wait_synced(a_client):
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        status = a_client.get("/replication/status").json()
        if status["consistency"] == "in_sync":
            return status
        time.sleep(0.1)
    raise AssertionError(status)


def test_crud_copies_complete_values(a_client, b_client):
    value = "Full values stay on BOTH nodes. " * 2000
    first = a_client.put("/key/example", json={"value": value})
    assert first.status_code == 200
    assert first.json()["replicated"] is True
    assert first.json()["version"] == 1
    assert a_client.get("/key/example").json()["value"] == value
    assert b_client.get("/key/example").json()["value"] == value
    result = a_client.put("/key/example", json={"value": ""}).json()
    assert result["version"] == 2 and result["replicated"]
    assert b_client.get("/key/example").json()["value"] == ""
    status = a_client.get("/replication/status").json()
    assert status["consistency"] == "in_sync"
    assert status["replication"] == {"attempts": 2, "succeeded": 2, "failed": 0}
    assert a_client.delete("/key/example").status_code == 200
    assert a_client.get("/key/example").status_code == 404
    assert b_client.get("/key/example").status_code == 404
    assert a_client.get("/keys").json()["keys"] == {}
    assert wait_synced(a_client)["node_b"]["example"]["deleted"] == 1


def test_failure_retries_latest_value_and_keeps_local_read(a_client, b_client):
    a_client.put("/key/item", json={"value": "original"})
    fault(a_client, "down")
    for value in ["intermediate", "latest"]:
        result = a_client.put("/key/item", json={"value": value}).json()
        assert result["replicated"] is False and result["error"]
    assert a_client.get("/key/item").json()["value"] == "latest"
    status = a_client.get("/replication/status").json()
    assert status["consistency"] == "unknown"
    assert status["node_b"] is None and status["pending"] == 1
    assert status["replication"]["failed"] >= 2
    fault(a_client, "none")
    status = wait_synced(a_client)
    assert status["pending"] == 0
    assert b_client.get("/key/item").json()["value"] == "latest"
    assert b_client.get("/key/item").json()["version"] == 3


def test_slow_node_times_out_and_recovers(a_client, b_client):
    fault(a_client, "slow")
    start = time.monotonic()
    result = a_client.put("/key/slow", json={"value": "retained"}).json()
    assert time.monotonic() - start < 3
    assert not result["replicated"]
    assert a_client.get("/key/slow").json()["value"] == "retained"
    fault(a_client, "none")
    wait_synced(a_client)
    assert b_client.get("/key/slow").json()["value"] == "retained"


def test_delete_during_outage_retries_and_recreate_is_newer(a_client, b_client):
    a_client.put("/key/deleted", json={"value": "v1"})
    fault(a_client, "down")
    deletion = a_client.delete("/key/deleted")
    assert deletion.status_code == 202
    assert a_client.get("/key/deleted").status_code == 404
    fault(a_client, "none")
    wait_synced(a_client)
    assert b_client.get("/key/deleted").status_code == 404
    stale = b_client.put("/replicate/deleted", json={"value": "v1", "version": 1})
    assert stale.status_code == 409
    new = a_client.put("/key/deleted", json={"value": "v3"}).json()
    assert new["version"] == 3 and new["replicated"]
    assert b_client.get("/key/deleted").json()["value"] == "v3"


def test_restart_retains_pending_values_and_counters(a_client, b_client, servers):
    fault(a_client, "down")
    a_client.put("/key/restart", json={"value": "survives restart"})
    before = a_client.get("/replication/status").json()["replication"]
    servers.restart_a()
    assert a_client.get("/key/restart").json()["value"] == "survives restart"
    after = a_client.get("/replication/status").json()["replication"]
    assert after["failed"] >= before["failed"]
    fault(a_client, "none")
    wait_synced(a_client)
    servers.stop("b")
    servers.start_b()
    servers.wait_health(servers.b_url)
    assert b_client.get("/key/restart").json()["value"] == "survives restart"
    assert wait_synced(a_client)["pending"] == 0


def test_node_b_idempotency_stale_and_conflicting_versions(b_client):
    payload = {"value": "accepted", "version": 4}
    assert b_client.put("/replicate/order", json=payload).status_code == 200
    assert b_client.put("/replicate/order", json=payload).status_code == 200
    for value, version in [("older", 3), ("conflict", 4)]:
        response = b_client.put("/replicate/order", json={"value": value, "version": version})
        assert response.status_code == 409
    assert b_client.get("/key/order").json()["value"] == "accepted"


def test_live_comparison_detects_divergence_and_write_reconciles(a_client, b_client):
    a_client.put("/key/conflict", json={"value": "primary"})
    b_client.put("/replicate/conflict", json={"value": "external", "version": 8})
    status = a_client.get("/replication/status").json()
    assert status["consistency"] == "diverged" and status["pending"] == 0
    result = a_client.put("/key/conflict", json={"value": "new primary"}).json()
    assert result["version"] == 9 and result["replicated"]
    assert wait_synced(a_client)["node_b"]["conflict"]["value"] == "new primary"


def test_missing_replica_is_detected(a_client, b_client):
    b_client.put("/replicate/extra", json={"value": "only on B", "version": 1})
    assert a_client.get("/replication/status").json()["consistency"] == "diverged"


def test_validation_slash_keys_and_alias(a_client, b_client):
    assert a_client.get("/key/missing").status_code == 404
    assert a_client.put("/key/name", json={}).status_code == 422
    assert a_client.put("/key/name", json={"value": 123}).status_code == 422
    assert a_client.put("/key/name", json={"value": "x" * 1000001}).status_code == 422
    assert a_client.put("/key/bad%00key", json={"value": "bad"}).status_code == 422
    assert a_client.put("/kv/folder/item", json={"value": "slash"}).status_code == 200
    assert a_client.get("/key/folder/item").json()["value"] == "slash"
    assert b_client.get("/key/folder/item").json()["value"] == "slash"
    assert a_client.post("/admin/node_b/fault", json={"mode": "invalid"}).status_code == 422
    assert (
        a_client.post("/admin/node_b/fault", json={"mode": "slow", "delay_s": -1}).status_code
        == 422
    )


def test_concurrent_updates_have_ordered_versions(a_client, b_client):
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(
            pool.map(
                lambda i: a_client.put("/key/concurrent", json={"value": str(i)}).json(), range(4)
            )
        )
    assert sorted(row["version"] for row in results) == [1, 2, 3, 4]
    assert all(row["replicated"] for row in results)
    a = a_client.get("/key/concurrent").json()
    b = b_client.get("/key/concurrent").json()
    assert a["version"] == b["version"] == 4 and a["value"] == b["value"]


def test_dashboard_and_api_describe_assignment(a_client):
    assert a_client.get("/", follow_redirects=False).headers["location"] == "/ui"
    page = a_client.get("/ui")
    assert page.status_code == 200
    assert "Replicated Key-Value Store" in page.text
    assert "prefers-reduced-motion" in page.text
    assert "Window Composition" not in page.text and "/context/" not in page.text
    schema = a_client.get("/openapi.json").json()
    assert "/key/{key}" in schema["paths"]
    assert not any("context" in path for path in schema["paths"])


def test_real_node_b_process_outage(a_client, b_client, servers):
    a_client.put("/key/local", json={"value": "full local copy"})
    servers.stop("b")
    assert a_client.get("/key/local").json()["value"] == "full local copy"
    result = a_client.put("/key/local", json={"value": "new local copy"}).json()
    assert result["replicated"] is False
    assert a_client.get("/replication/status").json()["consistency"] == "unknown"
    servers.start_b()
    servers.wait_health(servers.b_url)
    wait_synced(a_client)
    assert b_client.get("/key/local").json()["value"] == "new local copy"


def test_pending_deletion_survives_primary_restart(a_client, b_client, servers):
    a_client.put("/key/persistent-delete", json={"value": "remove me"})
    fault(a_client, "down")
    assert a_client.delete("/key/persistent-delete").status_code == 202
    servers.restart_a()
    assert a_client.get("/key/persistent-delete").status_code == 404
    fault(a_client, "none")
    wait_synced(a_client)
    assert b_client.get("/key/persistent-delete").status_code == 404


def test_literal_assignment_put_route(a_client, b_client):
    result = a_client.put("/key", json={"key": "assignment", "value": "literal route"})
    assert result.status_code == 200 and result.json()["replicated"]
    assert b_client.get("/key/assignment").json()["value"] == "literal route"
    assert a_client.put("/key", json={"key": "bad\nkey", "value": "rejected"}).status_code == 422


def test_admin_token_is_required_and_forwarded(a_client, servers):
    servers.env["PAGERKV_ADMIN_TOKEN"] = "local-test-token"
    servers.restart_a()
    servers.stop("b")
    servers.start_b()
    servers.wait_health(servers.b_url)
    assert a_client.get("/admin/node_b/fault").status_code == 403
    response = a_client.post(
        "/admin/node_b/fault", json={"mode": "down"}, headers={"X-Admin-Token": "local-test-token"}
    )
    assert response.status_code == 200 and response.json()["mode"] == "down"
    a_client.post(
        "/admin/node_b/fault", json={"mode": "none"}, headers={"X-Admin-Token": "local-test-token"}
    ).raise_for_status()
