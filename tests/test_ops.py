"""Admin guard, metrics, slim status, Node B delete, stubs, outage search."""

from __future__ import annotations

from types import SimpleNamespace

import httpx

from common.admin import TOKEN_ENV, admin_allowed
from common.summarizer import build_stub
from common.tokens import count_tokens


def _req(host: str | None, headers: dict | None = None):
    return SimpleNamespace(client=SimpleNamespace(host=host), headers=headers or {})


def test_admin_guard_loopback_only_without_token(monkeypatch) -> None:
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    assert admin_allowed(_req("127.0.0.1"))
    assert admin_allowed(_req("::1"))
    assert not admin_allowed(_req("192.168.1.20"))
    assert not admin_allowed(_req(None))


def test_admin_guard_token(monkeypatch) -> None:
    monkeypatch.setenv(TOKEN_ENV, "s3cret")
    assert admin_allowed(_req("10.0.0.5", {"X-Admin-Token": "s3cret"}))
    assert not admin_allowed(_req("127.0.0.1"))                   # token required
    assert not admin_allowed(_req("127.0.0.1", {"X-Admin-Token": "nope"}))


def test_metrics_endpoints(a_client: httpx.Client, b_client: httpx.Client) -> None:
    a = a_client.get("/metrics")
    assert a.status_code == 200
    assert "pagerkv_a_window_tokens " in a.text
    assert "# TYPE pagerkv_a_page_faults_total counter" in a.text
    b = b_client.get("/metrics")
    assert b.status_code == 200
    assert "pagerkv_b_archived_keys " in b.text


def test_status_without_text(a_client: httpx.Client, key_factory) -> None:
    key = key_factory("slim")
    a_client.put(f"/context/{key}", json={"text": "some text " * 50})
    full = a_client.get("/context/status").json()
    slim = a_client.get("/context/status", params={"include_text": "false"}).json()
    assert full["keys"][key]["prompt_view"]
    assert slim["keys"][key]["prompt_view"] is None
    assert slim["keys"][key]["version"] == full["keys"][key]["version"]


def test_node_b_versioned_delete(b_client: httpx.Client, key_factory) -> None:
    key = key_factory("bdel")
    b_client.post("/replicate", json={"key": key, "version": 3, "full_text": "x"})
    kept = b_client.delete(f"/archive/{key}", params={"version": 2}).json()
    assert kept == {"key": key, "deleted": False, "version": 3}
    gone = b_client.delete(f"/archive/{key}", params={"version": 3}).json()
    assert gone["deleted"] is True
    assert b_client.get(f"/archive/{key}").status_code == 404


def test_delete_removes_node_b_copy(a_client, b_client, key_factory) -> None:
    key = key_factory("adel")
    a_client.put(f"/context/{key}", json={"text": "to be deleted"})
    assert b_client.get(f"/archive/{key}").status_code == 200
    assert a_client.delete(f"/context/{key}").status_code == 204
    assert b_client.get(f"/archive/{key}").status_code == 404
    assert a_client.get("/context/status").json()["pending_deletes"] == 0


def test_stub_keeps_identifiers() -> None:
    text = (
        "Observation 7: billing telemetry update.\n"
        "Fact obs_7: deploy token is QX7K.\n"
        "The nightly job read 90000 rows from orders.total_cents."
    )
    stub = build_stub("obs_7", 1, text, count_tokens(text))
    assert "Observation 7: billing telemetry update." in stub
    assert "key terms: obs_7, QX7K" in stub
    assert count_tokens(stub) < 60


def test_search_falls_back_to_local_text_when_node_b_down(
    a_client, b_client, key_factory
) -> None:
    key = key_factory("outage_search")
    a_client.put(f"/context/{key}", json={"text": "the zebracorn migration finished"})
    b_client.post("/admin/fault", json={"mode": "down", "delay_s": 0})
    try:
        r = a_client.get("/context/search", params={"q": "zebracorn"})
        assert r.status_code == 200, r.text
        assert any(x["key"] == key for x in r.json()["results"])
    finally:
        b_client.post("/admin/fault", json={"mode": "none", "delay_s": 0})
