"""Dashboard route and the Node B fault-mode proxy it uses."""

from __future__ import annotations

import httpx


def test_ui_is_served(a_client: httpx.Client) -> None:
    root = a_client.get("/", follow_redirects=False)
    assert root.status_code == 307
    assert root.headers["location"] == "/ui"

    page = a_client.get("/ui")
    assert page.status_code == 200
    assert page.headers["content-type"].startswith("text/html")
    assert "<title>PagerKV Console</title>" in page.text


def test_fault_proxy_round_trip(a_client: httpx.Client, b_client: httpx.Client) -> None:
    assert a_client.get("/admin/node_b/fault").json()["mode"] == "none"

    set_down = a_client.post("/admin/node_b/fault", json={"mode": "down", "delay_s": 0})
    assert set_down.status_code == 200
    assert set_down.json()["mode"] == "down"
    # The proxy really changed Node B, not just a cached value on Node A.
    assert b_client.get("/admin/fault").json()["mode"] == "down"
    assert a_client.get("/context/search", params={"q": "x"}).status_code in (200, 503)

    a_client.post("/admin/node_b/fault", json={"mode": "none", "delay_s": 0})
    assert b_client.get("/admin/fault").json()["mode"] == "none"


def test_fault_proxy_rejects_bad_mode(a_client: httpx.Client) -> None:
    assert a_client.post("/admin/node_b/fault", json={"mode": "exploded"}).status_code == 422
