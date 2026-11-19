"""Dashboard route and the Node B fault-mode proxy it uses."""

from __future__ import annotations

import asyncio
import hashlib
import json

import httpx
import pytest

from node_a.app import app
from node_a.replicator import Replicator
from node_a.state import NodeAState


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


@pytest.mark.parametrize(
    ("comparison_path", "replica_path"),
    [("/replication/status", "/keys"), ("/replication/key/example", "/archive/example")],
)
def test_slow_replica_comparison_allows_local_reads_and_writes(
    monkeypatch, tmp_path, comparison_path, replica_path
) -> None:
    async def scenario() -> None:
        replica_sampled = asyncio.Event()
        release_replica = asyncio.Event()
        stored = {}

        async def replica(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/replicate":
                data = json.loads(request.content)
                value = data["full_text"]
                stored.update(
                    data, value=value, stub="summary", full_tokens=3,
                    sha256=hashlib.sha256(value.encode()).hexdigest(),
                )
                return httpx.Response(200, json=stored)
            if request.url.path == replica_path:
                sampled = dict(stored)
                replica_sampled.set()
                await release_replica.wait()
                return httpx.Response(
                    200, json={"keys": {"example": sampled}} if replica_path == "/keys" else sampled
                )
            return httpx.Response(200, json={"ok": True})

        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(replica)) as replica_client,
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://primary") as client,
        ):
            monkeypatch.setattr(app.state, "node_state", NodeAState(state_file=str(tmp_path / "a.json")), raising=False)
            monkeypatch.setattr(
                app.state, "replicator", Replicator(replica_client, "http://replica", timeout_s=2),
                raising=False,
            )
            first = await client.put("/key", json={"key": "example", "value": "original"})
            assert first.json()["replicated"]
            comparison = asyncio.create_task(client.get(comparison_path))
            try:
                await asyncio.wait_for(replica_sampled.wait(), timeout=2)
                read = await asyncio.wait_for(client.get("/key/example"), timeout=2)
                assert read.json()["value"] == "original"
                update = await asyncio.wait_for(
                    client.put("/key", json={"key": "example", "value": "updated"}), timeout=2
                )
                assert update.json()["replicated"]
            finally:
                release_replica.set()
                result = await comparison
            assert result.json()["comparison_status"] == "unknown"
            assert (await client.get(comparison_path)).json()["comparison_status"] == "in_sync"

    asyncio.run(scenario())
