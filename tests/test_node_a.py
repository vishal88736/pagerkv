"""Node A contract tests: bounded window, LRU paging, hydration, prompt."""

from __future__ import annotations

import hashlib
import time

from common.tokens import count_tokens
from node_a.state import compress_key_ranges


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _put(a_client, key: str, text: str) -> dict:
    response = a_client.put(f"/context/{key}", json={"text": text})
    assert response.status_code == 200, response.text
    return response.json()


def _status(a_client) -> dict:
    response = a_client.get("/context/status")
    assert response.status_code == 200, response.text
    return response.json()


def test_put_under_budget_keeps_full(a_client, key_factory, text_factory):
    key = key_factory("nodea_under")
    text = text_factory(500, seed=101)

    ack = _put(a_client, key, text)
    assert ack["version"] == 1
    assert ack["consistency_status"] == "SYNCED_ARCHIVED"
    assert ack["residence"] == "RESIDENT_FULL"
    assert ack["full_tokens"] == count_tokens(text)
    assert ack["active_tokens"] == ack["full_tokens"]

    status = _status(a_client)
    assert status["active_window_tokens"] <= 2000
    row = status["keys"][key]
    assert row["residence"] == "RESIDENT_FULL"
    assert row["consistency_status"] == "SYNCED_ARCHIVED"
    assert row["eviction_allowed"] is True

    view = a_client.get(f"/context/{key}").json()
    assert view["content"] == text
    assert view["is_stub"] is False


def test_put_over_budget_pages_out_lru(a_client, key_factory, text_factory):
    keys = [key_factory(f"nodea_lru_{i}") for i in range(3)]
    texts = {}
    for i, key in enumerate(keys):
        texts[key] = text_factory(850, seed=200 + i)
        _put(a_client, key, texts[key])

    status = _status(a_client)
    assert status["active_window_tokens"] <= 2000
    assert status["tokens_saved_by_replication"] > 0
    assert status["keys"][keys[0]]["residence"] == "PAGED_TO_NODE_B"
    assert status["keys"][keys[0]]["eviction_allowed"] is False
    # The most recently used key stays resident.
    assert status["keys"][keys[-1]]["residence"] == "RESIDENT_FULL"

    stubbed = a_client.get(f"/context/{keys[0]}").json()
    assert stubbed["is_stub"] is True
    assert stubbed["content"].startswith("[STUB v1")


def test_hydrate_restores_exact_text(a_client, key_factory, text_factory):
    keys = [key_factory(f"nodea_hydrate_{i}") for i in range(3)]
    texts = {}
    for i, key in enumerate(keys):
        texts[key] = text_factory(850, seed=300 + i)
        _put(a_client, key, texts[key])

    status = _status(a_client)
    paged_key = next(
        key
        for key, row in status["keys"].items()
        if row["residence"] == "PAGED_TO_NODE_B"
    )
    faults_before = status["page_faults"]

    response = a_client.get(f"/context/{paged_key}?hydrate=true")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["source"] == "L2_PAGE_FAULT"
    assert body["version"] == 1
    assert _sha(body["full_text"]) == _sha(texts[paged_key])

    after = _status(a_client)
    assert after["page_faults"] == faults_before + 1


def test_prompt_respects_budget(a_client, key_factory, text_factory):
    keys = [key_factory(f"nodea_prompt_{i}") for i in range(3)]
    for i, key in enumerate(keys):
        _put(a_client, key, text_factory(850, seed=400 + i))

    response = a_client.get("/context/prompt")
    assert response.status_code == 200
    body = response.json()
    assert body["tokens"] == count_tokens(body["prompt"])
    assert body["tokens"] <= 2000

    positions = [body["prompt"].index(f"### {key} (v1)") for key in keys]
    assert positions == sorted(positions), "blocks must be ordered by created_at"


def test_status_route_not_treated_as_key(a_client):
    status = a_client.get("/context/status")
    assert status.status_code == 200
    payload = status.json()
    assert "keys" in payload
    assert "max_window_tokens" in payload
    assert "status" not in payload["keys"]

    prompt = a_client.get("/context/prompt")
    assert prompt.status_code == 200
    assert "prompt" in prompt.json() and "tokens" in prompt.json()



def test_compress_key_ranges():
    assert compress_key_ranges([]) == ""
    assert compress_key_ranges(["obs_3"]) == "obs_3"
    assert compress_key_ranges(["obs_1", "obs_2", "obs_3", "obs_4", "obs_5"]) == "obs_1–obs_5"
    assert (
        compress_key_ranges(["obs_1", "obs_2", "obs_3", "obs_5", "obs_7", "obs_8", "obs_9"])
        == "obs_1–obs_3, obs_5, obs_7–obs_9"
    )
    assert (
        compress_key_ranges(["obs_10", "obs_9", "obs_8", "obs_1", "obs_2"])
        == "obs_1–obs_2, obs_8–obs_10"
    )
    assert compress_key_ranges(["foo", "bar", "baz"]) == "bar, baz, foo"
    assert (
        compress_key_ranges(["a1", "a2", "a3", "b1", "b2"]) == "a1–a3, b1–b2"
    )
    assert compress_key_ranges(["obs_1", "obs_3", "obs_5"]) == "obs_1, obs_3, obs_5"


def test_all_keys_discoverable(a_client, key_factory, text_factory):
    import re

    keys = [key_factory(f"disc_{i}") for i in range(120)]
    for i, key in enumerate(keys):
        _put(a_client, key, text_factory(50, seed=7000 + i))

    status = _status(a_client)
    prompt = a_client.get("/context/prompt").json()["prompt"]
    assert status["omitted_from_prompt"] > 0
    assert status["index_tokens"] > 0
    assert "### archive_index" in prompt

    # Expand the Keys: list in the index block (including ranges).
    index_section = prompt.split("### archive_index", 1)[1].split("###", 1)[0]
    keys_line = [line for line in index_section.splitlines() if line.startswith("Keys:")][0]
    listed_text = keys_line[len("Keys:"):].split(", +")[0]
    indexed: set[str] = set()
    for chunk in listed_text.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        m = re.match(r"^(\D+?)(\d+)–\1(\d+)$", chunk)
        if m:
            prefix, lo, hi = m.group(1), int(m.group(2)), int(m.group(3))
            for n in range(lo, hi + 1):
                indexed.add(f"{prefix}{n}")
        else:
            indexed.add(chunk)

    for key in keys:
        in_prompt = f"### {key} (v" in prompt
        in_index = key in indexed
        search_ok = False
        response = a_client.get("/context/search", params={"q": key})
        if response.status_code == 200:
            search_ok = any(r["key"] == key for r in response.json()["results"])
        assert in_prompt or in_index or search_ok, f"key {key} is not discoverable"

def test_update_key_bumps_version(a_client, key_factory, text_factory):
    key = key_factory("nodea_update")
    v1 = text_factory(400, seed=501)
    v2 = text_factory(420, seed=502)

    first = _put(a_client, key, v1)
    assert first["version"] == 1
    second = _put(a_client, key, v2)
    assert second["version"] == 2
    assert second["consistency_status"] == "SYNCED_ARCHIVED"

    view = a_client.get(f"/context/{key}").json()
    assert view["version"] == 2
    assert view["acked_version"] == 2
    assert view["consistency_status"] == "SYNCED_ARCHIVED"
    assert view["content"] == v2

    hydrated = a_client.get(f"/context/{key}?hydrate=true").json()
    assert hydrated["source"] == "L1"
    assert _sha(hydrated["full_text"]) == _sha(v2)


def test_keys_with_slashes_round_trip(a_client, b_client) -> None:
    """Node A accepts the same key space as Node B (``{key:path}``)."""
    key = "obs/turn-041/tool"
    text = "Tool output: query on orders scanned 1200 rows in 40 milliseconds."

    put = a_client.put(f"/context/{key}", json={"text": text})
    assert put.status_code == 200, put.text
    assert put.json()["key"] == key

    # Raw slashes and %2F-encoded slashes address the same key.
    for path in (f"/context/{key}", "/context/obs%2Fturn-041%2Ftool"):
        view = a_client.get(path)
        assert view.status_code == 200, view.text
        assert view.json()["key"] == key

    assert key in a_client.get("/context/status").json()["keys"]

    # Replication reached Node B under the same key, and hydrate returns it.
    for _ in range(50):
        if a_client.get(f"/context/{key}").json()["consistency_status"] == "SYNCED_ARCHIVED":
            break
        time.sleep(0.1)
    assert b_client.get(f"/archive/{key}").json()["full_text"] == text
    assert a_client.get(f"/context/{key}", params={"hydrate": "true"}).json()["full_text"] == text

    assert a_client.delete(f"/context/{key}").status_code == 204
    assert a_client.get(f"/context/{key}").status_code == 404


def test_reserved_keys_are_rejected(a_client) -> None:
    """Keys shadowed by static routes can't be written, so none become unreadable."""
    for key in ("status", "prompt", "search"):
        r = a_client.put(f"/context/{key}", json={"text": "should not be stored"})
        assert r.status_code == 422, r.text
        assert "reserved" in r.json()["detail"]
        assert key not in a_client.get("/context/status").json()["keys"]

    # Only the exact names are reserved; prefixes and nested paths are fine.
    for key in ("status-report", "search/history"):
        assert a_client.put(f"/context/{key}", json={"text": "fine"}).status_code == 200
        assert a_client.get(f"/context/{key}").status_code == 200
        assert a_client.delete(f"/context/{key}").status_code == 204



def test_peek_does_not_change_lru(tmp_path) -> None:
    """GET with peek=true (used by the dashboard) must not reorder eviction."""
    import asyncio

    from node_a.replicator import ReplicationResult
    from node_a.state import NodeAState

    class Ok:
        async def replicate(self, key, version, text):
            return ReplicationResult(ok=True, version=version, stub="[STUB]", full_tokens=1)

    async def scenario() -> None:
        state = NodeAState(state_file=str(tmp_path / "s.json"))
        await state.put("k", "text", Ok())
        stamp = state.entries["k"].last_access
        await asyncio.sleep(0.01)
        view = await state.get_view("k", touch=False)
        assert view is not None and view["content"] == "text"
        assert state.entries["k"].last_access == stamp
        await state.get_view("k")
        assert state.entries["k"].last_access > stamp

    asyncio.run(scenario())
