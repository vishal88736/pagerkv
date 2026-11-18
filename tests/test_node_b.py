"""Node B contract tests: durable round-trip, version guard, stubs, faults."""

from __future__ import annotations

import hashlib

from common.tokens import count_tokens


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def test_replicate_and_get_roundtrip(b_client, key_factory, text_factory):
    key = key_factory("nodeb_roundtrip")
    text = text_factory(700, seed=11)

    response = b_client.post("/replicate", json={"key": key, "version": 1, "full_text": text})
    assert response.status_code == 200
    ack = response.json()
    assert ack["key"] == key
    assert ack["version"] == 1
    assert ack["sha256"] == _sha(text)
    assert ack["stale_write"] is False

    archived = b_client.get(f"/archive/{key}")
    assert archived.status_code == 200
    row = archived.json()
    assert row["full_text"] == text
    assert row["sha256"] == _sha(text)
    assert row["version"] == 1


def test_stale_write_rejected(b_client, key_factory, text_factory):
    key = key_factory("nodeb_stale")
    v2_text = text_factory(300, seed=21)
    v1_text = text_factory(300, seed=22)

    assert (
        b_client.post(
            "/replicate", json={"key": key, "version": 2, "full_text": v2_text}
        ).status_code
        == 200
    )
    stale = b_client.post("/replicate", json={"key": key, "version": 1, "full_text": v1_text})
    assert stale.status_code == 200
    assert stale.json()["stale_write"] is True
    assert stale.json()["version"] == 2

    archived = b_client.get(f"/archive/{key}").json()
    assert archived["version"] == 2
    assert archived["full_text"] == v2_text
    assert archived["sha256"] == _sha(v2_text)


def test_stub_is_short(b_client, key_factory, text_factory):
    key = key_factory("nodeb_stub")
    text = text_factory(800, seed=31)
    assert count_tokens(text) >= 750

    ack = b_client.post("/replicate", json={"key": key, "version": 1, "full_text": text}).json()

    assert count_tokens(ack["stub"]) < 60
    assert ack["stub"].startswith("[STUB v1 | GET /context/")
    assert ack["full_tokens"] == count_tokens(text)


def test_same_version_same_text_is_idempotent(b_client, key_factory, text_factory):
    """A retried replication (same version, same bytes) succeeds without rewriting."""
    key = key_factory("nodeb_idem")
    text = text_factory(300, seed=51)

    first = b_client.post("/replicate", json={"key": key, "version": 1, "full_text": text})
    assert first.status_code == 200
    assert first.json()["stale_write"] is False

    retry = b_client.post("/replicate", json={"key": key, "version": 1, "full_text": text})
    assert retry.status_code == 200
    assert retry.json()["stale_write"] is False
    assert retry.json()["sha256"] == _sha(text)
    assert b_client.get(f"/archive/{key}").json()["full_text"] == text


def test_same_version_different_text_is_a_conflict(b_client, key_factory, text_factory):
    """Silently overwriting a same-version payload would lose acknowledged data."""
    key = key_factory("nodeb_conflict")
    original = text_factory(300, seed=61)
    conflicting = text_factory(300, seed=62)

    assert (
        b_client.post("/replicate", json={"key": key, "version": 1, "full_text": original}).json()[
            "stale_write"
        ]
        is False
    )
    conflict = b_client.post(
        "/replicate", json={"key": key, "version": 1, "full_text": conflicting}
    )
    assert conflict.status_code == 200
    body = conflict.json()
    assert body["stale_write"] is True
    assert body["version"] == 1

    archived = b_client.get(f"/archive/{key}").json()
    assert archived["full_text"] == original
    assert archived["sha256"] == _sha(original)


def test_delete_tombstone_blocks_late_replication(b_client, key_factory, text_factory):
    """A delayed PUT that arrives after DELETE must not resurrect the key."""
    key = key_factory("nodeb_tombstone")
    text = text_factory(300, seed=71)

    assert (
        b_client.post("/replicate", json={"key": key, "version": 1, "full_text": text}).status_code
        == 200
    )
    deleted = b_client.delete(f"/archive/{key}", params={"version": 1}).json()
    assert deleted["deleted"] is True

    late = b_client.post("/replicate", json={"key": key, "version": 1, "full_text": text})
    assert late.status_code == 200
    assert late.json()["stale_write"] is True
    assert b_client.get(f"/archive/{key}").status_code == 404

    # A genuinely newer write is still accepted after the tombstone.
    newer_text = text_factory(300, seed=72)
    accepted = b_client.post("/replicate", json={"key": key, "version": 2, "full_text": newer_text})
    assert accepted.json()["stale_write"] is False
    assert b_client.get(f"/archive/{key}").json()["full_text"] == newer_text


def test_ready_reflects_fault_mode(b_client, key_factory, text_factory):
    assert b_client.get("/ready").status_code == 200

    assert b_client.post("/admin/fault", json={"mode": "down", "delay_s": 0}).status_code == 200
    try:
        assert b_client.get("/health").status_code == 200
        assert b_client.get("/health").json()["ready"] is False
        assert b_client.get("/ready").status_code == 503
    finally:
        b_client.post("/admin/fault", json={"mode": "none", "delay_s": 0})
    assert b_client.get("/ready").status_code == 200


def test_fault_down_returns_503(b_client, key_factory, text_factory):
    key = key_factory("nodeb_fault")
    text = text_factory(200, seed=41)

    assert b_client.post("/admin/fault", json={"mode": "down", "delay_s": 0}).status_code == 200
    response = b_client.post("/replicate", json={"key": key, "version": 1, "full_text": text})
    assert response.status_code == 503
    # health and admin stay reachable while the data plane is down
    assert b_client.get("/health").status_code == 200
    assert b_client.post("/admin/fault", json={"mode": "none", "delay_s": 0}).status_code == 200

    recovered = b_client.post("/replicate", json={"key": key, "version": 1, "full_text": text})
    assert recovered.status_code == 200
