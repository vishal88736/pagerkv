"""Node B contract tests: durable round-trip, version guard, stubs, faults."""

from __future__ import annotations

import hashlib

from common.tokens import count_tokens


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def test_replicate_and_get_roundtrip(b_client, key_factory, text_factory):
    key = key_factory("nodeb_roundtrip")
    text = text_factory(700, seed=11)

    response = b_client.post(
        "/replicate", json={"key": key, "version": 1, "full_text": text}
    )
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
    stale = b_client.post(
        "/replicate", json={"key": key, "version": 1, "full_text": v1_text}
    )
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

    ack = b_client.post(
        "/replicate", json={"key": key, "version": 1, "full_text": text}
    ).json()

    assert count_tokens(ack["stub"]) < 60
    assert ack["stub"].startswith("[STUB v1 | GET /context/")
    assert ack["full_tokens"] == count_tokens(text)


def test_fault_down_returns_503(b_client, key_factory, text_factory):
    key = key_factory("nodeb_fault")
    text = text_factory(200, seed=41)

    assert (
        b_client.post(
            "/admin/fault", json={"mode": "down", "delay_s": 0}
        ).status_code
        == 200
    )
    response = b_client.post(
        "/replicate", json={"key": key, "version": 1, "full_text": text}
    )
    assert response.status_code == 503
    # health and admin stay reachable while the data plane is down
    assert b_client.get("/health").status_code == 200
    assert b_client.post(
        "/admin/fault", json={"mode": "none", "delay_s": 0}
    ).status_code == 200

    recovered = b_client.post(
        "/replicate", json={"key": key, "version": 1, "full_text": text}
    )
    assert recovered.status_code == 200
