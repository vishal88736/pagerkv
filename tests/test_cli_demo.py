"""The evaluator CLI must prove its outcomes and only clean up its own data."""

import os
import subprocess
import sys

import pytest

from demo import DemoError, PagerKVDemo


def run_cli(servers, token=None):
    env = os.environ.copy()
    env.pop("PAGERKV_ADMIN_TOKEN", None)
    if token is not None:
        env["PAGERKV_ADMIN_TOKEN"] = token
    return subprocess.run(
        [
            sys.executable,
            "demo.py",
            "--auto",
            "--delay",
            "0",
            "--node-a",
            servers.a_url,
            "--node-b",
            servers.b_url,
            "--recovery-timeout",
            "10",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )


def test_cli_verifies_all_steps_and_preserves_fixed_demo_names(servers, a_client, b_client):
    existing = [
        "demo/service_config",
        "demo/incident_report",
        "demo/large_schema_dump",
        "demo/filler_1",
    ]
    for key in existing:
        a_client.put("/key", json={"key": key, "value": "existing user data"}).raise_for_status()
    result = run_cli(servers)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "All five steps verified" in result.stdout
    assert "SHA-256 verified" in result.stdout and "L2_PAGE_FAULT and exact" in result.stdout
    assert sorted(a_client.get("/keys").json()["keys"]) == sorted(existing)
    assert sorted(b_client.get("/keys").json()["keys"]) == sorted(existing)
    for key in existing:
        assert a_client.get("/key/" + key).json()["value"] == "existing user data"
    assert a_client.get("/admin/node_b/fault").json()["mode"] == "none"


def test_cli_admin_auth_denial_fails_without_changes_and_valid_token_succeeds(servers, a_client):
    token = "isolated-cli-test-token"
    servers.env["PAGERKV_ADMIN_TOKEN"] = token
    servers.restart_a()
    servers.stop("b")
    servers.start_b()
    servers.wait_health(servers.b_url)
    denied = run_cli(servers)
    assert denied.returncode != 0 and "Admin access denied" in denied.stderr
    assert "All five steps verified" not in denied.stdout
    assert a_client.get("/keys").json()["keys"] == {}
    result = run_cli(servers, token)
    assert result.returncode == 0, result.stdout + result.stderr
    assert token not in result.stdout + result.stderr
    assert a_client.get("/keys").json()["keys"] == {}


def test_failed_verification_cleans_up_without_success(servers, a_client, monkeypatch, capsys):
    runner = PagerKVDemo(servers.a_url, servers.b_url, auto=True, delay=0)

    def fail(key):
        raise DemoError("Injected digest mismatch")

    monkeypatch.setattr(runner, "verify_archive", fail)
    with pytest.raises(DemoError, match="digest mismatch"):
        runner.run()
    assert a_client.get("/keys").json()["keys"] == {}
    assert not runner.demo_keys
    assert "All five steps verified" not in capsys.readouterr().out


def test_interruption_during_outage_restores_mode_and_removes_own_keys(
    servers, a_client, monkeypatch
):
    runner = PagerKVDemo(servers.a_url, servers.b_url, auto=True, delay=0)

    def interrupt():
        raise KeyboardInterrupt

    monkeypatch.setattr(runner, "step_3_local_read", interrupt)
    with pytest.raises(KeyboardInterrupt):
        runner.run()
    assert a_client.get("/admin/node_b/fault").json()["mode"] == "none"
    assert a_client.get("/keys").json()["keys"] == {}
    assert not runner.demo_keys


def test_reserve_key_refuses_existing_data(servers, a_client):
    a_client.put(
        "/key", json={"key": "demo/service_config", "value": "protected"}
    ).raise_for_status()
    runner = PagerKVDemo(servers.a_url, servers.b_url, auto=True, delay=0)
    runner.prefix = "demo"
    with pytest.raises(DemoError, match="refusing to overwrite"):
        runner.reserve_key("service_config")
    assert runner.demo_keys == []
    assert runner.cleanup() == []
    assert a_client.get("/key/demo/service_config").json()["value"] == "protected"


def test_digest_mismatch_is_rejected(monkeypatch):
    runner = PagerKVDemo("http://node-a", "http://node-b", auto=True, delay=0)
    runner.values["sample"] = "original"
    runner.versions["sample"] = 1
    monkeypatch.setattr(
        runner,
        "request",
        lambda *args: (
            200,
            {
                "key": "sample",
                "version": 1,
                "full_text": "original",
                "sha256": "wrong",
                "stub": "stub",
            },
        ),
    )
    with pytest.raises(DemoError, match="SHA-256 digest mismatch"):
        runner.verify_archive("sample")


def test_modified_hydration_text_fails_and_cleans_up(servers, a_client, monkeypatch, capsys):
    runner = PagerKVDemo(servers.a_url, servers.b_url, auto=True, delay=0, recovery_timeout=10)
    actual_request = runner.request

    def altered(url, method="GET", payload=None):
        status, body = actual_request(url, method, payload)
        if url.endswith("?hydrate=true") and status == 200:
            body["full_text"] += " altered response"
        return status, body

    monkeypatch.setattr(runner, "request", altered)
    with pytest.raises(DemoError, match="Hydrated text did not exactly equal"):
        runner.run()
    assert a_client.get("/keys").json()["keys"] == {}
    assert "All five steps verified" not in capsys.readouterr().out
