"""Run the real frontend guided-flow logic against the isolated service fixture."""

import os
import shutil
import subprocess

import pytest


@pytest.mark.skipif(
    shutil.which("node") is None, reason="Node.js is required for the live guided flow"
)
def test_guided_demo_uses_real_replication_and_preserves_existing_values(servers):
    env = os.environ.copy()
    env["PAGERKV_TEST_A_URL"] = servers.a_url
    result = subprocess.run(
        ["node", "tests/guided-demo-live.cjs"],
        env=env,
        text=True,
        capture_output=True,
        timeout=55,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Live guided demo passed" in result.stdout
    assert "Live interrupted-demo reset passed" in result.stdout
