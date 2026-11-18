"""Each HTTP test starts independent nodes and disposable databases."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _free_port() -> int:
    """Ask the OS for an unused TCP port, then release it for uvicorn."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class ServerManager:
    def __init__(self, tmp_dir: Path) -> None:
        self.tmp_dir = tmp_dir
        self.a_port = _free_port()
        self.b_port = _free_port()
        while self.b_port == self.a_port:  # pragma: no cover - astronomical
            self.b_port = _free_port()
        self.a_url = f"http://127.0.0.1:{self.a_port}"
        self.b_url = f"http://127.0.0.1:{self.b_port}"
        self.env = os.environ.copy()
        self.env.update(
            {
                "NODE_B_DB": str(tmp_dir / "node_b.db"),
                "NODE_A_DB": str(tmp_dir / "node_a.db"),
                "NODE_B_URL": self.b_url,
                "RETRY_INTERVAL_S": "0.5",
                "REPLICATION_TIMEOUT_S": "2.0",
                "PYTHONPATH": str(ROOT),
                "PYTHONUNBUFFERED": "1",
                "PAGERKV_ADMIN_TOKEN": "",
            }
        )
        self.procs: dict[str, subprocess.Popen[bytes] | None] = {
            "a": None,
            "b": None,
        }
        self.log_handles: list = []

    # -- process control -------------------------------------------------

    def _start(self, name: str, module: str, port: int) -> None:
        log_path = self.tmp_dir / f"{name}.log"
        handle = open(log_path, "ab", buffering=0)
        self.log_handles.append(handle)
        self.procs[name] = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                module,
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--log-level",
                "warning",
            ],
            cwd=str(ROOT),
            env=self.env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def start_b(self) -> None:
        self._start("b", "node_b.app:app", self.b_port)

    def start_a(self) -> None:
        self._start("a", "node_a.app:app", self.a_port)

    def stop(self, name: str) -> None:
        proc = self.procs.get(name)
        if proc is None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
        self.procs[name] = None

    def stop_all(self) -> None:
        for name in ("a", "b"):
            self.stop(name)
        for handle in self.log_handles:
            try:
                handle.close()
            except Exception:
                pass
        self.log_handles.clear()

    def restart_a(self) -> None:
        self.stop("a")
        self.start_a()
        self.wait_health(self.a_url)

    # -- readiness -------------------------------------------------------

    def wait_health(self, url: str, timeout: float = 10.0) -> None:
        deadline = time.time() + timeout
        last_error: Exception | None = None
        while time.time() < deadline:
            try:
                response = httpx.get(f"{url}/health", timeout=0.5)
                if response.status_code == 200:
                    return
                last_error = RuntimeError(f"health returned {response.status_code}")
            except Exception as exc:  # connection refused while booting
                last_error = exc
            time.sleep(0.05)
        raise RuntimeError(
            f"server at {url} did not become healthy: {last_error}\n{self.logs_tail()}"
        )

    def logs_tail(self, limit: int = 6000) -> str:
        chunks = []
        for handle in self.log_handles:
            try:
                handle.flush()
            except Exception:
                pass
        for name in ("b", "a"):
            path = self.tmp_dir / f"{name}.log"
            if path.exists():
                chunks.append(
                    f"--- {name}.log ---\n"
                    + path.read_text(encoding="utf-8", errors="replace")[-limit:]
                )
        return "\n".join(chunks)


@pytest.fixture
def servers(tmp_path: Path) -> ServerManager:
    manager = ServerManager(tmp_path)
    try:
        manager.start_b()
        manager.wait_health(manager.b_url)
        manager.start_a()
        manager.wait_health(manager.a_url)
    except Exception:
        manager.stop_all()
        raise

    yield manager
    manager.stop_all()


@pytest.fixture
def base_urls(servers: ServerManager) -> dict[str, str]:
    return {"a": servers.a_url, "b": servers.b_url}


@pytest.fixture
def a_client(servers: ServerManager) -> httpx.Client:
    with httpx.Client(base_url=servers.a_url, timeout=15.0) as client:
        yield client


@pytest.fixture
def b_client(servers: ServerManager) -> httpx.Client:
    with httpx.Client(base_url=servers.b_url, timeout=15.0) as client:
        yield client
