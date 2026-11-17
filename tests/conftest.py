"""Session fixtures: real Node B + Node A subprocesses on ports 8002/8001."""

from __future__ import annotations

import os
import random
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.tokens import count_tokens  # noqa: E402

A_URL = "http://127.0.0.1:8001"
B_URL = "http://127.0.0.1:8002"

_TEMPLATES = [
    "Fact {n}: table {table} has column {column} of type {dtype}.",
    "Observation {n}: service {service} returned status {status} during the {phase} run.",
    "Note {n}: the {service} job read {rows} rows from {table} and wrote {writes} rows.",
    "Tool output {n}: query on {table} scanned {rows} rows in {ms} milliseconds.",
    "Finding {n}: {service} depends on {table}, which is owned by the {team} team.",
    "Checkpoint {n}: the {phase} phase finished with {rows} processed records.",
]

_TABLES = ["orders", "users", "payments", "invoices", "sessions", "events"]
_COLUMNS = ["total_cents", "created_at", "status", "customer_id", "amount"]
_TYPES = ["integer", "timestamptz", "text", "numeric", "boolean"]
_SERVICES = ["billing", "gateway", "search", "ingest", "reporting"]
_STATUSES = [200, 201, 404, 500, 503]
_PHASES = ["nightly", "backfill", "smoke", "migration", "reconciliation"]
_TEAMS = ["core", "data", "platform", "payments"]
_DTYPES = ["integer", "text", "boolean", "numeric"]


def make_text(target_tokens: int, seed: int = 0) -> str:
    """Build deterministic, varied text of approximately ``target_tokens``."""
    rng = random.Random(seed)
    parts: list[str] = []
    n = 0
    while count_tokens("\n".join(parts)) < target_tokens:
        n += 1
        template = rng.choice(_TEMPLATES)
        parts.append(
            template.format(
                n=n,
                table=rng.choice(_TABLES),
                column=rng.choice(_COLUMNS),
                dtype=rng.choice(_DTYPES),
                service=rng.choice(_SERVICES),
                status=rng.choice(_STATUSES),
                phase=rng.choice(_PHASES),
                rows=rng.randint(10, 900_000),
                writes=rng.randint(0, 50_000),
                ms=rng.randint(2, 9000),
                team=rng.choice(_TEAMS),
            )
        )
    return "\n".join(parts)


class ServerManager:
    def __init__(self, tmp_dir: Path) -> None:
        self.tmp_dir = tmp_dir
        self.env = os.environ.copy()
        self.env.update(
            {
                "NODE_B_DB": str(tmp_dir / "node_b_archive.db"),
                "NODE_A_STATE_FILE": str(tmp_dir / "node_a_state.json"),
                "NODE_B_URL": B_URL,
                "RETRY_INTERVAL_S": "0.5",
                "REPLICATION_TIMEOUT_S": "2.0",
                "MAX_WINDOW_TOKENS": "2000",
                "PYTHONPATH": str(ROOT),
                "PYTHONUNBUFFERED": "1",
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
        self._start("b", "node_b.app:app", 8002)

    def start_a(self) -> None:
        self._start("a", "node_a.app:app", 8001)

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
        self.wait_health(A_URL)

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
            f"server at {url} did not become healthy: {last_error}\n"
            f"{self.logs_tail()}"
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


def _assert_port_free(url: str) -> None:
    try:
        response = httpx.get(f"{url}/health", timeout=0.3)
    except Exception:
        return
    if response.status_code == 200:
        raise RuntimeError(
            f"something is already listening at {url}; stop it before running tests"
        )


@pytest.fixture(scope="session")
def servers(tmp_path_factory: pytest.TempPathFactory) -> ServerManager:
    tmp_dir = tmp_path_factory.mktemp("pagerkv")
    _assert_port_free(B_URL)
    _assert_port_free(A_URL)

    manager = ServerManager(tmp_dir)
    try:
        manager.start_b()
        manager.wait_health(B_URL)
        manager.start_a()
        manager.wait_health(A_URL)
    except Exception:
        manager.stop_all()
        raise

    yield manager
    manager.stop_all()


@pytest.fixture(scope="session")
def base_urls() -> dict[str, str]:
    return {"a": A_URL, "b": B_URL}


@pytest.fixture
def a_client(servers: ServerManager) -> httpx.Client:
    with httpx.Client(base_url=A_URL, timeout=15.0) as client:
        yield client


@pytest.fixture
def b_client(servers: ServerManager) -> httpx.Client:
    with httpx.Client(base_url=B_URL, timeout=15.0) as client:
        yield client


@pytest.fixture(autouse=True)
def reset_node_b_fault(servers: ServerManager) -> None:
    """Every test starts (and ends) with Node B in normal mode."""
    try:
        httpx.post(
            f"{B_URL}/admin/fault",
            json={"mode": "none", "delay_s": 0},
            timeout=5.0,
        )
    except Exception:
        pass
    yield
    try:
        httpx.post(
            f"{B_URL}/admin/fault",
            json={"mode": "none", "delay_s": 0},
            timeout=5.0,
        )
    except Exception:
        pass


@pytest.fixture
def text_factory():
    return make_text


@pytest.fixture
def key_factory(a_client: httpx.Client):
    """Unique keys per test, deleted from Node A afterwards."""
    keys: list[str] = []

    def make(prefix: str = "key") -> str:
        key = f"{prefix}_{uuid.uuid4().hex[:10]}"
        keys.append(key)
        return key

    yield make

    for key in keys:
        for _ in range(3):
            try:
                a_client.delete(f"/context/{key}")
                break
            except Exception:
                time.sleep(0.05)
