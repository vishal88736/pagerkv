"""Benchmark: a baseline agent with no pager vs a PagerKV agent.

The benchmark is self-contained: it launches isolated Node B and Node A
processes on ports 18002/18001 with a temporary database and state file, runs
100 deterministic turns, and writes ``bench/results.csv``.

No LLM is involved.  ``random.seed(42)`` fixes every observation and every
hydration choice.
"""

from __future__ import annotations

import csv
import hashlib
import os
import random
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.tokens import count_tokens  # noqa: E402

BENCH_DIR = Path(__file__).resolve().parent
RESULTS_CSV = BENCH_DIR / "results.csv"

A_PORT = int(os.environ.get("BENCH_A_PORT", "18001"))
B_PORT = int(os.environ.get("BENCH_B_PORT", "18002"))
A_URL = f"http://127.0.0.1:{A_PORT}"
B_URL = f"http://127.0.0.1:{B_PORT}"

TURNS = 100
MAX_WINDOW_TOKENS = 2000
FAULT_START_TURN = 40
FAULT_END_TURN = 50

SERVICES = ["billing", "gateway", "search", "ingest", "reporting"]
TABLES = ["orders", "users", "payments", "invoices", "sessions", "events"]
COLUMNS = ["total_cents", "created_at", "status", "customer_id"]
PHASES = ["nightly", "backfill", "smoke", "migration"]
TYPES = ["integer", "timestamptz", "text", "numeric", "boolean"]


class ContextLengthExceededError(Exception):
    """Raised by the baseline agent when its prompt no longer fits."""


# ---------------------------------------------------------------------------
# Observations
# ---------------------------------------------------------------------------


def make_observation(turn: int, target_tokens: int, rng: random.Random) -> tuple[str, str]:
    """Varied text of approximately ``target_tokens``.

    The first sentence is deliberately short; it becomes the extractive stub.
    A unique fact with a 4-char code is embedded so recall can be tested.
    """
    code = "".join(rng.choice("ABCDEFGHJKLMNPQRSTUVWXYZ23456789") for _ in range(4))
    parts = [
        f"Observation {turn}: {rng.choice(SERVICES)} telemetry update.",
        f"Fact obs_{turn}: deploy token is {code}.",
    ]
    sentence = 0
    while count_tokens("\n".join(parts)) < target_tokens:
        sentence += 1
        parts.append(
            f"Sentence {sentence}: table {rng.choice(TABLES)} has column "
            f"{rng.choice(COLUMNS)} of type {rng.choice(TYPES)}; the "
            f"{rng.choice(PHASES)} job processed {rng.randint(5, 900000)} rows "
            f"and wrote {rng.randint(0, 50000)} records."
        )
    return "\n".join(parts), code


def build_observations() -> tuple[dict[int, str], dict[int, str]]:
    rng = random.Random(42)
    observations: dict[int, str] = {}
    codes: dict[int, str] = {}
    for turn in range(1, TURNS + 1):
        target = rng.randint(150, 900)
        text, code = make_observation(turn, target, rng)
        observations[turn] = text
        codes[turn] = code
    return observations, codes


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Baseline
# ---------------------------------------------------------------------------


class BaselineAgent:
    def __init__(self, max_tokens: int = MAX_WINDOW_TOKENS) -> None:
        self.max_tokens = max_tokens
        self.observations: list[str] = []

    def observe(self, text: str) -> int:
        self.observations.append(text)
        prompt = "\n\n".join(self.observations)
        tokens = count_tokens(prompt)
        if tokens > self.max_tokens:
            raise ContextLengthExceededError(
                f"baseline prompt is {tokens} tokens, over {self.max_tokens}"
            )
        return tokens


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------


def port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex(("127.0.0.1", port)) != 0


def wait_health(url: str, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            response = httpx.get(f"{url}/health", timeout=0.5)
            if response.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.05)
    raise RuntimeError(f"{url} did not become healthy")


class Nodes:
    def __init__(self, work_dir: Path) -> None:
        self.work_dir = work_dir
        self.env = os.environ.copy()
        self.env.update(
            {
                "NODE_B_DB": str(work_dir / "node_b.db"),
                "NODE_A_STATE_FILE": str(work_dir / "node_a_state.json"),
                "NODE_B_URL": B_URL,
                "RETRY_INTERVAL_S": "0.5",
                "REPLICATION_TIMEOUT_S": "2.0",
                "MAX_WINDOW_TOKENS": str(MAX_WINDOW_TOKENS),
                "PYTHONPATH": str(ROOT),
                "PYTHONUNBUFFERED": "1",
            }
        )
        self.procs: list[subprocess.Popen] = []
        self.logs: list = []

    def _start(self, module: str, port: int, name: str) -> subprocess.Popen:
        handle = open(self.work_dir / f"{name}.log", "wb")
        self.logs.append(handle)
        proc = subprocess.Popen(
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
        self.procs.append(proc)
        return proc

    def start(self) -> None:
        if not port_is_free(A_PORT) or not port_is_free(B_PORT):
            raise RuntimeError(
                f"benchmark ports {A_PORT}/{B_PORT} are in use; stop those servers"
            )
        self._start("node_b.app:app", B_PORT, "node_b")
        wait_health(B_URL)
        self._start("node_a.app:app", A_PORT, "node_a")
        wait_health(A_URL)

    def stop(self) -> None:
        for proc in reversed(self.procs):
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=10)
        for handle in self.logs:
            try:
                handle.close()
            except Exception:
                pass
        self.procs.clear()
        self.logs.clear()


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------


def set_fault(client: httpx.Client, mode: str, delay_s: float = 0.0) -> None:
    response = client.post(
        "/admin/fault", json={"mode": mode, "delay_s": delay_s}, timeout=10
    )
    response.raise_for_status()


def wait_for_clean_recovery(a_client: httpx.Client, timeout: float = 20.0) -> None:
    """After the outage, wait until no dirty keys remain."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = a_client.get("/context/status", timeout=30).json()
        dirty = sum(
            1
            for row in status["keys"].values()
            if row["consistency_status"] == "DIRTY_UNREPLICATED"
        )
        if dirty == 0:
            return
        time.sleep(0.2)
    raise RuntimeError("Node A did not recover from the fault window")


def run() -> dict:
    observations, codes = build_observations()

    # ---- baseline -----------------------------------------------------
    baseline = BaselineAgent()
    baseline_tokens: dict[int, int] = {}
    crash_turn: int | None = None
    for turn in range(1, TURNS + 1):
        try:
            baseline_tokens[turn] = baseline.observe(observations[turn])
        except ContextLengthExceededError:
            crash_turn = turn
            break

    # ---- PagerKV ------------------------------------------------------
    rows: list[dict] = []
    page_fault_checks: list[int] = []
    lossless = 0
    hydrate_rng = random.Random(99)
    recall_rng = random.Random(7)
    recall_results: list[tuple[int, int, bool]] = []  # (turn, key_turn, ok)

    def try_recall(key_turn: int, in_fault: bool) -> bool:
        key = f"obs_{key_turn}"
        code = codes[key_turn]
        # (a) prompt text
        try:
            prompt_text = client.get("/context/prompt").json()["prompt"]
        except Exception:
            prompt_text = ""
        if code in prompt_text:
            return True
        # (b) search
        try:
            search_resp = client.get(
                "/context/search", params={"q": f"Fact {key}"}
            )
        except Exception:
            return False
        if search_resp.status_code != 200:
            return False  # fault window: Node B down
        results = search_resp.json().get("results", [])
        target = key
        if not any(r.get("key") == key for r in results):
            if not results:
                return False
            target = results[0].get("key", key)
        # (c) hydrate
        try:
            hyd = client.get(f"/context/{target}?hydrate=true")
        except Exception:
            return False
        if hyd.status_code != 200:
            return False
        return code in hyd.json().get("full_text", "")

    with tempfile.TemporaryDirectory(prefix="pagerkv-bench-") as tmp:
        nodes = Nodes(Path(tmp))
        page_faults = 0
        try:
            nodes.start()
            with httpx.Client(base_url=A_URL, timeout=30.0) as client:
                with httpx.Client(base_url=B_URL, timeout=30.0) as node_b:
                    for turn in range(1, TURNS + 1):
                        if turn == FAULT_START_TURN:
                            set_fault(node_b, "down")
                        if turn == FAULT_END_TURN:
                            set_fault(node_b, "none")
                            wait_for_clean_recovery(client)

                        response = client.put(
                            f"/context/obs_{turn}",
                            json={"text": observations[turn]},
                        )
                        response.raise_for_status()

                        prompt = client.get("/context/prompt").json()
                        status = client.get(
                            "/context/status", params={"include_text": "false"}
                        ).json()
                        dirty = sum(
                            1
                            for row in status["keys"].values()
                            if row["consistency_status"] == "DIRTY_UNREPLICATED"
                        )
                        paged = sum(
                            1
                            for row in status["keys"].values()
                            if row["residence"] == "PAGED_TO_NODE_B"
                        )
                        spilled = sum(
                            1
                            for row in status["keys"].values()
                            if row["residence"] == "SPILLED_LOCAL"
                        )
                        rows.append(
                            {
                                "turn": turn,
                                "baseline_tokens": baseline_tokens.get(turn, ""),
                                "pagerkv_prompt_tokens": prompt["tokens"],
                                "dirty_keys": dirty,
                                "paged_out_keys": paged,
                                "spilled_keys": spilled,
                                "tokens_saved": status[
                                    "tokens_saved_by_replication"
                                ],
                                "recall_ok": "",
                            }
                        )

                        if turn % 10 == 0 and turn > 1:
                            older = [
                                key
                                for key, row in status["keys"].items()
                                if key.startswith("obs_")
                                and row["residence"] == "PAGED_TO_NODE_B"
                                and int(key.split("_")[1]) < turn
                            ]
                            if older:
                                chosen = hydrate_rng.choice(
                                    sorted(older, key=lambda k: int(k.split("_")[1]))
                                )
                                hydrated = client.get(
                                    f"/context/{chosen}?hydrate=true"
                                )
                                chosen_turn = int(chosen.split("_")[1])
                                if FAULT_START_TURN <= turn < FAULT_END_TURN:
                                    # Node B is down: hydration must fail safely
                                    # with the stub, never return wrong text.
                                    assert hydrated.status_code == 503
                                    assert hydrated.json()["stub"]
                                else:
                                    hydrated.raise_for_status()
                                    body = hydrated.json()
                                    if body["source"] == "L2_PAGE_FAULT":
                                        page_fault_checks.append(chosen_turn)
                                    assert sha256(
                                        body["full_text"]
                                    ) == sha256(observations[chosen_turn])

                            # Recall check: can the agent recover an older fact?
                            recall_key_turn = hydrate_rng.randrange(1, turn)
                            in_fault = FAULT_START_TURN <= turn < FAULT_END_TURN
                            ok = try_recall(recall_key_turn, in_fault)
                            recall_results.append((turn, recall_key_turn, ok))
                            rows[-1]["recall_ok"] = "1" if ok else "0"

                    # ---- end-to-end lossless check ---------------------
                    for turn in range(1, TURNS + 1):
                        response = client.get(
                            f"/context/obs_{turn}?hydrate=true"
                        )
                        response.raise_for_status()
                        body = response.json()
                        if sha256(body["full_text"]) == sha256(observations[turn]):
                            lossless += 1
                        else:
                            print(f"LOSSLESS MISMATCH at obs_{turn}")

                    # ---- recall: 20 random older keys -------------------
                    random_keys = recall_rng.sample(range(1, TURNS + 1), 20)
                    for kt in random_keys:
                        ok = try_recall(kt, False)
                        recall_results.append((TURNS + 1, kt, ok))

                    page_faults = client.get(
                        "/context/status"
                    ).json()["page_faults"]
        finally:
            nodes.stop()

    with open(RESULTS_CSV, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "turn",
                "baseline_tokens",
                "pagerkv_prompt_tokens",
                "dirty_keys",
                "paged_out_keys",
                "spilled_keys",
                "tokens_saved",
                "recall_ok",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    outside_fault = [
        row["pagerkv_prompt_tokens"]
        for row in rows
        if not (FAULT_START_TURN <= row["turn"] <= FAULT_END_TURN)
    ]
    max_outside_fault = max(outside_fault) if outside_fault else 0
    max_prompt = max(row["pagerkv_prompt_tokens"] for row in rows)

    outside_fault_recall = [
        ok for (turn, _kt, ok) in recall_results
        if not (FAULT_START_TURN <= turn <= FAULT_END_TURN)
    ]
    recall_success = sum(1 for ok in outside_fault_recall if ok)
    recall_total = len(outside_fault_recall)
    all_recall_success = sum(1 for (_t, _k, ok) in recall_results if ok)
    all_recall_total = len(recall_results)

    summary = {
        "crash_turn": crash_turn,
        "turns_run": len(rows),
        "max_prompt_tokens": max_prompt,
        "max_outside_fault_tokens": max_outside_fault,
        "lossless": lossless,
        "total": TURNS,
        "page_faults": page_faults,
        "page_fault_checks": len(page_fault_checks),
        "recall_success": all_recall_success,
        "recall_total": all_recall_total,
        "recall_outside_fault_success": recall_success,
        "recall_outside_fault_total": recall_total,
        "rows": rows,
    }

    print(
        f"Baseline crashed at turn {crash_turn} | "
        f"PagerKV ran {len(rows)} turns | "
        f"max prompt tokens {max_prompt} | "
        f"lossless {lossless}/{TURNS} | "
        f"page faults {page_faults}"
    )
    print(
        f"recall {all_recall_success}/{all_recall_total} "
        f"(outside-fault {recall_success}/{recall_total})"
    )
    print(f"Max prompt tokens outside the fault window: {max_outside_fault}")
    print(f"CSV: {RESULTS_CSV}")
    return summary


def check(summary: dict) -> list[str]:
    """Regression gates used by CI (``--check``)."""
    failures = []
    if summary["lossless"] != summary["total"]:
        failures.append(f"lossless {summary['lossless']}/{summary['total']}")
    if summary["max_prompt_tokens"] > MAX_WINDOW_TOKENS:
        failures.append(
            f"max prompt {summary['max_prompt_tokens']} > budget {MAX_WINDOW_TOKENS}"
        )
    if summary["recall_outside_fault_success"] != summary["recall_outside_fault_total"]:
        failures.append(
            f"recall outside fault {summary['recall_outside_fault_success']}"
            f"/{summary['recall_outside_fault_total']}"
        )
    return failures


if __name__ == "__main__":
    result = run()
    if "--check" in sys.argv[1:]:
        problems = check(result)
        if problems:
            print("BENCHMARK CHECK FAILED: " + "; ".join(problems))
            sys.exit(1)
        print("Benchmark check passed.")
