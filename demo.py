#!/usr/bin/env python3
"""Standard-library CLI proof of full replication, outage recovery, and context paging."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any


class DemoError(RuntimeError):
    """A required demonstration check did not succeed."""


def check(condition: Any, message: str) -> None:
    if not condition:
        raise DemoError(message)


class PagerKVDemo:
    def __init__(
        self,
        node_a: str,
        node_b: str,
        auto: bool = False,
        delay: float = 1.0,
        admin_token: str | None = None,
        recovery_timeout: float = 30.0,
        request_timeout: float = 8.0,
    ) -> None:
        self.node_a = node_a.rstrip("/")
        self.node_b = node_b.rstrip("/")
        self.auto = auto
        self.delay = delay
        self.admin_token = (
            os.environ.get("PAGERKV_ADMIN_TOKEN", "") if admin_token is None else admin_token
        )
        self.recovery_timeout = recovery_timeout
        self.request_timeout = request_timeout
        self.prefix = "demo/cli_" + uuid.uuid4().hex
        self.demo_keys: list[str] = []
        self.values: dict[str, str] = {}
        self.versions: dict[str, int] = {}
        self.fault_touched = False
        self.key = ""
        self.budget = 0

    def pause(self, message: str = "Press Enter to continue...") -> None:
        if self.auto:
            time.sleep(self.delay)
        else:
            input("\n" + message)

    def request(
        self, url: str, method: str = "GET", payload: dict | None = None
    ) -> tuple[int, Any]:
        headers = {"Accept": "application/json"}
        if self.admin_token and any(
            url.startswith(base + "/admin/") for base in (self.node_a, self.node_b)
        ):
            headers["X-Admin-Token"] = self.admin_token
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.request_timeout) as response:
                raw = response.read()
                if not raw:
                    return response.status, None
                decoded = raw.decode("utf-8", errors="replace")
                try:
                    return response.status, json.loads(decoded)
                except ValueError:
                    return response.status, decoded
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                return exc.code, json.loads(raw)
            except ValueError:
                return exc.code, raw
        except (OSError, ValueError) as exc:
            return 0, str(exc)

    def require_json(self, url: str, method: str = "GET", payload: dict | None = None) -> dict:
        status, body = self.request(url, method, payload)
        if status == 403:
            raise DemoError(
                "Admin access denied. Set PAGERKV_ADMIN_TOKEN to the token configured on both nodes."
            )
        check(
            status == 200 and isinstance(body, dict),
            f"{method} {urllib.parse.urlsplit(url).path}: expected HTTP 200 JSON, got {status or 'no response'}.",
        )
        return body

    @staticmethod
    def encoded(key: str) -> str:
        return urllib.parse.quote(key, safe="")

    def reserve_key(self, suffix: str) -> str:
        key = self.prefix + "/" + suffix
        for base, path in ((self.node_a, "/key/"), (self.node_b, "/archive/")):
            status, _ = self.request(base + path + self.encoded(key))
            check(
                status == 404,
                "The generated key is not confirmed absent on both nodes; refusing to overwrite it.",
            )
        self.demo_keys.append(key)
        return key

    def preflight(self) -> None:
        for base in (self.node_a, self.node_b):
            health = self.require_json(base + "/health")
            check(
                health.get("ok") is True,
                "A node is unhealthy. Start both services before running the demo.",
            )
        mode = self.require_json(self.node_a + "/admin/node_b/fault")
        check(
            mode.get("mode") == "none",
            "Start with Node B Online. The demo will not replace an existing failure mode.",
        )
        check(self.require_json(self.node_b + "/ready").get("ok") is True, "Node B is not ready.")
        self.budget = self.require_json(self.node_a + "/context/status?include_text=false")[
            "max_window_tokens"
        ]
        check(self.budget >= 512, "Use a context budget of at least 512 tokens for this demo.")
        print("[OK] Both services are ready; failure controls are authorized.")
        print("Sample namespace: " + self.prefix)
        print("Only this run's generated keys will be written or removed.")

    def set_fault(self, mode: str) -> None:
        # A timed-out control request might still have changed B; cleanup must restore it.
        self.fault_touched = True
        result = self.require_json(
            self.node_a + "/admin/node_b/fault", "POST", {"mode": mode, "delay_s": 0}
        )
        check(result.get("mode") == mode, "Node B did not confirm the requested mode.")
        actual = self.require_json(self.node_a + "/admin/node_b/fault")
        check(actual.get("mode") == mode, "Node B mode verification failed.")
        if mode == "none":
            self.fault_touched = False

    def write(self, key: str, value: str, replicated: bool = True, **policy: Any) -> dict:
        check(key in self.demo_keys, "Refusing to write a key not reserved by this run.")
        result = self.require_json(
            self.node_a + "/key", "PUT", {"key": key, "value": value, **policy}
        )
        version = result.get("version")
        if (
            not isinstance(version, int)
            or isinstance(version, bool)
            or version <= self.versions.get(key, 0)
        ):
            raise DemoError("Node A did not return a newer version.")
        self.values[key] = value
        self.versions[key] = version
        check(
            result.get("replicated") is replicated,
            f"Expected replicated={replicated}; the observed write did not match.",
        )
        expected = "SYNCED_ARCHIVED" if replicated else "DIRTY_UNREPLICATED"
        check(result.get("consistency_status") == expected, "Unexpected per-key replication state.")
        if not replicated:
            check(result.get("replication_error"), "The failed copy did not include an error.")
        return result

    def verify_archive(self, key: str) -> dict:
        archive = self.require_json(self.node_b + "/archive/" + self.encoded(key))
        expected_digest = hashlib.sha256(self.values[key].encode("utf-8")).hexdigest()
        check(archive.get("key") == key, "Archive key mismatch.")
        check(archive.get("version") == self.versions[key], "Archive version mismatch.")
        check(archive.get("full_text") == self.values[key], "Archive full-text mismatch.")
        check(archive.get("sha256") == expected_digest, "Archive SHA-256 digest mismatch.")
        check(isinstance(archive.get("stub"), str), "Archive response has no stub.")
        print(f"[OK] Full text, version and SHA-256 verified for v{archive['version']}.")
        return archive

    def step_1_ingest_replicate(self) -> None:
        print("\n[Step 1] PUT a full observation and verify its replica")
        self.key = self.reserve_key("service_config")
        text = (
            "Service configuration: worker_threads=8; postgres_pool_size=50. "
            + "The billing API reads orders and payments; retain the complete schema and request trace for exact context retrieval. "
            * 6
        )
        result = self.write(self.key, text, pinned=True)
        print(f"[OK] PUT /key: A accepted v{result['version']}; B acknowledged the full copy.")
        archive = self.verify_archive(self.key)
        print("Stub:", archive["stub"])
        print("The summary has a 25-token budget; the entire stub includes additional metadata.")
        self.pause()

    def step_2_network_partition(self) -> None:
        print("\n[Step 2] Simulate B rejecting requests; verify the dirty-bit guard")
        self.require_json(
            self.node_a + "/context/" + self.encoded(self.key), "PATCH", {"pinned": False}
        )
        self.set_fault("down")
        result = self.write(
            self.key, self.values[self.key] + " Updated: worker_threads=16.", replicated=False
        )
        snapshot = self.require_json(self.node_a + "/context/status?include_text=false")
        entry = snapshot.get("keys", {}).get(self.key)
        check(isinstance(entry, dict), "The accepted key is absent from Node A status.")
        check(
            entry.get("consistency_status") == "DIRTY_UNREPLICATED", "The key is not marked dirty."
        )
        check(
            entry.get("eviction_allowed") is False,
            "Dirty text was incorrectly marked safe to page to B.",
        )
        check(
            entry.get("residence") != "PAGED_TO_NODE_B",
            "Dirty text was incorrectly treated as safely archived.",
        )
        comparison = self.require_json(self.node_a + "/replication/status")
        check(
            comparison.get("comparison_status") == "unknown",
            "B outage did not produce unknown consistency.",
        )
        print(f"[OK] A accepted v{result['version']} locally; replica acknowledgment failed.")
        print("[OK] Dirty state and eviction_allowed=false verified. Consistency: unknown.")
        print(
            "This simulates HTTP 503 failure, not a physical network partition or formal CAP proof."
        )
        self.pause()

    def step_3_local_read(self) -> None:
        print("\n[Step 3] Read the exact complete value locally during the outage")
        body = self.require_json(self.node_a + "/key/" + self.encoded(self.key))
        check(
            body.get("value") == self.values[self.key],
            "Local read did not match the accepted text.",
        )
        check(body.get("version") == self.versions[self.key], "Local read version mismatch.")
        check(
            body.get("source") == "LOCAL_STORE", "Read was not served by the primary local store."
        )
        print("[OK] Exact value and version verified from LOCAL_STORE while B is down.")
        self.pause()

    def step_4_background_recovery(self) -> None:
        print("\n[Step 4] Restore B and wait for background retry; do not send another PUT")
        self.set_fault("none")
        deadline = time.monotonic() + self.recovery_timeout
        while time.monotonic() < deadline:
            pair = self.require_json(self.node_a + "/replication/key/" + self.encoded(self.key))
            if pair.get("comparison_status") == "in_sync":
                break
            time.sleep(0.25)
        else:
            raise DemoError("Background recovery did not converge before the configured deadline.")
        self.verify_archive(self.key)
        print("[OK] This sample converged through background retry without a replacement write.")
        print("Global consistency may also reflect other pre-existing keys.")
        self.pause()

    def step_5_context_paging(self) -> None:
        print(
            "\n[Step 5] Observe real paging, pause at the stub, and verify exact archive hydration"
        )
        self.require_json(
            self.node_a + "/context/" + self.encoded(self.key),
            "PATCH",
            {"pinned": False, "priority": -100},
        )
        entry = None
        snapshot = {}
        for i in range(4):
            filler = self.reserve_key("filler_" + str(i))
            sentence = "Context observation: service routing, database schema and exact request traces are retained for later retrieval. "
            repeats = min(6000, max(40, math.ceil(self.budget / 12)))
            text = f"Filler {i}: archived context sample. " + sentence * repeats
            self.write(filler, text)
            snapshot = self.require_json(self.node_a + "/context/status?include_text=true")
            entry = snapshot.get("keys", {}).get(self.key)
            if entry and entry.get("residence") == "PAGED_TO_NODE_B":
                break
        if not isinstance(entry, dict) or entry.get("residence") != "PAGED_TO_NODE_B":
            raise DemoError(
                "No actual sample paging was observed; use a smaller context budget and retry."
            )
        check(
            isinstance(entry.get("prompt_view"), str) and entry["prompt_view"].startswith("[STUB"),
            "The paged active view is not a stub.",
        )
        check(
            entry["acked_version"] == entry["version"] == self.versions[self.key],
            "Paged version was not acknowledged.",
        )
        check(
            entry["active_tokens_in_node_a"] < entry["full_tokens_on_node_b"],
            "Paging did not reduce this block's active token count.",
        )
        local = self.require_json(self.node_a + "/key/" + self.encoded(self.key))
        check(
            local.get("value") == self.values[self.key], "Paging changed the complete local value."
        )
        self.verify_archive(self.key)
        print("[OK] An acknowledged full block is now a shorter active-context stub.")
        print("Active view:", entry["prompt_view"])
        print(
            f"Actual prompt: {snapshot['active_window_tokens']}/{snapshot['max_window_tokens']} tokens; over_budget={snapshot['over_budget']}."
        )
        print(
            "Both stored values remain complete. Any remaining budget pressure is reported, not hidden."
        )
        self.pause("Inspect the stub, then press Enter to hydrate from B...")
        hydrated = self.require_json(
            self.node_a + "/context/" + self.encoded(self.key) + "?hydrate=true"
        )
        check(
            hydrated.get("source") == "L2_PAGE_FAULT",
            "Hydration was not an actual archive page fault.",
        )
        check(hydrated.get("version") == self.versions[self.key], "Hydration version mismatch.")
        check(
            hydrated.get("full_text") == self.values[self.key],
            "Hydrated text did not exactly equal the original.",
        )
        print("[OK] L2_PAGE_FAULT and exact original text/version equality verified.")

    def cleanup(self) -> list[str]:
        problems = []
        if self.fault_touched:
            try:
                self.set_fault("none")
            except DemoError as exc:
                problems.append(str(exc))
        for key in list(self.demo_keys):
            if not key.startswith(self.prefix + "/"):
                problems.append("Refused to delete a key outside this run's namespace.")
                continue
            status, _ = self.request(self.node_a + "/key/" + self.encoded(key), "DELETE")
            if status != 204:
                problems.append(f"Could not confirm local deletion of {key}.")
                continue
            pair_status, pair = self.request(self.node_a + "/replication/key/" + self.encoded(key))
            if (
                pair_status == 200
                and isinstance(pair, dict)
                and pair.get("node_a") is None
                and pair.get("node_b") is None
                and pair.get("node_b_state") == "missing"
            ):
                self.demo_keys.remove(key)
            else:
                problems.append(
                    f"Replica deletion is not verified for {key}; inspect pending deletions."
                )
        return problems

    def run(self) -> None:
        print("PagerKV: verified REST replication and context paging")
        try:
            self.preflight()
            self.step_1_ingest_replicate()
            self.step_2_network_partition()
            self.step_3_local_read()
            self.step_4_background_recovery()
            self.step_5_context_paging()
        finally:
            problems = self.cleanup()
            if problems:
                for problem in problems:
                    print("[CLEANUP INCOMPLETE] " + problem, file=sys.stderr)
                if sys.exc_info()[0] is None:
                    raise DemoError(
                        "Cleanup was incomplete; demonstration is not reported as successful."
                    )
        print("\n[OK] All five steps verified. Generated keys removed; existing values preserved.")


def nonnegative(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("must be a finite nonnegative number")
    return number


def positive(value: str) -> float:
    number = nonnegative(value)
    if number == 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return number


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-a", default="http://127.0.0.1:8001")
    parser.add_argument("--node-b", default="http://127.0.0.1:8002")
    parser.add_argument("--auto", "-y", action="store_true", help="run without interactive pauses")
    parser.add_argument(
        "--delay", type=nonnegative, default=1.0, help="seconds between automatic steps"
    )
    parser.add_argument("--recovery-timeout", type=positive, default=30.0)
    parser.add_argument("--request-timeout", type=positive, default=8.0)
    args = parser.parse_args()
    demo = PagerKVDemo(
        args.node_a,
        args.node_b,
        args.auto,
        args.delay,
        recovery_timeout=args.recovery_timeout,
        request_timeout=args.request_timeout,
    )
    try:
        demo.run()
    except (KeyboardInterrupt, EOFError):
        print("\n[INTERRUPTED] Cleanup attempted; the demo was not completed.", file=sys.stderr)
        return 130
    except DemoError as exc:
        print("[FAILED] " + str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
