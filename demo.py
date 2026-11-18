#!/usr/bin/env python3
"""Interactive & automated CLI demonstration for PagerKV.

Demonstrates:
  1. REST Ingestion & Full Replication (Node A -> Node B)
  2. Network Partition & Dirty-Bit Safety (eviction blocked while dirty)
  3. Local Availability during replica outage (CAP availability)
  4. Automatic background retry convergence (Eventual consistency)
  5. Semantic Context Paging & On-Demand Hydration (Page Fault)

Run:
  python demo.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

# ANSI Colors
BOLD = "\033[1m"
DIM = "\033[2m"
CYAN = "\033[36m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
MAGENTA = "\033[35m"
RESET = "\033[0m"


def header(title: str) -> None:
    print(f"\n{BOLD}{CYAN}{'=' * 72}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'=' * 72}{RESET}")


def step_banner(num: int, title: str, subtitle: str) -> None:
    print(f"\n{BOLD}{GREEN}[Step {num}] {title}{RESET}")
    print(f"{DIM}  ↳ {subtitle}{RESET}\n")


def info(msg: str) -> None:
    print(f"  {DIM}•{RESET} {msg}")


def success(msg: str) -> None:
    print(f"  {GREEN}✔{RESET} {BOLD}{msg}{RESET}")


def warn(msg: str) -> None:
    print(f"  {YELLOW}⚠{RESET} {msg}")


def err(msg: str) -> None:
    print(f"  {RED}✖{RESET} {msg}")


class PagerKVDemo:
    def __init__(self, node_a: str, node_b: str, auto: bool = False, delay: float = 1.0) -> None:
        self.node_a = node_a.rstrip("/")
        self.node_b = node_b.rstrip("/")
        self.auto = auto
        self.delay = delay
        self.demo_keys: list[str] = []

    def pause(self, prompt: str = "Press Enter to continue...") -> None:
        if self.auto:
            time.sleep(self.delay)
            return
        try:
            input(f"\n{DIM}{prompt}{RESET}")
        except (KeyboardInterrupt, EOFError):
            print("\nExiting demonstration...")
            self.cleanup()
            sys.exit(0)

    def request(
        self,
        url: str,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, Any] | str | None]:
        hdrs = {"Accept": "application/json"}
        if headers:
            hdrs.update(headers)
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            hdrs["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                status = resp.status
                body_bytes = resp.read()
                try:
                    return status, json.loads(body_bytes.decode("utf-8"))
                except ValueError:
                    return status, body_bytes.decode("utf-8")
        except urllib.error.HTTPError as e:
            body_bytes = e.read()
            try:
                return e.code, json.loads(body_bytes.decode("utf-8"))
            except Exception:
                return e.code, body_bytes.decode("utf-8")
        except Exception as e:
            return 0, str(e)

    def check_health(self) -> bool:
        status_a, res_a = self.request(f"{self.node_a}/health")
        if status_a != 200 or not isinstance(res_a, dict) or not res_a.get("ok"):
            err(f"Cannot reach Node A at {self.node_a} (HTTP {status_a}: {res_a})")
            info("Please start the cluster first:")
            print(f"    {CYAN}bash scripts/run_nodes.sh{RESET}")
            return False

        status_b, res_b = self.request(f"{self.node_b}/health")
        if status_b != 200 or not isinstance(res_b, dict) or not res_b.get("ok"):
            err(f"Cannot reach Node B at {self.node_b} (HTTP {status_b}: {res_b})")
            info("Please ensure Node B is running on port 8002.")
            return False

        success(f"Cluster online: Node A ({self.node_a}) & Node B ({self.node_b})")
        return True

    def set_fault(self, mode: str) -> bool:
        status, res = self.request(
            f"{self.node_a}/admin/node_b/fault",
            method="POST",
            payload={"mode": mode, "delay_s": 0},
        )
        return status == 200

    def cleanup(self) -> None:
        if not self.demo_keys:
            return
        info(f"Cleaning up {len(self.demo_keys)} demo keys...")
        self.set_fault("none")
        for key in self.demo_keys:
            self.request(f"{self.node_a}/key/{urllib.parse.quote(key, safe='')}", method="DELETE")
        self.demo_keys.clear()

    def run(self) -> None:
        header("PagerKV: Distributed Key-Value Store w/ Semantic Write-Back Paging")
        print(f"{DIM}Demonstrating replication consistency, dirty-bit safety, and semantic paging.{RESET}")

        if not self.check_health():
            sys.exit(1)

        # Ensure Node B starts clean and online
        self.set_fault("none")

        try:
            self.step_1_ingest_replicate()
            self.step_2_network_partition()
            self.step_3_local_read()
            self.step_4_background_recovery()
            self.step_5_semantic_paging()
        finally:
            self.cleanup()
            print(f"\n{BOLD}{GREEN}✔ Demonstration finished successfully.{RESET}\n")

    def step_1_ingest_replicate(self) -> None:
        step_banner(1, "Ingest & Replicate (Happy Path)", "PUT /key writes to Node A and synchronously replicates to Node B")

        key = "demo/service_config"
        self.demo_keys.append(key)
        text = (
            "Service Configuration v1.0:\n"
            "  - worker_threads: 8\n"
            "  - postgres_pool_size: 50\n"
            "  - redis_cache_ttl_seconds: 3600\n"
            "  - routing_strategy: round_robin\n"
            "All systems nominal and verified across primary clusters."
        )

        info(f"Writing key '{BOLD}{key}{RESET}' via PUT /key...")
        status, res = self.request(
            f"{self.node_a}/key",
            method="PUT",
            payload={"key": key, "value": text, "pinned": True},
        )

        if status != 200 or not isinstance(res, dict):
            err(f"PUT failed: {status} - {res}")
            return

        success(f"Node A accepted write: version={res.get('version')}")
        success(f"Node B replication status: {BOLD}{res.get('consistency_status')}{RESET} (replicated={res.get('replicated')})")

        # Inspect Node B archive
        _, b_archive = self.request(f"{self.node_b}/archive/{urllib.parse.quote(key, safe='')}")
        if isinstance(b_archive, dict):
            info(f"Node B SHA-256 Digest: {DIM}{b_archive.get('sha256')[:16]}...{RESET}")
            info(f"Node B Summary Stub: {CYAN}{b_archive.get('stub')}{RESET}")

        self.pause()

    def step_2_network_partition(self) -> None:
        step_banner(
            2,
            "Partition & Dirty-Bit Safety (OS Dirty Bit Pattern)",
            "Node B goes DOWN; Node A accepts write locally but blocks eviction (DIRTY_UNREPLICATED)",
        )

        info("Simulating network partition / replica outage: taking Node B DOWN...")
        self.set_fault("down")
        warn("Node B is now OFFLINE (HTTP 503 fault mode)")

        key = "demo/incident_report"
        self.demo_keys.append(key)
        text = (
            "CRITICAL INCIDENT REPORT #1042:\n"
            "  - database connection spikes observed\n"
            "  - payment service timeout in region us-east\n"
            "  - root cause: stale connection pool leak\n"
            "Agent must remember this incident forever until resolved."
        )

        info(f"Writing new key '{BOLD}{key}{RESET}' while Node B is down...")
        status, res = self.request(
            f"{self.node_a}/key",
            method="PUT",
            payload={"key": key, "value": text},
        )

        if status != 200 or not isinstance(res, dict):
            err(f"PUT failed: {status} - {res}")
            return

        success(f"Node A returned HTTP 200 OK (CAP Availability preserved!)")
        warn(f"Replication status: {BOLD}{res.get('consistency_status')}{RESET} (replicated={res.get('replicated')})")
        warn(f"Replication error recorded: {res.get('replication_error')}")

        # Check Node A status to verify dirty bit protection
        _, a_status = self.request(f"{self.node_a}/context/status?include_text=false")
        if isinstance(a_status, dict):
            k_info = a_status.get("keys", {}).get(key, {})
            eviction_allowed = k_info.get("eviction_allowed", False)
            print(f"\n  {BOLD}Dirty-Bit Guard Check:{RESET}")
            print(f"    • Residence:          {CYAN}{k_info.get('residence')}{RESET}")
            print(f"    • Consistency:        {YELLOW}{k_info.get('consistency_status')}{RESET}")
            print(f"    • Eviction Allowed:   {BOLD}{RED if not eviction_allowed else GREEN}{eviction_allowed}{RESET}")
            if not eviction_allowed:
                success("OS Dirty-Bit Guarantee holds: Eviction is strictly FORBIDDEN until Node B confirms replica!")

        self.pause()

    def step_3_local_read(self) -> None:
        step_banner(
            3,
            "Local Availability During Outage",
            "Node A serves the durable primary value even when Node B is unreachable",
        )

        key = "demo/incident_report"
        info(f"Reading '{BOLD}{key}{RESET}' from Node A while Node B remains down...")
        status, res = self.request(f"{self.node_a}/key/{urllib.parse.quote(key, safe='')}")

        if status == 200 and isinstance(res, dict):
            success(f"Retrieved full value successfully from Node A ({res.get('source')}):")
            lines = res.get("value", "").split("\n")
            for line in lines[:3]:
                print(f"    {DIM}|{RESET} {line}")
            if len(lines) > 3:
                print(f"    {DIM}| ... ({len(lines) - 3} more lines){RESET}")
        else:
            err(f"Failed to read local value: {status} - {res}")

        self.pause()

    def step_4_background_recovery(self) -> None:
        step_banner(
            4,
            "Recovery & Background Retry Convergence",
            "Restoring Node B; background retry loop automatically converges replicas (Eventual Consistency)",
        )

        info("Restoring Node B back to Online...")
        self.set_fault("none")
        success("Node B is back ONLINE")

        key = "demo/incident_report"
        info(f"Waiting for Node A background retry worker to replicate '{key}'...")

        converged = False
        for attempt in range(1, 15):
            time.sleep(0.5)
            _, status_res = self.request(f"{self.node_a}/replication/key/{urllib.parse.quote(key, safe='')}")
            if isinstance(status_res, dict) and status_res.get("comparison_status") == "in_sync":
                converged = True
                break

        if converged:
            success(f"Replication converged automatically in background!")
            _, rep_status = self.request(f"{self.node_a}/replication/status")
            if isinstance(rep_status, dict):
                print(f"\n  {BOLD}Replication Store Comparison:{RESET}")
                print(f"    • Status:           {BOLD}{GREEN}{rep_status.get('comparison_status')}{RESET}")
                print(f"    • Consistency:      {GREEN}{rep_status.get('consistency')}{RESET}")
                print(f"    • Succeeded Copies: {rep_status.get('replication', {}).get('succeeded')}")
                print(f"    • Pending Deletes:  {rep_status.get('pending_deletes')}")
        else:
            warn("Replication still settling. Inspect with /replication/status.")

        self.pause()

    def step_5_semantic_paging(self) -> None:
        step_banner(
            5,
            "Semantic Context Paging & On-Demand Hydration",
            "Fill context past budget -> cold synced blocks swap with stubs -> Page Fault restores exact text",
        )

        # Query budget
        _, a_status = self.request(f"{self.node_a}/context/status?include_text=false")
        max_tokens = a_status.get("max_window_tokens", 2000) if isinstance(a_status, dict) else 2000
        info(f"Node A Context Window Token Budget: {BOLD}{max_tokens} tokens{RESET}")

        target_key = "demo/large_schema_dump"
        self.demo_keys.append(target_key)
        large_text = (
            "Postgres Production Schema Dump:\n"
            "TABLE users (id UUID PRIMARY KEY, email TEXT, created_at TIMESTAMPTZ);\n"
            "TABLE orders (id UUID PRIMARY KEY, user_id UUID, total_cents INT, status TEXT);\n"
            "TABLE payments (id UUID PRIMARY KEY, order_id UUID, provider TEXT, txn_id TEXT);\n"
            "TABLE audit_logs (id BIGSERIAL PRIMARY KEY, event TEXT, payload JSONB);\n"
            + ("-- additional table schemas and index metadata\n" * 40)
        )

        info(f"Ingesting large observation '{BOLD}{target_key}{RESET}' (~300 tokens)...")
        self.request(
            f"{self.node_a}/key",
            method="PUT",
            payload={"key": target_key, "value": large_text, "priority": -10},
        )

        # Ingest filler to trigger compaction
        info("Ingesting filler observations to trigger window compaction...")
        for i in range(1, 4):
            f_key = f"demo/filler_{i}"
            self.demo_keys.append(f_key)
            self.request(
                f"{self.node_a}/key",
                method="PUT",
                payload={
                    "key": f_key,
                    "value": f"Filler observation {i}: " + ("System health trace metrics OK. " * 80),
                },
            )

        # Check status after compaction
        _, post_status = self.request(f"{self.node_a}/context/status?include_text=true")
        if isinstance(post_status, dict):
            entry = post_status.get("keys", {}).get(target_key, {})
            tokens_saved = post_status.get("tokens_saved_by_replication", 0)
            residence = entry.get("residence")

            print(f"\n  {BOLD}Context State for {target_key}:{RESET}")
            print(f"    • Residence:          {MAGENTA}{residence}{RESET}")
            print(f"    • Active Prompt View: {CYAN}{entry.get('prompt_view')}{RESET}")
            print(f"    • Tokens Saved:       {BOLD}{GREEN}{tokens_saved} tokens{RESET}")

            if residence == "PAGED_TO_NODE_B":
                success("Observation safely paged to Node B stub! Full prompt remains under budget.")

        # Page Fault / Hydration
        info(f"\nAgent now requests full details: calling {CYAN}GET /context/{target_key}?hydrate=true{RESET}...")
        status, hydrated = self.request(
            f"{self.node_a}/context/{urllib.parse.quote(target_key, safe='')}?hydrate=true"
        )

        if status == 200 and isinstance(hydrated, dict):
            success(f"Page Fault Handled: Source = {BOLD}{GREEN}{hydrated.get('source')}{RESET}!")
            success(f"Original text fully recovered: {len(hydrated.get('full_text', ''))} characters losslessly restored.")
        else:
            err(f"Hydration failed: {status} - {hydrated}")


def main() -> None:
    parser = argparse.ArgumentParser(description="PagerKV Interactive CLI Demo")
    parser.add_argument("--node-a", default="http://127.0.0.1:8001", help="Node A base URL")
    parser.add_argument("--node-b", default="http://127.0.0.1:8002", help="Node B base URL")
    parser.add_argument("--auto", "-y", action="store_true", help="Run without pausing for user input")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between steps in auto mode")
    args = parser.parse_args()

    demo = PagerKVDemo(node_a=args.node_a, node_b=args.node_b, auto=args.auto, delay=args.delay)
    demo.run()


if __name__ == "__main__":
    main()
