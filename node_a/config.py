"""Node A settings, read from environment variables with safe defaults."""

from __future__ import annotations

import os


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


NODE_B_URL: str = os.environ.get("NODE_B_URL", "http://127.0.0.1:8002").rstrip("/")
MAX_WINDOW_TOKENS: int = _env_int("MAX_WINDOW_TOKENS", 2000)
INDEX_BUDGET_TOKENS: int = _env_int("INDEX_BUDGET_TOKENS", 150)
REPLICATION_TIMEOUT_S: float = _env_float("REPLICATION_TIMEOUT_S", 2.0)
RETRY_INTERVAL_S: float = _env_float("RETRY_INTERVAL_S", 3.0)
NODE_A_STATE_FILE: str = os.environ.get("NODE_A_STATE_FILE", "node_a_state.json")
# Move dirty blocks out of the prompt (to the local state file) while Node B is
# unreachable, instead of letting the window go over budget.
SPILL_DIRTY: bool = _env_bool("SPILL_DIRTY", True)
