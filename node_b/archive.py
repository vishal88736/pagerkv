"""SQLite-backed lossless archive for Node B.

A fresh sqlite3 connection is opened for every operation.  Writes are
serialized with a process-wide lock so concurrent FastAPI requests cannot race
each other into a ``database is locked`` error.  SQLite itself still provides
durability across restarts.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time
from typing import Any

from common.tokens import count_tokens
from node_b.summarizer import build_stub

DEFAULT_DB = "node_b_archive.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS archive (
  key TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  full_text TEXT NOT NULL,
  full_tokens INTEGER NOT NULL,
  stub TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  updated_at REAL NOT NULL
);
"""


def db_path() -> str:
    """Resolved on every call so NODE_B_DB can be set by the launcher."""
    return os.environ.get("NODE_B_DB", DEFAULT_DB)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.execute(_SCHEMA)
        conn.commit()


_init_lock = threading.Lock()
_initialized = False


def _ensure_initialized() -> None:
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if not _initialized:
            init_db()
            _initialized = True


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


_write_lock = threading.Lock()


def save(key: str, version: int, full_text: str) -> dict[str, Any]:
    """Archive one version of ``key``.

    A stale write (incoming version lower than the stored version) is rejected:
    the existing row is returned unchanged with ``stale_write=True``.
    """
    _ensure_initialized()

    full_tokens = count_tokens(full_text)
    sha256 = _sha256(full_text)
    stub = build_stub(key, version, full_text, full_tokens)
    updated_at = time.time()

    with _write_lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT * FROM archive WHERE key = ?", (key,)
            ).fetchone()

            if row is not None and row["version"] > version:
                result = _row_to_dict(row)
                result["stale_write"] = True
                return result

            conn.execute(
                """
                INSERT INTO archive
                    (key, version, full_text, full_tokens, stub, sha256, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    version     = excluded.version,
                    full_text   = excluded.full_text,
                    full_tokens = excluded.full_tokens,
                    stub        = excluded.stub,
                    sha256      = excluded.sha256,
                    updated_at  = excluded.updated_at
                """,
                (key, version, full_text, full_tokens, stub, sha256, updated_at),
            )
            conn.commit()

            return {
                "key": key,
                "version": version,
                "full_text": full_text,
                "full_tokens": full_tokens,
                "stub": stub,
                "sha256": sha256,
                "updated_at": updated_at,
                "stale_write": False,
            }
        finally:
            conn.close()


def get(key: str) -> dict[str, Any] | None:
    _ensure_initialized()
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT * FROM archive WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        return _row_to_dict(row)
    finally:
        conn.close()
