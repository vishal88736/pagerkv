"""SQLite-backed lossless archive for Node B.

A fresh sqlite3 connection is opened for every operation.  Writes are
serialized with a process-wide lock.  When the SQLite build supports FTS5 a
virtual table mirrors the archive for full-text search; otherwise a LIKE-based
fallback is used.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time
from typing import Any

from common.tokens import count_tokens
from common.summarizer import build_stub

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

_FTS_AVAILABLE: bool | None = None


def db_path() -> str:
    return os.environ.get("NODE_B_DB", DEFAULT_DB)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def _detect_fts(conn: sqlite3.Connection) -> bool:
    global _FTS_AVAILABLE
    if _FTS_AVAILABLE is not None:
        return _FTS_AVAILABLE
    try:
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS _fts5_probe USING fts5(x)")
        conn.execute("DROP TABLE _fts5_probe")
        _FTS_AVAILABLE = True
    except sqlite3.OperationalError:
        _FTS_AVAILABLE = False
    return _FTS_AVAILABLE


def init_db() -> None:
    with _connect() as conn:
        conn.execute(_SCHEMA)
        if _detect_fts(conn):
            conn.execute(
                "CREATE VIRTUAL TABLE IF NOT EXISTS archive_fts USING fts5(key, full_text)"
            )
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

            if _FTS_AVAILABLE:
                conn.execute("DELETE FROM archive_fts WHERE key = ?", (key,))
                conn.execute(
                    "INSERT INTO archive_fts (key, full_text) VALUES (?, ?)",
                    (key, full_text),
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


def delete(key: str, max_version: int) -> dict[str, Any]:
    """Delete ``key`` only if its archived version is <= ``max_version``.

    The version bound makes deletes safe to retry: a delayed delete for v3
    can never remove a v4 written after the key was re-created.
    """
    _ensure_initialized()
    with _write_lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT version FROM archive WHERE key = ?", (key,)
            ).fetchone()
            if row is None:
                return {"key": key, "deleted": False, "version": None}
            if row["version"] > max_version:
                return {"key": key, "deleted": False, "version": row["version"]}
            conn.execute("DELETE FROM archive WHERE key = ?", (key,))
            if _FTS_AVAILABLE:
                conn.execute("DELETE FROM archive_fts WHERE key = ?", (key,))
            conn.commit()
            return {"key": key, "deleted": True, "version": row["version"]}
        finally:
            conn.close()


def stats() -> dict[str, int]:
    _ensure_initialized()
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(full_tokens), 0) AS t FROM archive"
        ).fetchone()
        return {"keys": int(row["n"]), "tokens": int(row["t"])}
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


def search(q: str, limit: int = 5) -> list[dict[str, Any]]:
    _ensure_initialized()
    q = (q or "").strip()
    if not q:
        return []
    limit = max(1, min(int(limit), 50))

    conn = _connect()
    try:
        if _FTS_AVAILABLE:
            try:
                rows = conn.execute(
                    """
                    SELECT a.key, a.version, a.stub, bm25(archive_fts) AS bm
                    FROM archive_fts f
                    JOIN archive a ON a.key = f.key
                    WHERE archive_fts MATCH ?
                    ORDER BY bm
                    LIMIT ?
                    """,
                    (q, limit),
                ).fetchall()
                results = []
                for row in rows:
                    bm = row["bm"]
                    score = -float(bm) if bm is not None else 0.0
                    results.append(
                        {
                            "key": row["key"],
                            "version": row["version"],
                            "stub": row["stub"],
                            "score": score,
                        }
                    )
                return results
            except sqlite3.OperationalError:
                pass  # fall through to LIKE

        words = [w for w in q.split() if w]
        rows = conn.execute(
            "SELECT key, version, stub, full_text FROM archive"
        ).fetchall()
        scored: list[tuple[int, sqlite3.Row]] = []
        for row in rows:
            text = (row["full_text"] or "").lower()
            if words:
                score = sum(1 for w in words if w.lower() in text)
            else:
                score = 1
            if score > 0:
                scored.append((score, row))
        scored.sort(key=lambda item: (-item[0], item[1]["key"]))
        results = []
        for score, row in scored[:limit]:
            results.append(
                {
                    "key": row["key"],
                    "version": row["version"],
                    "stub": row["stub"],
                    "score": score,
                }
            )
        return results
    finally:
        conn.close()