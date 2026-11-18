"""Durable full-value storage shared by the two independent nodes."""

from __future__ import annotations

import sqlite3
from pathlib import Path


class Store:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS records (
                key TEXT PRIMARY KEY, value TEXT NOT NULL, version INTEGER NOT NULL,
                deleted INTEGER NOT NULL DEFAULT 0, acked_version INTEGER NOT NULL DEFAULT 0,
                error TEXT
            );
            CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
        """)
        self.db.commit()

    def get(self, key: str) -> dict | None:
        row = self.db.execute("SELECT * FROM records WHERE key=?", (key,)).fetchone()
        return dict(row) if row else None

    def rows(self) -> dict[str, dict]:
        return {
            row["key"]: dict(row) for row in self.db.execute("SELECT * FROM records ORDER BY key")
        }

    def save(self, key: str, value: str, version: int, deleted: bool = False) -> dict:
        with self.db:
            self.db.execute(
                """INSERT INTO records(key,value,version,deleted) VALUES(?,?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value,version=excluded.version,
                deleted=excluded.deleted,error=NULL""",
                (key, value, version, int(deleted)),
            )
        return self.get(key) or {}

    def result(self, key: str, version: int, error: str | None) -> None:
        with self.db:
            if error is None:
                self.db.execute(
                    "UPDATE records SET acked_version=?,error=NULL WHERE key=? AND version=?",
                    (version, key, version),
                )
            else:
                self.db.execute(
                    "UPDATE records SET error=? WHERE key=? AND version=?", (error, key, version)
                )

    def increment(self, name: str) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO counters VALUES(?,1) ON CONFLICT(name) DO UPDATE SET value=value+1",
                (name,),
            )

    def counters(self) -> dict[str, int]:
        counts = {"attempts": 0, "succeeded": 0, "failed": 0}
        counts.update({r["name"]: r["value"] for r in self.db.execute("SELECT * FROM counters")})
        return counts

    def close(self) -> None:
        self.db.close()
