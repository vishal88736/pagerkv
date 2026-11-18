"""Input limits shared by Node A and Node B.

Keys end up in prompt headers, so control characters (especially newlines) are
rejected: a key must never be able to inject extra prompt structure.  Text is
bounded so one request cannot make the state file or SQLite archive unbounded.
"""

from __future__ import annotations

MAX_KEY_CHARS = 512
MAX_TEXT_CHARS = 1_000_000


def validate_key(key: str) -> None:
    """Raise ``ValueError`` when ``key`` is not a safe key name."""
    if not key:
        raise ValueError("key must not be empty")
    if len(key) > MAX_KEY_CHARS:
        raise ValueError(f"key must be at most {MAX_KEY_CHARS} characters")
    if any(ord(char) < 32 or ord(char) == 127 for char in key):
        raise ValueError("key must not contain control characters")
