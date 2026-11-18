"""Bound request size and reject control characters in key names."""

MAX_KEY_CHARS = 512
MAX_TEXT_CHARS = 1_000_000


def validate_key(key: str) -> None:
    if not key or len(key) > MAX_KEY_CHARS:
        raise ValueError(f"key must contain 1 to {MAX_KEY_CHARS} characters")
    if any(ord(char) < 32 or ord(char) == 127 for char in key):
        raise ValueError("key must not contain control characters")
