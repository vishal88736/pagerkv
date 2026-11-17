"""Single source of truth for token counting in PagerKV.

The encoder is loaded exactly once at module import time.  If tiktoken or the
``cl100k_base`` encoding cannot be loaded (offline install, missing Rust wheel,
...), we fall back to a deterministic approximation of roughly four characters
per token.
"""

from __future__ import annotations

from typing import Any

_ENCODING_NAME = "cl100k_base"
_ENCODER: Any

try:  # pragma: no cover - depends on the installation environment
    import tiktoken

    _ENCODER = tiktoken.get_encoding(_ENCODING_NAME)
except Exception:  # pragma: no cover - exercised only on broken installs
    _ENCODER = None


def count_tokens(text: str) -> int:
    """Return the token count of ``text``.

    Every token number in PagerKV comes from this function.
    """
    if not text:
        return 0
    if _ENCODER is not None:
        try:
            return len(_ENCODER.encode(text))
        except Exception:
            pass
    return max(1, len(text) // 4)
