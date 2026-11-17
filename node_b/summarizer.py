"""Deterministic, offline stub summaries for archived context blocks.

The default summarizer is extractive and needs no LLM.  It is intentionally
lossy: the full text always lives in the Node B archive, and hydration is what
makes the overall system lossless.

The optional ``STUB_LLM=1`` hook is deliberately not wired to a network
provider here.  PagerKV must run completely offline and its tests must not
depend on an API key, so the deterministic path is always used.
"""

from __future__ import annotations

import re

from common.tokens import count_tokens

_SENTENCE_END = re.compile(r"[.!?](?=\s|$)")


def make_stub_summary(text: str, max_tokens: int = 25) -> str:
    """Return a short, deterministic summary of ``text``.

    Collapse whitespace, take the first sentence, and if that sentence is over
    ``max_tokens`` cut it word by word until it fits, then append an ellipsis.
    """
    collapsed = " ".join(text.split())
    if not collapsed:
        return "(empty)"

    match = _SENTENCE_END.search(collapsed)
    first_sentence = collapsed[: match.end()] if match else collapsed

    if count_tokens(first_sentence) <= max_tokens:
        return first_sentence

    words = first_sentence.split()
    while words and count_tokens(" ".join(words) + "…") > max_tokens:
        words.pop()
    if not words:
        return "…"
    return " ".join(words) + "…"


def build_stub(
    key: str,
    version: int,
    full_text: str,
    full_tokens: int,
    max_tokens: int = 25,
) -> str:
    """Build the exact stub string stored in Node B and shown by Node A."""
    summary = make_stub_summary(full_text, max_tokens=max_tokens)
    return (
        f"[STUB v{version} | GET /context/{key}?hydrate=true "
        f"for full {full_tokens} tokens]: {summary}"
    )
