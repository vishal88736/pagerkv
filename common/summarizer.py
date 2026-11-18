"""Deterministic, offline stub summaries for archived context blocks.

The summarizer is extractive and needs no LLM.  A stub is:

* the first sentence (cut to fit), followed by
* ``key terms:`` the most distinctive identifiers in the text: tokens with an
  underscore, a digit mixed with letters, or an ALL-CAPS code, in order of
  first appearance.

Identifiers are what an agent usually needs to decide whether to hydrate a
block (table names, error codes, deploy tokens), and they are the part a
first-sentence summary drops.  The stub is still lossy: the full text lives on
Node B (or in Node A's local spill), and hydration is what makes the system
lossless.

The optional ``STUB_LLM=1`` hook is deliberately not wired to a network
provider.  PagerKV must run offline and its tests must not need an API key.
"""

from __future__ import annotations

import re

from common.tokens import count_tokens

_SENTENCE_END = re.compile(r"[.!?](?=\s|$)")
_WORD = re.compile(r"[A-Za-z0-9][A-Za-z0-9_\-]*[A-Za-z0-9]")
_SUMMARY_SHARE = 0.6  # share of the stub budget reserved for the first sentence


def _is_identifier(word: str) -> bool:
    has_alpha = any(c.isalpha() for c in word)
    has_digit = any(c.isdigit() for c in word)
    if not has_alpha:
        return False  # pure numbers are noise (row counts, timings)
    if "_" in word:
        return True
    if has_digit:
        return True
    return len(word) >= 3 and word.isupper()


def key_terms(text: str, exclude: str = "") -> list[str]:
    """Distinctive identifiers in ``text``, in order of first appearance."""
    seen = {w.lower() for w in _WORD.findall(exclude)}
    terms: list[str] = []
    for word in _WORD.findall(text):
        lowered = word.lower()
        if lowered in seen or not _is_identifier(word):
            continue
        seen.add(lowered)
        terms.append(word)
    return terms


def _fit_words(text: str, max_tokens: int) -> str:
    if count_tokens(text) <= max_tokens:
        return text
    words = text.split()
    while words and count_tokens(" ".join(words) + "…") > max_tokens:
        words.pop()
    return " ".join(words) + "…" if words else "…"


def make_stub_summary(text: str, max_tokens: int = 25) -> str:
    """Return a short, deterministic summary of ``text`` within ``max_tokens``."""
    collapsed = " ".join(text.split())
    if not collapsed:
        return "(empty)"

    match = _SENTENCE_END.search(collapsed)
    first_sentence = collapsed[: match.end()] if match else collapsed

    terms = key_terms(collapsed[len(first_sentence) :], exclude=first_sentence)
    if not terms:
        return _fit_words(first_sentence, max_tokens)

    summary = _fit_words(first_sentence, max(1, int(max_tokens * _SUMMARY_SHARE)))
    chosen: list[str] = []
    for term in terms:
        candidate = f"{summary} | key terms: {', '.join(chosen + [term])}"
        if count_tokens(candidate) > max_tokens:
            break
        chosen.append(term)
    if not chosen:
        return _fit_words(first_sentence, max_tokens)
    return f"{summary} | key terms: {', '.join(chosen)}"


def build_stub(
    key: str,
    version: int,
    full_text: str,
    full_tokens: int,
    max_tokens: int = 25,
    label: str = "STUB",
) -> str:
    """Build the exact stub string shown in the prompt for a non-resident key.

    ``label`` is ``STUB`` for blocks archived on Node B and ``SPILLED`` for
    dirty blocks Node A has moved to its own disk while Node B is unreachable.
    """
    summary = make_stub_summary(full_text, max_tokens=max_tokens)
    return (
        f"[{label} v{version} | GET /context/{key}?hydrate=true "
        f"for full {full_tokens} tokens]: {summary}"
    )
