"""Minimal Prometheus text-format rendering (no client library needed)."""

from __future__ import annotations

from collections.abc import Iterable


def render(metrics: Iterable[tuple[str, str, str, float | int | bool]]) -> str:
    """Render ``(name, type, help, value)`` tuples as Prometheus exposition text."""
    lines: list[str] = []
    for name, kind, help_text, value in metrics:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {kind}")
        lines.append(f"{name} {float(value):g}")
    return "\n".join(lines) + "\n"
