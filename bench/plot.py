"""Plot bench/results.csv -> bench/benchmark.png."""

from __future__ import annotations

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402

BENCH_DIR = Path(__file__).resolve().parent
RESULTS_CSV = BENCH_DIR / "results.csv"
OUTPUT_PNG = BENCH_DIR / "benchmark.png"

MAX_WINDOW_TOKENS = 2000
FAULT_START_TURN = 40
FAULT_END_TURN = 50


def load_results() -> dict[str, list]:
    turns: list[int] = []
    baseline: list[float | None] = []
    pagerkv: list[int] = []
    with open(RESULTS_CSV, newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            turns.append(int(row["turn"]))
            baseline.append(
                float(row["baseline_tokens"]) if row["baseline_tokens"] else None
            )
            pagerkv.append(int(row["pagerkv_prompt_tokens"]))
    return {"turns": turns, "baseline": baseline, "pagerkv": pagerkv}


def main() -> None:
    data = load_results()
    turns = data["turns"]
    baseline = data["baseline"]
    pagerkv = data["pagerkv"]

    crash_turns = [turn for turn, value in zip(turns, baseline, strict=True) if value is not None]
    crash_turn = max(crash_turns) + 1 if crash_turns else None

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(
        turns,
        baseline,
        color="#d62728",
        linewidth=2.2,
        marker="o",
        markersize=4,
        label="Baseline agent (no pager)",
    )
    ax.plot(
        turns,
        pagerkv,
        color="#1f77b4",
        linewidth=2.0,
        label="PagerKV prompt",
    )
    ax.axhline(
        MAX_WINDOW_TOKENS,
        color="black",
        linestyle="--",
        linewidth=1.2,
        label=f"{MAX_WINDOW_TOKENS}-token cap",
    )
    ax.axvspan(
        FAULT_START_TURN,
        FAULT_END_TURN,
        color="#ff7f0e",
        alpha=0.20,
        label=f"Node B down (turns {FAULT_START_TURN}-{FAULT_END_TURN})",
    )
    ax.text(
        (FAULT_START_TURN + FAULT_END_TURN) / 2,
        ax.get_ylim()[0] + 120,
        "Node B down",
        ha="center",
        va="bottom",
        fontsize=10,
        color="#8a4b00",
        fontweight="bold",
    )

    if crash_turn is not None:
        ax.annotate(
            f"Baseline crashes at turn {crash_turn}",
            xy=(crash_turn, MAX_WINDOW_TOKENS),
            xytext=(crash_turn + 9, MAX_WINDOW_TOKENS + 950),
            arrowprops={"arrowstyle": "->", "color": "#d62728"},
            color="#d62728",
            fontsize=10,
        )

    ax.set_title("PagerKV: bounded prompt vs. unbounded baseline")
    ax.set_xlabel("Turn")
    ax.set_ylabel("Prompt tokens")
    ax.set_xlim(1, max(turns))
    ax.grid(True, alpha=0.25)
    ax.legend(loc="upper left", framealpha=0.95)
    fig.tight_layout()
    fig.savefig(OUTPUT_PNG, dpi=150)
    plt.close(fig)
    print(f"Wrote {OUTPUT_PNG}")


if __name__ == "__main__":
    main()
