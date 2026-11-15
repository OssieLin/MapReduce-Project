#!/usr/bin/env python3
"""Plot the backup-task experiment (host only; needs matplotlib).

usage: python3 eval/plot_backup.py [eval/results/backup.csv] [shared/jobs/<id>/events.jsonl ...]

Writes eval/results/backup_runtime.png (mean runtime per config, +/- 1 std) and,
for each events file, eval/results/gantt-<job>.png (attempts per worker over time).
"""
import csv
import json
import os
import statistics
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt                      # noqa: E402
from matplotlib.patches import Patch                 # noqa: E402

BLUE, ORANGE = "#2a78d6", "#eb6834"                  # -B off / normal, -B on / backup
GRAY, INK, MUTED, SURFACE = "#9a9994", "#0b0b0b", "#52514e", "#fcfcfb"
OUT_DIR = "eval/results"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": MUTED, "axes.labelcolor": INK, "text.color": INK,
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.spines.top": False,
    "axes.spines.right": False, "font.size": 10,
})


def runtime_chart(path):
    runs = defaultdict(list)
    with open(path) as f:
        for row in csv.DictReader(f):
            runs[row["config"]].append(float(row["seconds"]))

    groups = [("straggler-no", "No straggler"), ("straggler-yes", "Straggler (worker4 at 1/4 CPU)")]
    series = [("backup-off", "-B off", BLUE), ("backup-on", "-B on", ORANGE)]
    fig, ax = plt.subplots(figsize=(6.5, 4))
    width = 0.36
    for i, (key, label, color) in enumerate(series):
        xs, means, errs = [], [], []
        for g, (gkey, _) in enumerate(groups):
            vals = runs.get(f"{gkey}+{key}", [])
            if not vals:
                continue
            xs.append(g + (i - 0.5) * (width + 0.02))
            means.append(statistics.mean(vals))
            errs.append(statistics.stdev(vals) if len(vals) > 1 else 0)
        bars = ax.bar(xs, means, width, yerr=errs, color=color, label=label,
                      error_kw={"ecolor": MUTED, "elinewidth": 1, "capsize": 3})
        ax.bar_label(bars, labels=[f"{m:.1f}s" for m in means], padding=3,
                     color=INK, fontsize=9)
    ax.set_xticks(range(len(groups)), [g[1] for g in groups])
    ax.set_ylabel("Job runtime (s), mean ± 1 std")
    ax.set_title("Backup tasks: runtime by config", loc="left")
    ax.yaxis.grid(True, color="#e4e3df", linewidth=0.8)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, loc="upper left")
    fig.tight_layout()
    out = os.path.join(OUT_DIR, "backup_runtime.png")
    fig.savefig(out, dpi=150)
    print("wrote", out)


def gantt_chart(path):
    attempts = {}                                    # last event per attempt wins
    for line in open(path):
        e = json.loads(line)
        attempts[e["attempt"]] = e
    t0 = min(e["start"] for e in attempts.values())
    workers = sorted({e["worker"] for e in attempts.values()})
    t_end = max((e["end"] or e["start"]) for e in attempts.values())

    fig, ax = plt.subplots(figsize=(9, 0.6 * len(workers) + 1.6))
    for e in attempts.values():
        y = workers.index(e["worker"])
        start, end = e["start"] - t0, (e["end"] or t_end) - t0
        wasted = e["status"] in ("CANCELLED", "LOST", "FAILED")
        color = ORANGE if e["backup"] else BLUE
        ax.barh(y, end - start, left=start, height=0.6, color=SURFACE if wasted else color,
                edgecolor=color, linewidth=1.2, hatch="///" if wasted else None)
        if end - start > 0.04 * (t_end - t0):
            ax.text(start + (end - start) / 2, y, e["attempt"], ha="center", va="center",
                    fontsize=7, color=INK if wasted else "white")
    ax.set_yticks(range(len(workers)), workers)
    ax.invert_yaxis()
    ax.set_xlabel("Seconds since first attempt")
    ax.set_title(f"Attempts per worker, job {next(iter(attempts.values()))['job']}", loc="left")
    ax.xaxis.grid(True, color="#e4e3df", linewidth=0.8)
    ax.set_axisbelow(True)
    ax.legend(handles=[Patch(color=BLUE, label="normal attempt"),
                       Patch(color=ORANGE, label="backup attempt"),
                       Patch(facecolor=SURFACE, edgecolor=GRAY, hatch="///",
                             label="cancelled / lost / failed")],
              frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=3)
    fig.tight_layout()
    out = os.path.join(OUT_DIR, f"gantt-{next(iter(attempts.values()))['job']}.png")
    fig.savefig(out, dpi=150)
    print("wrote", out)


if __name__ == "__main__":
    args = sys.argv[1:] or [os.path.join(OUT_DIR, "backup.csv")]
    os.makedirs(OUT_DIR, exist_ok=True)
    for a in args:
        gantt_chart(a) if a.endswith(".jsonl") else runtime_chart(a)
