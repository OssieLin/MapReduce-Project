#!/usr/bin/env python3
"""Evaluation experiments (design.md section 12) and feature experiments, with plots.

Runs from the host against the Docker Compose cluster (it starts, stops, kills
and pauses containers itself). Each module writes <results>/<name>.csv (one
row per job run) and <results>/<name>.png. Every run's output is checked
against the module's reference run.

usage: python3 eval/experiments.py <modules...> | --all
           [--trials 3] [--rows 3000000] [--sweep-rows 1000000]
           [--results eval/results] [--plot-only]

Env: BOSS_URL (default http://localhost:5050), HEARTBEAT_TIMEOUT (boss's, default 10).
Plots need matplotlib on the host: pip install -r eval/requirements.txt
"""
import argparse
import csv
import json
import math
import os
import random
import shutil
import statistics
import subprocess
import sys
import textwrap
import threading
import time
import uuid

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED = os.path.abspath(os.environ.get("SHARED_DIR", os.path.join(ROOT, "shared")))
BOSS_URL = os.environ.get("BOSS_URL", "http://localhost:5050")
HEARTBEAT_TIMEOUT = float(os.environ.get("HEARTBEAT_TIMEOUT", 10))
WORKERS = ["worker1", "worker2", "worker3", "worker4"]
BASE = ["docker-compose.yml"]
STRAGGLER = BASE + ["docker-compose.straggler.yml"]
AVG_CITY = "examples/avg_temp.py"
AVG_STATION = "examples/avg_temp_by_station.py"
M, R = 16, 4                                   # default num_maps / num_reduces
GRANULARITY = [1, 2, 4, 8, 16, 32]
SWEEP_KEYS = [10, 1_000, 100_000, 1_000_000]
FAILURE_SCENARIOS = ["none", "kill-map", "kill-reduce", "pause", "restart"]


def log(msg):
    print(msg, flush=True)


# ------------------------------------------------------------------ cluster
def compose(*args, files=BASE):
    cmd = ["docker", "compose"] + [x for f in files for x in ("-f", f)] + list(args)
    r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"{' '.join(cmd)} failed:\n{r.stderr}")
    return r.stdout


def wait_cluster(active=WORKERS, timeout=120):
    """Wait until the boss answers and every worker in `active` is alive and idle."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            ws = {w["worker_id"]: w for w in
                  requests.get(f"{BOSS_URL}/workers", timeout=5).json()["workers"]}
            if all(n in ws and ws[n]["status"] == "alive" and ws[n]["last_seen_age"] < 3
                   and ws[n]["task"] is None for n in active):
                return
        except (requests.RequestException, ValueError):
            pass
        time.sleep(0.5)
    raise RuntimeError(f"workers {active} not ready at {BOSS_URL} after {timeout}s")


def start_cluster(inp, files=BASE):
    """(Re)start the cluster and run one untimed warm-up job: the first job on
    fresh containers is much slower (cold start)."""
    compose("up", "-d", files=files)
    wait_cluster()
    run_job(inp, AVG_CITY, tag="warmup")


# --------------------------------------------------------------------- data
# Real names for datasets with <= 100 keys; a few contain commas (quoted in the CSV).
CITIES = [
    "Madison", "Chicago", "Miami", "Portland, OR", "Seattle", "Boston", "Denver", "Austin",
    "Phoenix", "Atlanta", "Detroit", "Minneapolis", "Nashville", "New Orleans", "Las Vegas",
    "San Diego", "San Francisco", "Los Angeles", "Sacramento", "Salt Lake City", "Albuquerque",
    "Tucson", "El Paso", "Dallas", "Houston", "San Antonio", "Oklahoma City", "Tulsa",
    "Kansas City, MO", "Kansas City, KS", "Omaha", "Des Moines", "Milwaukee", "St. Louis",
    "Indianapolis", "Columbus, OH", "Columbus, GA", "Cleveland", "Cincinnati", "Pittsburgh",
    "Philadelphia", "Baltimore", "Washington, DC", "Richmond", "Raleigh", "Charlotte",
    "Charleston", "Savannah", "Jacksonville", "Orlando", "Tampa", "Birmingham", "Memphis",
    "Louisville", "Lexington", "Buffalo", "Rochester", "Albany", "New York", "Newark",
    "Hartford", "Providence", "Portland, ME", "Burlington", "Anchorage", "Honolulu", "Boise",
    "Spokane", "Fargo", "Sioux Falls", "Billings", "Cheyenne", "Lincoln", "Wichita",
    "Little Rock", "Jackson", "Mobile", "Baton Rouge", "Shreveport", "Fort Worth", "Laredo",
    "Lubbock", "Amarillo", "Reno", "Fresno", "Oakland", "San Jose", "Eugene", "Tacoma",
    "Green Bay", "Duluth", "Ann Arbor", "Grand Rapids", "Toledo", "Akron", "Dayton",
    "Springfield, IL", "Springfield, MA", "Madison Heights", "Knoxville",
]
DAYS = [f"2026-{m:02d}-{d:02d}" for m in range(1, 13)
        for d in range(1, (31 if m in (1, 3, 5, 7, 8, 10, 12) else 30 if m != 2 else 28) + 1)]


def dataset(name, rows, keys, files=4, seed=42):
    """Generate (once) a CSV dataset under /input/eval/<name> with rows like
    `Madison,2026-03-14,4.2,ST00012345` or `"Portland, OR",2026-07-02,18.5,ST03311092`:
    `keys` distinct cities (real names up to 100, then City0000100...), each with
    its own typical temperature; dates within 2026; nearly unique station ids.
    Seeded, so the same arguments always give byte-identical files."""
    rel = f"input/eval/{name}"
    d = os.path.join(SHARED, rel)
    meta = {"rows": rows, "keys": keys, "files": files, "seed": seed, "format": 2}
    meta_path = os.path.join(d, "_meta.json")
    try:
        if json.load(open(meta_path)) == meta:
            return "/" + rel
    except (OSError, ValueError):
        pass
    log(f"generating {name}: {rows} rows, {keys} keys")
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    rnd = random.Random(seed)
    names = CITIES[:keys] if keys <= len(CITIES) else [f"City{i:07d}" for i in range(keys)]
    typical = [rnd.uniform(-10, 25) for _ in range(keys)]       # each city's usual temp
    for f in range(files):
        n = rows // files + (f < rows % files)
        with open(os.path.join(d, f"part{f}.csv"), "w", newline="") as out:
            w = csv.writer(out, lineterminator="\n")
            w.writerow(["city", "date", "temp", "station"])
            for _ in range(n):
                c = rnd.randrange(keys)
                w.writerow([names[c], DAYS[rnd.randrange(len(DAYS))],
                            f"{typical[c] + rnd.gauss(0, 6):.1f}",
                            f"ST{rnd.randrange(rows * 10):08d}"])
    with open(meta_path, "w") as f:
        json.dump(meta, f)
    return "/" + rel


def input_mb(inp):
    d = os.path.join(SHARED, inp.lstrip("/"))
    return sum(os.path.getsize(os.path.join(d, n)) for n in os.listdir(d) if n.endswith(".csv")) / 1e6


# --------------------------------------------------------------------- jobs
def read_output(out):
    d = os.path.join(SHARED, out.lstrip("/"))
    lines = []
    for n in os.listdir(d):
        if n.startswith("part-") and n.endswith(".jsonl"):
            with open(os.path.join(d, n)) as f:
                lines += f.read().splitlines()
    return sorted(lines)


def outputs_match(a, b, rel_tol=1e-9):
    """Same keys and values; floats may differ in the last digits (summation order)."""
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        (kx, vx), (ky, vy) = json.loads(x), json.loads(y)
        if kx != ky:
            return False
        if isinstance(vx, float) or isinstance(vy, float):
            if not math.isclose(vx, vy, rel_tol=rel_tol, abs_tol=1e-9):
                return False
        elif vx != vy:
            return False
    return True


def run_job(inp, job, maps=M, reduces=R, combiner=False, backup=False, during=None, tag="run"):
    """Submit, poll (calling during(status) every 0.2 s), return (final status, output lines).
    Output and intermediate files are deleted afterwards; events.jsonl is kept."""
    out = f"/output/eval/{tag}-{uuid.uuid4().hex[:6]}"
    r = requests.post(f"{BOSS_URL}/submit_job", json={
        "input": inp, "output": out, "job_source": open(os.path.join(ROOT, job)).read(),
        "num_maps": maps, "num_reduces": reduces,
        "use_combiner": combiner, "use_backup": backup}, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"submit failed: {r.text}")
    job_id = r.json()["job_id"]
    while True:
        s = requests.get(f"{BOSS_URL}/job/{job_id}", timeout=30).json()
        if s["status"] == "DONE":
            break
        if s["status"] == "FAILED":
            raise RuntimeError(f"job {job_id} failed: {s['error']}")
        if during:
            during(s)
        time.sleep(0.2)
    lines = read_output(out)
    shutil.rmtree(os.path.join(SHARED, out.lstrip("/")), ignore_errors=True)
    shutil.rmtree(os.path.join(SHARED, "jobs", job_id, "intermediate"), ignore_errors=True)
    return s, lines


def run_single_process(inp, job, tag):
    """Experiment A's baseline, in a one-off worker1 container (same 1-CPU limit)."""
    out = f"/output/eval/{tag}-{uuid.uuid4().hex[:6]}"
    stdout = compose("run", "--rm", "--no-deps",
                     "-v", f"{ROOT}/eval:/app/eval:ro", "-v", f"{ROOT}/examples:/app/examples:ro",
                     "worker1", "python", "eval/baseline.py", "/shared" + inp, job, "/shared" + out)
    seconds = json.loads(stdout.strip().splitlines()[-1])["seconds"]
    lines = read_output(out)
    shutil.rmtree(os.path.join(SHARED, out.lstrip("/")), ignore_errors=True)
    return seconds, lines


# ------------------------------------------------------------------ results
class Results:
    """Rows of one module, saved to <results>/<name>.csv after every run."""

    def __init__(self, name, args):
        self.name, self.path, self.rows, self.ref = name, csv_path(name, args), [], None

    def add(self, config, trial, s, mb, lines, **extra):
        if self.ref is None:
            self.ref = lines                    # the module's first run is the reference
        c = s.get("counters", {})
        row = {"config": config, "trial": trial, "seconds": s["elapsed"],
               "mb_per_s": round(mb / s["elapsed"], 3),
               "intermediate_bytes": c.get("intermediate_bytes", 0),
               "reexecutions": s.get("reexecutions", 0),
               "backups_launched": s.get("backups_launched", 0),
               "backups_won": s.get("backups_won", 0),
               "wasted_seconds": s.get("wasted_seconds", 0),
               "matches": outputs_match(self.ref, lines), "identical": self.ref == lines,
               "job_id": s.get("job_id", "")}
        row.update(extra)
        self.rows.append(row)
        log(f"  {self.name} {config} #{trial}: {row['seconds']:.2f}s"
            f"{'' if row['matches'] else '  OUTPUT MISMATCH'}")
        fields = list(dict.fromkeys(k for r in self.rows for k in r))
        with open(self.path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(self.rows)


def csv_path(name, args):
    return os.path.join(args.results, f"{name}.csv")


def load_rows(path):
    def conv(v):
        if v in ("True", "False"):
            return v == "True"
        for t in (int, float):
            try:
                return t(v)
            except ValueError:
                pass
        return v
    with open(path, newline="") as f:
        return [{k: conv(v) for k, v in r.items()} for r in csv.DictReader(f)]


# -------------------------------------------------------------- experiments
def big(args):
    return dataset("big", args.rows, 100)


def exp_baseline(args):
    """A: plain single-process Python vs. MapReduce on 4 workers."""
    inp = big(args)
    mb, res = input_mb(inp), Results("baseline", args)
    start_cluster(inp)
    for t in range(1, args.trials + 1):
        s, lines = run_job(inp, AVG_CITY, tag="A")
        res.add("mapreduce", t, s, mb, lines)
        secs, lines = run_single_process(inp, AVG_CITY, tag="A-single")
        res.add("single-process", t, {"elapsed": secs}, mb, lines)


def _combiner(args, name, job, tag):
    inp = big(args)
    mb, res = input_mb(inp), Results(name, args)
    start_cluster(inp)
    for t in range(1, args.trials + 1):
        for on in (False, True):
            s, lines = run_job(inp, job, combiner=on, tag=tag)
            res.add(f"combiner-{'on' if on else 'off'}", t, s, mb, lines)


def exp_combiner(args):
    """B: combiner off vs. on, repetitive keys (avg temp per city)."""
    _combiner(args, "combiner", AVG_CITY, "B")


def exp_combiner_unique(args):
    """B2: combiner off vs. on, nearly-unique keys (avg temp per station)."""
    _combiner(args, "combiner_unique", AVG_STATION, "B2")


def exp_scaling(args):
    """C: 1, 2, 3, 4 active workers."""
    inp = big(args)
    mb, res = input_mb(inp), Results("scaling", args)
    start_cluster(inp)
    try:
        for t in range(1, args.trials + 1):
            for n in range(1, len(WORKERS) + 1):
                if n < len(WORKERS):
                    compose("stop", *WORKERS[n:])
                compose("start", *WORKERS[:n])
                wait_cluster(WORKERS[:n])
                s, lines = run_job(inp, AVG_CITY, tag=f"C{n}")
                res.add(f"{n}-workers", t, s, mb, lines, workers=n)
    finally:
        compose("start", *WORKERS)
        wait_cluster()


def exp_fault(args):
    """D: normal run vs. a worker killed halfway through the map phase."""
    inp = big(args)
    mb, res = input_mb(inp), Results("fault", args)
    start_cluster(inp)
    for t in range(1, args.trials + 1):
        s, lines = run_job(inp, AVG_CITY, tag="D")
        res.add("normal", t, s, mb, lines)
        killed = []

        def during(st):
            if not killed and st["maps_done"] >= math.ceil(st["maps_total"] / 2):
                compose("kill", "worker2")
                killed.append(st["maps_done"])

        try:
            s, lines = run_job(inp, AVG_CITY, during=during, tag="D-kill")
        finally:
            compose("start", "worker2")
            wait_cluster()
        res.add("worker-killed", t, s, mb, lines, victim="worker2")


def exp_granularity(args):
    """E: num_maps 1, 2, 4, 8, 16, 32."""
    inp = big(args)
    mb, res = input_mb(inp), Results("granularity", args)
    start_cluster(inp)
    for t in range(1, args.trials + 1):
        for m in GRANULARITY:
            s, lines = run_job(inp, AVG_CITY, maps=m, tag=f"E{m}")
            res.add(f"maps={m}", t, s, mb, lines, num_maps=m)


def exp_straggler(args):
    """Backup tasks: {no straggler, straggler (worker4 at 1/4 CPU)} x {-B off, -B on}."""
    inp = big(args)
    mb, res = input_mb(inp), Results("straggler", args)
    try:
        for strag, files in (("no", BASE), ("yes", STRAGGLER)):
            start_cluster(inp, files)       # switching files recreates every container
            for t in range(1, args.trials + 1):
                for on in (False, True):
                    s, lines = run_job(inp, AVG_CITY, backup=on, tag="S")
                    res.add(f"straggler-{strag}+backup-{'on' if on else 'off'}", t, s, mb, lines,
                            straggler=strag == "yes", backup=on)
    finally:
        compose("up", "-d")
        wait_cluster()


def exp_combiner_sweep(args):
    """Combiner feature: off vs. on across key cardinalities (10 .. 1M distinct keys)."""
    res = Results("combiner_sweep", args)
    for i, keys in enumerate(SWEEP_KEYS):
        inp = dataset(f"keys{keys}", args.sweep_rows, keys)
        mb = input_mb(inp)
        if i == 0:
            start_cluster(inp)
        res.ref = None                      # each dataset has its own reference output
        for t in range(1, args.trials + 1):
            for on in (False, True):
                s, lines = run_job(inp, AVG_CITY, combiner=on, tag=f"K{keys}")
                res.add(f"keys={keys}+combiner-{'on' if on else 'off'}", t, s, mb, lines,
                        keys=keys, combiner=on, rows=args.sweep_rows)


def exp_failures(args):
    """Worker failures: none / kill in map / kill in reduce / pause > timeout / kill+restart."""
    inp = big(args)
    mb, res = input_mb(inp), Results("failures", args)
    start_cluster(inp)
    pause_secs = HEARTBEAT_TIMEOUT + 5
    for t in range(1, args.trials + 1):
        for sc in FAILURE_SCENARIOS:
            state = {}

            def during(st, sc=sc, state=state):
                if state:
                    return
                quarter = st["status"] == "MAP" and st["maps_done"] >= max(1, st["maps_total"] // 4)
                if sc == "kill-map" and quarter:
                    compose("kill", "worker2")
                    state["victim"] = "worker2"
                elif sc == "restart" and quarter:       # same container comes straight back
                    compose("kill", "worker2")
                    compose("start", "worker2")
                    state["victim"] = "worker2"
                elif sc == "pause" and quarter:         # wrongly presumed dead
                    compose("pause", "worker3")
                    state["victim"] = "worker3"
                    state["timer"] = threading.Timer(pause_secs, compose, ("unpause", "worker3"))
                    state["timer"].start()
                elif sc == "kill-reduce" and st["status"] == "REDUCE":
                    # a reducer well short of done, so its task surely needs a re-run
                    busy = [x["worker"] for x in st["tasks"] if x["task_id"].startswith("reduce")
                            and x["state"] == "RUNNING" and x["worker"] in WORKERS
                            and (x.get("progress") or 0) < 0.5]
                    if busy:
                        compose("kill", busy[0])
                        state["victim"] = busy[0]

            try:
                s, lines = run_job(inp, AVG_CITY, during=None if sc == "none" else during,
                                   tag=f"F-{sc}")
            finally:
                if "timer" in state:
                    state["timer"].join()
                elif "victim" in state:
                    compose("start", state["victim"])
                wait_cluster()
            res.add(sc, t, s, mb, lines, victim=state.get("victim", ""))


# -------------------------------------------------------------------- plots
BLUE, ORANGE = "#2a78d6", "#eb6834"              # validated categorical slots 1-2
INK, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def _plt():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "axes.edgecolor": MUTED, "axes.labelcolor": INK, "text.color": INK,
        "xtick.color": MUTED, "ytick.color": MUTED, "axes.spines.top": False,
        "axes.spines.right": False, "font.size": 10, "axes.titlelocation": "left",
        "axes.titlesize": 11, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
        "axes.axisbelow": True,
    })
    return plt


def summarize(rows, key="seconds"):
    """config -> (mean, sd, n), in first-seen order."""
    vals = {}
    for r in rows:
        vals.setdefault(r["config"], []).append(float(r[key]))
    return {c: (statistics.mean(v), statistics.stdev(v) if len(v) > 1 else 0.0, len(v))
            for c, v in vals.items()}


def _check_note(rows, key="matches", what="output matches reference"):
    ok = sum(bool(r.get(key)) for r in rows)
    return f"{what}: {ok}/{len(rows)} runs"


def _bars(ax, labels, stats, color=BLUE, fmt="{:.1f}s", x=None, width=0.6, label=None):
    x = list(range(len(labels))) if x is None else x
    means, sds = [stats[l][0] for l in labels], [stats[l][1] for l in labels]
    bars = ax.bar(x, means, width, yerr=sds, color=color, label=label,
                  error_kw={"ecolor": MUTED, "elinewidth": 1, "capsize": 3})
    if any(sds):    # dot on the whisker = the mean, so it reads apart from the +1 sd cap
        ax.plot(x, means, linestyle="none", marker="o", markersize=5, color=INK,
                markeredgecolor=SURFACE, markeredgewidth=1, zorder=3)
    ax.bar_label(bars, labels=[fmt.format(m) + (f"\n± {fmt.format(sd)}" if sd else "")
                               for m, sd in zip(means, sds)],
                 padding=3, fontsize=9, color=INK, linespacing=1.1)
    top = max(m + sd for m, sd in zip(means, sds)) or 1
    ax.set_ylim(0, max(ax.get_ylim()[1], top * (1.3 if any(sds) else 1.15)))   # room for labels
    ax.grid(axis="x", visible=False)
    return bars


LAYOUT_PROBLEMS = []     # (plot, problem) for text cut off or overlapping; checked by the tests


def _check_layout(fig, name):
    """Record any text that sticks out of the figure or overlaps other text."""
    from matplotlib.text import Text
    fig.canvas.draw()
    r = fig.canvas.get_renderer()
    w, h = fig.bbox.width, fig.bbox.height
    undrawn = set()     # tick labels outside the axis range exist but aren't drawn
    for ax in fig.axes:
        for axis in (ax.xaxis, ax.yaxis):
            lo, hi = sorted(axis.get_view_interval())
            eps = (hi - lo) * 1e-9
            for ticks, locs in ((axis.get_major_ticks(), axis.get_majorticklocs()),
                                (axis.get_minor_ticks(), axis.get_minorticklocs())):
                for tick, loc in zip(ticks, locs):
                    if not lo - eps <= loc <= hi + eps:
                        undrawn.update((id(tick.label1), id(tick.label2)))
    boxes = []
    for t in fig.findobj(Text):
        if not t.get_visible() or not t.get_text().strip() or id(t) in undrawn:
            continue
        b = t.get_window_extent(r)
        if b.x0 < -1 or b.y0 < -1 or b.x1 > w + 1 or b.y1 > h + 1:
            LAYOUT_PROBLEMS.append((name, f"cut off: {t.get_text()!r}"))
        boxes.append((t.get_text(), b))
    for i, (ta, a) in enumerate(boxes):
        for tb, b in boxes[i + 1:]:
            if a.overlaps(b) and a.intersection(a, b).width > 2 and a.intersection(a, b).height > 2:
                LAYOUT_PROBLEMS.append((name, f"overlap: {ta!r} / {tb!r}"))
    for plot, problem in LAYOUT_PROBLEMS:
        if plot == name:
            log(f"warning: {name}.png: {problem}")


def _write(plt, fig, args, name):
    _check_layout(fig, name)
    path = os.path.join(args.results, f"{name}.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    log(f"wrote {path}")


def _save(plt, fig, args, name, subtitle):
    text = textwrap.fill(subtitle, int(fig.get_figwidth() * 14))   # footnote fits the figure
    lines = text.count("\n") + 1
    fig.text(0.01, 0.01, text, fontsize=8, color=MUTED, va="bottom")
    top = 1 - 0.08 / fig.get_figheight() if fig._suptitle else 1     # room for a figure title
    fig.tight_layout(rect=(0, 0.01 + 0.035 * lines * 3.8 / fig.get_figheight(), 1, top))
    _write(plt, fig, args, name)


def _spread(rows):
    """Footnote part saying what the bars/dots and whiskers are."""
    n = max(int(r["trial"]) for r in rows)
    if n == 1:
        return "1 trial (no error bars)"
    return f"dot = mean of {n} trials, whiskers = ±1 standard deviation"


def plot_baseline(rows, args):
    plt = _plt()
    st, tp = summarize(rows), summarize(rows, "mb_per_s")
    labels = ["single-process", "mapreduce"]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(8, 3.6))
    _bars(a1, labels, st)
    a1.set_xticks(range(2), ["single process", "MapReduce (4 workers)"])
    a1.set_ylabel("Job runtime (s)")
    x = st["single-process"][0] / st["mapreduce"][0]
    fig.suptitle("One process vs. MapReduce on 4 workers: "
                 + (f"{x:.1f}x faster" if x >= 1 else f"{1 / x:.1f}x slower"),
                 x=0.01, ha="left", fontsize=12)
    a1.set_title("Runtime")
    _bars(a2, labels, tp, fmt="{:.1f}")
    a2.set_xticks(range(2), ["single process", "MapReduce (4 workers)"])
    a2.set_ylabel("Throughput (input MB/s)")
    a2.set_title("Throughput (input processed per second)")
    _save(plt, fig, args, "baseline", _spread(rows) + " · "
          + _check_note(rows))


def _plot_combiner(rows, args, name, title):
    plt = _plt()
    labels = ["combiner-off", "combiner-on"]
    st, ib = summarize(rows), summarize(rows, "intermediate_bytes")
    ib = {k: (m / 1e6, sd / 1e6, n) for k, (m, sd, n) in ib.items()}
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(8, 3.6))
    _bars(a1, labels, st)
    a1.set_ylabel("Job runtime (s)")
    fig.suptitle(title, x=0.01, ha="left", fontsize=12)
    a1.set_title("Runtime")
    _bars(a2, labels, ib, fmt="{:.1f} MB")
    a2.set_ylabel("Intermediate data written (MB)")
    a2.set_title("Data sent to reducers")
    for ax in (a1, a2):
        ax.set_xticks(range(2), ["off", "on (-C)"])
        ax.set_xlabel("Combiner")
    _save(plt, fig, args, name, _spread(rows) + " · "
          + _check_note(rows))


def plot_combiner(rows, args):
    _plot_combiner(rows, args, "combiner", "Combiner on vs. off: 100 city keys, repeated in every map task")


def plot_combiner_unique(rows, args):
    _plot_combiner(rows, args, "combiner_unique", "Combiner on vs. off: nearly unique station keys (control)")


def plot_scaling(rows, args):
    plt = _plt()
    st, tp = summarize(rows), summarize(rows, "mb_per_s")
    ns = sorted({int(r["workers"]) for r in rows})
    t = [st[f"{n}-workers"] for n in ns]
    x = [tp[f"{n}-workers"] for n in ns]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(8, 3.6))
    a1.errorbar(ns, [m for m, _, _ in t], yerr=[s for _, s, _ in t], color=BLUE, linewidth=2,
                marker="o", markersize=6, capsize=3, ecolor=MUTED, label="measured")
    a1.plot(ns, [t[0][0] / n for n in ns], "--", color=MUTED, linewidth=1.2, label="ideal (T1 / n)")
    a1.set_ylabel("Job runtime (s)")
    a1.set_title("Runtime as workers are added")
    a2.errorbar(ns, [m for m, _, _ in x], yerr=[s for _, s, _ in x], color=BLUE, linewidth=2,
                marker="o", markersize=6, capsize=3, ecolor=MUTED, label="measured")
    a2.plot(ns, [x[0][0] * n for n in ns], "--", color=MUTED, linewidth=1.2, label="ideal (n x X1)")
    a2.set_ylabel("Throughput (input MB/s)")
    a2.set_title("Throughput as workers are added")
    for ax in (a1, a2):
        ax.set_xticks(ns)
        ax.set_xlabel("Active workers (1 CPU each)")
        ax.set_ylim(bottom=0)
        ax.legend(frameon=False)
    _save(plt, fig, args, "scaling", _spread(rows) + " · "
          + _check_note(rows))


def plot_fault(rows, args):
    plt = _plt()
    st = summarize(rows)
    killed = [r for r in rows if r["config"] == "worker-killed"]
    fig, ax = plt.subplots(figsize=(5.5, 3.8))
    _bars(ax, ["normal", "worker-killed"], st)
    ax.set_xticks(range(2), ["normal", "worker2 killed\nhalfway (map phase)"])
    ax.set_ylabel("Job runtime (s)")
    extra = st["worker-killed"][0] - st["normal"][0]
    ax.set_title(f"Worker killed mid-job: +{extra:.1f}s to recover")
    reexec = statistics.mean(float(r["reexecutions"]) for r in killed)
    _save(plt, fig, args, "fault", _spread(rows) + " · "
          + _check_note(killed, "identical", "output identical to the normal run")
          + f" · {reexec:.1f} tasks re-executed per killed run")


def plot_granularity(rows, args):
    plt = _plt()
    st = summarize(rows)
    ms = sorted({int(r["num_maps"]) for r in rows})
    t = [st[f"maps={m}"] for m in ms]
    fig, ax = plt.subplots(figsize=(6, 3.8))
    ax.errorbar(ms, [m for m, _, _ in t], yerr=[s for _, s, _ in t], color=BLUE, linewidth=2,
                marker="o", markersize=6, capsize=3, ecolor=MUTED)
    best = min(range(len(ms)), key=lambda i: t[i][0])
    # below the lowest point (the line never goes there), aligned away from the
    # plot edges so it can't hit the axis labels
    ha = "left" if best == 0 else "right" if best == len(ms) - 1 else "center"
    ax.annotate(f"fastest: {t[best][0]:.1f}s", (ms[best], t[best][0] - t[best][1]),
                textcoords="offset points", xytext=(0, -10), ha=ha, va="top",
                fontsize=9, color=INK)
    ax.set_xscale("log", base=2)
    ax.set_xticks(ms, [str(m) for m in ms])
    ax.set_xlabel("num_maps (num_reduces = 4)")
    ax.set_ylabel("Job runtime (s)")
    ax.set_ylim(bottom=0)
    ax.set_title("Runtime vs. number of map tasks")
    _save(plt, fig, args, "granularity", _spread(rows) + " · "
          + _check_note(rows))


def plot_straggler(rows, args):
    plt = _plt()
    st = summarize(rows)
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    w = 0.38
    for i, (b, label, color) in enumerate((("off", "backup tasks off", BLUE), ("on", "backup tasks on (-B)", ORANGE))):
        cfgs = [f"straggler-{s}+backup-{b}" for s in ("no", "yes")]
        _bars(ax, cfgs, st, color=color, x=[g + (i - 0.5) * (w + 0.02) for g in range(2)],
              width=w, label=label)
    ax.set_xticks(range(2), ["4 equal workers", "worker4 slowed to 1/4 CPU"])
    ax.set_ylabel("Job runtime (s)")
    ax.set_title("Backup tasks with and without a slow worker")
    ax.legend(frameon=False, loc="upper left")
    on = [r for r in rows if r["config"] == "straggler-yes+backup-on"]
    note = (f"slow worker + backup tasks: {statistics.mean(float(r['backups_launched']) for r in on):.1f} "
            f"backups launched, {statistics.mean(float(r['backups_won']) for r in on):.1f} won, "
            f"{statistics.mean(float(r['wasted_seconds']) for r in on):.1f}s wasted per run")
    _save(plt, fig, args, "straggler", _spread(rows) + " · "
          + _check_note(rows) + " · " + note)
    if on:
        gantt(on[0]["job_id"], args, "straggler_gantt",
              "Slow worker4, backup tasks on, combiner off: attempts per worker")


def plot_combiner_sweep(rows, args):
    plt = _plt()
    keys = sorted({int(r["keys"]) for r in rows})
    st, ib = summarize(rows), summarize(rows, "intermediate_bytes")
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(9, 3.8))
    for on, label, color in ((False, "combiner off", BLUE), (True, "combiner on", ORANGE)):
        cfg = [f"keys={k}+combiner-{'on' if on else 'off'}" for k in keys]
        a1.errorbar(keys, [ib[c][0] / 1e6 for c in cfg], yerr=[ib[c][1] / 1e6 for c in cfg],
                    color=color, linewidth=2, marker="o", markersize=6, capsize=3,
                    ecolor=MUTED, label=label)
        a2.errorbar(keys, [st[c][0] for c in cfg], yerr=[st[c][1] for c in cfg], color=color,
                    linewidth=2, marker="o", markersize=6, capsize=3, ecolor=MUTED, label=label)
    a1.set_ylabel("Intermediate data written (MB)")
    fig.suptitle("Combiner on vs. off as keys become unique", x=0.01, ha="left", fontsize=12)
    a1.set_title("Data sent to reducers")
    a2.set_ylabel("Job runtime (s)")
    a2.set_title("Runtime")
    for ax in (a1, a2):
        ax.set_xscale("log")
        ax.set_xlabel("Distinct keys in the dataset")
        ax.set_ylim(bottom=0)
        ax.legend(frameon=False)
    _save(plt, fig, args, "combiner_sweep", f"{int(rows[0]['rows']):,} rows per dataset · "
          + _spread(rows) + " · " + _check_note(rows))


def plot_failures(rows, args):
    plt = _plt()
    st = summarize(rows)
    labels = [s for s in FAILURE_SCENARIOS if s in st]
    names = {"none": "no failure", "kill-map": "kill in\nmap phase", "kill-reduce": "kill in\nreduce phase",
             "pause": f"pause {HEARTBEAT_TIMEOUT + 5:.0f}s\n(> timeout)", "restart": "kill +\nrestart"}
    fig, ax = plt.subplots(figsize=(7.5, 3.9))
    bars = _bars(ax, labels, st)
    for b, sc in zip(bars, labels):
        rx = statistics.mean(float(r["reexecutions"]) for r in rows if r["config"] == sc)
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() / 2, f"{rx:.1f}\nre-run",
                ha="center", va="center", fontsize=8, color="white")
    ax.set_xticks(range(len(labels)), [names[s] for s in labels])
    ax.set_ylabel("Job runtime (s)")
    ax.set_title("Runtime under different worker failures")
    _save(plt, fig, args, "failures", _spread(rows) + " · "
          + _check_note(rows, "identical", "output identical to the no-failure worker run")
          + f" · HEARTBEAT_TIMEOUT {HEARTBEAT_TIMEOUT:.0f}s")
    km = [r for r in rows if r["config"] == "kill-map"]
    if km:
        gantt(km[0]["job_id"], args, "failures_gantt",
              "Worker killed during the map phase: attempts per worker")


def gantt(job_id, args, name, title):
    """Attempts per worker over time, from shared/jobs/<id>/events.jsonl."""
    path = os.path.join(SHARED, "jobs", str(job_id), "events.jsonl")
    if not os.path.exists(path):
        log(f"skip {name}: {path} not found")
        return
    plt = _plt()
    from matplotlib.patches import Patch
    attempts = {}
    for line in open(path):
        e = json.loads(line)
        attempts[e["attempt"]] = e                   # last event per attempt wins
    t0 = min(e["start"] for e in attempts.values())
    t_end = max(e["end"] or e["start"] for e in attempts.values())
    workers = sorted({e["worker"] for e in attempts.values()})
    fig, ax = plt.subplots(figsize=(9, 0.55 * len(workers) + 1.8))
    for e in attempts.values():
        y = workers.index(e["worker"])
        start, end = e["start"] - t0, (e["end"] or t_end) - t0
        wasted = e["status"] in ("CANCELLED", "LOST", "FAILED")
        color = ORANGE if e["backup"] else BLUE
        ax.barh(y, end - start, left=start, height=0.62, color=SURFACE if wasted else color,
                edgecolor=color if wasted else SURFACE, linewidth=1.2 if wasted else 2,
                hatch="///" if wasted else None)
        if end - start > 0.09 * (t_end - t0):           # only where the label fits
            ax.text(start + (end - start) / 2, y, e["attempt"], ha="center", va="center",
                    fontsize=7, color=INK if wasted else "white")
    ax.set_yticks(range(len(workers)), workers)
    ax.invert_yaxis()
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Seconds since first attempt")
    ax.set_title(title)
    ax.legend(handles=[Patch(color=BLUE, label="attempt"), Patch(color=ORANGE, label="backup attempt"),
                       Patch(facecolor=SURFACE, edgecolor=MUTED, hatch="///",
                             label="failed / cancelled / lost")],
              frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.25), ncol=3)
    fig.tight_layout()
    _write(plt, fig, args, name)


# --------------------------------------------------------------------- main
MODULES = [   # (name, flag, run, plot)
    ("baseline", "--baseline", exp_baseline, plot_baseline),
    ("combiner", "--combiner", exp_combiner, plot_combiner),
    ("combiner_unique", "--combiner-unique", exp_combiner_unique, plot_combiner_unique),
    ("scaling", "--scaling", exp_scaling, plot_scaling),
    ("fault", "--fault", exp_fault, plot_fault),
    ("granularity", "--granularity", exp_granularity, plot_granularity),
    ("straggler", "--straggler", exp_straggler, plot_straggler),
    ("combiner_sweep", "--combiner-sweep", exp_combiner_sweep, plot_combiner_sweep),
    ("failures", "--failures", exp_failures, plot_failures),
]


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_argument_group("modules (pick any, or --all)")
    for name, flag, run, _ in MODULES:
        g.add_argument(flag, dest=name, action="store_true", help=run.__doc__)
    g.add_argument("--all", action="store_true", help="run all 9 modules")
    p.add_argument("--trials", type=int, default=3, help="runs per configuration (default 3)")
    p.add_argument("--rows", type=int, default=3_000_000, help="rows in the main dataset")
    p.add_argument("--sweep-rows", type=int, default=1_000_000,
                   help="rows per dataset in --combiner-sweep")
    p.add_argument("--results", default=os.path.join(ROOT, "eval", "results"),
                   help="directory for CSVs and plots (default eval/results)")
    p.add_argument("--plot-only", action="store_true", help="re-plot existing CSVs, run nothing")
    a = p.parse_args(argv)
    a.modules = [m for m in MODULES if a.all or getattr(a, m[0])]
    if not a.modules:
        p.print_help()
        sys.exit(2)
    return a


def main(argv=None):
    args = parse_args(argv)
    os.makedirs(args.results, exist_ok=True)
    if not args.plot_only:
        for attempt in range(30):            # the boss may still be starting
            try:
                requests.get(f"{BOSS_URL}/workers", timeout=5)
                break
            except requests.RequestException:
                time.sleep(1)
        else:
            sys.exit(f"error: no boss at {BOSS_URL}; start the cluster (docker compose up -d) "
                     "and set BOSS_URL if the port isn't 5050")
    failed = []
    for name, _, run, plot in args.modules:
        try:
            if not args.plot_only:
                log(f"== {name}")
                t0 = time.time()
                run(args)
                log(f"== {name} finished in {time.time() - t0:.0f}s")
            try:
                plot(load_rows(csv_path(name, args)), args)
            except ImportError:
                log("skip plots: matplotlib not installed (pip install -r eval/requirements.txt)")
        except Exception as e:                       # keep going with the other modules
            log(f"!! {name} failed: {e}")
            failed.append(name)
    if failed:
        sys.exit(f"failed modules: {', '.join(failed)}")


if __name__ == "__main__":
    main()
