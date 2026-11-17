"""Unit tests for eval/experiments.py and eval/baseline.py (no Docker)."""
import csv
import importlib.util
import json
import os
import re

import pytest

from mapreduce import common

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load(name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(REPO, "eval", f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ex = _load("experiments")
baseline = _load("baseline")


@pytest.fixture
def shared(tmp_path, monkeypatch):
    monkeypatch.setattr(ex, "SHARED", str(tmp_path))
    return tmp_path


def args(tmp_path, *argv):
    return ex.parse_args([*argv, "--results", str(tmp_path / "results")])


# ------------------------------------------------------------------ data
def test_dataset_deterministic_and_cached(shared):
    inp = ex.dataset("d", 1000, 7, files=3)
    d = shared / inp.lstrip("/")
    files = sorted(p.name for p in d.glob("*.csv"))
    assert inp == "/input/eval/d" and files == ["part0.csv", "part1.csv", "part2.csv"]
    rows = [r for p in sorted(d.glob("*.csv")) for r in list(csv.reader(open(p)))[1:]]
    assert len(rows) == 1000
    assert len({r[0] for r in rows}) == 7                      # distinct cities = keys
    assert len({r[3] for r in rows}) > 950                     # stations nearly unique
    first = (d / "part0.csv").read_bytes()
    mtime = (d / "part0.csv").stat().st_mtime_ns
    assert ex.dataset("d", 1000, 7, files=3) == inp
    assert (d / "part0.csv").stat().st_mtime_ns == mtime       # cached, not regenerated
    ex.dataset("d", 1000, 3, files=3)                          # params changed: regenerate
    assert (d / "part0.csv").read_bytes() != first
    assert ex.input_mb(inp) > 0


def test_dataset_is_deterministic_and_readable(shared, tmp_path, monkeypatch):
    a = (shared / ex.dataset("d", 2000, 100).lstrip("/") / "part0.csv").read_bytes()
    other = tmp_path / "other"
    monkeypatch.setattr(ex, "SHARED", str(other))
    b = (other / ex.dataset("d", 2000, 100).lstrip("/") / "part0.csv").read_bytes()
    assert a == b                                              # same seed, same bytes
    text = a.decode()
    assert text.startswith("city,date,temp,station\n")
    assert '"Portland, OR",' in text                          # commas in names are quoted
    for city, date, temp, station in list(csv.reader(text.splitlines()))[1:]:
        assert city in ex.CITIES
        assert re.fullmatch(r"2026-\d\d-\d\d", date)
        assert re.fullmatch(r"-?\d+\.\d", temp)
        assert re.fullmatch(r"ST\d{8}", station)
    many = ex.dataset("many", 500, 10_000)                     # > 100 keys: synthetic names
    first = (other / many.lstrip("/") / "part0.csv").read_text().splitlines()[1]
    assert re.fullmatch(r"City\d{7}", first.split(",")[0])


def test_station_job_defines_combiner():
    common.check_job_source(open(os.path.join(REPO, ex.AVG_STATION)).read(), require_combiner=True)


# -------------------------------------------------------- output checks
def test_outputs_match():
    a = ['["A", 1.0]', '["B", 2]']
    assert ex.outputs_match(a, ['["A", 1.0000000000001]', '["B", 2]'])
    assert not ex.outputs_match(a, ['["A", 1.01]', '["B", 2]'])
    assert not ex.outputs_match(a, ['["A", 1.0]', '["C", 2]'])
    assert not ex.outputs_match(a, a[:1])
    assert not ex.outputs_match(a, ['["A", 1.0]', '["B", 3]'])


def test_read_output_sorts_parts(shared):
    d = shared / "output" / "o"
    d.mkdir(parents=True)
    (d / "part-1.jsonl").write_text('["B", 2]\n')
    (d / "part-0.jsonl").write_text('["C", 3]\n["A", 1]\n')
    (d / "_SUCCESS").write_text("")
    assert ex.read_output("/output/o") == ['["A", 1]', '["B", 2]', '["C", 3]']


# ------------------------------------------------------------ results
def test_results_csv_roundtrip(tmp_path):
    a = args(tmp_path, "--fault")
    os.makedirs(a.results)
    res = ex.Results("fault", a)
    st = {"elapsed": 2.0, "job_id": "j1", "reexecutions": 0,
          "counters": {"intermediate_bytes": 100}}
    res.add("normal", 1, st, 10.0, ['["A", 1.0]'])
    res.add("worker-killed", 1, dict(st, elapsed=4.0, reexecutions=1), 10.0,
            ['["A", 1.0000000001]'], victim="worker2")
    rows = ex.load_rows(ex.csv_path("fault", a))
    assert [r["config"] for r in rows] == ["normal", "worker-killed"]
    assert rows[1]["seconds"] == 4.0 and rows[1]["mb_per_s"] == 2.5
    assert rows[1]["reexecutions"] == 1 and rows[1]["victim"] == "worker2"
    assert rows[1]["matches"] is True and rows[1]["identical"] is False
    assert rows[0]["intermediate_bytes"] == 100


def test_summarize():
    rows = [{"config": "a", "seconds": 1}, {"config": "a", "seconds": 3}, {"config": "b", "seconds": 5}]
    assert ex.summarize(rows) == {"a": (2, pytest.approx(1.414, rel=1e-3), 2), "b": (5, 0.0, 1)}


# --------------------------------------------------------------- flags
def test_no_flags_prints_help_and_exits_2(tmp_path, capsys):
    with pytest.raises(SystemExit) as e:
        ex.parse_args([])
    assert e.value.code == 2 and "--combiner-sweep" in capsys.readouterr().out


def test_flags_select_modules(tmp_path):
    assert len(args(tmp_path, "--all").modules) == 9
    picked = args(tmp_path, "--scaling", "--combiner-unique", "--trials", "2").modules
    assert [m[0] for m in picked] == ["combiner_unique", "scaling"]
    assert args(tmp_path, "--failures").trials == 3


# ------------------------------------------------------------ baseline
def test_baseline_single_process(tmp_path):
    d = tmp_path / "in"
    d.mkdir()
    (d / "a.csv").write_text("city,date,temp\nX,1,1.0\nY,1,5.0\n")
    (d / "b.csv").write_text("city,date,temp\nX,2,3.0\n")
    secs = baseline.run(str(d), os.path.join(REPO, ex.AVG_CITY), str(tmp_path / "out"))
    assert secs >= 0
    assert (tmp_path / "out" / "part-0.jsonl").read_text() == '["X", 2.0]\n["Y", 5.0]\n'


# --------------------------------------------------------------- plots
def _rows(configs, trials=2, **extra):
    out = []
    for t in range(1, trials + 1):
        for i, c in enumerate(configs):
            r = {"config": c, "trial": t, "seconds": 10 + i + t, "mb_per_s": 5.0 + i,
                 "intermediate_bytes": 1e6 * (i + 1), "reexecutions": i % 2,
                 "backups_launched": 1, "backups_won": 1, "wasted_seconds": 2.0,
                 "matches": True, "identical": True, "job_id": "jg"}
            r.update({k: (v(c) if callable(v) else v) for k, v in extra.items()})
            out.append(r)
    return out


PLOT_CASES = {
    "baseline": _rows(["mapreduce", "single-process"]),
    "combiner": _rows(["combiner-off", "combiner-on"]),
    "combiner_unique": _rows(["combiner-off", "combiner-on"]),
    "scaling": _rows([f"{n}-workers" for n in range(1, 5)], workers=lambda c: int(c[0])),
    "fault": _rows(["normal", "worker-killed"]),
    "granularity": _rows([f"maps={m}" for m in ex.GRANULARITY]
                         + [f"reduces={r}" for r in ex.GRANULARITY_R]),
    "straggler": _rows([f"straggler-{s}+backup-{b}" for s in ("no", "yes") for b in ("off", "on")]),
    "combiner_sweep": _rows([f"keys={k}+combiner-{o}" for k in ex.SWEEP_KEYS for o in ("off", "on")],
                            keys=lambda c: int(c.split("=")[1].split("+")[0]), rows=1000),
    "failures": _rows(ex.FAILURE_SCENARIOS),
}


@pytest.mark.parametrize("name", list(PLOT_CASES))
def test_plots(name, tmp_path, shared):
    pytest.importorskip("matplotlib")
    events = shared / "jobs" / "jg"
    events.mkdir(parents=True)
    (events / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in [
        {"job": "jg", "task": "map-0", "attempt": "map-0.a0", "worker": "worker1", "backup": False,
         "start": 0, "end": 4, "status": "FAILED", "progress": 0.3},
        {"job": "jg", "task": "map-0", "attempt": "map-0.a1", "worker": "worker2", "backup": True,
         "start": 2, "end": 6, "status": "WON", "progress": 1.0},
        {"job": "jg", "task": "map-1", "attempt": "map-1.a0", "worker": "worker3", "backup": False,
         "start": 0, "end": None, "status": "RUNNING", "progress": 0.5},
        {"job": "jg", "task": "reduce-0", "attempt": "reduce-0.a0", "worker": "worker4",
         "backup": False, "start": 0.5, "end": 3, "status": "FAILED", "progress": 0.2},
        {"job": "jg", "task": "reduce-0", "attempt": "reduce-0.a1", "worker": "worker1",
         "backup": False, "start": 3.1, "end": 3.4, "status": "WON", "progress": 1.0},
    ]))
    a = args(tmp_path, "--all")
    os.makedirs(a.results)
    plot = {m[0]: m[3] for m in ex.MODULES}[name]
    ex.LAYOUT_PROBLEMS.clear()
    plot(PLOT_CASES[name], a)
    assert (tmp_path / "results" / f"{name}.png").stat().st_size > 10_000
    assert ex.LAYOUT_PROBLEMS == []          # no text cut off or overlapping
    if name in ("straggler", "failures"):
        assert (tmp_path / "results" / f"{name}_gantt.png").exists()
