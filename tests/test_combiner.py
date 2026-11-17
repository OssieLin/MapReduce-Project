"""Combiner tests. Runs map/reduce tasks directly against a temp SHARED_DIR."""
import json
import math
import os
import random
import subprocess
import sys
import uuid

import pytest

from mapreduce import boss, common, storage, worker

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AVG_TEMP = os.path.join(REPO, "examples", "avg_temp.py")
NO_COMBINER = """
def map_fn(key, row):
    yield row["city"], 1

def reduce_fn(key, values):
    yield key, sum(values)
"""


@pytest.fixture
def shared(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "ROOT", str(tmp_path))
    storage.prepare_root()
    return tmp_path


def write_input(shared, rows):
    path = shared / "input" / "data.csv"
    lines = ["city,date,temp"] + [f'"{c}",2026-01-01,{t}' for c, t in rows]
    path.write_text("\n".join(lines) + "\n")
    return "input/data.csv"


def random_rows(n, seed):
    rnd = random.Random(seed)
    cities = [f"city{i}" for i in range(15)] + ["Portland, OR", "Zürich"]
    return [(rnd.choice(cities), round(rnd.uniform(-30, 40), 1)) for _ in range(n)]


def run_job(shared, rows, use_combiner, M=4, R=3, source=None):
    """Run every map then every reduce task in-process; return (results, stats)."""
    job_id = uuid.uuid4().hex[:8]
    source = source or open(AVG_TEMP).read()
    storage.write_atomic(f"jobs/{job_id}/job.py", source)
    splits = common.compute_splits(storage.list_files(write_input(shared, rows)), M)
    out = f"output/{job_id}"
    base = {"job_id": job_id, "num_maps": M, "num_reduces": R, "output": out,
            "use_combiner": use_combiner}
    map_stats = [worker.run_map(dict(base, type="map", index=m, split=s))
                 for m, s in enumerate(splits)]
    red_stats = [worker.run_reduce(dict(base, type="reduce", index=r)) for r in range(R)]
    results = []
    for r in range(R):
        with open(shared / out / f"part-{r}.jsonl") as f:
            results += [json.loads(line) for line in f]
    return job_id, sorted(results), map_stats, red_stats


def assert_same(a, b):
    assert [k for k, _ in a] == [k for k, _ in b]
    for (_, x), (_, y) in zip(a, b):
        assert math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-9)


def intermediate(shared, job_id):
    d = shared / "jobs" / job_id / "intermediate"
    return {p.name: p.read_bytes() for p in sorted(d.iterdir())}


# ------------------------------------------------------------ check_job_source
def test_check_job_source_require_combiner():
    common.check_job_source(NO_COMBINER)
    common.check_job_source(NO_COMBINER, require_combiner=False)
    with pytest.raises(ValueError, match="combine_fn"):
        common.check_job_source(NO_COMBINER, require_combiner=True)
    common.check_job_source(open(AVG_TEMP).read(), require_combiner=True)


# ----------------------------------------------------------------- equivalence
@pytest.mark.parametrize("seed", [0, 1, 2])
def test_combiner_equivalence(shared, seed):
    rows = random_rows(2000, seed)
    _, plain, _, _ = run_job(shared, rows, use_combiner=False)
    _, comb, _, _ = run_job(shared, rows, use_combiner=True)
    assert_same(plain, comb)

    # against a plain single-process computation
    sums = {}
    for c, t in rows:
        s, n = sums.get(c, (0.0, 0))
        sums[c] = (s + t, n + 1)
    assert_same(comb, sorted([c, s / n] for c, (s, n) in sums.items()))


def test_mid_buffer_combining(shared, monkeypatch):
    monkeypatch.setenv("COMBINE_BUFFER", "10")
    rows = random_rows(3000, 7)
    _, plain, _, _ = run_job(shared, rows, use_combiner=False)
    _, comb, mstats, _ = run_job(shared, rows, use_combiner=True)
    assert_same(plain, comb)
    for s in mstats:
        assert s["combine_output_records"] <= 17 * 3     # <= distinct keys


def test_mid_buffer_unique_keys(shared, monkeypatch):
    # combiner can't shrink anything: still correct, no per-row re-combining
    monkeypatch.setenv("COMBINE_BUFFER", "10")
    rows = [(f"c{i}", float(i)) for i in range(500)]
    _, plain, _, _ = run_job(shared, rows, use_combiner=False)
    _, comb, _, _ = run_job(shared, rows, use_combiner=True)
    assert_same(plain, comb)


# -------------------------------------------------------------------- counters
def test_counters(shared):
    rows = random_rows(2000, 3)
    pid, _, p_map, p_red = run_job(shared, rows, use_combiner=False)
    cid, _, c_map, c_red = run_job(shared, rows, use_combiner=True)
    total = lambda stats, k: sum(s.get(k, 0) for s in stats)

    for s in c_map:
        assert s["combine_input_records"] == s["map_output_records"]
        assert s["combine_output_records"] <= s["combine_input_records"]
    for s in p_map:
        assert s.get("combine_input_records", 0) == 0
        assert s.get("combine_output_records", 0) == 0

    assert total(p_map, "map_input_records") == total(c_map, "map_input_records") == 2000
    assert total(c_map, "combine_output_records") < total(c_map, "map_output_records")
    assert total(c_map, "intermediate_bytes") < total(p_map, "intermediate_bytes")
    for maps, reds in ((p_map, p_red), (c_map, c_red)):
        assert total(maps, "intermediate_bytes") == total(reds, "shuffle_bytes")
        assert total(reds, "reduce_input_groups") == total(reds, "reduce_output_records") == 17
    assert total(p_red, "reduce_input_records") == 2000
    assert total(c_red, "reduce_input_records") == total(c_map, "combine_output_records")


def test_combiner_off_intermediate_unchanged(shared):
    """With -C off, intermediate files match the pre-combiner writer exactly."""
    rows = random_rows(500, 4)
    job_id, _, _, _ = run_job(shared, rows, use_combiner=False, M=2, R=2)
    job = common.load_job(f"jobs/{job_id}/job.py")
    splits = common.compute_splits(storage.list_files("input/data.csv"), 2)
    expected = {}
    for m, split in enumerate(splits):
        buffers = [[] for _ in range(2)]
        for seg in split:
            for offset, row in common.iter_rows(seg):
                for k, v in job.map_fn(offset, row):
                    buffers[common.partition(k, 2)].append((k, v))
        for r in range(2):
            expected[f"map-{m}-part-{r}.jsonl"] = "".join(
                json.dumps([k, v]) + "\n" for k, v in buffers[r]).encode()
    assert intermediate(shared, job_id) == expected


def test_use_combiner_without_combine_fn_is_ignored_by_worker(shared):
    rows = random_rows(200, 5)
    _, a, _, _ = run_job(shared, rows, use_combiner=False, source=NO_COMBINER)
    _, b, stats, _ = run_job(shared, rows, use_combiner=True, source=NO_COMBINER)
    assert a == b
    assert all("combine_output_records" not in s for s in stats)


# ------------------------------------------------------------------------ boss
def test_boss_use_combiner_and_counters(shared):
    write_input(shared, random_rows(100, 6))
    client = boss.app.test_client()
    req = {"input": "/input", "output": "/output/x", "num_maps": 2, "num_reduces": 1}

    r = client.post("/submit_job", json=dict(req, job_source=NO_COMBINER, use_combiner=True))
    assert r.status_code == 400 and "combine_fn" in r.get_json()["error"]

    r = client.post("/submit_job", json=dict(req, job_source=open(AVG_TEMP).read(),
                                             use_combiner=True))
    job_id = r.get_json()["job_id"]
    task = client.post("/get_task", json={"worker_id": "w"}).get_json()["task"]
    assert task["use_combiner"] is True
    done = {"job_id": job_id, "task_id": task["task_id"], "success": True,
            "stats": {"map_output_records": 5, "intermediate_bytes": 40}}
    client.post("/task_done", json=done)
    client.post("/task_done", json=done)                 # duplicate: ignored
    s = client.get(f"/job/{job_id}").get_json()
    assert s["counters"] == {"map_output_records": 5, "intermediate_bytes": 40}

    r = client.post("/submit_job", json=dict(req, job_source=NO_COMBINER))
    assert r.status_code == 200                          # default: combiner off


# ------------------------------------------------------------------------- CLI
def _submit(*args):
    return subprocess.run([sys.executable, os.path.join(REPO, "submit"), *args],
                          capture_output=True, text=True,
                          env=dict(os.environ, BOSS_URL="http://127.0.0.1:9"))


def test_cli_no_flags_prints_usage():
    p = _submit()
    assert p.returncode == 2 and "usage:" in p.stdout and "-C" in p.stdout


def test_cli_combiner_requires_combine_fn(tmp_path):
    job = tmp_path / "job.py"
    job.write_text(NO_COMBINER)
    p = _submit("-i", "/in", "-o", "/out", "-j", str(job), "-m", "1", "-r", "1", "-C")
    assert p.returncode != 0
    assert "combine_fn" in p.stderr and "cannot reach boss" not in p.stderr
