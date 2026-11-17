"""Visualization support: /api/state snapshots, saved history, the page route,
and the per-partition map stats the page draws."""
import json
import os

import pytest

from mapreduce import boss, common, storage, worker

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AVG_TEMP = open(os.path.join(REPO, "examples", "avg_temp.py")).read()
ROWS = "".join(f"{c},2026-01-0{d},{t}\n" for d, (c, t) in enumerate(
    [("Madison", -5.0), ("Chicago", 1.0), ("Miami", 25.0), ("Madison", 3.0),
     ("Chicago", -1.0), ("Boston", 4.0), ("Miami", 27.0), ("Boston", 2.0)], 1))


@pytest.fixture(autouse=True)
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "ROOT", str(tmp_path))
    storage.prepare_root()
    monkeypatch.setattr(boss, "jobs", {})
    monkeypatch.setattr(boss, "workers", {})
    monkeypatch.setattr(boss, "history", [])
    (tmp_path / "input" / "w.csv").write_text("city,date,temp\n" + ROWS)
    return tmp_path


@pytest.fixture
def client():
    return boss.app.test_client()


def run_job(client, out="/output/o", maps=3, reduces=2, combiner=False):
    """Submit and drive a job to DONE with fake workers that run the real tasks."""
    job_id = client.post("/submit_job", json={
        "input": "/input", "output": out, "job_source": AVG_TEMP, "num_maps": maps,
        "num_reduces": reduces, "use_combiner": combiner}).get_json()["job_id"]
    for i in range(maps + reduces):
        w = f"worker{i % 2 + 1}"
        task = client.post("/get_task", json={"worker_id": w}).get_json()["task"]
        stats = (worker.run_map if task["type"] == "map" else worker.run_reduce)(task)
        client.post("/task_done", json={
            "job_id": job_id, "task_id": task["task_id"], "attempt_id": task["attempt_id"],
            "worker_id": w, "success": True, "stats": stats})
    return job_id


def test_api_state_empty(client):
    s = client.get("/api/state").get_json()
    assert s["jobs"] == [] and s["workers"] == [] and isinstance(s["now"], float)


def test_api_state_snapshot(client, env):
    job_id = run_job(client)
    s = client.get("/api/state").get_json()
    assert [w["worker_id"] for w in s["workers"]] == ["worker1", "worker2"]
    (job,) = s["jobs"]
    assert job["id"] == job_id and job["status"] == "DONE" and (job["M"], job["R"]) == (3, 2)
    assert job["finished"] >= job["submitted"]
    maps = [t for t in job["tasks"] if t["type"] == "map"]
    reds = [t for t in job["tasks"] if t["type"] == "reduce"]
    assert len(maps) == 3 and len(reds) == 2
    for t in job["tasks"]:
        (a,) = t["attempts"]
        assert a["status"] == "WON" and a["finished"] >= a["started"] >= job["submitted"]
    data_bytes = len(ROWS.encode())
    assert sum(t["split_bytes"] for t in maps) == data_bytes
    for t in maps:
        st = t["stats"]
        assert len(st["part_bytes"]) == len(st["part_records"]) == 2
        assert sum(st["part_bytes"]) == st["intermediate_bytes"]
    assert sum(t["stats"]["map_input_records"] for t in maps) == 8
    for k in ("shuffle_bytes", "reduce_input_records", "reduce_input_groups",
              "reduce_output_records"):
        assert all(k in t["stats"] for t in reds)
    assert sum(t["stats"]["reduce_output_records"] for t in reds) == 4      # 4 cities
    # same numbers as /job/<id> counters
    counters = client.get(f"/job/{job_id}").get_json()["counters"]
    assert counters["intermediate_bytes"] == sum(t["stats"]["intermediate_bytes"] for t in maps)


def test_running_job_snapshot(client):
    job_id = client.post("/submit_job", json={
        "input": "/input", "output": "/output/r", "job_source": AVG_TEMP,
        "num_maps": 2, "num_reduces": 2}).get_json()["job_id"]
    task = client.post("/get_task", json={"worker_id": "worker1"}).get_json()["task"]
    client.post("/heartbeat", json={"worker_id": "worker1", "job_id": job_id,
                                    "task_id": task["task_id"],
                                    "attempt_id": task["attempt_id"], "progress": 0.4})
    (job,) = client.get("/api/state").get_json()["jobs"]
    assert job["status"] == "MAP" and job["finished"] is None
    assert [t["state"] for t in job["tasks"]] == ["RUNNING", "PENDING"]   # no reduces yet
    a = job["tasks"][0]["attempts"][0]
    assert a["status"] == "RUNNING" and a["progress"] == 0.4 and a["finished"] is None


def test_finished_jobs_saved_and_reloaded(client, env):
    ids = [run_job(client, out=f"/output/o{i}") for i in range(8)]
    for i in ids:
        assert json.loads((env / "jobs" / i / "state.json").read_text())["status"] == "DONE"
    assert [j["id"] for j in client.get("/api/state").get_json()["jobs"]] == ids[-6:]

    boss.jobs.clear()                                 # a restarted boss
    boss.load_history()
    assert [j["id"] for j in boss.history] == ids[-6:]
    s = client.get("/api/state").get_json()
    assert [j["id"] for j in s["jobs"]] == ids[-6:]
    assert all(j["status"] == "DONE" and len(j["tasks"]) == 5 for j in s["jobs"])

    new = run_job(client, out="/output/new")          # live jobs join the history
    assert [j["id"] for j in client.get("/api/state").get_json()["jobs"]] == ids[-5:] + [new]


def test_failed_job_saved(client, env):
    job_id = client.post("/submit_job", json={
        "input": "/input", "output": "/output/f", "job_source": AVG_TEMP,
        "num_maps": 1, "num_reduces": 1}).get_json()["job_id"]
    task = client.post("/get_task", json={"worker_id": "worker1"}).get_json()["task"]
    client.post("/task_done", json={"job_id": job_id, "task_id": task["task_id"],
                                    "attempt_id": task["attempt_id"], "worker_id": "worker1",
                                    "success": False, "error": "boom"})
    snap = json.loads((env / "jobs" / job_id / "state.json").read_text())
    assert snap["status"] == "FAILED" and "boom" in snap["error"]
    assert snap["tasks"][0]["attempts"][0]["status"] == "FAILED"


def test_visualization_page(client):
    for path in ("/visualization", "/visualization/"):
        r = client.get(path)
        assert r.status_code == 200
        assert b"<title>Mini-MapReduce Workflow Visualization</title>" in r.data


@pytest.mark.parametrize("combiner", [False, True])
def test_run_map_part_stats(env, combiner):
    storage.write_atomic("jobs/j/job.py", AVG_TEMP)
    split = common.compute_splits(storage.list_files("/input", suffix=".csv"), 1)[0]
    stats = worker.run_map({"job_id": "j", "index": 0, "num_reduces": 3, "split": split,
                            "use_combiner": combiner})
    assert len(stats["part_bytes"]) == len(stats["part_records"]) == 3
    assert sum(stats["part_bytes"]) == stats["intermediate_bytes"]
    written = stats["combine_output_records"] if combiner else stats["map_output_records"]
    assert sum(stats["part_records"]) == written
    for r in range(3):
        lines = (env / "jobs" / "j" / "intermediate" / f"map-0-part-{r}.jsonl").read_text()
        assert len(lines.splitlines()) == stats["part_records"][r]
        assert len(lines.encode()) == stats["part_bytes"][r]
