"""Backup tasks for stragglers: pick_backup, first-finisher-wins, cancellation."""
import os
import threading

import pytest

from mapreduce import boss, common, storage, worker

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AVG_TEMP = open(os.path.join(REPO, "examples", "avg_temp.py")).read()
CFG = {"min_done": 0.25, "slowdown": 1.5, "min_seconds": 2, "max_fraction": 0.10}
NOW = 1000.0


# ------------------------------------------------------- pick_backup (pure)
def attempt(worker, started, progress=None, status="RUNNING", finished=None, backup=False):
    return {"attempt_id": "x", "worker": worker, "started": started, "progress": progress,
            "status": status, "finished": finished, "backup": backup}


def done_task(duration=4.0):
    return {"task_id": "d", "state": "DONE",
            "attempts": [attempt("wd", 0, 1.0, "WON", duration)]}


def running(worker="w1", elapsed=8.0, progress=0.5, extra=()):
    return {"task_id": "r", "state": "RUNNING",
            "attempts": [attempt(worker, NOW - elapsed, progress), *extra]}


def phase(*tasks, n_done=3):
    return [done_task() for _ in range(n_done)] + list(tasks)    # median 4.0 s


def test_picks_straggler():
    t = running()
    assert boss.pick_backup(phase(t), "w2", NOW, CFG) is t


def test_no_backup_while_pending():
    pending = {"task_id": "p", "state": "PENDING", "attempts": []}
    assert boss.pick_backup(phase(running(), pending), "w2", NOW, CFG) is None


def test_no_backup_before_min_done():
    tasks = phase(running(), *[running() for _ in range(9)], n_done=1)   # 1/11 < 25 %
    assert boss.pick_backup(tasks, "w2", NOW, CFG, total=100) is None


def test_no_backup_without_any_done():
    assert boss.pick_backup([running()], "w2", NOW, dict(CFG, min_done=0)) is None


@pytest.mark.parametrize("elapsed,progress,cfg", [
    (6.0, 0.1, CFG),                               # elapsed == 1.5 x median
    (1.9, 0.01, dict(CFG, slowdown=0.1)),          # elapsed <= BACKUP_MIN_SECONDS
    (8.0, 0.7, CFG),                               # est_remaining 3.4 <= median 4.0
    (12.0, 0.75, CFG),                             # est_remaining == median (4.0)
])
def test_thresholds(elapsed, progress, cfg):
    assert boss.pick_backup(phase(running(elapsed=elapsed, progress=progress)),
                            "w2", NOW, cfg) is None


def test_zero_progress_is_infinite_estimate():
    t = running(progress=0.0)
    assert boss.pick_backup(phase(t), "w2", NOW, CFG) is t


def test_never_on_original_worker():
    assert boss.pick_backup(phase(running("w1")), "w1", NOW, CFG) is None


def test_max_one_backup_per_task():
    # original timed out, backup still running: one RUNNING attempt, but it's the backup
    t = running("w3", extra=[attempt("w1", NOW - 20, 0.1, "FAILED", NOW - 5)])
    t["attempts"][0]["backup"] = True
    assert boss.pick_backup(phase(t), "w2", NOW, CFG) is None
    # two attempts already running
    t2 = running("w1", extra=[attempt("w3", NOW - 7, 0.1, backup=True)])
    assert boss.pick_backup(phase(t2), "w2", NOW, CFG) is None


def test_job_cap():
    tasks = phase(running())
    assert boss.pick_backup(tasks, "w2", NOW, CFG, launched=0, total=4) is not None  # cap 1
    assert boss.pick_backup(tasks, "w2", NOW, CFG, launched=1, total=4) is None      # small job
    assert boss.pick_backup(tasks, "w2", NOW, CFG, launched=1, total=20) is not None  # cap 2
    assert boss.pick_backup(tasks, "w2", NOW, CFG, launched=2, total=20) is None


def test_picks_largest_est_remaining():
    a = running(elapsed=8.0, progress=0.5)     # est 8
    b = running(elapsed=10.0, progress=0.2)    # est 40
    c = running(elapsed=12.0, progress=0.6)    # est 8
    assert boss.pick_backup(phase(a, b, c, n_done=6), "w2", NOW, CFG) is b


# -------------------------------------------------------- through the boss
class Clock:
    t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "ROOT", str(tmp_path))
    storage.prepare_root()
    monkeypatch.setattr(boss, "jobs", {})
    monkeypatch.setattr(boss, "workers", {})
    clock = Clock()
    monkeypatch.setattr(boss, "now", clock)
    (tmp_path / "input" / "d.csv").write_text(
        "city,date,temp\n" + "".join(f"c{i % 7},x,{i % 30}\n" for i in range(800)))
    return tmp_path, clock, boss.app.test_client()


def submit(client, backup=True, maps=4, reduces=2):
    return client.post("/submit_job", json={
        "input": "/input", "output": "/output/o", "job_source": AVG_TEMP,
        "num_maps": maps, "num_reduces": reduces, "use_backup": backup}).get_json()["job_id"]


def get(client, w):
    return client.post("/get_task", json={"worker_id": w}).get_json()["task"]


def done(client, w, task, success=True, stats=None):
    return client.post("/task_done", json={
        "job_id": task["job_id"], "task_id": task["task_id"], "attempt_id": task["attempt_id"],
        "worker_id": w, "success": success, "stats": stats or {},
        "error": None if success else "boom"}).get_json()


def hb(client, w, task, progress=None):
    return client.post("/heartbeat", json={
        "worker_id": w, "job_id": task["job_id"], "task_id": task["task_id"],
        "attempt_id": task["attempt_id"], "progress": progress}).get_json()


def job(client, job_id):
    return client.get(f"/job/{job_id}").get_json()


def straggle(client, clock, backup=True):
    """4 maps: w1..w3 finish theirs in 4 s, w4's map-3 is at 25 % after 9 s."""
    job_id = submit(client, backup=backup)
    ts = {w: get(client, w) for w in ("w1", "w2", "w3", "w4")}
    clock.t += 4
    for w in ("w1", "w2", "w3"):
        done(client, w, ts[w])
    clock.t += 5
    hb(client, "w4", ts["w4"], progress=0.25)
    return job_id, ts["w4"]


def test_backup_off_never_returns_backup(env):
    _, clock, client = env
    job_id, _ = straggle(client, clock, backup=False)
    assert get(client, "w1") is None
    assert job(client, job_id)["backups_launched"] == 0


def test_first_finisher_wins(env):
    shared, clock, client = env
    job_id, slow = straggle(client, clock)
    b = get(client, "w1")
    assert (b["task_id"], b["attempt_id"]) == ("map-3", "map-3.a1")
    assert get(client, "w2") is None                   # max one backup per task
    clock.t += 1
    assert done(client, "w1", b, stats={"map_output_records": 10}) == {"ok": True}
    j = job(client, job_id)
    assert j["status"] == "REDUCE" and j["backups_launched"] == 1 and j["backups_won"] == 1
    reduces = boss.jobs[job_id]["reduce_tasks"]
    assert hb(client, "w4", slow, progress=0.9)["cancel"] is True   # loser told to stop
    assert hb(client, "w1", b)["cancel"] is False
    clock.t += 1
    assert done(client, "w4", slow, stats={"map_output_records": 99})["ignored"] is True
    assert boss.jobs[job_id]["reduce_tasks"] is reduces              # created once
    statuses = [a["status"] for a in job(client, job_id)["tasks"][3]["attempts"]]
    assert statuses == ["LOST", "WON"]
    j = job(client, job_id)
    assert j["counters"] == {"map_output_records": 10}               # WON stats only
    assert j["wasted_seconds"] == pytest.approx(11.0)                # map-3.a0 ran 11 s
    events = (shared / "jobs" / job_id / "events.jsonl").read_text().splitlines()
    assert len(events) == 4 + 4 + 3      # 4 starts, 3 WON, backup start, WON, CANCELLED, LOST


def test_failure_of_one_attempt_does_not_fail_job(env):
    _, clock, client = env
    job_id, slow = straggle(client, clock)
    b = get(client, "w1")
    assert done(client, "w1", b, success=False) == {"ok": True}
    j = job(client, job_id)
    assert j["status"] == "MAP" and j["tasks"][3]["state"] == "RUNNING"
    assert done(client, "w4", slow)["ok"] is True
    assert job(client, job_id)["status"] == "REDUCE"


def test_last_running_attempt_failure_fails_job(env):
    _, clock, client = env
    job_id, slow = straggle(client, clock)
    b = get(client, "w1")
    done(client, "w1", b, success=False)
    done(client, "w4", slow, success=False)
    assert job(client, job_id)["status"] == "FAILED"


def test_timeout_of_original_keeps_backup_running(env):
    _, clock, client = env
    job_id, slow = straggle(client, clock)
    b = get(client, "w1")
    clock.t += boss.HEARTBEAT_TIMEOUT + 1
    hb(client, "w1", b, progress=0.5)                  # backup alive, w4 silent
    with boss.lock:
        assert boss.check_timeouts(clock.t) == [(job_id, "map-3", "w4")]
    t = job(client, job_id)["tasks"][3]
    assert t["state"] == "RUNNING" and t["worker"] == "w1"
    assert [a["status"] for a in t["attempts"]] == ["FAILED", "RUNNING"]
    assert get(client, "w2") is None                   # not PENDING; no second backup


def test_no_backups_without_straggler(env):
    _, clock, client = env
    job_id = submit(client)
    ts = {w: get(client, w) for w in ("w1", "w2", "w3", "w4")}
    for w in ts:
        clock.t += 1
        done(client, w, ts[w])
    assert job(client, job_id)["backups_launched"] == 0


# ------------------------------------------------------------------- worker
def test_run_map_cancelled_mid_split(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "ROOT", str(tmp_path))
    storage.prepare_root()
    (tmp_path / "input" / "d.csv").write_text(
        "city,date,temp\n" + "".join(f"c{i % 7},x,{i % 30}\n" for i in range(5000)))
    storage.write_atomic("jobs/jc/job.py", AVG_TEMP)
    split = common.compute_splits(storage.list_files("input/d.csv"), 1)[0]
    task = {"job_id": "jc", "task_id": "map-0", "attempt_id": "map-0.a1", "type": "map",
            "index": 0, "num_maps": 1, "num_reduces": 2, "output": "output/o",
            "use_combiner": False, "split": split}
    seen = []
    real_map = worker._job("jc").map_fn

    def map_fn(key, row):                  # boss cancels us after 1500 rows
        seen.append(1)
        if len(seen) == 1500:
            worker._cancel.set()
        yield from real_map(key, row)

    monkeypatch.setattr(worker._jobs["jc"], "map_fn", map_fn)
    monkeypatch.setattr(worker, "_current", {"progress": 0.0})
    worker._cancel.clear()
    try:
        with pytest.raises(worker.TaskCancelled):
            worker.run_map(task)
    finally:
        worker._cancel.clear()
    assert len(seen) == 1999                           # stopped at the next check (row 2000)
    assert 0.3 < worker._current["progress"] < 0.5
    assert list((tmp_path / "jobs" / "jc").rglob("*.jsonl*")) == []   # no tmp, no final


def test_atomic_open_removes_temp_on_cancel(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "ROOT", str(tmp_path))
    with pytest.raises(worker.TaskCancelled):
        with storage.atomic_open("output/x/part-0.jsonl") as f:
            f.write("partial\n")
            raise worker.TaskCancelled()
    assert list((tmp_path / "output" / "x").iterdir()) == []


def test_heartbeat_cancel_only_for_same_attempt(monkeypatch):
    """A cancel reply for a finished attempt must not cancel the next one."""
    calls = []

    class Resp:
        def json(self):
            return {"cancel": True}

    first = {"attempt_id": "a0", "progress": 0.0}

    class Stop(Exception):
        pass

    def post(url, json, timeout):
        calls.append(json["attempt_id"])
        worker._current = {"attempt_id": "a1", "progress": 0.0}   # moved on mid-request
        return Resp()

    monkeypatch.setattr(worker, "_current", first)
    monkeypatch.setattr(worker.requests, "post", post)
    monkeypatch.setattr(worker.time, "sleep", lambda s: (_ for _ in ()).throw(Stop()))
    worker._cancel.clear()
    with pytest.raises(Stop):
        worker._heartbeat_loop()
    assert calls == ["a0"] and not worker._cancel.is_set()
