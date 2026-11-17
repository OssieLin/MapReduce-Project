"""Worker failure handling: heartbeats, timeouts, reassignment, late results."""
import json
import math
import os
import random

import pytest

from mapreduce import boss, storage, worker

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AVG_TEMP = open(os.path.join(REPO, "examples", "avg_temp.py")).read()
TIMEOUT = boss.HEARTBEAT_TIMEOUT


class Clock:
    t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture(autouse=True)
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "ROOT", str(tmp_path))
    storage.prepare_root()
    monkeypatch.setattr(boss, "jobs", {})
    monkeypatch.setattr(boss, "workers", {})
    clock = Clock()
    monkeypatch.setattr(boss, "now", clock)
    rnd = random.Random(0)
    rows = [(rnd.choice(["A", "B", "C", "D", "E"]), rnd.randint(-20, 40)) for _ in range(400)]
    (tmp_path / "input" / "d.csv").write_text(
        "city,date,temp\n" + "".join(f"{c},x,{t}\n" for c, t in rows))
    return tmp_path, clock, rows


@pytest.fixture
def client():
    return boss.app.test_client()


def submit(client, maps=4, reduces=2):
    r = client.post("/submit_job", json={"input": "/input", "output": "/output/o",
                                         "job_source": AVG_TEMP, "num_maps": maps,
                                         "num_reduces": reduces})
    return r.get_json()["job_id"]


def get(client, w):
    return client.post("/get_task", json={"worker_id": w}).get_json()["task"]


def done(client, w, task, success=True, run=False):
    stats = {}
    if run and success:
        stats = (worker.run_map if task["type"] == "map" else worker.run_reduce)(task)
    return client.post("/task_done", json={
        "job_id": task["job_id"], "task_id": task["task_id"], "worker_id": w,
        "attempt_id": task["attempt_id"],
        "success": success, "stats": stats, "error": None if success else "boom"}).get_json()


def hb(client, w, task=None, **extra):
    body = {"worker_id": w, **extra}
    if task:
        body.update(job_id=task["job_id"], task_id=task["task_id"],
                    attempt_id=task["attempt_id"])
    client.post("/heartbeat", json=body)


def tasks(client, job_id):
    return {t["task_id"]: t for t in client.get(f"/job/{job_id}").get_json()["tasks"]}


def check(clock):
    with boss.lock:
        return boss.check_timeouts(clock.t)


def finish_maps(client, job_id, w="wX"):
    while (t := get(client, w)) and t["type"] == "map":
        done(client, w, t, run=True)
    return t                                  # first reduce task (now RUNNING on w)


# ------------------------------------------------------------------ detection
def test_heartbeats_keep_task_alive_then_timeout_resets(client, env):
    _, clock, _ = env
    job_id = submit(client)
    t = get(client, "w1")
    for _ in range(5):                        # 25 s of heartbeats every 5 s
        clock.t += 5
        hb(client, "w1", t)
        assert check(clock) == []
    assert tasks(client, job_id)["map-0"]["state"] == "RUNNING"

    clock.t += TIMEOUT + 0.5                  # silence
    assert check(clock) == [(job_id, "map-0", "w1")]
    m0 = tasks(client, job_id)["map-0"]
    assert (m0["state"], m0["worker"]) == ("PENDING", None)
    assert [a["status"] for a in m0["attempts"]] == ["FAILED"]   # next run is attempt 2
    assert boss.workers["w1"]["status"] == "failed"
    assert client.get(f"/job/{job_id}").get_json()["reexecutions"] == 1


def test_never_heartbeats_detected_via_started(client, env):
    _, clock, _ = env
    job_id = submit(client)
    get(client, "w1")
    boss.workers["w1"]["last_seen"] = 0        # as if it never called after /get_task
    clock.t += TIMEOUT - 1
    assert check(clock) == []                 # started is recent
    clock.t += 2
    assert check(clock) == [(job_id, "map-0", "w1")]


def test_reset_task_goes_to_another_worker(client, env):
    _, clock, _ = env
    job_id = submit(client)
    get(client, "w1")
    clock.t += TIMEOUT + 1
    check(clock)
    t = get(client, "w2")
    assert t["task_id"] == "map-0"
    m0 = tasks(client, job_id)["map-0"]
    assert (m0["state"], m0["worker"], len(m0["attempts"])) == ("RUNNING", "w2", 2)
    workers = {w["worker_id"]: w for w in client.get("/workers").get_json()["workers"]}
    assert workers["w2"]["task"] == f"{job_id}/map-0.a1" and workers["w1"]["task"] is None
    assert workers["w1"]["status"] == "failed" and workers["w2"]["status"] == "alive"


def test_heartbeat_revives_worker_but_not_its_task(client, env):
    _, clock, _ = env
    job_id = submit(client)
    old = get(client, "w1")
    clock.t += TIMEOUT + 1
    check(clock)
    new = get(client, "w2")
    hb(client, "w1", old, progress=0.9)       # paused worker wakes up
    assert boss.workers["w1"]["status"] == "alive"
    m0 = tasks(client, job_id)["map-0"]
    assert (m0["worker"], m0["progress"]) == ("w2", None)
    hb(client, "w2", new, progress=0.5)
    assert tasks(client, job_id)["map-0"]["progress"] == 0.5


def test_restart_fast_path(client, env):
    job_id = submit(client)
    get(client, "w1")                         # w1 crashes, restarts, asks again
    t = get(client, "w1")
    ts = tasks(client, job_id)
    assert t["task_id"] == "map-0"            # reset, then handed out again
    assert len(ts["map-0"]["attempts"]) == 2 and ts["map-0"]["worker"] == "w1"
    assert ts["map-1"]["state"] == "PENDING"


# -------------------------------------------------------------- late results
def test_late_success_marks_done_once(client, env):
    _, clock, _ = env
    job_id = submit(client, maps=1, reduces=2)
    t = get(client, "w1")
    clock.t += TIMEOUT + 1
    check(clock)
    t2 = get(client, "w2")
    assert t2["task_id"] == t["task_id"]
    assert done(client, "w1", t, run=True) == {"ok": True}      # late, accepted
    j = client.get(f"/job/{job_id}").get_json()
    assert j["status"] == "REDUCE" and j["maps_done"] == 1
    reduces = boss.jobs[job_id]["reduce_tasks"]
    assert done(client, "w2", t2, run=True)["ignored"] is True  # second attempt ignored
    assert boss.jobs[job_id]["reduce_tasks"] is reduces          # not recreated
    assert tasks(client, job_id)["map-0"]["worker"] == "w1"


def test_late_failure_from_non_assignee_ignored(client, env):
    _, clock, _ = env
    job_id = submit(client)
    t = get(client, "w1")
    clock.t += TIMEOUT + 1
    check(clock)
    assert done(client, "w1", t, success=False)["ignored"] is True   # still PENDING
    get(client, "w2")
    assert done(client, "w1", t, success=False)["ignored"] is True   # RUNNING on w2
    assert client.get(f"/job/{job_id}").get_json()["status"] == "MAP"


def test_failure_from_assignee_fails_job(client):
    job_id = submit(client)
    t = get(client, "w1")
    done(client, "w1", t, success=False)
    assert client.get(f"/job/{job_id}").get_json()["status"] == "FAILED"


# ------------------------------------------------------------------ recovery
def test_completed_maps_of_failed_worker_stay_done(client, env):
    _, clock, _ = env
    job_id = submit(client)
    done(client, "w1", get(client, "w1"), run=True)    # map-0 DONE by w1
    get(client, "w1")                                  # map-1, then w1 dies
    clock.t += TIMEOUT + 1
    assert check(clock) == [(job_id, "map-1", "w1")]
    ts = tasks(client, job_id)
    assert ts["map-0"]["state"] == "DONE" and len(ts["map-0"]["attempts"]) == 1
    assert get(client, "w2")["task_id"] == "map-1"


def test_reduce_phase_failure_resets_only_reduce_task(client, env):
    _, clock, _ = env
    job_id = submit(client)
    r0 = finish_maps(client, job_id, "w1")             # w1 holds reduce-0, dies
    assert r0["task_id"] == "reduce-0"
    clock.t += TIMEOUT + 1
    assert check(clock) == [(job_id, "reduce-0", "w1")]
    ts = tasks(client, job_id)
    assert all(ts[f"map-{m}"]["state"] == "DONE" for m in range(4))
    assert ts["reduce-0"]["state"] == "PENDING" and len(ts["reduce-0"]["attempts"]) == 1
    assert ts["reduce-1"]["state"] == "PENDING" and ts["reduce-1"]["attempts"] == []
    assert boss.jobs[job_id]["status"] == "REDUCE"


def test_all_workers_dead_job_waits(client, env):
    _, clock, _ = env
    job_id = submit(client)
    get(client, "w1")
    clock.t += 100
    check(clock)
    clock.t += 100
    assert check(clock) == []
    assert client.get(f"/job/{job_id}").get_json()["status"] == "MAP"
    assert get(client, "w9")["task_id"] == "map-0"    # resumes when one returns


def test_full_job_correct_with_resets_and_temp_cleanup(client, env):
    shared, clock, rows = env
    job_id = submit(client)
    dead = get(client, "w1")                          # map-0: w1 dies mid-write
    (shared / "jobs" / job_id / "intermediate").mkdir(parents=True)
    (shared / "jobs" / job_id / "intermediate" / "map-0-part-0.jsonl.tmp-dead").write_text("x")
    clock.t += TIMEOUT + 1
    check(clock)
    r0 = finish_maps(client, job_id, "w2")            # w2 redoes map-0, gets reduce-0
    (shared / "output" / "o").mkdir(parents=True, exist_ok=True)
    (shared / "output" / "o" / "part-0.jsonl.tmp-dead").write_text("x")
    again = get(client, "w2")                         # w2 restarts: reduce-0 reset,
    assert again["task_id"] == r0["task_id"]          # then handed back to it
    done(client, "w2", again, run=True)
    clock.t += 1
    while (t := get(client, "w3")):
        done(client, "w3", t, run=True)
    assert done(client, "w1", dead, success=False)["ignored"] is True

    j = client.get(f"/job/{job_id}").get_json()
    assert j["status"] == "DONE" and j["reexecutions"] >= 2
    out = sorted(json.loads(line) for r in range(2)
                 for line in open(shared / "output" / "o" / f"part-{r}.jsonl"))
    sums = {}
    for c, t in rows:
        s, n = sums.get(c, (0, 0))
        sums[c] = (s + t, n + 1)
    expected = sorted([c, s / n] for c, (s, n) in sums.items())
    assert [k for k, _ in out] == [k for k, _ in expected]
    assert all(math.isclose(a, b) for (_, a), (_, b) in zip(out, expected))
    leftovers = [p for p in shared.rglob("*") if ".tmp-" in p.name]
    assert leftovers == [] and (shared / "output" / "o" / "_SUCCESS").exists()
