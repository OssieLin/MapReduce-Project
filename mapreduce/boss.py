"""Boss (master): tracks jobs/tasks and hands tasks to pulling workers."""
import json
import math
import os
import statistics
import threading
import time
import uuid

from flask import Flask, jsonify, request

from mapreduce import common, storage

app = Flask(__name__)

# A global mutex lock ensures that concurrent HTTP requests from multiple workers
# do not corrupt the job state or assign the same task to two workers at once.
lock = threading.Lock()

# In-memory database of all jobs. Key: job_id, Value: job dictionary.
jobs = {}            # job_id -> job dict (insertion ordered)

# Liveness of every worker that has ever called us.
workers = {}         # worker_id -> {"last_seen", "status": "alive"|"failed", "task"}
HEARTBEAT_TIMEOUT = float(os.environ.get("HEARTBEAT_TIMEOUT", 10))

# Backup tasks for stragglers (only for jobs submitted with -B).
BACKUP_CFG = {
    "min_done": float(os.environ.get("BACKUP_MIN_DONE", 0.25)),        # fraction of phase DONE
    "slowdown": float(os.environ.get("BACKUP_SLOWDOWN", 1.5)),         # x median duration
    "min_seconds": float(os.environ.get("BACKUP_MIN_SECONDS", 2)),
    "max_fraction": float(os.environ.get("BACKUP_MAX_FRACTION", 0.10)),  # of all tasks
}


def now():
    """Single clock source, so tests can monkeypatch time."""
    return time.time()


def _new_task(job_id, ttype, index, extra=None):
    """
    Internal factory function to create a standardized task dictionary.
    Initializes timestamps to None and state to PENDING.
    """
    return {"task_id": f"{ttype}-{index}", "type": ttype, "index": index,
            "state": "PENDING", "worker": None,
            "started": None, "finished": None, "attempts": [],
            "stats": {}, "extra": extra or {}} # extra is used for split


def _payload(job, task, attempt):
    """
    Formats the JSON payload that gets sent to a worker when it asks for a task.
    Combines job-level configuration (like num_reduces) with task-specific data (like byte splits).
    """
    p = {"job_id": job["id"], "task_id": task["task_id"],
         "attempt_id": attempt["attempt_id"], "type": task["type"],
         "index": task["index"], "num_maps": job["num_maps"],
         "num_reduces": job["num_reduces"],
         "output": job["output"], "use_combiner": job["use_combiner"]}
    p.update(task["extra"])
    return p


def _err(msg, code=400):
    return jsonify({"error": msg}), code


def _find_task(job, task_id):
    """Look up "map-3" / "reduce-1" in a job; None if it doesn't exist."""
    try:
        ttype, index = str(task_id).split("-")
        return job["map_tasks" if ttype == "map" else "reduce_tasks"][int(index)]
    except (ValueError, IndexError):
        return None


def _find_attempt(task, attempt_id, worker):
    """Exact attempt_id; for clients that don't send one, the worker's latest
    attempt (or the task's latest)."""
    if attempt_id is not None:
        return next((a for a in task["attempts"] if a["attempt_id"] == attempt_id), None)
    mine = [a for a in task["attempts"] if a["worker"] == worker]
    return (mine or task["attempts"] or [None])[-1]


def _touch(worker_id):
    """Any call from a worker proves it is alive."""
    w = workers.setdefault(worker_id, {"last_seen": None, "status": "alive", "task": None})
    w.update(last_seen=now(), status="alive")
    return w


def _running(task):
    return [a for a in task["attempts"] if a["status"] == "RUNNING"]


def _log_event(job, task, a):
    """One line per attempt start/finish, for experiments (small append)."""
    storage.append_line(f"jobs/{job['id']}/events.jsonl", json.dumps({
        "job": job["id"], "task": task["task_id"], "attempt": a["attempt_id"],
        "worker": a["worker"], "backup": a["backup"], "start": a["started"],
        "end": a["finished"], "status": a["status"], "progress": a["progress"]}))


def _start_attempt(job, task, worker, backup=False):
    a = {"attempt_id": f"{task['task_id']}.a{len(task['attempts'])}", "worker": worker,
         "started": now(), "last_heartbeat": None, "progress": None, "backup": backup,
         "status": "RUNNING", "finished": None}
    task["attempts"].append(a)
    task.update(state="RUNNING", worker=worker, started=a["started"], finished=None)
    workers[worker]["task"] = (job["id"], a["attempt_id"])
    _log_event(job, task, a)
    return a


def _end_attempt(job, task, a, status):
    """Close an attempt (WON/CANCELLED/LOST/FAILED). A FAILED attempt puts the
    task back to PENDING unless another attempt of it is still running."""
    a.update(status=status, finished=now())
    w = workers.get(a["worker"])
    if w and w["task"] == (job["id"], a["attempt_id"]):
        w["task"] = None
    _log_event(job, task, a)
    if status == "FAILED" and task["state"] == "RUNNING" and not _running(task):
        task.update(state="PENDING", worker=None, started=None)


def _active_tasks(job):
    if job["status"] == "MAP":
        return job["map_tasks"]
    if job["status"] == "REDUCE":
        return job["reduce_tasks"]
    return []


def check_timeouts(now):
    """Fail RUNNING attempts whose worker has been silent > HEARTBEAT_TIMEOUT.
    Caller holds `lock`. Returns [(job_id, task_id, worker_id)] that timed out."""
    reset = []
    for job in jobs.values():
        for t in _active_tasks(job):
            for a in _running(t):
                w = workers.get(a["worker"])
                # max() covers a worker that dies before its first heartbeat
                silent = now - max(a["started"], w["last_seen"] if w else 0)
                if silent > HEARTBEAT_TIMEOUT:
                    if w:
                        w["status"] = "failed"
                    _end_attempt(job, t, a, "FAILED")
                    print(f"worker {a['worker']} failed: no heartbeat for {silent:.1f} s; "
                          f"{a['attempt_id']} of job {job['id']} -> {t['state']}", flush=True)
                    reset.append((job["id"], t["task_id"], a["worker"]))
    # idle workers can die too; keep /workers honest
    for wid, w in workers.items():
        if w["status"] == "alive" and now - w["last_seen"] > HEARTBEAT_TIMEOUT:
            w["status"] = "failed"
            print(f"worker {wid} failed: no heartbeat for {now - w['last_seen']:.1f} s", flush=True)
    return reset


def _monitor():
    while True:
        time.sleep(1)
        with lock:
            check_timeouts(now())


# ------------------------------------------------------------ backup tasks
def _phase_median(tasks):
    """Median duration of the winning attempts of DONE tasks, or None."""
    durs = [a["finished"] - a["started"] for t in tasks if t["state"] == "DONE"
            for a in t["attempts"] if a["status"] == "WON"]
    return statistics.median(durs) if durs else None


def _estimate(a, now):
    """(elapsed, progress, estimated remaining seconds) of a running attempt."""
    elapsed, p = now - a["started"], a["progress"] or 0.0
    return elapsed, p, (elapsed * (1 - p) / p if p > 0 else math.inf)


def pick_backup(tasks, worker_id, now, cfg, launched=0, total=None):
    """Return the RUNNING task of this phase most worth a speculative copy on
    worker_id, or None. `launched`/`total` are the job's backups so far and its
    task count, for the cap. Pure: reads task dicts only."""
    if not tasks or any(t["state"] == "PENDING" for t in tasks):
        return None
    total = len(tasks) if total is None else total
    if launched >= max(1, math.ceil(cfg["max_fraction"] * total)):
        return None
    median = _phase_median(tasks)
    done = sum(t["state"] == "DONE" for t in tasks)
    if median is None or done < cfg["min_done"] * len(tasks):
        return None
    best, best_est = None, -1.0
    for t in tasks:
        running = _running(t)
        if (t["state"] != "RUNNING" or len(running) != 1
                or any(a["backup"] for a in t["attempts"])):     # max one backup per task
            continue
        a = running[0]
        elapsed, _, est = _estimate(a, now)
        if (a["worker"] != worker_id
                and elapsed > cfg["slowdown"] * median
                and elapsed > cfg["min_seconds"]
                and est > median                                 # a fresh copy could still win
                and est > best_est):
            best, best_est = t, est
    return best


@app.post("/submit_job")
def submit_job():
    """
    Endpoint for clients to submit a new MapReduce job.
    1. Validates inputs and input files.
    2. Computes byte-range splits.
    3. Saves the MapReduce Python code to shared storage.
    4. Initializes the job state in the global 'jobs' dictionary.
    """
    d = request.get_json(force=True)
    try:
        num_maps, num_reduces = int(d["num_maps"]), int(d["num_reduces"])
        input_path, output = d["input"], d["output"]
        source = d["job_source"]
        use_combiner = bool(d.get("use_combiner", False))
        use_backup = bool(d.get("use_backup", False))
        if num_maps < 1 or num_reduces < 1:
            raise ValueError("num_maps and num_reduces must be >= 1")
        common.check_job_source(source, require_combiner=use_combiner)
        files = storage.list_files(input_path, suffix=".csv")
        if not files:
            raise ValueError(f"no .csv input files found at {input_path}")
        splits = common.compute_splits(files, num_maps)
    except (KeyError, ValueError, TypeError) as e:
        return _err(str(e))

    job_id = uuid.uuid4().hex[:8]

    # Save the user's map/reduce Python functions to a shared volume
    # so workers can dynamically import them later.
    storage.write_atomic(f"jobs/{job_id}/job.py", source)

    # Initialize the job tracker dictionary
    job = {"id": job_id, "input": input_path, "output": output,
           "num_maps": num_maps, "num_reduces": num_reduces,
           "use_combiner": use_combiner, "use_backup": use_backup, "backups": 0,
           "status": "MAP", "error": None,
           "submitted": now(), "finished": None,
           "map_tasks": [_new_task(job_id, "map", i, {"split": s})
                         for i, s in enumerate(splits)],
           "reduce_tasks": []}
    with lock:
        jobs[job_id] = job
    return jsonify({"job_id": job_id})


@app.post("/get_task")
def get_task():
    """
    Endpoint for workers to request work.
    Scans active jobs for PENDING tasks and assigns the first one found.
    With none left, a -B job may get a backup copy of a straggling task.
    """
    worker = (request.get_json(force=True) or {}).get("worker_id", "?")
    with lock:
        _touch(worker)
        # A worker runs one task at a time: if it asks for work while still
        # running an attempt, it restarted or lost its task_done.
        for job in jobs.values():
            for t in _active_tasks(job):
                for a in _running(t):
                    if a["worker"] == worker:
                        _end_attempt(job, t, a, "FAILED")
                        print(f"worker {worker} asked for work while running "
                              f"{a['attempt_id']} of job {job['id']} -> {t['state']}", flush=True)
        for job in jobs.values():
            for t in _active_tasks(job):
                if t["state"] == "PENDING":
                    a = _start_attempt(job, t, worker)
                    return jsonify({"task": _payload(job, t, a)})
        t_now = now()
        for job in jobs.values():
            if not job["use_backup"]:
                continue
            tasks = _active_tasks(job)
            t = pick_backup(tasks, worker, t_now, BACKUP_CFG, job["backups"],
                            job["num_maps"] + job["num_reduces"])
            if t:
                elapsed, p, _ = _estimate(_running(t)[0], t_now)
                median = _phase_median(tasks)
                a = _start_attempt(job, t, worker, backup=True)
                job["backups"] += 1
                print(f"backup {a['attempt_id']} on {worker}: elapsed {elapsed:.1f}s, "
                      f"median {median:.1f}s, progress {p:.2f}", flush=True)
                return jsonify({"task": _payload(job, t, a)})
    return jsonify({"task": None})


@app.post("/task_done")
def task_done():
    """
    Endpoint for workers to report task completion (success or failure).
    Also handles phase transitions (MAP -> REDUCE -> DONE).
    """
    d = request.get_json(force=True)
    worker = d.get("worker_id", "?")
    finished = False
    with lock:
        _touch(worker)
        job = jobs.get(d.get("job_id"))
        if job is None:
            return _err("unknown job", 404)

        # Parse "map-0" into ttype="map", index=0
        task = _find_task(job, d.get("task_id"))
        if task is None:
            return _err("unknown task", 404)
        a = _find_attempt(task, d.get("attempt_id"), worker)
        if a is None:
            return jsonify({"ok": True, "ignored": True})

        # Idempotency guard: ignore duplicate callbacks. A copy that finished
        # after another attempt already won is LOST.
        if task["state"] == "DONE" or job["status"] in ("DONE", "FAILED"):
            if d.get("success") and a["status"] in ("RUNNING", "CANCELLED"):
                _end_attempt(job, task, a, "LOST")
            return jsonify({"ok": True, "ignored": True})

        # If a worker reports a crash/error, fail the entire job immediately --
        # but only if it was the last running attempt. A stale attempt (timed
        # out, cancelled) is ignored; a failing copy lets the other continue.
        if not d.get("success"):
            if a["status"] != "RUNNING":
                return jsonify({"ok": True, "ignored": True})
            _end_attempt(job, task, a, "FAILED")
            if _running(task):
                return jsonify({"ok": True})
            job.update(status="FAILED", finished=now(),
                       error=f"{task['task_id']} failed: {d.get('error')}")
            return jsonify({"ok": True})


        # Mark the individual task as completed: first finisher wins. A success
        # from a worker we presumed dead is accepted too: output files are
        # deterministic and atomically renamed, so they're complete.
        _end_attempt(job, task, a, "WON")
        task.update(state="DONE", worker=a["worker"], finished=a["finished"],
                    stats=d.get("stats") or {})
        for other in _running(task):
            _end_attempt(job, task, other, "CANCELLED")

        # BARRIER 1: If in MAP phase, check if ALL map tasks are now DONE
        if job["status"] == "MAP" and all(t["state"] == "DONE" for t in job["map_tasks"]):
            job["reduce_tasks"] = [_new_task(job["id"], "reduce", r)
                                   for r in range(job["num_reduces"])]
            job["status"] = "REDUCE"

        # BARRIER 2: If in REDUCE phase, check if ALL reduce tasks are now DONE
        elif job["status"] == "REDUCE" and all(t["state"] == "DONE" for t in job["reduce_tasks"]):
            storage.write_atomic(f"{job['output'].rstrip('/')}/_SUCCESS", "")
            job.update(status="DONE", finished=now())
            finished = True
    if finished:
        # killed workers may have left .tmp-* files behind (outside the lock)
        storage.remove_temp_files(job["output"])
        storage.remove_temp_files(f"jobs/{job['id']}/intermediate")
    return jsonify({"ok": True})


@app.post("/heartbeat")
def heartbeat():
    """Workers call this every HEARTBEAT_SECS, busy or idle. The reply tells a
    worker to abandon an attempt that lost, was cancelled or timed out."""
    d = request.get_json(force=True) or {}
    worker = d.get("worker_id", "?")
    cancel = False
    with lock:
        _touch(worker)
        job = jobs.get(d.get("job_id"))
        task = _find_task(job, d.get("task_id")) if job else None
        a = _find_attempt(task, d.get("attempt_id"), worker) if task else None
        # only the attempt's own worker may update its progress
        if a and a["worker"] == worker:
            if a["status"] == "RUNNING":
                a["last_heartbeat"] = now()
                if d.get("progress") is not None:
                    a["progress"] = min(max(float(d["progress"]), 0.0), 1.0)
            cancel = a["status"] in ("CANCELLED", "LOST", "FAILED") or job["status"] == "FAILED"
    return jsonify({"ok": True, "cancel": cancel})


@app.get("/workers")
def list_workers():
    with lock:
        t = now()
        return jsonify({"workers": [
            {"worker_id": wid, "status": w["status"],
             "last_seen_age": round(t - w["last_seen"], 1),
             "task": "/".join(w["task"]) if w["task"] else None}
            for wid, w in sorted(workers.items())]})


def _task_progress(task):
    if task["state"] == "DONE":
        return 1.0
    ps = [a["progress"] for a in _running(task) if a["progress"] is not None]
    return max(ps) if ps else None


@app.get("/job/<job_id>")
def job_status(job_id):
    """
    Endpoint for external clients to monitor job progress and view statistics.
    """
    with lock:
        job = jobs.get(job_id)
        if job is None:
            return _err("unknown job", 404)
        count = lambda ts: sum(t["state"] == "DONE" for t in ts)
        tasks = job["map_tasks"] + job["reduce_tasks"]
        attempts = [a for t in tasks for a in t["attempts"]]
        # task["stats"] come from the WON attempt only
        counters = {}
        for t in tasks:
            if t["state"] == "DONE":
                for k, v in t["stats"].items():
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        counters[k] = counters.get(k, 0) + v
        end = job["finished"] or now()
        return jsonify({
            "job_id": job_id, "status": job["status"], "error": job["error"],
            "output": job["output"],
            "maps_done": count(job["map_tasks"]), "maps_total": job["num_maps"],
            "reduces_done": count(job["reduce_tasks"]),
            "reduces_total": job["num_reduces"],
            "elapsed": round(end - job["submitted"], 3),
            "use_combiner": job["use_combiner"], "use_backup": job["use_backup"],
            "counters": counters,
            # attempts lost to a dead/restarted worker (each one is re-run)
            "reexecutions": sum(a["status"] == "FAILED" for a in attempts),
            "backups_launched": job["backups"],
            "backups_won": sum(a["backup"] and a["status"] == "WON" for a in attempts),
            "wasted_seconds": round(sum(a["finished"] - a["started"] for a in attempts
                                        if a["status"] in ("CANCELLED", "LOST")), 3),
            "tasks": [{"task_id": t["task_id"], "state": t["state"], "worker": t["worker"],
                       "started": t["started"], "finished": t["finished"],
                       "progress": _task_progress(t),
                       "attempts": [dict(a) for a in t["attempts"]]}
                      for t in tasks],
        })


if __name__ == "__main__":
    storage.prepare_root()
    threading.Thread(target=_monitor, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), threaded=True)
