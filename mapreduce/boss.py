"""Boss (master): tracks jobs/tasks and hands tasks to pulling workers."""
import json
import math
import os
import statistics
import threading
import time
import uuid

from flask import Flask, jsonify, request, send_from_directory

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

# Snapshots of finished jobs read from shared/jobs/<id>/state.json at start, so the
# visualization still lists them after a boss restart. Read-only after loading.
history = []
HISTORY = 6          # jobs shown by /api/state: the current one + 5 previous
VISUALIZATION_DIR = os.environ.get("VISUALIZATION_DIR", os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "visualization"))

# Backup tasks for stragglers (only for jobs submitted with -B).
BACKUP_CFG = {
    "min_done": float(os.environ.get("BACKUP_MIN_DONE", 0.25)),        # fraction of phase DONE
    "slowdown": float(os.environ.get("BACKUP_SLOWDOWN", 1.5)),         # x median duration
    "min_seconds": float(os.environ.get("BACKUP_MIN_SECONDS", 2)),
    "max_fraction": float(os.environ.get("BACKUP_MAX_FRACTION", 0.10)),  # of all tasks
}


def now():
      """The boss's only clock: all timestamps and timeouts use it, so tests can
    replace it with a fake clock and skip ahead instead of really waiting."""
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
    """
    Generate a unique 'attempt_id' (e.g., map-0.a0, map-0.a1). We append this to the task's attempt list.
    This allows the Boss to track the history of exactly who tried this task, 
    when they started, and whether this specific attempt is a 'backup' copy.
    """
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
    """
    Runs in a background thread every 1 second. It loops over all RUNNING attempts.
    If 'now - last_seen > HEARTBEAT_TIMEOUT' (default 10s), the worker is declared dead.
    """
    reset = []
    for job in jobs.values():
        for t in _active_tasks(job):
            for a in _running(t):
                w = workers.get(a["worker"])

                # max() covers a worker that dies before its first heartbeat
                # Calculate time since last heartbeat
                silent = now - max(a["started"], w["last_seen"] if w else 0)

                if silent > HEARTBEAT_TIMEOUT:
                    if w:
                        w["status"] = "failed"
                    # _end_attempt contains logic to flip the overall task state 
                    # back to 'PENDING' so another healthy worker can pick it up!
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
    """
    Return the RUNNING task of this phase that needs to hand off its work to other worker on
    worker_id, or None,the most because of stragglin. `launched`/`total` are the job's backups so far and its
    task count, for the cap. Pure: reads task dicts only.
    """
    if not tasks or any(t["state"] == "PENDING" for t in tasks):
        return None # don't choose a backup if the task hasn't started yet

    total = len(tasks) if total is None else total # total number of tasks in this job

    if launched >= max(1, math.ceil(cfg["max_fraction"] * total)):
        return None # hit the maximum of backup tasks


    median = _phase_median(tasks)
    done = sum(t["state"] == "DONE" for t in tasks)
    if median is None or done < cfg["min_done"] * len(tasks):
        return None
    best, best_est = None, -1.0 # actual dictionary of the taks and the estimated time
    for t in tasks:
        running = _running(t)
        if (t["state"] != "RUNNING" or len(running) != 1
                or any(a["backup"] for a in t["attempts"])):     # max one backup per task
            continue
        a = running[0]
        elapsed, _, est = _estimate(a, now)


        # STRAGGLER CONDITIONS:
        # 1. Has it been running 1.5x longer than the median task? (cfg["slowdown"])
        # 2. Is its estimated time remaining (est) STILL longer than a median task?
        # 3. is the estimated tim remaining still larger than the median 
        # 4. Is the esimated worse than than any straggler
        # If yes, a fresh worker starting from 0% could finish it faster than this slow worker.
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

    If no PENDING tasks are left, the Boss checks if it should spawn a backup.
    If pick_backup returns a slow task, it assigns that SAME task to this new worker, 
    but with backup=True. Now two workers are running the same byte-split.
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
# d now equals {"worker_id": "worker-1", "job_id": "abc", "task_id": "map-0", "success": True, "stats": {...}}

    worker = d.get("worker_id", "?")
    finished = False
    snap = None          # snapshot of a job that just ended, saved after the lock

    """
    Worker A and Worker B are running "map-0". Worker B finishes first and calls task_done.
    """
    with lock:
        # 1. Idempotency Guard: If the overall task is already DONE, ignore this callback.
        # This happens when the slow worker finally finishes and reports back late.
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
            snap = _snapshot(job)

        else:
            # Mark the individual task as completed: first finisher wins. A success
            # from a worker we presumed dead is accepted too: output files are
            # deterministic and atomically renamed, so they're complete.
            _end_attempt(job, task, a, "WON")
            task.update(state="DONE", worker=a["worker"], finished=a["finished"],
                        stats=d.get("stats") or {})
            for other in _running(task):
                _end_attempt(job, task, other, "CANCELLED")

            # BARRIER 1: If in MAP phase, check if ALL map tasks are now DONE
            # MAP -> REDUCE
            if job["status"] == "MAP" and all(t["state"] == "DONE" for t in job["map_tasks"]):
                job["reduce_tasks"] = [_new_task(job["id"], "reduce", r)
                                       for r in range(job["num_reduces"])]
                job["status"] = "REDUCE"

            # BARRIER 2: If in REDUCE phase, check if ALL reduce tasks are now DONE
            # REDUCE -> DONE
            elif job["status"] == "REDUCE" and all(t["state"] == "DONE" for t in job["reduce_tasks"]):
                storage.write_atomic(f"{job['output'].rstrip('/')}/_SUCCESS", "")
                job.update(status="DONE", finished=now())
                finished = True
                snap = _snapshot(job)
    if snap:
        _save_snapshot(snap)
    if finished:
        # killed workers may have left .tmp-* files behind (outside the lock)
        storage.remove_temp_files(job["output"])
        storage.remove_temp_files(f"jobs/{job['id']}/intermediate")
    return jsonify({"ok": True})


@app.post("/heartbeat")
def heartbeat():
    """
    Workers hit this endpoint constantly.
    1. It updates `_touch(worker)` which records 'last_seen = now()'.
    2. It updates the specific attempt's 'progress' (e.g., 0.45 for 45% done).
    3. Crucially, if the Boss previously cancelled this attempt (because a faster 
       backup worker won the race), it returns 'cancel: True' to tell this worker to stop.
    """

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
            # Tell the worker to abort if it lost the race or the job failed
            cancel = a["status"] in ("CANCELLED", "LOST", "FAILED") or job["status"] == "FAILED"
    return jsonify({"ok": True, "cancel": cancel})


def _workers_list(t):
    return [{"worker_id": wid, "status": w["status"],
             "last_seen_age": round(t - w["last_seen"], 1),
             "task": "/".join(w["task"]) if w["task"] else None}
            for wid, w in sorted(workers.items())]


@app.get("/workers")
def list_workers():
    with lock:
        return jsonify({"workers": _workers_list(now())})


# ----------------------------------------------------------- visualization
ATTEMPT_FIELDS = ("attempt_id", "worker", "started", "finished", "progress", "backup", "status")


def _snapshot(job):
    """JSON-safe copy of a job for /api/state and state.json. Call under `lock`;
    copies everything, so the result can be used after the lock is released."""
    def task(t):
        s = {"task_id": t["task_id"], "type": t["type"], "index": t["index"],
             "state": t["state"],
             "stats": {k: list(v) if isinstance(v, list) else v for k, v in t["stats"].items()},
             "attempts": [{k: a[k] for k in ATTEMPT_FIELDS} for a in t["attempts"]]}
        if t["type"] == "map":
            s["split_bytes"] = sum(seg["end"] - seg["start"] for seg in t["extra"]["split"])
        return s
    return {"id": job["id"], "input": job["input"], "output": job["output"],
            "M": job["num_maps"], "R": job["num_reduces"],
            "use_combiner": job["use_combiner"], "use_backup": job["use_backup"],
            "status": job["status"], "error": job["error"],
            "submitted": job["submitted"], "finished": job["finished"],
            "tasks": [task(t) for t in job["map_tasks"] + job["reduce_tasks"]]}


def _save_snapshot(snap):
    """Persist a finished job (outside the lock) so it survives a boss restart."""
    try:
        storage.write_atomic(f"jobs/{snap['id']}/state.json", json.dumps(snap))
    except OSError as e:
        print(f"could not save state of job {snap['id']}: {e}", flush=True)


def load_history():
    """Load the latest HISTORY state.json files (newest by write time) into `history`."""
    global history
    found = []
    try:
        names = os.listdir(storage.resolve("jobs"))
    except OSError:
        names = []
    for n in names:
        path = storage.resolve(f"jobs/{n}/state.json")
        try:
            found.append((os.path.getmtime(path), path))
        except OSError:
            pass
    snaps = []
    for _, path in sorted(found)[-HISTORY:]:
        try:
            with open(path, encoding="utf-8") as f:
                snaps.append(json.load(f))
        except (OSError, ValueError):
            pass
    snaps.sort(key=lambda s: s.get("submitted") or 0)
    with lock:
        history = snaps


@app.get("/api/state")
def api_state():
    """Workers plus the current job and up to 5 previous ones (oldest first)."""
    with lock:
        t = now()
        ws = _workers_list(t)
        live = [_snapshot(j) for j in list(jobs.values())[-HISTORY:]]
        hist = list(history)
    merged = {s["id"]: s for s in hist}
    merged.update({s["id"]: s for s in live})             # live wins on equal id
    shown = sorted(merged.values(), key=lambda s: s.get("submitted") or 0)[-HISTORY:]
    return jsonify({"now": t, "workers": ws, "jobs": shown})


@app.get("/visualization")
@app.get("/visualization/")
def visualization():
    return send_from_directory(VISUALIZATION_DIR, "index.html")


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
    load_history()
    threading.Thread(target=_monitor, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), threaded=True)
