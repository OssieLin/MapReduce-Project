"""Worker: pull a task from the boss, run it against shared storage, report."""
import json
import os
import socket
import threading
import time
import traceback

import requests

from mapreduce import common, storage

BOSS_URL = os.environ.get("BOSS_URL", "http://localhost:5000")
WORKER_ID = os.environ.get("WORKER_ID", socket.gethostname())
HEARTBEAT_SECS = float(os.environ.get("HEARTBEAT_SECS", 2))
SLOW_MS_PER_ROW = float(os.environ.get("SLOW_MS_PER_ROW", 0))   # straggler injection
CHECK_EVERY = 1000   # records between progress updates / cancel checks
_jobs = {}
# {"job_id", "task_id", "attempt_id", "progress"} of the running attempt, read
# by the heartbeat thread. Swapped (never resized) under _current_lock.
_current = {}
_current_lock = threading.Lock()
_cancel = threading.Event()        # set when the boss says our attempt lost


class TaskCancelled(Exception):
    """Another attempt of this task already won; abandon this one."""


def _checkpoint(progress):
    """Publish progress for the heartbeat thread; abort if cancelled."""
    _current["progress"] = round(progress, 4)
    if _cancel.is_set():
        raise TaskCancelled()


def _heartbeat_loop():
    """Runs for the life of the process, idle or busy, independent of task
    execution, so a long CPU-bound map still proves we're alive."""
    while True:
        cur = _current
        try:
            r = requests.post(f"{BOSS_URL}/heartbeat",
                              json={"worker_id": WORKER_ID, **cur},
                              timeout=HEARTBEAT_SECS)
            if r.json().get("cancel"):
                with _current_lock:
                    if _current is cur:     # still the attempt we asked about
                        _cancel.set()
        except (requests.RequestException, ValueError):
            pass                            # retry on the next tick
        time.sleep(HEARTBEAT_SECS)


def _job(job_id):
    """
    Lazy-loads the user's map_fn and reduce_fn from the shared storage.
    Caches it so the worker doesn't re-download the Python file for every task.
    """
    if job_id not in _jobs:
        _jobs[job_id] = common.load_job(f"jobs/{job_id}/job.py")
    return _jobs[job_id]


def _combine(combine_fn, pairs):
    """
    Groups a list of (key, value) pairs by key, then runs the user's combine_fn.
    Uses json.dumps on the key to ensure complex types (like dicts/lists) 
    can be safely hashed as string keys in the 'groups' dictionary.
    """
    groups = {}
    for k, v in pairs:
        groups.setdefault(json.dumps(k, sort_keys=True), (k, []))[1].append(v) # retrieve a key's value if it exists
        # or insert the key with specified value if the value is missing
        # group.setdefault(key_string, (key, value))
        # '{"city": "Madison"}': ( {"city": "Madison"}, [ {"sum": 40.0, "count": 1, "max": 40.0} ] )
    return [kv for k, values in groups.values() for kv in combine_fn(k, iter(values))]


def run_map(task):
    """
    Executes the Map phase for a specific byte-range split.
    1. Reads raw bytes and yields complete rows.
    2. Calls user's map_fn.
    3. Hashes keys to partition data for reducers.
    4. Periodically combines data to save memory.
    5. Spills intermediate files to disk.
    """
    job = _job(task["job_id"])
    R, m = task["num_reduces"], task["index"]

    # Load combiner if enabled by the job submitter
    combine_fn = getattr(job, "combine_fn", None) if task.get("use_combiner") else None
    if not callable(combine_fn):
        combine_fn = None

    # Memory management: Create R separate buckets, one for each reducer
    limit = int(os.environ.get("COMBINE_BUFFER", 200000))
    buffers = [[] for _ in range(R)]
    stats = {"skipped_rows": 0, "map_input_records": 0, "map_output_records": 0}
    buffered, flush_at = 0, limit
    split_bytes = sum(seg["end"] - seg["start"] for seg in task["split"]) or 1
    consumed = 0

    # task["split"] contains the byte-range(s) assigned by the Boss
    for seg in task["split"]:
        _checkpoint(consumed / split_bytes)
        # iter_rows handles the CSV byte boundaries (skipping partial lines)
        for offset, row in common.iter_rows(seg, stats):
            stats["map_input_records"] += 1
            if SLOW_MS_PER_ROW:
                time.sleep(SLOW_MS_PER_ROW / 1000)
            if SLOW_MS_PER_ROW or stats["map_input_records"] % CHECK_EVERY == 0:
                _checkpoint((consumed + max(offset - seg["start"], 0)) / split_bytes)

            # Execute the user's map function
            for k, v in job.map_fn(offset, row):
                buffers[common.partition(k, R)].append((k, v))
                stats["map_output_records"] += 1
                buffered += 1

            # If in-memory buffers get too large, pause and run the combiner
            if combine_fn and buffered >= flush_at:   # bound memory mid-split
                buffers = [_combine(combine_fn, b) for b in buffers]
                buffered = sum(map(len, buffers))
                flush_at = buffered + limit           # no re-combine every row if keys are unique
        consumed += seg["end"] - seg["start"]
    _checkpoint(1.0)

    # Final combine pass before writing to disk
    if combine_fn:
        buffers = [_combine(combine_fn, b) for b in buffers]
        stats["combine_input_records"] = stats["map_output_records"]
        stats["combine_output_records"] = sum(map(len, buffers))

    # Spill partitioned intermediate data to shared storage
    base = f"jobs/{task['job_id']}/intermediate"
    nbytes = 0
    for r in range(R):
        with storage.atomic_open(f"{base}/map-{m}-part-{r}.jsonl") as f:
            for k, v in buffers[r]:
                line = json.dumps([k, v]) + "\n"
                f.write(line)
                nbytes += len(line.encode("utf-8"))
    stats["intermediate_bytes"] = nbytes
    return stats


def run_reduce(task):
    """
    Executes the Reduce phase for a specific partition.
    1. Reads intermediate files generated by ALL mappers for this partition.
    2. Groups and sorts the data by key.
    3. Calls user's reduce_fn.
    4. Writes final output.
    """
    job = _job(task["job_id"])
    r = task["index"]
    base = f"jobs/{task['job_id']}/intermediate"
    groups = {}
    stats = {"shuffle_bytes": 0, "reduce_input_records": 0, "reduce_output_records": 0}

    M = task["num_maps"]

    # SHUFFLE: Read the intermediate file from every single map task (0 to M-1)
    # progress = 0.5 * files read / M + 0.5 * keys reduced / unique keys
    for m in range(M):
        _checkpoint(0.5 * m / M)
        with storage.open_read(f"{base}/map-{m}-part-{r}.jsonl") as f:
            for line in f:
                stats["shuffle_bytes"] += len(line)
                stats["reduce_input_records"] += 1
                if stats["reduce_input_records"] % CHECK_EVERY == 0:
                    _checkpoint(0.5 * m / M)
                k, v = json.loads(line)
                canon = json.dumps(k, sort_keys=True)
                groups.setdefault(canon, (k, []))[1].append(v)
    items = list(groups.items())

    # Sort keys to guarantee deterministic output order
    try:
        items.sort(key=lambda kv: kv[1][0])
    except TypeError:                       # mixed key types
        items.sort(key=lambda kv: kv[0])

    # Write the final output to the user's requested output directory   
    out = f"{task['output'].rstrip('/')}/part-{r}.jsonl"
    with storage.atomic_open(out) as f:
        for i, (_, (k, values)) in enumerate(items):
            _checkpoint(0.5 + 0.5 * i / len(items))
            for ok, ov in job.reduce_fn(k, iter(values)):
                f.write(json.dumps([ok, ov]) + "\n")
                stats["reduce_output_records"] += 1
    stats["reduce_input_groups"] = len(items)
    return stats


def main():
    """
    The main worker loop. Continuously polls the Boss for tasks, executes them,
    and reports success or failure back to the Boss.
    """
    global _current
    print(f"worker {WORKER_ID} -> {BOSS_URL}", flush=True)
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    while True:
        try:
            task = requests.post(f"{BOSS_URL}/get_task", json={"worker_id": WORKER_ID},
                                 timeout=10).json()["task"]
        except (requests.RequestException, ValueError, KeyError):
            time.sleep(1)
            continue
        if task is None:
            time.sleep(0.3)
            continue
        print(f"running {task['job_id']}/{task['task_id']}", flush=True)
        ids = {k: task.get(k) for k in ("job_id", "task_id", "attempt_id")}
        report = dict(ids, worker_id=WORKER_ID)
        with _current_lock:
            _current = dict(ids, progress=0.0)
            _cancel.clear()
        try:
            stats = run_map(task) if task["type"] == "map" else run_reduce(task)
            report.update(success=True, stats=stats)
        except TaskCancelled:
            # another attempt won; atomic_open already removed our temp file
            print(f"cancelled {task['job_id']}/{task.get('attempt_id')}", flush=True)
            with _current_lock:
                _current = {}
            continue
        except Exception:
            report.update(success=False, error=traceback.format_exc(limit=5))
            print(report["error"], flush=True)
        with _current_lock:
            _current = {}
        for _ in range(5):
            try:
                requests.post(f"{BOSS_URL}/task_done", json=report, timeout=10)
                break
            except requests.RequestException:
                time.sleep(1)


if __name__ == "__main__":
    main()
