"""Worker: pull a task from the boss, run it against shared storage, report."""
import json
import os
import socket
import time
import traceback

import requests

from mapreduce import common, storage

BOSS_URL = os.environ.get("BOSS_URL", "http://localhost:5000")
WORKER_ID = os.environ.get("WORKER_ID", socket.gethostname())
_jobs = {}


def _job(job_id):
    if job_id not in _jobs:
        _jobs[job_id] = common.load_job(f"jobs/{job_id}/job.py")
    return _jobs[job_id]


def run_map(task):
    job = _job(task["job_id"])
    R, m = task["num_reduces"], task["index"]
    buffers = [[] for _ in range(R)]
    stats = {}
    for seg in task["split"]:
        for offset, row in common.iter_rows(seg, stats):
            for k, v in job.map_fn(offset, row):
                buffers[common.partition(k, R)].append((k, v))
    base = f"jobs/{task['job_id']}/intermediate"
    for r in range(R):
        with storage.atomic_open(f"{base}/map-{m}-part-{r}.jsonl") as f:
            for k, v in buffers[r]:
                f.write(json.dumps([k, v]) + "\n")
    return stats


def run_reduce(task):
    job = _job(task["job_id"])
    r = task["index"]
    base = f"jobs/{task['job_id']}/intermediate"
    groups = {}
    for m in range(task["num_maps"]):
        with storage.open_read(f"{base}/map-{m}-part-{r}.jsonl") as f:
            for line in f:
                k, v = json.loads(line)
                canon = json.dumps(k, sort_keys=True)
                groups.setdefault(canon, (k, []))[1].append(v)
    items = list(groups.items())
    try:
        items.sort(key=lambda kv: kv[1][0])
    except TypeError:                       # mixed key types
        items.sort(key=lambda kv: kv[0])
    out = f"{task['output'].rstrip('/')}/part-{r}.jsonl"
    with storage.atomic_open(out) as f:
        for _, (k, values) in items:
            for ok, ov in job.reduce_fn(k, iter(values)):
                f.write(json.dumps([ok, ov]) + "\n")
    return {}


def main():
    print(f"worker {WORKER_ID} -> {BOSS_URL}", flush=True)
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
        report = {"job_id": task["job_id"], "task_id": task["task_id"],
                  "worker_id": WORKER_ID}
        try:
            stats = run_map(task) if task["type"] == "map" else run_reduce(task)
            report.update(success=True, stats=stats)
        except Exception:
            report.update(success=False, error=traceback.format_exc(limit=5))
            print(report["error"], flush=True)
        for _ in range(5):
            try:
                requests.post(f"{BOSS_URL}/task_done", json=report, timeout=10)
                break
            except requests.RequestException:
                time.sleep(1)


if __name__ == "__main__":
    main()
