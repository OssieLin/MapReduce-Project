"""Boss (master): tracks jobs/tasks and hands tasks to pulling workers."""
import os
import threading
import time
import uuid

from flask import Flask, jsonify, request

from mapreduce import common, storage

app = Flask(__name__)
lock = threading.Lock()
jobs = {}            # job_id -> job dict (insertion ordered)


def _new_task(job_id, ttype, index, extra=None):
    return {"task_id": f"{ttype}-{index}", "type": ttype, "index": index,
            "state": "PENDING", "worker": None,
            "started": None, "finished": None, "extra": extra or {}}


def _payload(job, task):
    p = {"job_id": job["id"], "task_id": task["task_id"], "type": task["type"],
         "index": task["index"], "num_maps": job["num_maps"],
         "num_reduces": job["num_reduces"],
         "output": job["output"]}
    p.update(task["extra"])
    return p


def _err(msg, code=400):
    return jsonify({"error": msg}), code


@app.post("/submit_job")
def submit_job():
    d = request.get_json(force=True)
    try:
        num_maps, num_reduces = int(d["num_maps"]), int(d["num_reduces"])
        input_path, output = d["input"], d["output"]
        source = d["job_source"]
        if num_maps < 1 or num_reduces < 1:
            raise ValueError("num_maps and num_reduces must be >= 1")
        common.check_job_source(source)
        files = storage.list_files(input_path, suffix=".csv")
        if not files:
            raise ValueError(f"no .csv input files found at {input_path}")
        splits = common.compute_splits(files, num_maps)
    except (KeyError, ValueError, TypeError) as e:
        return _err(str(e))

    job_id = uuid.uuid4().hex[:8]
    storage.write_atomic(f"jobs/{job_id}/job.py", source)
    job = {"id": job_id, "input": input_path, "output": output,
           "num_maps": num_maps, "num_reduces": num_reduces,
           "status": "MAP", "error": None,
           "submitted": time.time(), "finished": None,
           "map_tasks": [_new_task(job_id, "map", i, {"split": s})
                         for i, s in enumerate(splits)],
           "reduce_tasks": []}
    with lock:
        jobs[job_id] = job
    return jsonify({"job_id": job_id})


@app.post("/get_task")
def get_task():
    worker = (request.get_json(force=True) or {}).get("worker_id", "?")
    with lock:
        for job in jobs.values():
            if job["status"] == "MAP":
                tasks = job["map_tasks"]
            elif job["status"] == "REDUCE":
                tasks = job["reduce_tasks"]
            else:
                continue
            for t in tasks:
                if t["state"] == "PENDING":
                    t.update(state="RUNNING", worker=worker,
                             started=time.time(), finished=None)
                    return jsonify({"task": _payload(job, t)})
    return jsonify({"task": None})


@app.post("/task_done")
def task_done():
    d = request.get_json(force=True)
    with lock:
        job = jobs.get(d.get("job_id"))
        if job is None:
            return _err("unknown job", 404)
        ttype, index = d["task_id"].split("-")
        tasks = job["map_tasks" if ttype == "map" else "reduce_tasks"]
        task = tasks[int(index)]
        if task["state"] == "DONE" or job["status"] in ("DONE", "FAILED"):
            return jsonify({"ok": True, "ignored": True})

        if not d.get("success"):
            job.update(status="FAILED", finished=time.time(),
                       error=f"{task['task_id']} failed: {d.get('error')}")
            return jsonify({"ok": True})

        task.update(state="DONE", finished=time.time())
        if job["status"] == "MAP" and all(t["state"] == "DONE" for t in job["map_tasks"]):
            job["reduce_tasks"] = [_new_task(job["id"], "reduce", r)
                                   for r in range(job["num_reduces"])]
            job["status"] = "REDUCE"
        elif job["status"] == "REDUCE" and all(t["state"] == "DONE" for t in job["reduce_tasks"]):
            storage.write_atomic(f"{job['output'].rstrip('/')}/_SUCCESS", "")
            job.update(status="DONE", finished=time.time())
    return jsonify({"ok": True})


@app.get("/job/<job_id>")
def job_status(job_id):
    with lock:
        job = jobs.get(job_id)
        if job is None:
            return _err("unknown job", 404)
        count = lambda ts: sum(t["state"] == "DONE" for t in ts)
        end = job["finished"] or time.time()
        return jsonify({
            "job_id": job_id, "status": job["status"], "error": job["error"],
            "output": job["output"],
            "maps_done": count(job["map_tasks"]), "maps_total": job["num_maps"],
            "reduces_done": count(job["reduce_tasks"]),
            "reduces_total": job["num_reduces"],
            "elapsed": round(end - job["submitted"], 3),
            "tasks": [{k: t[k] for k in ("task_id", "state", "worker",
                                         "started", "finished")}
                      for t in job["map_tasks"] + job["reduce_tasks"]],
        })


if __name__ == "__main__":
    storage.prepare_root()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), threaded=True)
