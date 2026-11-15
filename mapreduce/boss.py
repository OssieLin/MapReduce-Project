"""Boss (master): tracks jobs/tasks and hands tasks to pulling workers."""
import os
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


def _new_task(job_id, ttype, index, extra=None):
    return {"task_id": f"{ttype}-{index}", "type": ttype, "index": index,
            "state": "PENDING", "worker": None,
            "started": None, "finished": None, "stats": {}, "extra": extra or {}}


def _payload(job, task):
    p = {"job_id": job["id"], "task_id": task["task_id"], "type": task["type"],
         "index": task["index"], "num_maps": job["num_maps"],
         "num_reduces": job["num_reduces"],
         "output": job["output"], "use_combiner": job["use_combiner"]}
    p.update(task["extra"])
    return p


def _err(msg, code=400):
    return jsonify({"error": msg}), code


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
           "use_combiner": use_combiner,
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
    """
    Endpoint for workers to request work. 
    Scans active jobs for PENDING tasks and assigns the first one found.
    """
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
    """
    Endpoint for workers to report task completion (success or failure).
    Also handles phase transitions (MAP -> REDUCE -> DONE).
    """
    d = request.get_json(force=True)
    with lock:
        job = jobs.get(d.get("job_id"))
        if job is None:
            return _err("unknown job", 404)

        # Parse "map-0" into ttype="map", index=0
        ttype, index = d["task_id"].split("-")
        tasks = job["map_tasks" if ttype == "map" else "reduce_tasks"]
        task = tasks[int(index)]

        # Idempotency guard: ignore duplicate callbacks
        if task["state"] == "DONE" or job["status"] in ("DONE", "FAILED"):
            return jsonify({"ok": True, "ignored": True})
        
        # If a worker reports a crash/error, fail the entire job immediately
        if not d.get("success"):
            job.update(status="FAILED", finished=time.time(),
                       error=f"{task['task_id']} failed: {d.get('error')}")
            return jsonify({"ok": True})
    

        # Mark the individual task as completed
        task.update(state="DONE", finished=time.time(), stats=d.get("stats") or {})

        # BARRIER 1: If in MAP phase, check if ALL map tasks are now DONE
        if job["status"] == "MAP" and all(t["state"] == "DONE" for t in job["map_tasks"]):
            job["reduce_tasks"] = [_new_task(job["id"], "reduce", r)
                                   for r in range(job["num_reduces"])]
            job["status"] = "REDUCE"

        # BARRIER 2: If in REDUCE phase, check if ALL reduce tasks are now DONE
        elif job["status"] == "REDUCE" and all(t["state"] == "DONE" for t in job["reduce_tasks"]):
            storage.write_atomic(f"{job['output'].rstrip('/')}/_SUCCESS", "")
            job.update(status="DONE", finished=time.time())
    return jsonify({"ok": True})


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
        counters = {}
        for t in job["map_tasks"] + job["reduce_tasks"]:
            if t["state"] == "DONE":
                for k, v in t["stats"].items():
                    if isinstance(v, (int, float)) and not isinstance(v, bool):
                        counters[k] = counters.get(k, 0) + v
        end = job["finished"] or time.time()
        return jsonify({
            "job_id": job_id, "status": job["status"], "error": job["error"],
            "output": job["output"],
            "maps_done": count(job["map_tasks"]), "maps_total": job["num_maps"],
            "reduces_done": count(job["reduce_tasks"]),
            "reduces_total": job["num_reduces"],
            "elapsed": round(end - job["submitted"], 3),
            "use_combiner": job["use_combiner"], "counters": counters,
            "tasks": [{k: t[k] for k in ("task_id", "state", "worker",
                                         "started", "finished")}
                      for t in job["map_tasks"] + job["reduce_tasks"]],
        })


if __name__ == "__main__":
    storage.prepare_root()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), threaded=True)
