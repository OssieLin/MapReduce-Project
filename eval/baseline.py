#!/usr/bin/env python3
"""Plain single-process reference for experiment A: run a job's map_fn and
reduce_fn over every CSV in a directory, with no partitioning, intermediate
files or workers. Prints {"seconds": ...} (read + map + reduce + write).

usage: baseline.py <input_dir> <job.py> <output_dir>
"""
import csv
import glob
import importlib.util
import json
import os
import sys
import time


def run(input_dir, job_path, output_dir):
    spec = importlib.util.spec_from_file_location("job", job_path)
    job = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(job)

    t0 = time.perf_counter()
    groups = {}
    for path in sorted(glob.glob(os.path.join(input_dir, "**", "*.csv"), recursive=True)):
        with open(path, newline="", encoding="utf-8") as f:
            for i, row in enumerate(csv.DictReader(f)):
                for k, v in job.map_fn(i, row):
                    groups.setdefault(json.dumps(k, sort_keys=True), (k, []))[1].append(v)
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "part-0.jsonl"), "w", encoding="utf-8") as f:
        for _, (k, values) in sorted(groups.items()):
            for ok, ov in job.reduce_fn(k, iter(values)):
                f.write(json.dumps([ok, ov]) + "\n")
    return time.perf_counter() - t0


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    os.umask(0)          # run as root in a container; keep output deletable by the host user
    print(json.dumps({"seconds": round(run(*sys.argv[1:]), 3)}))
