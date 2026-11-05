# Design Doc: MapReduce (Honors Project 1)

**Team:** _<Eric>_, _<Osmond>_
**Last updated:** Nov 6, 2026

## 1. Overview

We are building a small MapReduce system that runs on a single VM with Docker Compose. The architecture follows the master/worker design from the original MapReduce paper: one **boss** (master) container coordinates four **worker** containers, each limited to 1 CPU, so performance improves when tasks are spread across all workers.

- **Input:** CSV files (with a header row)
- **Output:** JSON
- **User jobs:** a Python file defining `map_fn` and `reduce_fn` (and optionally `combine_fn`)
- **Special features:** (1) **combiners**, (2) **worker failure handling**
<img width="1840" height="1200" alt="architecture" src="https://github.com/user-attachments/assets/864364c4-fe66-446f-9009-29013eec1380" />



## 2. Client Interface

The client is two command-line tools that run on the host, outside any container. Running either tool with no flags prints its usage message and exits with code 2.

### `upload_data`: upload starter data to shared storage

```
usage: ./upload_data -f <local_path> -d <storage_path> [-t <type>]

Upload a local file or directory to shared storage.

required:
  -f, --file <local_path>     local file or directory to upload
  -d, --dest <storage_path>   destination path in shared storage
                              (e.g. /input/sales)
optional:
  -t, --type <type>           input data type; only "csv" is
                              supported (default: csv)
  -h, --help                  show this message
```

Example:

```bash
./upload_data -f examples/data/weather/ -d /input/weather
```

### `submit`: submit a MapReduce job

```
usage: ./submit -i <input_path> -o <output_path> -j <job_file.py>
                -m <num_maps> -r <num_reduces> [-C]

Submit a MapReduce job and wait for it to finish.

required:
  -i, --input <input_path>      input CSV file or directory in shared storage
  -o, --output <output_path>    output location in shared storage (JSON)
  -j, --job <job_file.py>       file containing map_fn and reduce_fn
  -m, --maps <num_maps>         number of map tasks
  -r, --reduces <num_reduces>   number of reduce tasks
optional:
  -C, --combiner                enable the combiner (default: off)
  -h, --help                    show this message
```

Example:

```bash
./submit -i /input/weather -o /output/avg_temp -j examples/avg_temp.py -m 8 -r 4 -C
```

Input must be CSV and output is JSON. All paths are relative to shared storage (`./shared` on the host, `/shared` in containers).

### Writing a job

A job is a Python file with these functions:

```python
# examples/avg_temp.py
def map_fn(key, row):
    """key = byte offset; row = {"city": ..., "date": ..., "temp": ...}.
    All CSV values arrive as strings. Yield (k, v) pairs."""
    yield row["city"], (float(row["temp"]), 1)        # (sum, count)

def reduce_fn(key, values):
    """values = iterable of all values for this key. Yield (k, v) pairs."""
    total, count = 0.0, 0
    for s, c in values:
        total += s
        count += c
    yield key, total / count

# Optional: used when -C is passed
def combine_fn(key, values):
    total, count = 0.0, 0
    for s, c in values:
        total += s
        count += c
    yield key, (total, count)
```

The combiner emits `(sum, count)` pairs rather than averages, because averages are not associative. The combiner's output must be something `reduce_fn` can still process correctly.

## 3. Language

**Python 3.11** for the boss, workers, and both client tools. User-supplied map and reduce functions are Python, so workers can load them directly with `importlib`.

## 4. Communication and Architecture

We replicate the **master/worker architecture** from the MapReduce paper (the figure in our brainstorm): a single boss (master) assigns map and reduce tasks to workers, and workers read and write data through shared storage.

| Path | Mechanism |
|---|---|
| client -> boss | HTTP + JSON (`POST /submit_job`, `GET /job/<id>`) |
| worker -> boss | HTTP + JSON: `POST /get_task`, `POST /task_done`, `POST /heartbeat` |
| worker <-> worker | none directly; workers exchange data only through shared storage |

Flask on the boss, `requests` in workers and client. HTTP/JSON is easy to debug with `curl` and needs no code generation. Messages are tiny (task descriptions, not data).

**Pull-based scheduling:** workers ask the boss for work (`/get_task`) instead of the boss pushing tasks to them. The boss never needs worker addresses, load balancing is automatic (faster workers ask more often), and failure handling is simpler.

**Boss responsibilities:**
- Split input into `num_maps` map tasks.
- Hand out tasks and track their state: `PENDING -> RUNNING -> DONE`.
- Start the reduce phase only after **all** map tasks finish.
- Detect failed workers and reassign their tasks (Section 8).
- Write a `_SUCCESS` marker when the job completes.

**Worker responsibilities:** loop forever: request a task, run it, report back. Workers are stateless; everything durable is in shared storage.

## 5. Shared Storage

A **shared volume mount** across all containers (`./shared` on the host, `/shared` in every container):

```yaml
volumes:
  - ./shared:/shared
```

This is the simplest correct choice for a single-VM demo, and the host-side client tools can read and write `./shared` directly, so "uploading" data is just a file copy.

Layout:

```
shared/
  input/<dataset>/...                      # uploaded CSV files
  jobs/<job_id>/
    job.py                                 # copy of the user's job file
    intermediate/map-<m>-part-<r>.jsonl    # map task m's output for reducer r
  output/<name>/part-<r>.json[l]           # final output, one file per reducer
  output/<name>/_SUCCESS
```

**Atomic writes:** every task writes to a temp file (`.tmp-<uuid>`) and then `os.rename()`s it to its final name. Readers never see half-written files, and running a task twice (after a failure or a duplicate assignment) is safe because the final contents are identical.

**Alternatives considered:** HDFS would enable data-locality scheduling and replication, but it adds significant setup complexity (NameNode, DataNodes, and host-client networking problems), competes with the 1-CPU worker limit, and offers little real benefit on a single VM, where all data is on the same disk. To keep the option open, all task code accesses storage through a small helper module (`list`, `read_range`, `write_atomic`) rather than calling `open()` directly.

## 6. Data Formats

| Data | Format |
|---|---|
| Input | CSV with a header row; each row is passed to `map_fn` as a dict keyed by column name |
| Intermediate | JSON Lines: one `[key, value]` per line |
| Output | JSON Lines: one `[key, value]` per line, sorted by key within each reducer's output file |

Keys and values must be JSON-serializable. All CSV values arrive as strings, and the user's `map_fn` converts types itself.

### Input splitting

The boss divides the total input bytes into `num_maps` byte ranges (ranges may span files, and a large file may be split into several ranges). A map task that doesn't start at byte 0 skips its first partial line, and every task reads through the end of the line crossing its end boundary, so each row is processed exactly once.

### CSV header handling

The boss reads the first line of each input file once when creating splits and includes that file's column names in the map task description. Only the task starting at byte 0 of a file skips the header line. Tasks starting mid-file use the column names from their task description. Rows are parsed with Python's `csv` module, so quoted fields containing commas work. Rows with the wrong number of columns are skipped and counted in a per-task `skipped_rows` counter.

**Restriction:** CSV fields may not contain embedded newlines (it would break byte-range splitting).

### Partitioning

`partition(key) = md5(json.dumps(key)) mod num_reduces`. We don't use Python's built-in `hash()` because it is randomized per process, which would send the same key to different reducers from different workers.

## 7. Special Feature 1: Combiners

An optional `combine_fn` (enabled with `-C`) that each map task applies to its own output **before writing intermediate files**. For example, instead of writing `("Madison", (18.5, 1))` a million times, a map task writes `("Madison", (sum, count))` once.

How it works:
1. The map task runs `map_fn` over its split and groups output in memory by partition and key.
2. If `-C` was passed, it runs `combine_fn(key, values)` for each key to collapse the values.
3. It writes the smaller result to `intermediate/map-<m>-part-<r>.jsonl`.

To bound memory, the map task also combines whenever its buffer exceeds a size limit (e.g. 200k entries). The reducer still applies `reduce_fn` to everything it reads, so correctness never depends on the combiner having run on all data. **Requirement:** the final output must be identical with and without the combiner (we test this).

## 8. Special Feature 2: Worker Failure Handling

The boss must detect a worker that dies mid-job and make sure its work is redone elsewhere so the final output is still correct.

**Detection:** each worker sends a heartbeat to the boss every few seconds while running a task. If the boss records a `RUNNING` task whose worker has missed heartbeats for longer than a timeout (e.g. 10 s), it marks the worker as failed.

**Recovery:**
- Tasks assigned to the failed worker go back to `PENDING` and are picked up by the remaining workers.
- Late results from a worker that was wrongly presumed dead are harmless: output filenames are deterministic and written with atomic rename, so duplicate execution just overwrites a file with identical contents. The boss ignores a `task_done` for a task that is already `DONE`.
- Unlike the original paper, **completed map tasks of a failed worker do not need to be re-run**, because our intermediate files are on shared storage rather than the worker's local disk.

**Demo and test:** run the job normally and save the output. Run it again and `docker kill` one worker halfway through. The job should still finish, with output identical to the first run.

## 9. Task Lifecycle

1. The client sends the job to the boss. The boss copies the job file to `shared/jobs/<id>/job.py`, computes input splits, and creates `M` map tasks.
2. Workers call `/get_task`. The boss returns a task like `{job_id, task_id, type: "map", split, csv_columns, num_reduces, use_combiner}`.
3. The worker runs the map task, writes intermediate files, and calls `/task_done`.
4. When all `M` map tasks are `DONE`, the boss creates `R` reduce tasks.
5. A reduce task reads `map-*-part-<r>.jsonl` from all map tasks, groups by key, calls `reduce_fn`, and writes its output file.
6. When all `R` reduce tasks are `DONE`, the boss writes `_SUCCESS` and marks the job complete. The `submit` tool reports the output location.

The boss guards its shared state with a lock, since Flask may serve requests from multiple threads.

## 10. Docker Compose Deployment

- `boss`: one container, exposes a port to the host for the client tools.
- `worker1..worker4`: same image, each with `cpus: "1"`, so using all four yields real speedup on the 4-CPU VM.
- All services mount `./shared:/shared`. Workers find the boss at `http://boss:5000` through Compose networking.

## 11. Testing Plan

We test **correctness** and **performance** (latency and throughput).

### Unit tests (pytest, run in CI on every PR)
- Partition function is deterministic and stays in `[0, R)`.
- Input splitting: every line is processed exactly once across all splits (including boundaries that fall mid-line, and files with no trailing newline).
- Combiner: output equals no-combiner output on random inputs.
- Atomic file-writing helper.
- Loading a user job file (valid, missing `map_fn`, missing `combine_fn`).
- CSV parsing: header skipped only by the task starting at byte 0; mid-file tasks use the supplied column names; quoted fields with commas; malformed rows skipped and counted.
- CSV splitting: every data row is processed exactly once across all splits, with boundaries falling mid-row, at the header, and between files.
- Client CLIs: no flags prints usage and exits with code 2.

### Integration tests (against a small `docker compose` cluster, in CI or locally)
- Job on a tiny CSV dataset; compare to a plain single-process Python implementation.
- Edge cases: empty input file, `reduces=1`, `maps` greater than the number of rows, unicode keys.
- Combiner on vs. off produce identical output.
- Kill a worker container mid-job; the job still finishes with correct output.

## 12. Performance Evaluation

All experiments run on the 4-CPU VM using a generated CSV dataset (millions of rows). Each configuration is run 3 times and we plot the mean with error bars. The boss logs timestamps (job submit, task start/end, job done) so plots are generated reproducibly by `eval/plot.py`.

**Metrics:** *latency* = end-to-end job completion time; *throughput* = input MB processed per second.

| # | Experiment | Plot | What it shows |
|---|---|---|---|
| A | **Baseline:** plain single-process Python implementation vs. our system | bar chart of runtime | reference point for everything else |
| B | **Combiner off vs. on** (repetitive-key job, e.g. avg temp per city) | bars for runtime and intermediate bytes written | the combiner makes repetitive jobs faster and shrinks intermediate data |
| B2 | Same, with a nearly-unique-key job as a control | same bars | combiner helps little when keys are not repeated (honest data point) |
| C | **Scaling:** 1, 2, 3, 4 active workers | line chart of runtime and throughput vs. workers, with an ideal-speedup line | work is truly distributed across CPU-limited workers |
| D | **Fault tolerance:** normal run vs. run with a worker killed halfway | runtime bars, plus a check that outputs are byte-identical | failures are detected, tasks are reassigned, and the output stays correct (with the extra runtime cost shown) |
| E | **Task granularity:** vary `num_maps` (1, 2, 4, 8, 16, 32) | line chart of runtime | load-balancing benefit vs. per-task overhead |

## 13. Repo Layout

```
.
├── README.md              # build / install / upload / write a job / submit
├── design.md              # this file
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── upload_data            # client CLI (executable)
├── submit                 # client CLI (executable)
├── mapreduce/
│   ├── boss.py
│   ├── worker.py
│   ├── storage.py         # list / read_range / write_atomic helpers
│   └── common.py          # partitioning, splitting, CSV parsing, job loading
├── examples/
│   ├── data/weather/      # small example CSV with a header row
│   └── avg_temp.py
├── tests/
├── eval/                  # data generator, benchmark and plot scripts, results
└── .github/workflows/     # CI running pytest on pull requests
```

## 14. Planned PR sequence
1. Repo skeleton, Dockerfile, Compose with boss + 4 workers, CI running pytest
2. Common utilities (partitioning, splitting, CSV parsing, atomic writes, job loading) + unit tests
3. Boss scheduling (submit job, map tasks, `get_task`, `task_done`)
4. Worker map execution
5. Reduce phase + job completion
6. Client CLIs (`upload_data`, `submit`) with usage messages and tests
7. Example job + integration test + README
8. Combiner support + tests
9. Worker failure handling (heartbeats, reassignment) + kill-a-worker test
10. Evaluation scripts and plots

## 15. Limitations

- Single boss with no fault tolerance for the boss itself.
- Input must be CSV with a header row, and fields may not contain embedded newlines.
- Keys and values must be JSON-serializable, and CSV values arrive as strings.
- Shared storage is a local bind mount, so there is no data-locality optimization.
