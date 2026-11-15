# MapReduce

A small MapReduce system that runs on one VM with Docker Compose: one **boss**
and four **workers** (1 CPU each). See `design.md` for the design.


## 1. What to build
```bash
docker compose build
```
No protobufs or code generation are needed.

## 2. What to install (client, on the host)
Python 3.11+ and `requests`:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt   # or: pip install requests
```

## 3. Start the cluster
```bash
docker compose up -d          # boss on localhost:5000, workers 1-4
docker compose logs -f        # optional
```
Shared storage is `./shared` on the host, `/shared` in every container.

If port 5000 is already taken (macOS AirPlay Receiver, another project), use another port:
```bash
BOSS_PORT=5001 docker compose up -d
export BOSS_URL=http://localhost:5001     # tells the client tools where the boss is
```

## 4. Upload example data
```bash
python3 upload_data -f examples/data/weather/ -d /input/weather
```

## 5. Write a job
A job is a Python file with `map_fn` and `reduce_fn`. Complete example: `examples/avg_temp.py`.
```python
def map_fn(key, row):          # key = byte offset, row = dict of strings
    yield row["city"], (float(row["temp"]), 1)

def reduce_fn(key, values):    # values = iterable of all values for key
    total = count = 0
    for s, c in values:
        total += s; count += c
    yield key, total / count
```
Keys/values must be JSON-serializable.

Optionally define `combine_fn(key, values)` (same signature as `reduce_fn`) and pass
`-C` to `submit`: each map task pre-aggregates its own output before writing it.
Its output values must be valid input to both `combine_fn` and `reduce_fn` (yield
`(sum, count)`, not an average), and it may run zero, one or several times per key.
In `examples/avg_temp.py`:
```python
def combine_fn(key, values):
    total = count = 0
    for s, c in values:
        total += s; count += c
    yield key, (total, count)
```

## 6. Submit it
```bash
python3 ./submit -i /input/weather -o /output/avg_temp -j examples/avg_temp.py -m 4 -r 2
cat shared/output/avg_temp/part-*.jsonl
# same job with the combiner (output is identical):
python3 ./submit -i /input/weather -o /output/avg_temp_c -j examples/avg_temp.py -m 4 -r 2 -C
```
| Flag | Required | Meaning |
|---|---|---|
| `-i, --input <path>` | yes | CSV file or directory in shared storage, e.g. `/input/weather` |
| `-o, --output <path>` | yes | Output directory in shared storage; **must not exist yet** |
| `-j, --job <file.py>` | yes | Local job file defining `map_fn` and `reduce_fn` |
| `-m, --maps <M>` | yes | Number of map tasks (input is split into M byte ranges) |
| `-r, --reduces <R>` | yes | Number of reduce tasks = number of output files |
| `-C, --combiner` | no | Run `combine_fn` on each map task's output before the shuffle |
| `-B, --backup` | no | Launch backup copies of straggling tasks near the end of each phase; first finisher wins |
| `-h, --help` | no | Show usage (also shown when run with no flags) |
 
Expected output (one `[key, value]` per line, sorted by key within each file):

```
["Chicago", 1.0]
["Madison", -2.0]
["Miami", 25.0]
["Portland, OR", 8.0]
```
(split across the `part-<r>.jsonl` files; a `_SUCCESS` file marks completion.)
BOSS_URL (default http://localhost:5001) and SHARED_DIR (default ./shared) can be overridden by environment variables for the client tools.

`curl -s localhost:5000/job/<id>` includes a `counters` object summed over finished
tasks (`map_input_records`, `map_output_records`, `combine_input_records`,
`combine_output_records`, `intermediate_bytes`, `shuffle_bytes`, `reduce_input_records`,
`reduce_input_groups`, `reduce_output_records`, `skipped_rows`). The map-side
combine buffer size is set by `COMBINE_BUFFER` on the workers (default 200000 pairs).

Worker failure handling: workers heartbeat every `HEARTBEAT_SECS` (worker env, default 2).
The boss resets a RUNNING task to PENDING when its worker has been silent for more than
`HEARTBEAT_TIMEOUT` (boss env, default 10 s); `GET /job/<id>` shows per-task `attempts`
and `reexecutions` (attempts lost to a dead/restarted worker), and `GET /workers` shows each
worker's status, last-seen age and current attempt.

Backup tasks (`-B`): once a phase has no PENDING tasks, an idle worker may get a second copy
of a slow RUNNING task; the first copy to finish wins and the other is cancelled through its
next heartbeat reply. Boss env vars:

| Variable | Default | A task gets a backup only if... |
|---|---|---|
| `BACKUP_MIN_DONE` | 0.25 | at least this fraction of the phase's tasks is DONE |
| `BACKUP_SLOWDOWN` | 1.5 | it has run longer than this x the median DONE task duration |
| `BACKUP_MIN_SECONDS` | 2 | it has run longer than this many seconds |
| `BACKUP_MAX_FRACTION` | 0.10 | the job has launched fewer than `max(1, ceil(this x tasks))` backups |

...and its estimated remaining time (`elapsed x (1-progress)/progress`) is above the median,
it isn't on the asking worker, and it has no backup yet. `GET /job/<id>` adds
`backups_launched`, `backups_won`, `wasted_seconds` and per-attempt detail; every attempt
start/finish is appended to `shared/jobs/<id>/events.jsonl`.

To try it, make worker4 a straggler (a quarter of a CPU; `SLOW_MS_PER_ROW=<ms>` adds a
sleep per input row on worker4 too):
```bash
docker compose -f docker-compose.yml -f docker-compose.straggler.yml up -d
python3 ./submit -i /input/big -o /output/b -j examples/avg_temp.py -m 16 -r 4 -B
docker compose logs boss | grep backup
docker compose up -d          # back to 4 equal workers
```
Switching between the two compose setups recreates every container, including the boss,
so jobs from before the switch are forgotten. `bash eval/backup_experiment.sh` runs the
full experiment (3 trials of {no straggler, straggler} x {-B off, on}) into
`eval/results/backup.csv`; `python3 eval/plot_backup.py [csv] [events.jsonl ...]` plots it
(needs matplotlib on the host).

`BOSS_URL` (default `http://localhost:5000`) and `SHARED_DIR` (default `./shared`)
can be overridden by environment variables for the client tools.

## Debug
| Symptom | Check | Fix |
|---|---|---|
| `submit` errors with `HTTP 403 ... not a reply from the MapReduce boss` | `curl -i $BOSS_URL/job/x` should return `{"error": "unknown job"}` | Something else owns the port: use `BOSS_PORT` and `BOSS_URL` as in step 3 |
| `service "boss" is not running` | `docker compose ps -a` shows boss as `Created` | Port conflict at startup: same fix |
| `cannot reach boss` | `docker compose ps` | `docker compose up -d` |
| `zsh: no matches found: shared/output/.../part-*.jsonl` | the job never finished | Read `submit`'s error message |
| Job paused at `maps k/M` after a worker died | `docker compose logs boss \| grep failed`, `curl -s $BOSS_URL/workers` | Fixed: its task goes back to PENDING after `HEARTBEAT_TIMEOUT` (10 s) and another worker reruns it. If **all** workers are dead the job waits; `docker compose up -d` resumes it |


## Layout
- `mapreduce/boss.py` – Flask boss: job/task state, `/submit_job`, `/get_task`, `/task_done`, `/job/<id>`
- `mapreduce/worker.py` – pull loop; runs map and reduce tasks
- `mapreduce/common.py` – partitioning, byte-range splitting, CSV parsing, job loading
- `mapreduce/storage.py` – shared-storage helpers (atomic writes)
- `upload_data`, `submit` – client CLIs
- `docker-compose.straggler.yml` – override that slows worker4 down
- `eval/` – backup-task experiment script and plots

## Implementation notes
- Input is split by **data bytes** (after each file's header line) into `num_maps`
  ranges; a range may span files. A range not starting at a file's first data byte
  skips its partial first line; every range reads through the line crossing its end.
- Files are written to `.tmp-<uuid>` then `os.rename`d, so duplicate execution is safe.
- A task may run more than once (worker killed, paused, or restarted). Correctness relies on
  deterministic user functions + deterministic file names + atomic rename: every attempt
  writes identical files. The first success wins; later reports are ignored, and a failure
  from a worker that no longer owns the task does not fail the job. Completed map tasks are
  never re-run, since their output is on shared storage. Leftover `.tmp-*` files are deleted
  when the job finishes.
- Reduce tasks are created only after all maps finish. If a task raises an
  exception (reported by its current owner), the job is marked FAILED.
- Containers run as root, so files under `./shared` may need `sudo` to delete.
