# Mini-MapReduce

A small MapReduce system that runs on one VM with Docker Compose: one **boss**
and four **workers** (1 CPU each). See `design.md` for the design.

Follow steps 1 to 6 to submit a MapReduce job.


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

## 3. How to start the cluster
```bash
docker compose up -d          # boss on localhost:5050, worker1..worker4
docker compose logs -f        # optional
```
Shared storage is `./shared` on the host, `/shared` in every container.
To use a different port or storage folder, see [Ports and storage](#ports-and-storage).


## 4. How to upload example data
```bash
./upload_data -f examples/data/weather/ -d /input/weather
```

## 5. How to write a MapReduce job
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

## 6. How to submit the job
```bash
./submit -i /input/weather -o /output/avg_temp -j examples/avg_temp.py -m 4 -r 2
cat shared/output/avg_temp/part-*.jsonl
# same job with the combiner (output is identical):
./submit -i /input/weather -o /output/avg_temp_c -j examples/avg_temp.py -m 4 -r 2 -C
```
| Flag | Required | Meaning |
|---|---|---|
| `-i, --input <path>` | yes | CSV file or directory in shared storage, e.g. `/input/weather` |
| `-o, --output <path>` | yes | Output directory in shared storage; **use a new one for every run** |
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
The lines are split across the `part-<r>.jsonl` files; `_SUCCESS` marks completion.
**Use a new `-o` for every run** (or `rm -rf` the old one first): the boss does not
check, and leftover `part-*` files from an earlier run would duplicate keys.


And that's how you submit a job in this Mini-MapReduce! Feel free to read more of the following for customization and notes.


---



## Inspecting a job
```bash
curl -s localhost:5050/job/<id>  # status, per-task attempts, counters, backup stats
curl -s localhost:5050/workers   # each worker's status, last-seen age, current task
```
`counters` is summed over finished tasks: `map_input_records`, `map_output_records`,
`combine_input_records`, `combine_output_records`, `intermediate_bytes`, `shuffle_bytes`,
`reduce_input_records`, `reduce_input_groups`, `reduce_output_records`, `skipped_rows`.
 
## Ports and storage
By default the boss is published on host port **5050**, and `./submit` and
`eval/backup_experiment.sh` connect to `http://localhost:5050`. Shared storage is the
repo's `shared/` folder. Override them with environment variables:

```bash
export BOSS_URL=http://localhost:6000    # where ./submit and the eval script find the boss
export SHARED_DIR=$HOME/mr-shared        # where ./upload_data writes input files
```

Both sides must agree:
- **Port:** `BOSS_URL` must match the host port in `docker-compose.yml`
  (`"127.0.0.1:5050:5000"`; change the first number, e.g. to `6000`, then
  `docker compose up -d`). The `5000` after it is the boss's port inside Docker and stays.
- **Storage:** `SHARED_DIR` must be the folder mounted into the containers
  (`./shared:/shared` in `docker-compose.yml`). If you change one, change the other, or the
  workers won't see the uploaded files.

`export` only lasts for the current terminal; add the lines to `~/.zshrc` to keep them.
 
## Implementation notes
 
### Splitting and partitioning
- Input is split by **data bytes** (after each file's header line) into `M` ranges; a
  range may span files. A range not starting at a file's first data byte skips its
  partial first line; every range reads through the line crossing its end.
- Each key goes to reducer `md5(json(key)) mod R`, which is the same in every process.
- Reduce tasks are created only after all map tasks finish.
### Output files and re-execution
- Files are written to `.tmp-<uuid>` and then `os.rename`d, so a reader never sees a
  half-written file. File names are deterministic, so a task that runs twice writes
  identical files; the first success wins and later reports are ignored.
- Intermediate files live on shared storage, so completed map tasks are never re-run.
- Leftover `.tmp-*` files are deleted when the job finishes.
- If a task raises an exception (reported by the worker that currently owns it), the
  job is marked FAILED.
### Combiner (`-C`)
- `combine_fn(key, values)` has the same signature as `reduce_fn`. Each map task runs it
  on its own output before writing intermediate files.
- Its output values must be valid input to both `combine_fn` and `reduce_fn` (yield
  `(sum, count)`, not an average), because it may run zero, one or several times per key.
- Map output is combined whenever the buffer reaches `COMBINE_BUFFER` pairs (worker env,
  default 200000) to bound memory.
- Output is identical with and without `-C`; compare `combine_input_records` with
  `combine_output_records` and `shuffle_bytes` in the job's counters.
### Worker failure handling
- Workers send a heartbeat every `HEARTBEAT_SECS` (worker env, default 2).
- When a RUNNING task's worker has been silent for more than `HEARTBEAT_TIMEOUT`
  (boss env, default 10 s), the boss marks the worker failed and puts the task back to
  PENDING; another worker reruns it.
- A worker that was wrongly presumed dead (e.g. paused) may still report later: a success
  is accepted or ignored as above, and a failure from a worker that no longer owns the
  task does not fail the job.
- If every worker is dead the job waits; `docker compose up -d` resumes it.
- `GET /job/<id>` shows per-task `attempts` and `reexecutions`.
### Backup tasks for stragglers (`-B`)
- Once a phase has no PENDING tasks, an idle worker may get a second copy of a slow
  RUNNING task. The first copy to finish wins; the other is cancelled through its next
  heartbeat reply.
- A task gets a backup only if all of these hold (boss env vars):

  | Variable | Default | Condition |
  |---|---|---|
  | `BACKUP_MIN_DONE` | 0.25 | at least this fraction of the phase's tasks is DONE |
  | `BACKUP_SLOWDOWN` | 1.5 | it has run longer than this × the median DONE task duration |
  | `BACKUP_MIN_SECONDS` | 2 | it has run longer than this many seconds |
  | `BACKUP_MAX_FRACTION` | 0.10 | the job has launched fewer than `max(1, ceil(this × tasks))` backups |

  It must also have an estimated remaining time (`elapsed × (1 − progress) / progress`)
  above the median, run on a different worker than the one asking, and have no backup yet.
- `GET /job/<id>` adds `backups_launched`, `backups_won`, `wasted_seconds` and per-attempt
  detail; every attempt start and finish is appended to `shared/jobs/<id>/events.jsonl`.
- Try it by making worker4 a straggler (a quarter of a CPU; `SLOW_MS_PER_ROW=<ms>` also
  adds a sleep per input row on worker4):
```bash
docker compose -f docker-compose.yml -f docker-compose.straggler.yml up -d
./submit -i /input/big -o /output/b -j examples/avg_temp.py -m 16 -r 4 -B
docker compose logs boss | grep backup
docker compose up -d          # back to 4 equal workers
```
  Switching compose setups recreates every container, including the boss, so earlier
  jobs are forgotten.
- Experiment: `bash eval/backup_experiment.sh` runs 3 trials of {no straggler, straggler}
  × {`-B` off, on} into `eval/results/backup.csv` (it needs `/input/big` uploaded; it
  uses `BOSS_URL` if set, otherwise `http://localhost:5050`). `python3 eval/plot_backup.py [csv] [events.jsonl ...]` plots it
  (needs matplotlib on the host).
### Storage permissions
- Containers run as root and set `umask(0)`, so the host user can normally delete files
  under `./shared`; if not, use `sudo rm`.
## Debug
| Symptom | Check | Fix |
|---|---|---|
| `submit` fails with a JSON or `403` error | `curl -i http://localhost:5050/job/x` should return `{"error": "unknown job"}` | `BOSS_URL` points at something else: `unset BOSS_URL`, or set it to the boss's port (see [Ports and storage](#ports-and-storage)) |
| `service "boss" is not running` | `docker compose ps -a` shows boss as `Created` | Host port 5050 is taken: pick another port as in [Ports and storage](#ports-and-storage) |
| `cannot reach boss` | `docker compose ps` | `docker compose up -d` |
| `zsh: no matches found: shared/output/.../part-*.jsonl` | the job never finished | Read `submit`'s error message |
| Duplicate keys in `cat part-*.jsonl` | `ls shared/output/<name>` shows more `part-*` files than `-r` | The output directory was reused; delete it or pick a new `-o` |
| Job paused at `maps k/M` after a worker died | `docker compose logs boss \| grep failed`, `curl -s localhost:5050/workers` | Expected: the task returns to PENDING after `HEARTBEAT_TIMEOUT` (10 s) and is rerun |
 
## Layout
- `mapreduce/boss.py` – Flask boss: job/task state, scheduling, heartbeat timeouts, backups;
  `/submit_job`, `/get_task`, `/task_done`, `/heartbeat`, `/job/<id>`, `/workers`
- `mapreduce/worker.py` – pull loop; map (+ combiner) and reduce tasks; heartbeat thread
- `mapreduce/common.py` – partitioning, byte-range splitting, CSV parsing, job loading
- `mapreduce/storage.py` – shared-storage helpers (atomic writes)
- `upload_data`, `submit` – client CLIs
- `docker-compose.straggler.yml` – override that slows worker4 down
- `eval/` – backup-task experiment script and plots
- `tests/` – `python3 -m pytest -q` (combiner, failure handling, backups)
