# Mini-MapReduce

A small MapReduce system that runs on one VM with Docker Compose: one **boss**
and four **workers** (1 CPU each). See `design.md` for the design.

### Requirements

- Docker with Docker Compose. Jobs run correctly on any such machine, including a Mac.
- To reproduce the benchmark numbers, use a Linux machine or VM with **at least 4
  CPUs that nothing else is using** (we used a 4-vCPU Xeon VM; see
  [eval/benchmark.md](eval/benchmark.md#testing-machine)). Each worker is capped at
  1 CPU. On fewer cores, or on a busy laptop,
  workers compete for CPU time and runtimes become slower and noisy.


Follow steps 0 to 6 to submit a MapReduce job, then step 7 to monitor it. Step 8
tries the combiner, worker failures and backup tasks on a 1M-row dataset.


## 0. Setup

The cluster and its shared storage live on one machine (we use a Linux VM).
Clone the repo **on that machine**; steps 1–6 all run there:
```bash
git clone https://github.com/OssieLin/Mini-MapReduce.git
cd Mini-MapReduce
```
That one clone is all you need. To also watch or submit from your laptop, see
[Watching from your laptop](#watching-from-your-laptop) in step 7.

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
pip install -r requirements.txt   
```

## 3. How to start the cluster
```bash
docker compose up -d          # boss on localhost:5050, worker1..worker4
```
Shared storage is `./shared` on the host, `/shared` in every container.
To use a different port or storage folder, see [Ports and storage](#ports-and-storage).


## 4. How to upload example data
```bash
./upload_data -f examples/data/weather/ -d /input/weather
```
| Flag | Required | Meaning |
|---|---|---|
| `-f, --file <local_path>` | yes | Local `.csv` file, or a directory whose `.csv` files are all uploaded |
| `-d, --dest <storage_path>` | yes | Destination in shared storage, e.g. `/input/weather`; pass it to `./submit -i` |
| `-h, --help` | no | Show usage (also shown when run with no flags) |

It copies the files into `./shared` on the machine it runs on, so run it on the VM.

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


## Advanced features usage
Each feature below is run on the same 1M-row dataset, so the difference shows up in the
visualization at http://localhost:5050/visualization (open it before you submit; from
a laptop, see [Watching from your laptop](#watching-from-your-laptop)). All commands
run on the VM in the repo folder, with the cluster from step 3 running.

**Get the example data** (once). The 1M-row dataset is generated rather than committed
(about 37 MB in 4 CSV files, git-ignored). It uses the same generator and seed as the
benchmark, so every run gives the same files. Columns are `city,date,temp,station`, with 100 cities:
```bash
python3 examples/data/make_weather_1m.py                    # -> examples/data/weather_1m/
./upload_data -f examples/data/weather_1m/ -d /input/weather_1m
```
Each `./submit` below needs a new `-o`; to rerun a block, delete its outputs first
(`rm -rf shared/output/adv_*`).

### Combiner (`-C`)
Run the same job without and with the combiner:
```bash
./submit -i /input/weather_1m -o /output/adv_nocomb -j examples/avg_temp.py -m 16 -r 4
./submit -i /input/weather_1m -o /output/adv_comb   -j examples/avg_temp.py -m 16 -r 4 -C
```
In the visualization, select either job and press **Combiner**. It compares the two
runs (same input, M and R). Without `-C`, every one of the 1M map records is written
and shuffled. With `-C`, each map task writes just one record per city (100), so
intermediate and shuffle bytes drop from tens of MB to tens of KB, and the job is
faster. The output keys and averages are the same.

### Worker failure (kill a worker by hand)
`examples/avg_temp_slow.py` is `avg_temp.py` with a short sleep per row, so its map
phase lasts about half a minute: long enough to kill a worker in the middle of it.
Use two terminals on the VM.

Terminal 1, first a reference run, then the run you will break:
```bash
./submit -i /input/weather_1m -o /output/adv_ref  -j examples/avg_temp_slow.py -m 16 -r 4
./submit -i /input/weather_1m -o /output/adv_kill -j examples/avg_temp_slow.py -m 16 -r 4
```
Terminal 2, while the second job's map phase is running (a few map tasks DONE in the
visualization):
```bash
docker compose kill worker2     # SIGKILL: no clean exit, its heartbeats just stop
# watch the visualization for ~10 s, then bring it back:
docker compose start worker2
```
**What to see:** for about 10 s (`HEARTBEAT_TIMEOUT`) nothing changes, because the boss
can't yet tell a dead worker from a slow one. Then worker2's running map attempt turns
into a *failed / lost* chip, its task goes back to PENDING, and another worker reruns it.
The job still finishes, about 10 s later than the reference run, and
`curl -s localhost:5050/job/<id>` shows the extra attempt under `reexecutions`. The
output is the same as the reference:
```bash
diff <(cat shared/output/adv_ref/part-*.jsonl) <(cat shared/output/adv_kill/part-*.jsonl) && echo identical
```
Variations:
- **Pause instead of kill** (a worker wrongly presumed dead):
  `docker compose pause worker3`, wait ~15 s, then `docker compose unpause worker3`.
  Its task is rerun elsewhere, and its late report doesn't break the job.
- **Kill during reduce:** kill a worker once the job shows REDUCE. That phase is
  short, so use more reduce tasks (`-r 8`) for a longer window.
- **Kill every worker:** the job waits. `docker compose up -d` brings them back and
  it resumes.

### Straggler and backup tasks (`-B`)
Make worker4 slow: a quarter of a CPU, plus a 0.1 ms sleep per input row:
```bash
SLOW_MS_PER_ROW=0.1 docker compose -f docker-compose.yml -f docker-compose.straggler.yml up -d
```
This recreates every container, including the boss, so the visualization's earlier
jobs are forgotten (output files stay). Then run the same job without and with backups:
```bash
./submit -i /input/weather_1m -o /output/adv_strag   -j examples/avg_temp.py -m 16 -r 4
./submit -i /input/weather_1m -o /output/adv_backup -j examples/avg_temp.py -m 16 -r 4 -B
docker compose logs boss | grep -i backup    # which tasks got a backup, and who won
```
**What to see:** without `-B`, the other three workers finish their maps in a couple
of seconds and then sit idle while worker4 grinds through its task. The job ends
many seconds later. With `-B`, once the other tasks are done, an idle worker gets a copy
of worker4's task, marked **backup**. The backup finishes first, worker4's copy turns
into a *cancelled* chip, and the job ends much sooner. `curl -s localhost:5050/job/<id>`
shows `backups_launched`, `backups_won` and `wasted_seconds`. Both runs give the same output:
```bash
diff <(cat shared/output/adv_strag/part-*.jsonl) <(cat shared/output/adv_backup/part-*.jsonl) && echo identical
```
Afterwards, go back to 4 equal workers (this also recreates the boss):
```bash
docker compose up -d
```

And that's how you submit and monitor a job in this Mini-MapReduce! Feel free to read
more of the following for customization and notes.


---


## Job monitoring
These views are most useful once a job from step 6 is running or finished. All
commands below run on the VM unless a heading says otherwise.

### Logs
```bash
docker compose logs -f | grep -v -E '/get_task|/heartbeat'
```
Idle workers ask the boss for work about three times a second, so the raw
`docker compose logs -f` is mostly `POST /get_task ... 200` lines; that is normal.
The filter above hides them. During a job, workers print `running <job_id>/<task_id>`,
and any traceback shows up here too.

### Inspecting a job
```bash
curl -s localhost:5050/job/<id>  # status, per-task attempts, counters, backup stats
curl -s localhost:5050/workers   # each worker's status, last-seen age, current task
```
`counters` is summed over finished tasks: `map_input_records`, `map_output_records`,
`combine_input_records`, `combine_output_records`, `intermediate_bytes`, `shuffle_bytes`,
`reduce_input_records`, `reduce_input_groups`, `reduce_output_records`, `skipped_rows`.

### Watching from your laptop
The cluster must already be running on the VM (step 3), and
`curl -s localhost:5050/workers` there should print JSON. The boss only listens on
the VM's localhost, so forward its port over SSH from the laptop (leave this running):
```bash
ssh -N -L 5050:localhost:5050 <user>@<vm>
```
The second `5050` is the boss's host port on the VM. If you changed it (see
[Ports and storage](#ports-and-storage)), use that number instead.
- **Watch jobs:** open http://localhost:5050/visualization in the laptop's
  browser. No clone needed. The `curl` commands above also work from the laptop.
- **Submit jobs:** this needs a second clone on the laptop, plus step 2 there,
  because `./submit` reads the job file locally. Input data must already be on
  the VM (`./upload_data` copies into the local `./shared`, so always run it on
  the VM), and output appears in the VM's `./shared/output`.

If `ssh` prints `bind [127.0.0.1]:5050: Address already in use`, something on the
laptop (usually an earlier tunnel) already holds port 5050. Stop it, or pick another
local port, then use that port in the laptop's URLs and in `BOSS_URL`:
```bash
lsof -nP -iTCP:5050 -sTCP:LISTEN     # shows the PID holding the port
kill <PID>                           # if it's an old ssh tunnel
# or forward a different local port instead:
ssh -N -L 5051:localhost:5050 <user>@<vm>   # then http://localhost:5051/visualization
```
If browsing gives `channel N: open failed: connect failed: Connection refused`, the
tunnel is fine, but nothing listens on that port on the VM. Check `docker compose ps -a`
on the VM (see [Debug](#debug)).

 
## Ports and storage
By default the boss is published on host port **5050**, and `./submit` and
`eval/experiments.py` connect to `http://localhost:5050`. Shared storage is the
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
  | `BACKUP_MIN_DONE` | 0.25 | at least `BACKUP_MIN_DONE` fraction of the phase's tasks is DONE |
  | `BACKUP_SLOWDOWN` | 1.5 | it has run longer than `BACKUP_SLOWDOWN` * the median DONE task duration |
  | `BACKUP_MIN_SECONDS` | 2 | it has run longer than `BACKUP_MIN_SECONDS` many seconds |
  | `BACKUP_MAX_FRACTION` | 0.10 | the job has launched fewer than `max(1, ceil(BACKUP_MAX_FRACTION × tasks))` backups |

  It must also have an estimated remaining time (`elapsed × (1 − progress) / progress`)
  above the median, run on a different worker than the one asking, and have no backup yet.
- `GET /job/<id>` adds `backups_launched`, `backups_won`, `wasted_seconds` and per-attempt
  detail; every attempt start and finish is appended to `shared/jobs/<id>/events.jsonl`.
- Try it by making worker4 a straggler (a quarter of a CPU; `SLOW_MS_PER_ROW=<ms>` also
  adds a sleep per input row on worker4): see
  [Straggler and backup tasks](#straggler-and-backup-tasks--b) in step 8.
- Experiment: `python3 eval/experiments.py --straggler` (see [Experiments](#experiments)).

## Benchmark
We benchmarked this implementation on a 4-vCPU Intel Xeon Linux VM (boss + 4 one-CPU
workers, 3M rows ≈ 110 MB, 16 map / 4 reduce tasks, 5 trials each). Tested:
- **speed vs. a single process** (1.34× faster), **scaling** over 1–4 workers (3.44× at 4), and **task granularity**;
- **the combiner** with repeated keys (3.1× faster), nearly unique keys, and across key counts;
- **worker failures:** kills in each phase, a pause past the timeout, and a quick restart;
- **backup tasks** against a slow worker (36.6 s → 11.0 s).

See **[eval/benchmark.md](eval/benchmark.md)** for the machine, the setup, every plot with its interpretation, and how to run your
own variations.

## Experiments
`eval/experiments.py` runs the evaluation from the host: it generates its own datasets
under `shared/input/eval/`, drives the cluster (stopping, killing and pausing containers
as needed), checks every run's output against a reference run, and writes
`eval/results/<module>.csv` plus `<module>.png`. Pick modules by flag, or `--all`:

| Flag | Experiment | Plot |
|---|---|---|
| `--baseline` | A. single-process Python (`eval/baseline.py`, 1 CPU) vs. MapReduce on 4 workers | runtime and throughput bars |
| `--combiner` | B. combiner off/on, avg temp per city (100 keys) | runtime and intermediate-MB bars |
| `--combiner-unique` | B2. same, avg temp per station (nearly unique keys: `examples/avg_temp_by_station.py`) | same |
| `--scaling` | C. 1, 2, 3, 4 active workers | runtime and throughput vs. workers, with ideal lines |
| `--fault` | D. normal vs. worker2 killed halfway through the map phase | runtime bars; byte-identical check |
| `--granularity` | E. `num_maps` 1–32 (4 reducers), then `num_reduces` 1–16 (16 maps) | runtime vs. num_maps and vs. num_reduces |
| `--straggler` | {no straggler, worker4 at 1/4 CPU} × {`-B` off, on} | grouped bars + Gantt of a backup run |
| `--combiner-sweep` | combiner off/on on datasets with 10 … 1M distinct keys | intermediate MB and runtime vs. keys |
| `--failures` | no failure / kill in map / kill in reduce / pause > timeout / kill + restart | runtime bars with re-runs + Gantt |

```bash
pip install -r eval/requirements.txt              # matplotlib, host only, for the plots
python3 eval/experiments.py --all                 # everything: roughly an hour
python3 eval/experiments.py --combiner --fault    # just some modules
python3 eval/experiments.py --all --trials 1 --rows 300000 --sweep-rows 100000  # quick smoke run
python3 eval/experiments.py --all --plot-only     # redraw plots from existing CSVs
```
Defaults: 3 trials, 3M rows (~110 MB), `-m 16 -r 4`. Generated data is seeded (same
`--rows` → byte-identical files, regenerated only when the settings change) and reads like
`Madison,2026-03-14,4.2,ST00012345`: `city,date,temp,station` with real city names
(`"Portland, OR"` quoted), dates in 2026 and nearly unique station ids; datasets with more
than 100 keys (the combiner sweep) use `City0000123` names. Inspect it with
`head shared/input/eval/big/part0.csv`. Each module starts with an untimed
warm-up job (fresh containers are slow on their first job). Output and intermediate
files are deleted after each run; `shared/jobs/<id>/events.jsonl` is kept for the Gantt
charts. `--straggler` recreates the containers (and so the boss) twice and leaves the
normal 4-worker cluster running.
### Storage permissions
- Containers run as root and set `umask(0)`, so the host user can normally delete files
  under `./shared`; if not, use `sudo rm`.
## Debug
| Symptom | Check | Fix |
|---|---|---|
| `submit` fails with a JSON or `403` error | `curl -i http://localhost:5050/job/x` should return `{"error": "unknown job"}` | `BOSS_URL` points at something else: `unset BOSS_URL`, or set it to the boss's port (see [Ports and storage](#ports-and-storage)) |
| `service "boss" is not running` | `docker compose ps -a` shows boss as `Created` | Host port 5050 is taken: pick another port as in [Ports and storage](#ports-and-storage) |
| `cannot reach boss` | `docker compose ps` | `docker compose up -d` |
| `bind [127.0.0.1]:5050: Address already in use` (laptop `ssh -L`) | `lsof -nP -iTCP:5050 -sTCP:LISTEN` on the laptop | Kill the old tunnel, or forward another local port, e.g. `-L 5051:localhost:5050` |
| `channel N: open failed: connect failed: Connection refused` (laptop `ssh -L`) | On the VM: `docker compose ps -a`, `docker compose port boss 5000` | Start the cluster, or forward the port the boss is actually published on |
| `zsh: no matches found: shared/output/.../part-*.jsonl` | the job never finished | Read `submit`'s error message |
| Duplicate keys in `cat part-*.jsonl` | `ls shared/output/<name>` shows more `part-*` files than `-r` | The output directory was reused; delete it or pick a new `-o` |
| Job paused at `maps k/M` after a worker died | `docker compose logs boss \| grep failed`, `curl -s localhost:5050/workers` | Expected: the task returns to PENDING after `HEARTBEAT_TIMEOUT` (10 s) and is rerun |
 
## Layout
- `mapreduce/boss.py` – Flask boss: job/task state, scheduling, heartbeat timeouts, backups;
  `/submit_job`, `/get_task`, `/task_done`, `/heartbeat`, `/job/<id>`, `/workers`,
  `/api/state` (jobs for the visualization), `/visualization` (serves the page)
- `mapreduce/worker.py` – pull loop; map (+ combiner) and reduce tasks; heartbeat thread
- `mapreduce/common.py` – partitioning, byte-range splitting, CSV parsing, job loading
- `mapreduce/storage.py` – shared-storage helpers (atomic writes)
- `upload_data`, `submit` – client CLIs
- `visualization/index.html` – live workflow page served by the boss at `/visualization` (`?demo=1` for sample data)
- `docker-compose.straggler.yml` – override that slows worker4 down
- `eval/` – `experiments.py` (all experiments and plots), `baseline.py` (single-process reference), `results/`
- `tests/` – `python3 -m pytest -q` (combiner, failure handling, backups)
