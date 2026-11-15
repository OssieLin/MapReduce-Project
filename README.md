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
| `-h, --help` | no | Show usage (also shown when run with no flags) |
 
Expected output (one `[key, value]` per line, sorted by key within each file):

```
["Chicago", 1.0]
["Madison", -2.0]
["Miami", 25.0]
["Portland, OR", 8.0]
```
(split across the `part-<r>.jsonl` files; a `_SUCCESS` file marks completion.)

`curl -s localhost:5000/job/<id>` includes a `counters` object summed over finished
tasks (`map_input_records`, `map_output_records`, `combine_input_records`,
`combine_output_records`, `intermediate_bytes`, `shuffle_bytes`, `reduce_input_records`,
`reduce_input_groups`, `reduce_output_records`, `skipped_rows`). The map-side
combine buffer size is set by `COMBINE_BUFFER` on the workers (default 200000 pairs).

`BOSS_URL` (default `http://localhost:5000`) and `SHARED_DIR` (default `./shared`)
can be overridden by environment variables for the client tools.

## Debug
| Symptom | Check | Fix |
|---|---|---|
| `submit` errors with `HTTP 403 ... not a reply from the MapReduce boss` | `curl -i $BOSS_URL/job/x` should return `{"error": "unknown job"}` | Something else owns the port: use `BOSS_PORT` and `BOSS_URL` as in step 3 |
| `service "boss" is not running` | `docker compose ps -a` shows boss as `Created` | Port conflict at startup: same fix |
| `cannot reach boss` | `docker compose ps` | `docker compose up -d` |
| `zsh: no matches found: shared/output/.../part-*.jsonl` | the job never finished | Read `submit`'s error message |
| Job stuck at `maps k/M` | `docker compose logs worker1` | A killed worker's task is not reassigned yet: restart the job with a new `-o` |


## Layout
- `mapreduce/boss.py` – Flask boss: job/task state, `/submit_job`, `/get_task`, `/task_done`, `/job/<id>`
- `mapreduce/worker.py` – pull loop; runs map and reduce tasks
- `mapreduce/common.py` – partitioning, byte-range splitting, CSV parsing, job loading
- `mapreduce/storage.py` – shared-storage helpers (atomic writes)
- `upload_data`, `submit` – client CLIs

## Implementation notes
- Input is split by **data bytes** (after each file's header line) into `num_maps`
  ranges; a range may span files. A range not starting at a file's first data byte
  skips its partial first line; every range reads through the line crossing its end.
- Files are written to `.tmp-<uuid>` then `os.rename`d, so duplicate execution is safe.
- Reduce tasks are created only after all maps finish. If a task raises an
  exception, the job is marked FAILED.
- Containers run as root, so files under `./shared` may need `sudo` to delete.
