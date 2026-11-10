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
pip install -r requirements.txt   # or: pip install requests
```

## 3. Start the cluster
```bash
docker compose up -d          # boss on localhost:5000, workers 1-4
docker compose logs -f        # optional
```
Shared storage is `./shared` on the host, `/shared` in every container.

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

## 6. Submit it
```bash
python3 ./submit -i /input/weather -o /output/avg_temp -j examples/avg_temp.py -m 4 -r 2
cat shared/output/avg_temp/part-*.jsonl
```
Expected output (one `[key, value]` per line, sorted by key per file):
```
["Chicago", 1.0]
["Madison", -2.0]
["Miami", 25.0]
["Portland, OR", 8.0]
```
(split across the `part-<r>.jsonl` files; a `_SUCCESS` file marks completion.)

`BOSS_URL` (default `http://localhost:5000`) and `SHARED_DIR` (default `./shared`)
can be overridden by environment variables for the client tools.

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
