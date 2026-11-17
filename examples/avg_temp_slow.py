"""avg_temp.py with a short sleep per input row, so a job runs long enough
(about half a minute on 1M rows with 4 workers) to kill or pause a worker by hand
in the middle of it. Same output as examples/avg_temp.py.
Used by the README's "Advanced features usage" (worker failure)."""
import time

SLEEP_PER_ROW = 0.0001      # seconds; real sleeps are a bit longer than asked


def map_fn(key, row):
    time.sleep(SLEEP_PER_ROW)
    yield row["city"], (float(row["temp"]), 1)    # (sum, count)


def combine_fn(key, values):
    total = count = 0
    for s, c in values:
        total += s
        count += c
    yield key, (total, count)


def reduce_fn(key, values):
    total = count = 0
    for s, c in values:
        total += s
        count += c
    yield key, total / count
