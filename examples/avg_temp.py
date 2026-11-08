"""Average temperature per city. Works with examples/data/weather/."""


def map_fn(key, row):
    # key = byte offset; row = {"city": ..., "date": ..., "temp": ...} (strings)
    yield row["city"], (float(row["temp"]), 1)      # (sum, count)


def reduce_fn(key, values):
    total, count = 0.0, 0
    for s, c in values:
        total += s
        count += c
    yield key, total / count
