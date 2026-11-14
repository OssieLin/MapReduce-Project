"""Partitioning, input splitting, CSV parsing, and job loading."""
import ast
import csv
import hashlib
import importlib.util
import json

from mapreduce import storage


def partition(key, num_reduces):
    # md5, not hash(): Python's hash() is randomized per process.
    digest = hashlib.md5(json.dumps(key).encode("utf-8")).hexdigest()
    return int(digest, 16) % num_reduces


# ---------------------------------------------------------------- splitting
def _read_header(path):
    with storage.open_read(path) as f:
        line = f.readline()
    if not line:
        return None
    text = line.decode("utf-8-sig").rstrip("\r\n")
    return next(csv.reader([text])), len(line)


def compute_splits(files, num_maps):
    """Divide the *data* bytes (everything after each file's header line) of
    all files into num_maps contiguous byte ranges. A range may span files.

    Returns a list of length num_maps; each element is a list of segments
    {file, columns, data_start, start, end}. Segments are never empty, but a
    task may have zero segments if there is less data than tasks.
    """
    infos = []
    for path, size in files:
        header = _read_header(path)
        if header is None:
            continue
        cols, header_len = header
        if size > header_len:
            infos.append((path, cols, header_len, size))
    total = sum(size - ds for _, _, ds, size in infos)

    splits = []
    for i in range(num_maps):
        lo, hi = total * i // num_maps, total * (i + 1) // num_maps
        segs, base = [], 0
        for path, cols, ds, size in infos:
            n = size - ds
            s, e = max(lo, base), min(hi, base + n)
            if s < e:
                segs.append({"file": path, "columns": cols, "data_start": ds,
                             "start": ds + (s - base), "end": ds + (e - base)})
            base += n
        splits.append(segs)
    return splits


def iter_rows(seg, stats=None):
    """Yield (byte_offset, row_dict) for every row *starting* in [start, end).

    A segment that doesn't begin at the first data byte skips the partial line
    at its start (the previous segment reads through its end-boundary line),
    so each row is processed exactly once.
    """
    cols = seg["columns"]
    start, end = seg["start"], seg["end"]
    with storage.open_read(seg["file"]) as f:
        if start > seg["data_start"]:
            f.seek(start - 1)
            f.readline()
        else:
            f.seek(start)
        pos = f.tell()
        while pos < end:
            line = f.readline()
            if not line:
                break
            offset, pos = pos, pos + len(line)
            text = line.decode("utf-8").rstrip("\r\n")
            if not text.strip():
                continue
            try:
                fields = next(csv.reader([text]))
            except (csv.Error, StopIteration):
                fields = None
            if fields is None or len(fields) != len(cols):
                if stats is not None:
                    stats["skipped_rows"] = stats.get("skipped_rows", 0) + 1
                continue
            yield offset, dict(zip(cols, fields))


# --------------------------------------------------------------------- jobs
def check_job_source(source):
    """Static check (no execution) that the job defines map_fn and reduce_fn."""
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        raise ValueError(f"job file has a syntax error: {e}")
    names = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
    missing = [n for n in ("map_fn", "reduce_fn") if n not in names]
    if missing:
        raise ValueError("job file must define: " + ", ".join(missing))


def load_job(path):
    """Import a user job file from shared storage."""
    spec = importlib.util.spec_from_file_location("user_job", storage.resolve(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for name in ("map_fn", "reduce_fn"):
        if not callable(getattr(mod, name, None)):
            raise ValueError(f"job is missing {name}")
    return mod
