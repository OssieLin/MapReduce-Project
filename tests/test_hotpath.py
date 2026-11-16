"""The map/reduce fast paths must behave exactly like the general code."""
import csv
import io
import json

import pytest

from mapreduce import common, storage, worker

MIXED_KEYS_JOB = """
def map_fn(key, row):
    # keys that collide as Python dict keys but not as JSON
    for k in (1, 1.0, True, "1", [1], "[1]", {"a": 1}):
        yield k, 1

def reduce_fn(key, values):
    yield key, sum(values)
"""


@pytest.fixture
def shared(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "ROOT", str(tmp_path))
    storage.prepare_root()
    return tmp_path


def rows_of(shared, text):
    (shared / "input" / "d.csv").write_text(text)
    seg = common.compute_splits(storage.list_files("input/d.csv"), 1)[0][0]
    stats = {}
    return [r for _, r in common.iter_rows(seg, stats)], stats


@pytest.mark.parametrize("line", [
    'Madison,2026-01-01,-5.0',
    '"Portland, OR",2026-01-01,8.0',
    '"say ""hi""",x,1',
    ',,',
    ' padded , x ,1',
])
def test_iter_rows_same_as_csv_reader(shared, line):
    rows, _ = rows_of(shared, f"a,b,c\n{line}\n")
    assert rows == [dict(zip("abc", next(csv.reader(io.StringIO(line)))))]


def test_iter_rows_skips_bad_rows_on_both_paths(shared):
    rows, stats = rows_of(shared, 'a,b,c\nx,1\n"y",1\nok,1,2\n')
    assert rows == [{"a": "ok", "b": "1", "c": "2"}] and stats["skipped_rows"] == 2


def test_mixed_key_types_partition_and_group_like_json(shared):
    storage.write_atomic("jobs/jm/job.py", MIXED_KEYS_JOB)
    (shared / "input" / "d.csv").write_text("a\n" + "x\n" * 5)
    split = common.compute_splits(storage.list_files("input/d.csv"), 1)[0]
    base = {"job_id": "jm", "num_maps": 1, "num_reduces": 3, "output": "output/o",
            "use_combiner": False}
    worker.run_map(dict(base, type="map", index=0, split=split))
    for r in range(3):
        part = (shared / "jobs/jm/intermediate" / f"map-0-part-{r}.jsonl").read_text()
        for k, _ in map(json.loads, part.splitlines()):
            assert common.partition(k, 3) == r            # cache never misroutes
        worker.run_reduce(dict(base, type="reduce", index=r))
    out = sorted(json.dumps(json.loads(line)) for r in range(3)
                 for line in (shared / "output/o" / f"part-{r}.jsonl").read_text().splitlines())
    # 1 and true and 1.0 stay distinct keys, as do "[1]" and [1]
    assert out == sorted(json.dumps([k, 5]) for k in (1, 1.0, True, "1", [1], "[1]", {"a": 1}))
