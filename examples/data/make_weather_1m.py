#!/usr/bin/env python3
"""Write the 1M-row example dataset used in the README's "Advanced features usage".

    python3 examples/data/make_weather_1m.py      # -> examples/data/weather_1m/part0..3.csv

Same generator and seed as the benchmark data (eval/experiments.py): columns
city,date,temp,station; 100 cities, so keys repeat a lot (good for the combiner);
about 35 MB in 4 files. Seeded, so every run gives byte-identical files. The files
are git-ignored; rerun this after a fresh clone.
"""
import importlib.util
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
ROWS, KEYS, FILES, SEED = 1_000_000, 100, 4, 42


def main():
    spec = importlib.util.spec_from_file_location(
        "experiments", os.path.join(ROOT, "eval", "experiments.py"))
    ex = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ex)
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "weather_1m")
    print(f"writing {ROWS:,} rows to {out} ...", flush=True)
    ex.write_csvs(out, ROWS, KEYS, FILES, SEED)
    mb = sum(os.path.getsize(os.path.join(out, f)) for f in os.listdir(out)) / 1e6
    print(f"done: {FILES} files, {mb:.0f} MB")


if __name__ == "__main__":
    main()
