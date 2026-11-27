"""TEMPORARY: why a sub-millisecond timestamp row is skipped by scan_delta's equality filter
in a file dbt-polars' merge append committed (fork 5f8e11a, max truncated as Polars' sink does),
when the same stats from sink_delta are fine. Each case: a first file, then an append of one row
at .678901; prints the add actions and what the filter returns. Local files, no OneLake."""

import json
import os
import tempfile
from datetime import datetime, timezone

import deltalake
import polars as pl
from dbt.adapters.polars.catalogs.formats.delta import DeltaFormat

print(f"polars {pl.__version__}, deltalake {deltalake.__version__}")
schema = {"at": pl.Datetime("us", "UTC"), "v": pl.Int64}
first = pl.DataFrame({"at": [datetime(2026, 1, 1, tzinfo=timezone.utc)], "v": [1]}, schema=schema)
at = datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)
row = pl.LazyFrame({"at": [at], "v": [2]}, schema=schema)


def seed_write_delta(path):
    first.write_delta(path)


def seed_sink_delta(path):
    first.lazy().sink_delta(path)


def append_sink_delta(path):
    row.sink_delta(path, mode="append")


def append_merge(path):
    DeltaFormat.merge(path, row, ["at"], None, None, False)


for seed in (seed_write_delta, seed_sink_delta):
    for append in (append_sink_delta, append_merge):
        path = os.path.join(tempfile.mkdtemp(), "t")
        seed(path)
        append(path)
        dt = deltalake.DeltaTable(path)
        print(f"\n== {seed.__name__} + {append.__name__}: version {dt.version()}, "
              f"protocol {dt.protocol()}, config {dt.metadata().configuration}")
        for version in range(dt.version() + 1):
            with open(os.path.join(path, "_delta_log", f"{version:020d}.json")) as f:
                for line in f:
                    action = json.loads(line)
                    if "add" in action:
                        add = action["add"]
                        print(f"  v{version} add {add['path']} size {add['size']} stats {add['stats']}")
        for name, frame in [
            ("==", pl.scan_delta(path).filter(pl.col("at") == at)),
            (">=", pl.scan_delta(path).filter(pl.col("at") >= at)),
            ("all", pl.scan_delta(path)),
        ]:
            print(f"  scan_delta {name}: {frame.collect().height} row(s)")
        print("  parquet of the last add:", pl.read_parquet_schema(os.path.join(path, add["path"])))
