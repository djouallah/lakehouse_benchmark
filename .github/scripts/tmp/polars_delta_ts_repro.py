"""TEMPORARY: does scan_delta skip a row whose timestamp has sub-millisecond digits, in a
file written by Polars' own sink_delta? Delta stats hold timestamps in milliseconds: the sink
truncates the max, and a reader must widen it by 1 ms. Local files, no OneLake."""

import json
import os
import tempfile
from datetime import datetime, timezone

import deltalake
import polars as pl

print(f"polars {pl.__version__}, deltalake {deltalake.__version__}")
path = os.path.join(tempfile.mkdtemp(), "t")
schema = {"at": pl.Datetime("us", "UTC"), "v": pl.Int64}
first = datetime(2026, 1, 1, tzinfo=timezone.utc)
at = datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)
# Two files, as an incremental table has: a first build, then an append.
pl.LazyFrame({"at": [first], "v": [1]}, schema=schema).sink_delta(path)
pl.LazyFrame({"at": [at], "v": [2]}, schema=schema).sink_delta(path, mode="append")

for version in (0, 1):
    log = os.path.join(path, "_delta_log", f"{version:020d}.json")
    with open(log) as f:
        adds = [json.loads(line)["add"] for line in f if '"add"' in line]
    print(f"stats v{version}:", adds[0]["stats"])

for name, frame in [
    ("scan_delta, at == value", pl.scan_delta(path).filter(pl.col("at") == at)),
    ("scan_delta, at >= value", pl.scan_delta(path).filter(pl.col("at") >= at)),
    ("scan_delta, no filter", pl.scan_delta(path)),
]:
    print(f"{name}: {frame.collect().height} row(s)")
