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
at = datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)
pl.LazyFrame({"at": [at]}, schema={"at": pl.Datetime("us", "UTC")}).sink_delta(path)

log = os.path.join(path, "_delta_log", "00000000000000000000.json")
with open(log) as f:
    adds = [json.loads(line)["add"] for line in f if '"add"' in line]
print("stats:", adds[0]["stats"])

for name, frame in [
    ("scan_delta, at == value", pl.scan_delta(path).filter(pl.col("at") == at)),
    ("scan_delta, at >= value", pl.scan_delta(path).filter(pl.col("at") >= at)),
    ("scan_delta, no filter", pl.scan_delta(path)),
]:
    print(f"{name}: {frame.collect().height} row(s)")
