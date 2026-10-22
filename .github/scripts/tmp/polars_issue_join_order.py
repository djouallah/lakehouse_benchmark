import os, resource, subprocess, sys
from datetime import datetime, timedelta

import numpy as np
import polars as pl

D = "/tmp/jo"
if len(sys.argv) == 1:
    os.makedirs(D, exist_ok=True)
    ts = pl.datetime_range(datetime(2024, 1, 1), datetime(2025, 2, 3, 23, 55), "5m", eager=True)  # 400 days
    duid = pl.DataFrame({"duid": [f"U{j:03d}" for j in range(400)]})
    for i in range(0, len(ts), 20 * 288):  # fact: 46M rows in 20 files
        f = pl.DataFrame({"ts": ts[i : i + 20 * 288]}).join(duid, how="cross")
        f.with_columns(mw=pl.Series(np.random.default_rng(i).random(f.height))).write_parquet(f"{D}/fact_{i:06d}.parquet")
    duid.with_columns(region=pl.format("R{}", pl.int_range(400) % 5)).write_parquet(f"{D}/dim.parquet")
    pl.DataFrame({"ts": ts}).join(pl.DataFrame({"region": [f"R{j}" for j in range(5)]}), how="cross").with_columns(
        rrp=pl.lit(50.0)
    ).write_parquet(f"{D}/price.parquet")

ctx = pl.SQLContext(
    fact=pl.scan_parquet(f"{D}/fact_*.parquet"),
    dim=pl.scan_parquet(f"{D}/dim.parquet"),
    price=pl.scan_parquet(f"{D}/price.parquet"),
)
q = ctx.execute("""
    SELECT f.ts, f.duid, f.mw, p.rrp
    FROM fact f
    JOIN dim d ON f.duid = d.duid
    JOIN price p ON f.ts = p.ts AND d.region = p.region
""").select(pl.len())

if len(sys.argv) == 1:
    print(pl.__version__)
    print(q.explain())
    for flag in ("True", "False"):
        subprocess.run([sys.executable, __file__, flag])
else:
    n = q.collect(optimizations=pl.QueryOptFlags(join_order=sys.argv[1] == "True")).item()
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    print(f"join_order={sys.argv[1]:5s} rows={n:,} peak RSS {rss:,} MB", flush=True)
