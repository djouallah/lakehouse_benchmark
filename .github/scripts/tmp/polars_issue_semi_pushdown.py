import os, resource, subprocess, sys
from datetime import date, datetime, timedelta

import numpy as np
import polars as pl

D = "/tmp/sp"
if len(sys.argv) == 1:
    os.makedirs(D, exist_ok=True)
    ts = pl.datetime_range(datetime(2024, 1, 1), datetime(2025, 2, 3, 23, 55), "5m", eager=True)  # 400 days
    duid = pl.DataFrame({"duid": [f"U{j:03d}" for j in range(400)]})
    for i in range(0, len(ts), 20 * 288):  # fact: 46M rows in 20 files
        f = pl.DataFrame({"ts": ts[i : i + 20 * 288]}).join(duid, how="cross")
        f.with_columns(date=pl.col("ts").dt.date(), mw=pl.Series(np.random.default_rng(i).random(f.height))).write_parquet(
            f"{D}/fact_{i:06d}.parquet"
        )
    duid.with_columns(region=pl.format("R{}", pl.int_range(400) % 5)).write_parquet(f"{D}/dim.parquet")
    pl.DataFrame({"ts": ts}).join(pl.DataFrame({"region": [f"R{j}" for j in range(5)]}), how="cross").with_columns(
        rrp=pl.lit(50.0)
    ).write_parquet(f"{D}/price.parquet")
    pl.DataFrame({"date": [date(2025, 2, 3) - timedelta(days=k) for k in range(7)]}).write_parquet(f"{D}/dates.parquet")

fact = pl.scan_parquet(f"{D}/fact_*.parquet")
dim = pl.scan_parquet(f"{D}/dim.parquet")
price = pl.scan_parquet(f"{D}/price.parquet")
dates = pl.scan_parquet(f"{D}/dates.parquet")  # 7 rows
# Join reordering off, so the only difference between the two plans is where the semi join runs.
OPT = pl.QueryOptFlags(join_order=False)


def semi_last():  # the semi join's key is a fact column, so it could run on fact alone
    return fact.join(dim, on="duid").join(price, on=["ts", "region"]).join(dates, on="date", how="semi")


def semi_first():  # the same thing, by hand
    return fact.join(dates, on="date", how="semi").join(dim, on="duid").join(price, on=["ts", "region"])


if len(sys.argv) == 1:
    print(pl.__version__)
    print(semi_last().explain(optimizations=OPT))
    for name in ("semi_last", "semi_first"):
        subprocess.run([sys.executable, __file__, name])
else:
    n = globals()[sys.argv[1]]().select(pl.len()).collect(optimizations=OPT).item()
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    print(f"{sys.argv[1]:10s} rows={n:,} peak RSS {rss:,} MB", flush=True)
