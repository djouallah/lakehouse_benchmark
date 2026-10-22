"""TEMPORARY: does Polars push an `IN (subquery)` filter on a fact column below the inner joins?

The shape of workload/models/aemo/polars/marts/fct_summary.sql, on synthetic data:
fact (scada) JOIN dim ON duid JOIN price ON (ts, region), GROUP BY, filtered to a few
"rebuild" dates by `s.date IN (SELECT ...)`. Each variant runs in its own process and reports
its peak RSS; `explain()` shows where the semi join lands.
"""

import os
import resource
import subprocess
import sys
import time
from datetime import datetime, timedelta

DATA = "/tmp/semijoin"
DAYS, DUIDS, REGIONS = int(os.environ.get("DAYS", "400")), 400, 5

FILTER_LAST = """
SELECT s.date, s.ts, s.duid, MAX(s.mw) AS mw, MAX(p.rrp) AS price
FROM scada s
JOIN dim d ON s.duid = d.duid
JOIN price p ON s.ts = p.ts AND d.region = p.region
WHERE s.intervention = 0 AND p.intervention = 0
  AND s.date IN ({rebuild})
GROUP BY ALL
"""
FILTER_FIRST = """
WITH s AS (SELECT * FROM scada WHERE intervention = 0 AND date IN ({rebuild}))
SELECT s.date, s.ts, s.duid, MAX(s.mw) AS mw, MAX(p.rrp) AS price
FROM s
JOIN dim d ON s.duid = d.duid
JOIN price p ON s.ts = p.ts AND d.region = p.region
WHERE p.intervention = 0
GROUP BY ALL
"""
REBUILD = {
    # a small separate table of dates
    "table": "SELECT date FROM rebuild",
    # derived from the fact itself, as fct_summary's rebuild_dates window is
    "derived": "SELECT DISTINCT date FROM scada"
    " WHERE date >= (SELECT MAX(date) - INTERVAL 6 DAY FROM scada)",
}
VARIANTS = {
    f"{shape}/{src}": sql.format(rebuild=REBUILD[src])
    for shape, sql in (("filter_last", FILTER_LAST), ("filter_first", FILTER_FIRST))
    for src in REBUILD
}


def gen():
    import numpy as np
    import polars as pl

    os.makedirs(DATA, exist_ok=True)
    start = datetime(2024, 1, 1)
    ts = pl.datetime_range(
        start, start + timedelta(days=DAYS) - timedelta(minutes=5), "5m", eager=True
    )
    for i in range(0, DAYS, 20):  # one file per 20 days, like many data files
        t = ts.slice(i * 288, 20 * 288)
        f = pl.DataFrame({"ts": t}).join(
            pl.DataFrame({"duid": [f"U{j:04d}" for j in range(DUIDS)]}), how="cross"
        )
        rng = np.random.default_rng(i)
        f.with_columns(
            date=pl.col("ts").dt.date(),
            mw=pl.Series(rng.random(f.height) * 100),
            intervention=pl.lit(0, pl.Int32),
        ).write_parquet(f"{DATA}/scada_{i:05d}.parquet")
    pl.DataFrame(
        {
            "duid": [f"U{j:04d}" for j in range(DUIDS)],
            "region": [f"R{j % REGIONS}" for j in range(DUIDS)],
        }
    ).write_parquet(f"{DATA}/dim.parquet")
    p = pl.DataFrame({"ts": ts}).join(
        pl.DataFrame({"region": [f"R{j}" for j in range(REGIONS)]}), how="cross"
    )
    p.with_columns(
        date=pl.col("ts").dt.date(), rrp=pl.lit(50.0), intervention=pl.lit(0, pl.Int32)
    ).write_parquet(f"{DATA}/price.parquet")
    last = start.date() + timedelta(days=DAYS - 1)
    pl.DataFrame({"date": [last - timedelta(days=k) for k in range(7)]}).write_parquet(
        f"{DATA}/rebuild.parquet"
    )


def lazy_api():
    """The same filter_last/table query in the LazyFrame API (for the upstream issue)."""
    import polars as pl

    scada = pl.scan_parquet(f"{DATA}/scada_*.parquet").filter(pl.col("intervention") == 0)
    dim = pl.scan_parquet(f"{DATA}/dim.parquet")
    price = pl.scan_parquet(f"{DATA}/price.parquet").filter(pl.col("intervention") == 0)
    rebuild = pl.scan_parquet(f"{DATA}/rebuild.parquet")
    return (
        scada.join(dim, on="duid")
        .join(price, on=["ts", "region"])
        .join(rebuild, on="date", how="semi")
        .group_by("date", "ts", "duid")
        .agg(pl.col("mw").max(), pl.col("rrp").max())
    )


def query(name):
    import polars as pl

    if name.startswith("lazy_api"):
        lf = lazy_api()
    else:
        lf = ctx().execute(VARIANTS[name.removesuffix("/no_join_order")])
    flags = (
        pl.QueryOptFlags(join_order=False)
        if name.endswith("/no_join_order")
        else pl.QueryOptFlags()
    )
    return lf, flags


def ctx():
    import polars as pl

    return pl.SQLContext(
        scada=pl.scan_parquet(f"{DATA}/scada_*.parquet"),
        dim=pl.scan_parquet(f"{DATA}/dim.parquet"),
        price=pl.scan_parquet(f"{DATA}/price.parquet"),
        rebuild=pl.scan_parquet(f"{DATA}/rebuild.parquet"),
    )


def run(name):
    t0 = time.time()
    lf, flags = query(name)
    df = lf.collect(optimizations=flags)  # default engine, as dbt-polars collects
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    print(
        f"{name:26s} rows={df.height:>9,}  {time.time() - t0:6.1f} s  peak RSS {peak:8,.0f} MB",
        flush=True,
    )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run(sys.argv[1])
        sys.stdout.flush()
        os._exit(0)
    import polars as pl

    print(pl.__version__, pl.build_info().get("git", {}), flush=True)
    gen()
    scada_rows = pl.scan_parquet(f"{DATA}/scada_*.parquet").select(pl.len()).collect().item()
    print(f"scada rows: {scada_rows:,}", flush=True)
    names = [*VARIANTS, "filter_last/table/no_join_order", "lazy_api", "lazy_api/no_join_order"]
    for name in names:
        lf, flags = query(name)
        print(f"\n===== explain {name}\n{lf.explain(optimizations=flags)}", flush=True)
    # Is the join order stable? Plan each variant PLANS times; a plan's signature is its
    # sequence of join keys. "dim x price on region first" is the near-cross join.
    import collections
    import re

    plans = int(os.environ.get("PLANS", "20"))
    print(f"\n===== plan stability ({plans} plannings each)", flush=True)
    for name in names:
        seen = collections.Counter()
        for _ in range(plans):
            lf, flags = query(name)
            text = lf.explain(optimizations=flags)
            keys = re.findall(r"PLAN ON: (\[.*?\])", text)
            sig = " | ".join(re.sub(r"__POLARS_JOIN_ORDER_\d+_", "", k) for k in keys)
            seen[sig] += 1
        for sig, n in seen.most_common():
            print(f"{name:32s} {n:3d}x  {sig}", flush=True)
    print(flush=True)
    for name in names:
        for _ in range(int(os.environ.get("RUNS", "3"))):
            subprocess.run([sys.executable, __file__, name], timeout=900)
