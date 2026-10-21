"""TEMPORARY: does Polars' streaming engine hold the workload's grain test on fct_scada's size?

    python tmp_polars_grain.py gen  <dir> <files>   # <files> parquet files, ~155k rows each
    python tmp_polars_grain.py test <dir> <files>   # the grain test over the first <files>

The workload's polars runner died in assert_fct_scada_grain (run 39524954142): a GROUP BY on the
merge key over ~100M rows (635 files then), i.e. one group per row. Same shape here: per file,
~540 DUIDs x 288 intervals, a `file` string, INTERVENTION 0. No OneLake.
"""

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

DUIDS = 540
INTERVALS = 288


def gen(out: Path, files: int) -> None:
    out.mkdir(parents=True, exist_ok=True)
    duids = pl.Series("DUID", [f"DUID{i:04d}" for i in range(DUIDS)])
    for f in range(files):
        day = datetime(2019, 1, 1) + timedelta(days=f)
        times = pl.datetime_range(
            day, day + timedelta(minutes=5 * (INTERVALS - 1)), "5m", eager=True
        ).alias("SETTLEMENTDATE")
        name = f"PUBLIC_DAILY_{day:%Y%m%d}0000_00000000{f:010d}.CSV"
        df = (
            pl.DataFrame(duids)
            .join(pl.DataFrame(times), how="cross")
            .with_columns(
                pl.lit(name).alias("file"),
                pl.lit(0.0).alias("INTERVENTION"),
                pl.lit(1.5).alias("INITIALMW"),
            )
        )
        df.write_parquet(out / f"part_{f:05d}.parquet")
    print(f"generated {files} files, {files * DUIDS * INTERVALS:,} rows", flush=True)


def test(src: Path, files: int) -> None:
    paths = sorted(src.glob("*.parquet"))[:files]
    ctx = pl.SQLContext(fct_scada=pl.scan_parquet(paths))
    q = ctx.execute(
        "SELECT file, DUID, SETTLEMENTDATE, INTERVENTION, COUNT(*) AS n FROM fct_scada "
        "GROUP BY ALL HAVING COUNT(*) > 1",
        eager=False,
    )
    t = time.time()
    out = q.collect()
    print(
        f"{files} files, {files * DUIDS * INTERVALS:,} rows: {out.height} duplicate keys, "
        f"{time.time() - t:.1f} s, polars {pl.__version__}",
        flush=True,
    )


if __name__ == "__main__":
    mode, path, n = sys.argv[1], Path(sys.argv[2]), int(sys.argv[3])
    (gen if mode == "gen" else test)(path, n)
