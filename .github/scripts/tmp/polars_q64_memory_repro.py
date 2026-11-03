"""TPC-DS Q64 on local parquet: peak RSS of the streaming engine against POLARS_OOC_MEMORY_BUDGET_MB.

TEMPORARY, for a Polars issue draft. The bench's TPC-DS SF=300 Q64 takes the 16 GB runner down 48 s
in, with a 10 GB budget (run 41727713405, main f07718c97e). This checks the same query off OneLake:
tpcgen-cli parquet on local disk, pl.scan_parquet, one fresh process per budget.

    SF=30 BUDGETS=none,2000,1000 python polars_q64_memory_repro.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path

TABLES = ["store_sales", "store_returns", "catalog_sales", "catalog_returns", "date_dim", "store",
          "customer", "customer_demographics", "promotion", "household_demographics",
          "customer_address", "income_band", "item"]
SQL = Path(__file__).resolve().parents[3] / "sql" / "tpcds.sql"


def q64() -> str:
    text = SQL.read_text()
    body = text.split("-- Query 64")[1].split("-- Query 65")[0]
    return body.replace("`{schema}.", "").replace("`", "").strip().rstrip(";")


def generate(sf: int, out: Path) -> None:
    for table in TABLES:
        if (out / table).exists():
            continue
        t0 = time.time()
        subprocess.run(["tpcgen-cli", "tpcds", "parquet", "-s", str(sf), "--tables", table,
                        "--parts", "8" if table.endswith("sales") else "1",
                        "--output-dir", str(out / table)], check=True)
        print(f"  generated {table} in {time.time() - t0:.0f}s", flush=True)


def child(out: Path) -> None:
    """One query in this process. Prints peak RSS, sampled every 50 ms, as the runner sees it."""
    import psutil
    import polars as pl

    proc, peak, done = psutil.Process(), [0], threading.Event()

    def sample() -> None:
        while not done.is_set():
            peak[0] = max(peak[0], proc.memory_info().rss)
            time.sleep(0.05)

    ctx = pl.SQLContext({t: pl.scan_parquet(f"{out / t}/**/*.parquet") for t in TABLES})
    threading.Thread(target=sample, daemon=True).start()
    t0 = time.time()
    rows = ctx.execute(q64()).collect(engine="streaming").height
    done.set()
    print(f"RESULT rows={rows} seconds={time.time() - t0:.1f} peak_rss_mb={peak[0] / 2**20:.0f}",
          flush=True)


def main() -> None:
    sf = int(os.environ.get("SF", "30"))
    out = Path(os.environ.get("DATA_DIR", f"tpcds_sf{sf}"))
    generate(sf, out)
    import polars as pl
    print(f"polars {pl.__version__}, build {os.environ.get('POLARS_SHA', '?')}, SF={sf}", flush=True)
    for budget in os.environ.get("BUDGETS", "none,2000,1000").split(","):
        env = {**os.environ, "POLARS_MAX_THREADS": "4"}
        if budget != "none":
            env["POLARS_OOC_MEMORY_BUDGET_MB"] = budget
        result = subprocess.run([sys.executable, __file__, "child", str(out)], env=env,
                                capture_output=True, text=True, timeout=1800)
        line = next((l for l in result.stdout.splitlines() if l.startswith("RESULT")),
                    f"exit {result.returncode} {result.stderr.strip()[-300:]}")
        print(f"budget={budget} MB: {line}", flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "child":
        child(Path(sys.argv[2]))
    else:
        main()
