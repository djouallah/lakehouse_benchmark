"""TEMPORARY: the 99 TPC-DS queries, Polars 2.0.0 vs the main build. Delete with its workflow.

Bench run 37859527146 (TPC-DS SF=10 over OneLake, Polars main c36581695d) was still in its cold
pass after 38 minutes, where 2.0.0 had answered Q1-Q71 in ~6. The suspicion is that the Q72 fix
(pola-rs/polars#29798) sent another query south. This takes OneLake out: dsdgen -> parquet ->
Iceberg in a local SqlCatalog (landed as bench/tpcds/generate.py lands it), then every query
through the bench's own Polars engine, each in a child process with a time and an RSS cap.

    gen <sf>                 generate and land the tables
    run <sf> <label>         run the 99 queries, write $RUNNER_TEMP/polars-<label>-sf<sf>.json
    compare <dir>            print both versions side by side from the downloaded JSONs
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from bench.tpcds.config import TpcdsConfig
from bench.tpch.queries import load

LIMIT_GIB = 13.0
TIMEOUT_S = {1: 120, 10: 300}


def data_dir(sf: int) -> Path:
    return Path(os.environ.get("RUNNER_TEMP", ".")) / f"tpcds-sf{sf}"


def catalog(sf: int):
    from pyiceberg.catalog.sql import SqlCatalog

    root = data_dir(sf) / "warehouse"
    root.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "local", uri=f"sqlite:///{root / 'catalog.db'}", warehouse=root.resolve().as_uri()
    )


def gen(sf: int) -> None:
    import pyarrow.parquet as pq

    from bench.duckdb_cli import DuckDBCli
    from bench.tpcds.generate import field_ids_clause, load_tpcds_extension

    cfg = TpcdsConfig("", "", sf)
    dest = data_dir(sf)
    dest.mkdir(parents=True, exist_ok=True)
    con = DuckDBCli(str(dest / "gen.duckdb"))
    load_tpcds_extension(con)
    started = time.perf_counter()
    con.sql(f"CALL dsdgen(sf = {sf})")
    cat = catalog(sf)
    cat.create_namespace_if_not_exists(cfg.schema)
    for table in cfg.TABLES:
        src = (dest / f"{table}.parquet").as_posix()
        con.sql(f"COPY {table} TO '{src}' (FORMAT parquet)")
        tbl = cat.create_table(f"{cfg.schema}.{table}", schema=pq.read_schema(src))
        ids = {f.name: f.field_id for f in tbl.schema().fields}
        out = dest / "warehouse" / "data" / table
        out.mkdir(parents=True, exist_ok=True)
        con.sql(
            f"COPY (SELECT * FROM read_parquet('{src}')) TO '{out.as_posix()}' "
            f"(FORMAT parquet, FILE_SIZE_BYTES '200MB', FIELD_IDS {field_ids_clause(ids)})"
        )
        tbl.add_files([p.resolve().as_uri() for p in sorted(out.glob("*.parquet"))])
    con.close()
    (dest / "gen.duckdb").unlink()
    print(f"sf={sf}: {len(cfg.TABLES)} Iceberg tables in {time.perf_counter() - started:.0f}s")


def child(sf: int, n: int) -> None:
    """Query n exactly as the bench runs it on Polars (bench run 37859527146 used load_table)."""
    os.environ.setdefault("POLARS_MAX_THREADS", "4")
    import polars as pl

    from bench.tpch.engines.polars_iceberg import PolarsIceberg

    cfg = TpcdsConfig("", "", sf)
    cat = catalog(sf)
    engine = PolarsIceberg(cfg)
    engine._ctx = pl.SQLContext()
    for table in cfg.TABLES:
        name = f"{cfg.schema}.{table}"
        engine._ctx.register(name, pl.scan_iceberg(cat.load_table(name)))
    sql = load("polars_iceberg", cfg.schema, sf, cfg.SQL_PATH, cfg.N_QUERIES)[n - 1]
    started = time.perf_counter()
    rows = engine.execute(sql)
    print(json.dumps({"rows": rows, "secs": round(time.perf_counter() - started, 3)}))


def run(sf: int, label: str) -> None:
    import subprocess

    import polars as pl
    import psutil

    results = {"polars": pl.__version__, "label": label, "sf": sf, "queries": {}}
    for n in range(1, TpcdsConfig.N_QUERIES + 1):
        proc = psutil.Popen(
            [sys.executable, __file__, "child", str(sf), str(n)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        peak, started, outcome = 0, time.perf_counter(), None
        while proc.poll() is None:
            try:
                rss = sum(p.memory_info().rss for p in [proc, *proc.children(recursive=True)])
            except psutil.NoSuchProcess:
                break
            peak = max(peak, rss)
            if rss > LIMIT_GIB * 2**30:
                outcome = "oom"
                proc.kill()
            elif time.perf_counter() - started > TIMEOUT_S[sf]:
                outcome = "timeout"
                proc.kill()
            time.sleep(0.2)
        out, err = proc.communicate()
        took = time.perf_counter() - started
        entry = {
            "status": outcome or "ok",
            "wall": round(took, 1),
            "peak_gib": round(peak / 2**30, 2),
        }
        if outcome is None:
            if proc.returncode == 0:
                entry.update(json.loads(out.strip().splitlines()[-1]))
            else:
                entry["status"] = "error"
                entry["error"] = (err.strip().splitlines() or ["?"])[-1][:300]
        results["queries"][n] = entry
        print(
            f"  q{n:02d} {entry['status']:7} {entry.get('secs', took):8.1f}s "
            f"rows={entry.get('rows')} peak={entry['peak_gib']:.2f}GiB {entry.get('error', '')}",
            flush=True,
        )
    path = Path(os.environ.get("RUNNER_TEMP", ".")) / f"polars-{label}-sf{sf}.json"
    path.write_text(json.dumps(results, indent=1))
    ok = [q for q in results["queries"].values() if q["status"] == "ok"]
    print(
        f"polars {pl.__version__} ({label}) sf={sf}: {len(ok)}/99 ok, "
        f"{sum(q['secs'] for q in ok):.0f}s over the ok ones"
    )


def _fmt(q: dict) -> str:
    return f"{q['status']} {q.get('secs', q['wall']):.1f}s r={q.get('rows')} {q['peak_gib']}G"


def _twice(slow: dict, fast: dict) -> bool:
    both_ok = slow["status"] == fast["status"] == "ok"
    return both_ok and slow["secs"] > max(2 * fast["secs"], fast["secs"] + 2)


def compare(folder: str) -> None:
    runs = [json.loads(p.read_text()) for p in sorted(Path(folder).rglob("polars-*.json"))]
    for sf in sorted({r["sf"] for r in runs}):
        by = {r["label"]: r for r in runs if r["sf"] == sf}
        if set(by) != {"release", "main"}:
            print(f"sf={sf}: missing a side, have {sorted(by)}")
            continue
        a, b = by["release"], by["main"]
        print(f"\n=== sf={sf}: release {a['polars']} vs main {b['polars']} -- queries that differ")
        print(f"{'q':>3} {'release':>26} {'main':>26}")
        for n in map(str, range(1, 100)):
            x, y = a["queries"][n], b["queries"][n]
            slower, faster = _twice(y, x), _twice(x, y)
            if x["status"] != y["status"] or x.get("rows") != y.get("rows") or slower or faster:
                tag = "SLOWER" if slower else "faster" if faster else "DIFF"
                print(f"{n:>3} {_fmt(x):>26} {_fmt(y):>26}  {tag}")
        for side in (a, b):
            ok = [q for q in side["queries"].values() if q["status"] == "ok"]
            print(f"  {side['label']}: {len(ok)}/99 ok, {sum(q['secs'] for q in ok):.0f}s")


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "gen":
        gen(int(sys.argv[2]))
    elif cmd == "run":
        run(int(sys.argv[2]), sys.argv[3])
    elif cmd == "child":
        child(int(sys.argv[2]), int(sys.argv[3]))
    elif cmd == "compare":
        compare(sys.argv[2])
