"""TEMPORARY: TPC-DS Q72 on Polars 2.0.0, plain parquet, peak RSS. Delete with its workflow.

Bench run 37469601767 (TPC-DS SF=10 over OneLake) answered Q1-Q71, then Q72 ran 15 minutes and
the process was killed (exit 143) on a 16 GB runner. This takes Iceberg out: dsdgen -> parquet,
Q72 through the bench's own Polars engine (`collect(engine="streaming")`) in a child process,
the parent sampling the child's RSS. DuckDB runs the same files for comparison.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import psutil

from bench.tpcds.config import TpcdsConfig
from bench.tpch.queries import load

TABLES = (
    "catalog_sales", "inventory", "warehouse", "item", "customer_demographics",
    "household_demographics", "date_dim", "promotion", "catalog_returns",
)  # fmt: skip
LIMIT_GIB = 13.0
TIMEOUT_S = 1200


def data_dir(sf: int) -> Path:
    return Path(os.environ.get("RUNNER_TEMP", ".")) / f"tpcds-sf{sf}"


def generate(sf: int) -> None:
    from bench.duckdb_cli import DuckDBCli
    from bench.tpcds.generate import load_tpcds_extension

    dest = data_dir(sf)
    dest.mkdir(parents=True, exist_ok=True)
    con = DuckDBCli(str(dest / "gen.duckdb"))
    load_tpcds_extension(con)
    started = time.perf_counter()
    con.sql(f"CALL dsdgen(sf = {sf})")
    for table in TABLES:
        con.sql(f"COPY {table} TO '{(dest / f'{table}.parquet').as_posix()}' (FORMAT parquet)")
    con.close()
    (dest / "gen.duckdb").unlink()
    sizes = {t: (dest / f"{t}.parquet").stat().st_size / 2**20 for t in TABLES}
    print(f"sf={sf}: generated in {time.perf_counter() - started:.0f}s; MB {sizes}", flush=True)


def iceberg(sf: int) -> None:
    """The same tables as Iceberg in a local SqlCatalog, landed the way bench/tpcds/generate.py
    lands them: table created from a 0-row sample, ~200 MB parquet parts COPYed with the table's
    FIELD_IDS, registered with one add_files per table."""
    import pyarrow.parquet as pq
    from pyiceberg.catalog.sql import SqlCatalog

    from bench.duckdb_cli import DuckDBCli
    from bench.tpcds.generate import field_ids_clause

    root = data_dir(sf) / "warehouse"
    root.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog(
        "local", uri=f"sqlite:///{root / 'catalog.db'}", warehouse=root.resolve().as_uri()
    )
    cfg = TpcdsConfig("", "", sf)
    catalog.create_namespace_if_not_exists(cfg.schema)
    con = DuckDBCli()
    for table in TABLES:
        src = (data_dir(sf) / f"{table}.parquet").as_posix()
        identifier = f"{cfg.schema}.{table}"
        if catalog.table_exists(identifier):
            catalog.drop_table(identifier)
        tbl = catalog.create_table(identifier, schema=pq.read_schema(src))
        ids = {f.name: f.field_id for f in tbl.schema().fields}
        out = root / "data" / table
        out.mkdir(parents=True, exist_ok=True)
        con.sql(
            f"COPY (SELECT * FROM read_parquet('{src}')) TO '{out.as_posix()}' "
            f"(FORMAT parquet, FILE_SIZE_BYTES '200MB', FIELD_IDS {field_ids_clause(ids)})"
        )
        tbl.add_files([p.resolve().as_uri() for p in sorted(out.glob("*.parquet"))])
    con.close()
    print(f"sf={sf}: iceberg tables in {root}", flush=True)


def q72(engine: str, sf: int) -> str:
    cfg = TpcdsConfig("", "", sf)
    return load(engine, cfg.schema, sf, TpcdsConfig.SQL_PATH, TpcdsConfig.N_QUERIES)[71]


def child(sf: int, source: str) -> None:
    """Q72 exactly as the bench runs it on Polars, over parquet or over the local Iceberg."""
    os.environ.setdefault("POLARS_MAX_THREADS", "4")
    import polars as pl

    from bench.tpch.engines.polars_iceberg import PolarsIceberg

    cfg = TpcdsConfig("", "", sf)
    engine = PolarsIceberg(cfg)
    engine._ctx = pl.SQLContext()
    if source == "iceberg":
        from pyiceberg.catalog.sql import SqlCatalog

        root = data_dir(sf) / "warehouse"
        catalog = SqlCatalog(
            "local", uri=f"sqlite:///{root / 'catalog.db'}", warehouse=root.resolve().as_uri()
        )
    for table in TABLES:
        if source == "iceberg":
            frame = pl.scan_iceberg(catalog.load_table(f"{cfg.schema}.{table}"))
        else:
            frame = pl.scan_parquet(data_dir(sf) / f"{table}.parquet")
        engine._ctx.register(f"{cfg.schema}.{table}", frame)
    started = time.perf_counter()
    rows = engine.execute(q72("polars_iceberg", sf))
    took = time.perf_counter() - started
    print(f"  polars {pl.__version__} over {source}: {rows} rows in {took:.1f}s")


def polars(sf: int, source: str) -> None:
    proc = psutil.Popen([sys.executable, __file__, "child", str(sf), source])
    peak, started, outcome = 0, time.perf_counter(), None
    while proc.poll() is None:
        try:
            rss = sum(p.memory_info().rss for p in [proc, *proc.children(recursive=True)])
        except psutil.Error:
            rss = 0
        peak = max(peak, rss)
        took = time.perf_counter() - started
        if rss > LIMIT_GIB * 2**30:
            outcome = f"KILLED at {rss / 2**30:.1f} GiB RSS after {took:.0f}s"
            proc.kill()
        elif took > TIMEOUT_S:
            outcome = f"KILLED after {TIMEOUT_S}s, RSS {rss / 2**30:.1f} GiB"
            proc.kill()
        time.sleep(0.2)
    took = time.perf_counter() - started
    outcome = outcome or f"exit {proc.returncode} after {took:.0f}s"
    print(f"sf={sf} polars Q72 over {source}: {outcome}; peak RSS {peak / 2**30:.2f} GiB")


def duckdb(sf: int) -> None:
    from bench.duckdb_cli import DuckDBCli

    cfg = TpcdsConfig("", "", sf)
    con = DuckDBCli()
    con.sql(f"CREATE SCHEMA {cfg.schema}")
    for table in TABLES:
        path = (data_dir(sf) / f"{table}.parquet").as_posix()
        con.sql(f"CREATE VIEW {cfg.schema}.{table} AS SELECT * FROM read_parquet('{path}')")
    con.sql(f"USE {cfg.schema}")
    started = time.perf_counter()
    rows = len(con.sql(q72("duckdb_iceberg", sf)).fetchall())
    print(f"sf={sf} duckdb Q72: {rows} rows in {time.perf_counter() - started:.1f}s", flush=True)
    con.close()


if __name__ == "__main__":
    if sys.argv[1] == "child":
        child(int(sys.argv[2]), sys.argv[3])
    elif sys.argv[1] == "sql":
        print(q72("polars_iceberg", 10))
    else:
        sf = int(sys.argv[1])
        generate(sf)
        duckdb(sf)
        polars(sf, "parquet")
        iceberg(sf)
        polars(sf, "iceberg")
