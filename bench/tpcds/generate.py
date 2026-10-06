"""Generate TPC-DS at scale factor N into OneLake as Iceberg: DuckDB's dsdgen, then TPC-H's landing.

WHY DUCKDB AND NOT tpcgen. The tpcgen-rs project (the tpchgen-cli that bench/tpch/generate.py
runs) added TPC-DS in its v3.0.0, but as of 2026-09-22 the binary that carries it, `tpcgen-cli`,
is not on PyPI, the crates.io build is 0.1.0-alpha.1 without `--parts` for TPC-DS, and multi-part
TPC-DS output has an open correctness issue. DuckDB's `tpcds` extension is a pip install away,
deterministic, and `CALL dsdgen(sf = 10)` is the whole generator.

WHY THE WRITE PATH IS TPC-H'S AND NOT DUCKDB'S. The first version had DuckDB's own Iceberg
writer CTAS every table into OneLake through the ETL's write-capable ATTACH. It worked at SF=1
(24 tables, 173 s) and stalled at SF=10: store_sales went through, then `inventory` -- 133 M
rows, ~340 MB, the highest row count of any table -- sat for 45 minutes with no output (run
35726316639; store_sales, 1.2 GB, had taken 30 s). And what it does write is its own shape: one
file per table at SF=1, three ~400 MB files for store_sales at SF=10, nothing a reader can be
told. So the writer is gone. dsdgen fills a local DuckDB database, DuckDB's PARQUET
writer COPYs each table out in ~200 MB files, the files upload to Tables/<ns>/<table>/ as-is,
and pyiceberg registers them with one add_files per table -- the TPC-H path, proven at SF=10
with all six engines reading the result.

NO FIELD-ID REWRITE. TPC-H pays a pyarrow decode/encode of every file to stamp the Iceberg field
ids Polars needs. DuckDB's parquet writer stamps them itself given `FIELD_IDS {col: id, ...}`
(checked on 1.5.5 and the 2.0 nightly: none by default, 'auto' counts from 0 which pyiceberg
does not, an explicit map lands exactly). So the Iceberg table is created FIRST, from a 0-row
sample, and its ids are handed to COPY. add_files validates the ids against the table schema:
a mismatch is a loud failure, never a silently mis-registered file.

THE GENERATOR IS THE DUCKDB ENGINE'S BUILD, and that is load-bearing. dsdgen's output is not the
same across DuckDB builds: measured 2026-09-22, stable 1.5.5 and the 2.0 nightly the DuckDB
engine runs give identical row counts and DIFFERENT values (prices, dates, text), so 18 of the 99
queries return different row counts on the two datasets. tpcds.yml's prepare job installs the
nightly CLI with the duckdb-nightly action and hands its run id to the DuckDB engine job, and
smoke.yml does the same for its SF=1 copy -- one generator per dataset, never two.

RUNNER BUDGET. dsdgen(sf=10) is ~3-4 GB of DuckDB storage on a 14 GB disk / 16 GB RAM runner.
The database is FILE-BACKED so generation spills instead of dying, memory_limit sits below the
runner's RAM, and the parquet for ONE table at a time sits beside it (store_sales, the largest,
~1.2 GB) and is deleted as it uploads. SF=30 and SF=100 need more: tpcds.yml
frees the preinstalled toolchains and moves this scratch to /mnt (TPCDS_SCRATCH) for them.

IDEMPOTENT the way the TPC-H generate is, through the same two functions: the completion marker
is the same table property, written on this suite's MARKER_TABLE (`web_site`, last in TABLES).
Per table, one that already has data files is skipped so a crashed run resumes rather than
restarts, and an empty husk -- created by a run that died before its files were registered --
just gets its files. `force` (tpcds.yml's `regenerate` input) purges every existing table first
and writes the namespace from scratch; that is how the DuckDB-written DS0010 was replaced.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from bench import auth, onelake, scrub
from bench.tpcds.config import TpcdsConfig
from bench.tpch.config import TARGET_PART_MB
from bench.tpch.generate import _ensure_table, _mark_complete, is_complete

# Below the runner's 16 GB: the ADLS client, the Python process and the OS need the rest.
MEMORY_LIMIT = "10GB"
THREADS = 4


def _log(message: str) -> None:
    scrub.safe_print(message)


def _scratch() -> Path:
    """Where the local database, DuckDB's spill files and the parquet-in-flight live: the
    runner's temp, gone with it -- or TPCDS_SCRATCH, which tpcds.yml points at /mnt for SF>=30."""
    root = Path(
        os.environ.get("TPCDS_SCRATCH")
        or Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir())) / "tpcds"
    )
    (root / "tmp").mkdir(parents=True, exist_ok=True)
    return root


def load_tpcds_extension(con) -> None:
    """INSTALL and LOAD `tpcds`, falling back to the nightly repository.

    The DuckDB jobs run a nightly CLI (bench/duckdb_cli.py), and a dev build's extensions may be
    served from `core_nightly` rather than the default repository. Trying the
    default first keeps a stable build on the release extension. Shared with
    .github/scripts/smoke_sql.py, which generates SF=1 locally.
    """
    try:
        con.sql("INSTALL tpcds")
    except Exception as exc:  # noqa: BLE001 - the fallback is the point
        _log(f"  INSTALL tpcds failed ({scrub.scrub_exc(exc, 160)}); trying core_nightly")
        con.sql("INSTALL tpcds FROM core_nightly")
    con.sql("LOAD tpcds")


def plan_table(exists: bool, has_files: bool, force: bool) -> str:
    """The resume decision for one table: 'skip', 'write' or 'purge' (then write).

    Pure, so tests/test_tpcds_generate.py can pin it. A table with data files is done and is
    skipped; a husk is written into; `force` throws away whatever is there first.
    """
    if force:
        return "purge" if exists else "write"
    if exists and has_files:
        return "skip"
    return "write"


def field_ids_clause(ids: dict[str, int]) -> str:
    """The `FIELD_IDS {col: id, ...}` struct literal DuckDB's parquet writer takes."""
    return "{" + ", ".join(f"{name}: {field_id}" for name, field_id in ids.items()) + "}"


def _has_data(catalog, identifier: str) -> bool:
    return any(True for _ in catalog.load_table(identifier).scan().plan_files())


def _purge(catalog, identifier: str) -> None:
    """Drop the table and, where OneLake allows it, its files. Same fallback as the ETL's
    recreate(): purge needs more permission than drop, and metadata-only is still a fresh
    table -- the new files carry new names, so the orphans cost space and nothing else."""
    try:
        catalog.purge_table(identifier)
    except Exception as exc:  # noqa: BLE001 - purge needs more permission than drop
        _log(f"  purge refused ({scrub.scrub_exc(exc, 160)}); dropping metadata only")
        catalog.drop_table(identifier)


def _upload(fs, local: Path, remote: str, concurrency: int) -> int:
    nbytes = local.stat().st_size
    with open(local, "rb") as handle:
        fs.get_file_client(remote).upload_data(handle, overwrite=True, max_concurrency=concurrency)
    local.unlink(missing_ok=True)
    return nbytes


def build_table(con, catalog, fs, cfg: TpcdsConfig, table: str, tmp: Path, pool) -> dict:
    """COPY one table out of the local database in ~200 MB parquet files, upload them, register
    them. The Iceberg table is created first so COPY can stamp its field ids."""
    started = time.perf_counter()
    out_dir = tmp / table
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = int(con.sql(f"SELECT count(*) FROM main.{table}").fetchone()[0])
    _log(f"--- {table} ({rows:,} rows) ---")

    # A 0-row file carries the schema; the Iceberg table comes from it, and its ids go to COPY.
    sample = out_dir / "sample.parquet"
    con.sql(f"COPY (SELECT * FROM main.{table} LIMIT 0) TO '{sample.as_posix()}' (FORMAT parquet)")
    tbl = _ensure_table(catalog, cfg, table, sample)
    sample.unlink()
    ids = {field.name: field.field_id for field in tbl.schema().fields}

    copy_started = time.perf_counter()
    con.sql(
        f"COPY main.{table} TO '{out_dir.as_posix()}' "
        f"(FORMAT parquet, FILE_SIZE_BYTES '{TARGET_PART_MB}MB', FIELD_IDS {field_ids_clause(ids)})"
    )
    files = sorted(out_dir.glob("*.parquet"))
    if not files:
        raise RuntimeError(f"{table}: COPY produced no parquet under {out_dir}")
    copy_s = time.perf_counter() - copy_started

    upload_started = time.perf_counter()
    registered, futures = [], []
    for index, path in enumerate(files):
        name = f"{index:05d}-{path.name}"
        registered.append(f"{onelake.table_root(cfg, table)}/{name}")
        remote = onelake.relative(cfg, table, name)
        futures.append(pool.submit(_upload, fs, path, remote, cfg.upload_concurrency))
    nbytes = sum(future.result() for future in futures)
    upload_s = time.perf_counter() - upload_started

    register_started = time.perf_counter()
    # One add_files for the whole table: each call is an Iceberg commit, and one commit per
    # file would be `len(files)` round-trips and snapshots for no benefit.
    tbl.add_files(registered, check_duplicate_files=False)
    register_s = time.perf_counter() - register_started
    shutil.rmtree(out_dir, ignore_errors=True)

    wall = time.perf_counter() - started
    _log(
        f"  {len(files)} files, {rows:,} rows, {nbytes / 2**30:.2f} GiB | copy {copy_s:.1f}s, "
        f"upload {upload_s:.1f}s, add_files {register_s:.1f}s, wall {wall:.1f}s"
    )
    return {
        "files": len(files),
        "rows": rows,
        "bytes": nbytes,
        "copy_s": round(copy_s, 1),
        "upload_s": round(upload_s, 1),
        "register_s": round(register_s, 1),
        "wall_s": round(wall, 1),
    }


def generate(cfg: TpcdsConfig, force: bool = False) -> dict:
    """Generate every TPC-DS table for `cfg.sf` into OneLake. Idempotent; `force` regenerates."""
    from bench.duckdb_cli import DuckDBCli

    os.environ.setdefault("PYICEBERG_MAX_WORKERS", str(cfg.pyiceberg_workers))
    catalog = auth.catalog(cfg)
    if not force and is_complete(catalog, cfg):
        _log(f"{cfg.schema} already generated at SF={cfg.sf} - skipping")
        return {"sf": cfg.sf, "skipped": True, "namespace": cfg.schema, "tables": {}}

    scratch = _scratch()
    con = DuckDBCli(str(scratch / f"sf{cfg.sf}.duckdb"))
    con.sql(
        f"SET memory_limit = '{MEMORY_LIMIT}'; SET threads = {THREADS}; "
        f"SET temp_directory = '{(scratch / 'tmp').as_posix()}'"
    )
    load_tpcds_extension(con)

    _log(f"Generating TPC-DS SF={cfg.sf} with dsdgen under {scratch}")
    overall = time.perf_counter()
    started = time.perf_counter()
    con.sql(f"CALL dsdgen(sf = {cfg.sf})")
    gen_s = time.perf_counter() - started
    _log(f"  dsdgen {gen_s:.1f}s")

    catalog.create_namespace_if_not_exists(cfg.schema)
    fs = onelake.file_system(cfg)
    results: dict[str, dict] = {}
    totals = {"gen_s": gen_s, "copy_s": 0.0, "upload_s": 0.0, "bytes": 0, "rows": 0}
    with (
        tempfile.TemporaryDirectory(dir=scratch) as tmp,
        ThreadPoolExecutor(cfg.upload_threads) as pool,
    ):
        for table in cfg.TABLES:
            identifier = f"{cfg.schema}.{table}"
            exists = catalog.table_exists(identifier)
            action = plan_table(exists, exists and _has_data(catalog, identifier), force)
            if action == "skip":
                _log(f"--- {table}: already has data files, skipped")
                results[table] = {"skipped": True}
                continue
            if action == "purge":
                _log(f"--- {table}: exists, regenerating")
                _purge(catalog, identifier)
            stats = build_table(con, catalog, fs, cfg, table, Path(tmp), pool)
            results[table] = stats
            for key in ("copy_s", "upload_s", "bytes", "rows"):
                totals[key] += stats[key]

    con.close()
    _mark_complete(catalog, cfg)
    elapsed = time.perf_counter() - overall
    _log(
        f"total {elapsed:.1f}s wall | dsdgen {totals['gen_s']:.1f}s | copy {totals['copy_s']:.1f}s "
        f"| upload {totals['upload_s']:.1f}s | {totals['bytes'] / 2**30:.2f} GiB, "
        f"{totals['rows']:,} rows"
    )
    return {
        "sf": cfg.sf,
        "skipped": False,
        "namespace": cfg.schema,
        "tables": results,
        "elapsed_s": elapsed,
        **totals,
    }


if __name__ == "__main__":  # pragma: no cover
    sys.exit(0 if generate(TpcdsConfig.from_env()) else 1)
