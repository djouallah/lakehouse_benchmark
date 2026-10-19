"""Generate TPC-DS at scale factor N into OneLake as Iceberg: tpcgen-cli, then TPC-H's landing.

THE GENERATOR IS tpcgen-cli 4.0.0 (`tpcgen-cli tpcds parquet`), the same binary that writes TPC-H
(bench/tpch/generate.py), pinned in requirements/catalog.txt. Its output matches the C `dsdgen`.
Until 2026-10-20 this was DuckDB's `CALL dsdgen()`: tpcgen's TPC-DS was not on PyPI yet, and
DuckDB had to be the DuckDB engine's own nightly because dsdgen's values differ between DuckDB
builds (LEARNING.md). tpcgen removes both problems: one pinned build generates every scale, here
and in smoke.yml, and no engine is also the generator.

THE LANDING IS TPC-H'S. Per table, tpcgen-cli writes every part in parallel (--parts N) to local
scratch, the files upload to Tables/<ns>/<table>/ as written, and pyiceberg registers them with
one add_files. No re-encode: the files are zstd with ~128 MB row groups (PARQUET_FLAGS) and carry
Iceberg field ids, 1-based by position, which are exactly the ids pyiceberg gives the table it
creates -- add_files checks them, so a mismatch is a loud failure. (DuckDB's own Iceberg writer
was tried first and dropped: it hung on `inventory` at SF=10, run 35726316639.)

RUNNER BUDGET. Only one table is on local disk at a time, and each file is deleted as it uploads.
store_sales, the largest, is ~12 GB at SF=100: tpcds.yml frees the preinstalled toolchains and
moves this scratch to /mnt (TPCDS_SCRATCH) for SF>=30.

IDEMPOTENT the way the TPC-H generate is, through the same two functions: the completion marker
is the same table property, written on this suite's MARKER_TABLE (`web_site`, last in TABLES).
Per table, one that already has data files is skipped so a crashed run resumes rather than
restarts, and an empty husk -- created by a run that died before its files were registered --
just gets its files. `force` (tpcds.yml's `regenerate` input) purges every existing table first
and writes the namespace from scratch.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from bench import auth, onelake, scrub
from bench.tpcds.config import TpcdsConfig, parts_for
from bench.tpch.generate import PARQUET_FLAGS, _ensure_table, _mark_complete, is_complete


def _log(message: str) -> None:
    scrub.safe_print(message)


def _scratch() -> Path:
    """Where the parquet-in-flight lives: the runner's temp, gone with it -- or TPCDS_SCRATCH,
    which tpcds.yml points at /mnt for SF>=30."""
    root = Path(
        os.environ.get("TPCDS_SCRATCH")
        or Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir())) / "tpcds"
    )
    root.mkdir(parents=True, exist_ok=True)
    return root


def tpcgen_command(sf: int, table: str, parts: int, out_dir: Path) -> list[str]:
    """The tpcgen-cli call for one table. Pure, so tests/test_tpcds_generate.py can pin it."""
    command = ["tpcgen-cli", "tpcds", "parquet", *PARQUET_FLAGS, "-s", str(sf)]
    command += ["--tables", table, "--output-dir", str(out_dir)]
    if parts > 1:
        command += ["--parts", str(parts)]
    return command


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


def build_table(catalog, fs, cfg: TpcdsConfig, table: str, tmp: Path, pool) -> dict:
    """Generate one table with tpcgen-cli, upload its files as written, register them."""
    import pyarrow.parquet as pq

    started = time.perf_counter()
    parts = parts_for(table, cfg.sf)
    out_dir = tmp / table
    out_dir.mkdir(parents=True, exist_ok=True)
    _log(f"--- {table} ({parts} parts) ---")

    subprocess.run(tpcgen_command(cfg.sf, table, parts, out_dir), check=True)
    files = sorted(out_dir.rglob("*.parquet"))
    if not files:
        raise RuntimeError(f"{table}: tpcgen-cli produced no parquet under {out_dir}")
    gen_s = time.perf_counter() - started
    rows = sum(pq.read_metadata(path).num_rows for path in files)

    tbl = _ensure_table(catalog, cfg, table, files[0])

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
    # file would be `len(files)` round-trips and snapshots for no benefit. It also checks the
    # files' field ids against the table schema.
    tbl.add_files(registered, check_duplicate_files=False)
    register_s = time.perf_counter() - register_started
    shutil.rmtree(out_dir, ignore_errors=True)

    wall = time.perf_counter() - started
    _log(
        f"  {len(files)} files, {rows:,} rows, {nbytes / 2**30:.2f} GiB | gen {gen_s:.1f}s, "
        f"upload {upload_s:.1f}s, add_files {register_s:.1f}s, wall {wall:.1f}s"
    )
    return {
        "files": len(files),
        "rows": rows,
        "bytes": nbytes,
        "gen_s": round(gen_s, 1),
        "upload_s": round(upload_s, 1),
        "register_s": round(register_s, 1),
        "wall_s": round(wall, 1),
    }


def generate(cfg: TpcdsConfig, force: bool = False) -> dict:
    """Generate every TPC-DS table for `cfg.sf` into OneLake. Idempotent; `force` regenerates."""
    os.environ.setdefault("PYICEBERG_MAX_WORKERS", str(cfg.pyiceberg_workers))
    catalog = auth.catalog(cfg)
    if not force and is_complete(catalog, cfg):
        _log(f"{cfg.schema} already generated at SF={cfg.sf} - skipping")
        return {"sf": cfg.sf, "skipped": True, "namespace": cfg.schema, "tables": {}}

    if shutil.which("tpcgen-cli") is None:
        raise RuntimeError("tpcgen-cli not on PATH; install requirements/catalog.txt")

    scratch = _scratch()
    _log(f"Generating TPC-DS SF={cfg.sf} with tpcgen-cli under {scratch}")
    overall = time.perf_counter()
    catalog.create_namespace_if_not_exists(cfg.schema)
    fs = onelake.file_system(cfg)
    results: dict[str, dict] = {}
    totals = {"gen_s": 0.0, "upload_s": 0.0, "bytes": 0, "rows": 0}
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
            stats = build_table(catalog, fs, cfg, table, Path(tmp), pool)
            results[table] = stats
            for key in totals:
                totals[key] += stats[key]

    _mark_complete(catalog, cfg)
    elapsed = time.perf_counter() - overall
    _log(
        f"total {elapsed:.1f}s wall | gen {totals['gen_s']:.1f}s | upload {totals['upload_s']:.1f}s "
        f"| {totals['bytes'] / 2**30:.2f} GiB, {totals['rows']:,} rows"
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
