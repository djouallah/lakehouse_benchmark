"""Generate TPC-H at scale factor N straight into OneLake as Iceberg.

Port of cell 9. The design is the notebook's and it is a good one, quoted from its own docstring:

    tpchgen-cli writes each parquet once, the bytes upload as-is, and the table resolves columns
    via a default name mapping - no re-encode, no field IDs, no query engine.

Generation and upload overlap: the main thread only runs `tpcgen-cli tpch`, each part's upload goes
to a thread. A semaphore caps how many parts sit on local disk at once, which is what keeps peak
disk proportional to PART SIZE rather than to dataset size. That is the property that lets SF=10
(2.7 GiB in OneLake) run on a 14 GB runner.

THE FILES CARRY ICEBERG FIELD IDS AND ARE UPLOADED AS WRITTEN. Polars' `scan_iceberg` reads the
ids out of the parquet footer and fails without them (`SchemaFieldNotFoundError: ... failed to
load 'PARQUET:field_id'`). Until tpcgen-rs 4.0.0 the generator wrote none, and every file paid a
pyarrow decode/encode here to stamp them. 4.0.0 writes them itself, 1-based by column position
(tpcgen-rs#509, #537) -- exactly the ids pyiceberg gives a flat schema it creates -- and
`add_files` checks them against the table, so a mismatch is a loud failure, never a silent one.
Compression and row-group size, which the rewrite used to set, are now tpcgen-cli flags.

WHAT CHANGED from the notebook:

* `StaticToken` is gone -- see bench/onelake.py for why it was actively harmful.
* The lazy mid-run `pip install` of tpchgen-cli and azure-storage-file-datalake is gone; the
  workflow installs requirements before Python starts.
* Part counts come from `config.parts_plan`, which targets a FILE SIZE instead of scaling off
  SF=1000. See config.TARGET_PART_MB.
* The completion guard is a table property, not `table_exists`. See `_mark_complete`.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from bench import onelake, scrub
from bench.config import Config
from bench.tpch.config import TpchConfig, parts_plan

# The property that says "this namespace is fully generated", written on the suite's
# MARKER_TABLE (`supplier` here, last in generation order) once every table has landed. The
# TPC-DS generator writes the same property on its own last table, through the same two
# functions below.
#
# NOT `catalog.table_exists(f"{namespace}.supplier")`, which is what cell 9 used. That works only
# because `supplier` happens to be last in the generation order, and it cannot distinguish a
# finished table from one that `create_table_if_not_exists` created a moment before the process
# died -- an EMPTY supplier reads as "done", and every later run skips generation and then
# benchmarks nothing.
COMPLETE_PROPERTY = "bench.generation-complete"

# The file shape tpcgen-cli writes and the files upload as: zstd, ~128 MB (uncompressed) row
# groups -- about the 1M-row groups the old pyarrow rewrite produced for lineitem. The TPC-DS
# generator writes the same shape (bench/tpcds/generate.py).
PARQUET_FLAGS = ("--compression", "ZSTD(1)", "--row-group-bytes", "128MB")


def _log(message: str) -> None:
    scrub.safe_print(message)


def is_complete(catalog, cfg: Config) -> bool:
    """True if this namespace already holds a usable dataset at this scale factor.

    Two ways to be complete, and the second one matters more than it looks:

    1. THE MARKER, written by this code after every table has landed.
    2. `supplier` EXISTS AND HAS DATA FILES. The Fabric notebook generated CH0100 long before
       this repo existed, so that namespace has real data and no marker. Without this clause the
       guard says "incomplete", generation runs again, and `add_files(check_duplicate_files=False)`
       REGISTERS THE SAME FILES A SECOND TIME -- every row counted twice, silently, in a table
       somebody is still using. Regenerating is never the safe default when data is already there.

    What both clauses still reject is an EMPTY supplier, which is what a crash between
    `create_table_if_not_exists` and `add_files` leaves behind. That is the case the notebook's
    bare `table_exists` check got wrong: it read the husk as "done" and benchmarked nothing.
    """
    identifier = f"{cfg.schema}.{cfg.MARKER_TABLE}"
    if not catalog.table_exists(identifier):
        return False
    table = catalog.load_table(identifier)
    if table.properties.get(COMPLETE_PROPERTY, "").startswith(f"{cfg.sf}|"):
        return True
    return any(True for _ in table.scan().plan_files())


def _mark_complete(catalog, cfg: Config) -> None:
    table = catalog.load_table(f"{cfg.schema}.{cfg.MARKER_TABLE}")
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with table.transaction() as tx:
        tx.set_properties(**{COMPLETE_PROPERTY: f"{cfg.sf}|{stamp}"})


def _all_optional(schema):
    """Every field nullable.

    tpcgen-cli marks most columns NOT NULL. Iceberg's required/optional flag is part of the
    schema identity, and a required field that any engine reads as optional is a mismatch at scan
    time, so the table is declared entirely optional and the parquet is left untouched.
    """
    import pyarrow as pa

    return pa.schema([pa.field(f.name, f.type, nullable=True, metadata=f.metadata) for f in schema])


def _ensure_table(catalog, cfg: Config, table: str, sample: Path):
    """Create the Iceberg table from a sample parquet's schema, with a default name mapping.

    The name mapping is kept even though the files carry field ids: it costs one property and it
    is what lets an engine that ignores ids still resolve columns by name.
    """
    import pyarrow.parquet as pq
    from pyiceberg.table.name_mapping import create_mapping_from_schema

    identifier = f"{cfg.schema}.{table}"
    tbl = catalog.create_table_if_not_exists(
        identifier=identifier,
        schema=_all_optional(pq.read_schema(sample)),
        location=onelake.table_root(cfg, table),
    )
    if "schema.name-mapping.default" not in tbl.properties:
        with tbl.transaction() as tx:
            tx.set_properties(
                **{
                    "schema.name-mapping.default": create_mapping_from_schema(
                        tbl.schema()
                    ).model_dump_json()
                }
            )
    return tbl


def generate(cfg: TpchConfig, force: bool = False) -> dict:
    """Generate every TPC-H table for `cfg.sf` into OneLake. Idempotent."""
    import pyarrow.parquet as pq

    from bench import auth

    os.environ.setdefault("PYICEBERG_MAX_WORKERS", str(cfg.pyiceberg_workers))
    catalog = auth.catalog(cfg)

    if not force and is_complete(catalog, cfg):
        _log(f"{cfg.schema} already generated at SF={cfg.sf} - skipping")
        return {"sf": cfg.sf, "skipped": True, "namespace": cfg.schema, "tables": {}}

    if shutil.which("tpcgen-cli") is None:
        raise RuntimeError("tpcgen-cli not on PATH; install requirements/catalog.txt")

    catalog.create_namespace_if_not_exists(cfg.schema)
    fs = onelake.file_system(cfg)
    plan = parts_plan(cfg.sf)

    slots = threading.Semaphore(cfg.max_local_parts)
    lock = threading.Lock()
    totals = {"gen_s": 0.0, "upload_s": 0.0, "bytes": 0}

    def finish_part(files, part_dir: Path, stats: dict) -> None:
        """Upload one part's files as written. Runs on a pool thread."""
        try:
            for local, remote in files:
                nrows = pq.read_metadata(local).num_rows
                nbytes = local.stat().st_size
                uploading = time.perf_counter()
                with open(local, "rb") as handle:
                    fs.get_file_client(remote).upload_data(
                        handle, overwrite=True, max_concurrency=cfg.upload_concurrency
                    )
                local.unlink(missing_ok=True)
                done = time.perf_counter()
                with lock:
                    for bucket in (totals, stats):
                        bucket["upload_s"] += done - uploading
                        bucket["bytes"] += nbytes
                    stats["rows"] += nrows
                    stats["files"] += 1
        finally:
            # The part's local copy is released here and nowhere else. Releasing the semaphore in
            # `finally` is what stops a failed upload from deadlocking generation.
            shutil.rmtree(part_dir, ignore_errors=True)
            slots.release()

    def build_table(table: str, total_parts: int, temp_root: Path, pool) -> dict:
        stats = {
            "parts": total_parts,
            "files": 0,
            "rows": 0,
            "bytes": 0,
            "gen_s": 0.0,
            "upload_s": 0.0,
            "register_s": 0.0,
        }
        started, tbl, futures, registered = time.perf_counter(), None, [], []
        _log(f"--- {table} ({total_parts} parts) ---")

        for part in range(1, total_parts + 1):
            slots.acquire()
            part_dir = temp_root / f"{table}-{part}"
            part_dir.mkdir(parents=True, exist_ok=True)
            gen_started = time.perf_counter()
            try:
                subprocess.run(
                    [
                        "tpcgen-cli",
                        "tpch",
                        "parquet",
                        *PARQUET_FLAGS,
                        "-s",
                        str(cfg.sf),
                        "--tables",
                        table,
                        "--output-dir",
                        str(part_dir),
                        "--parts",
                        str(total_parts),
                        "--part",
                        str(part),
                    ],
                    check=True,
                )
                files = sorted(part_dir.rglob("*.parquet"))
                if not files:
                    raise RuntimeError(f"{table} part {part}: tpcgen-cli produced no parquet")
            except Exception:
                shutil.rmtree(part_dir, ignore_errors=True)
                slots.release()
                raise
            elapsed = time.perf_counter() - gen_started
            with lock:
                totals["gen_s"] += elapsed
                stats["gen_s"] += elapsed

            if tbl is None:
                tbl = _ensure_table(catalog, cfg, table, files[0])

            jobs = []
            for index, path in enumerate(files):
                name = f"part-{part:05d}-{index:03d}-{path.name}"
                jobs.append((path, onelake.relative(cfg, table, name)))
                registered.append(f"{onelake.table_root(cfg, table)}/{name}")
            futures.append(pool.submit(finish_part, jobs, part_dir, stats))
            _log(f"  part {part}/{total_parts} generated ({elapsed:.1f}s), queued")

        for future in futures:
            future.result()

        register_started = time.perf_counter()
        # One add_files for the whole table: each call is an Iceberg commit, and one commit per
        # part would be `total_parts` round-trips and `total_parts` snapshots for no benefit.
        # It also checks the files' field ids against the table schema.
        tbl.add_files(registered, check_duplicate_files=False)
        stats["register_s"] = time.perf_counter() - register_started
        stats["wall_s"] = time.perf_counter() - started
        _log(
            f"  {stats['files']} files, {stats['rows']:,} rows, "
            f"{stats['bytes'] / 2**30:.2f} GiB, add_files {stats['register_s']:.1f}s, "
            f"wall {stats['wall_s']:.1f}s"
        )
        return stats

    _log(f"Generating TPC-H SF={cfg.sf} into {cfg.schema}")
    overall = time.perf_counter()
    results: dict[str, dict] = {}
    # One pool thread per disk slot: a part holding a slot is always uploading, never queued
    # behind another part.
    with (
        tempfile.TemporaryDirectory() as tmp,
        ThreadPoolExecutor(cfg.max_local_parts) as pool,
    ):
        for table, parts in plan.items():
            results[table] = build_table(table, parts, Path(tmp), pool)

    _mark_complete(catalog, cfg)
    elapsed = time.perf_counter() - overall
    _log(
        f"total {elapsed:.1f}s wall | gen {totals['gen_s']:.1f}s | "
        f"upload {totals['upload_s']:.1f}s | {totals['bytes'] / 2**30:.2f} GiB"
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
    sys.exit(0 if generate(TpchConfig.from_env()) else 1)
