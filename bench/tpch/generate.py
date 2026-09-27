"""Generate TPC-H at scale factor N straight into OneLake as Iceberg.

Port of cell 9. The design is the notebook's and it is a good one, quoted from its own docstring:

    tpchgen-cli writes each parquet once, the bytes upload as-is, and the table resolves columns
    via a default name mapping - no re-encode, no field IDs, no query engine.

Generation, the field-id rewrite and upload overlap: the main thread only runs tpchgen-cli, each
part's rewrite goes to a process pool and its upload to a thread. A semaphore caps how many parts
sit on local disk at once, which is what keeps peak disk proportional to PART SIZE rather than to
dataset size. That is the property that lets SF=10 (2.7 GiB in OneLake) run on a 14 GB runner.

THE REWRITE IS OFF THE MAIN THREAD FOR A MEASURED REASON. At SF=300 it ran inline: each lineitem
part took 4.2s in tpchgen-cli (all cores) then ~7s in a single-threaded pyarrow rewrite, so the
runner's cores sat idle ~60% of a 53-minute prepare. Uploads were never the bottleneck (420s of it).

WHAT CHANGED:

* `StaticToken` is gone -- see bench/onelake.py for why it was actively harmful.
* The lazy mid-run `pip install` of tpchgen-cli and azure-storage-file-datalake is gone; the
  workflow installs requirements before Python starts.
* Part counts come from `config.parts_plan`, which targets a FILE SIZE instead of scaling off
  SF=1000. See config.TARGET_PART_MB.
* The completion guard is a table property, not `table_exists`. See `_mark_complete`.
"""

from __future__ import annotations

import multiprocessing
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
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

# Rows per row group in every uploaded file: pyarrow's `write_table` default, which is what the
# data already in OneLake at SF<=100 was written with. Stated explicitly because the rewrite now
# streams, and a streaming writer would otherwise emit tpchgen-cli's ~7MB row groups instead.
ROW_GROUP_ROWS = 1024 * 1024


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

    tpchgen-cli marks most columns NOT NULL. Iceberg's required/optional flag is part of the
    schema identity, and a required field that any engine reads as optional is a mismatch at scan
    time, so the table is declared entirely optional and the parquet is left untouched.
    """
    import pyarrow as pa

    return pa.schema([pa.field(f.name, f.type, nullable=True, metadata=f.metadata) for f in schema])


def field_id_schema(iceberg_schema, arrow_schema):
    """`arrow_schema` with each field carrying the Iceberg table's field id.

    WHY THE PARQUET HAS TO CARRY IDS AT ALL. The notebook this was ported from uploaded
    tpchgen-cli's bytes unmodified and relied purely on the Iceberg name mapping -- its own
    docstring said "no re-encode, no field IDs". DuckDB and chDB honour that mapping. Polars does
    not: `pl.scan_iceberg` reads the ids out of the parquet footer and fails without them,

        SchemaFieldNotFoundError: IcebergSchema: failed to load 'PARQUET:field_id'
        for field l_orderkey: metadata was None

    which is why the notebook's Polars branch cannot ever have run green.

    The ids MUST come from the Iceberg table rather than being invented, because `add_files`
    matches the file's columns to the table's schema by them. pyiceberg assigns them when the
    table is created, so this is read back off the created table, never guessed.
    """
    import pyarrow as pa

    ids = {f.name: f.field_id for f in iceberg_schema.fields}
    return pa.schema(
        [
            pa.field(
                f.name,
                f.type,
                nullable=True,
                metadata={b"PARQUET:field_id": str(ids[f.name]).encode()},
            )
            for f in arrow_schema
        ]
    )


def add_field_ids(path: Path, schema, compression: str = "zstd") -> tuple[int, int]:
    """Rewrite one parquet in place so its footer carries the field ids; return (rows, bytes).

    This is the cost of supporting Polars: a decode/encode of every file, where the notebook did
    none. It is paid once per scale factor at generation, never at query time. Field ids live in
    the parquet footer's schema element, so there is no way to attach them without rewriting the
    file.

    Module-level because it runs in a process pool (pyarrow's writer is single-threaded), and
    STREAMED one row group at a time so N workers hold N row groups, not N whole parts -- a
    lineitem part at SF=300 is ~1.6 GB of Arrow. Output is what `pq.write_table` produced when
    this read the whole file: same schema, same compression, ROW_GROUP_ROWS-row row groups.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = Path(path)
    staged = path.with_name(path.name + ".ids")
    rows, pending = 0, []

    def flush(writer, batches) -> None:
        slab = pa.Table.from_batches(batches)
        writer.write_table(
            pa.Table.from_arrays(slab.columns, schema=schema), row_group_size=ROW_GROUP_ROWS
        )

    with pq.ParquetWriter(staged, schema, compression=compression) as writer:
        for batch in pq.ParquetFile(path).iter_batches(batch_size=ROW_GROUP_ROWS):
            pending.append(batch)
            rows += batch.num_rows
            if sum(b.num_rows for b in pending) >= ROW_GROUP_ROWS:
                slab = pa.Table.from_batches(pending)
                flush(writer, slab.slice(0, ROW_GROUP_ROWS).to_batches())
                pending = slab.slice(ROW_GROUP_ROWS).to_batches()
        if pending:
            flush(writer, pending)
    os.replace(staged, path)
    return rows, path.stat().st_size


def _ensure_table(catalog, cfg: Config, table: str, sample: Path):
    """Create the Iceberg table from a sample parquet's schema, with a default name mapping.

    The name mapping is kept even though the files now carry field ids: it costs one property and
    it is what lets an engine that ignores ids still resolve columns by name.
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

    if shutil.which("tpchgen-cli") is None:
        raise RuntimeError("tpchgen-cli not on PATH; install requirements/catalog.txt")

    catalog.create_namespace_if_not_exists(cfg.schema)
    fs = onelake.file_system(cfg)
    plan = parts_plan(cfg.sf)

    slots = threading.Semaphore(cfg.max_local_parts)
    lock = threading.Lock()
    totals = {"gen_s": 0.0, "ids_s": 0.0, "upload_s": 0.0, "bytes": 0}

    def finish_part(files, part_dir: Path, id_schema, stats: dict) -> None:
        """Field ids, then upload, for one part's files. Runs on a pool thread."""
        try:
            rewrites = [procs.submit(add_field_ids, local, id_schema) for local, _ in files]
            for (local, remote), rewrite in zip(files, rewrites, strict=True):
                waited = time.perf_counter()
                nrows, nbytes = rewrite.result()
                uploading = time.perf_counter()
                with open(local, "rb") as handle:
                    fs.get_file_client(remote).upload_data(
                        handle, overwrite=True, max_concurrency=cfg.upload_concurrency
                    )
                local.unlink(missing_ok=True)
                done = time.perf_counter()
                with lock:
                    for bucket in (totals, stats):
                        bucket["ids_s"] += uploading - waited
                        bucket["upload_s"] += done - uploading
                        bucket["bytes"] += nbytes
                    stats["rows"] += nrows
                    stats["files"] += 1
        finally:
            # The part's local copy is released here and nowhere else. Releasing the semaphore in
            # `finally` is what stops a failed rewrite or upload from deadlocking generation.
            shutil.rmtree(part_dir, ignore_errors=True)
            slots.release()

    def build_table(table: str, total_parts: int, temp_root: Path, pool) -> dict:
        stats = {
            "parts": total_parts,
            "files": 0,
            "rows": 0,
            "bytes": 0,
            "gen_s": 0.0,
            "ids_s": 0.0,
            "upload_s": 0.0,
            "register_s": 0.0,
        }
        started, tbl, futures, registered = time.perf_counter(), None, [], []
        id_schema = None
        _log(f"--- {table} ({total_parts} parts) ---")

        for part in range(1, total_parts + 1):
            slots.acquire()
            part_dir = temp_root / f"{table}-{part}"
            part_dir.mkdir(parents=True, exist_ok=True)
            gen_started = time.perf_counter()
            try:
                subprocess.run(
                    [
                        "tpchgen-cli",
                        "parquet",
                        # The rewrite re-encodes to zstd anyway, so any codec here is thrown away:
                        # UNCOMPRESSED generates ~1.9x faster than the SNAPPY default (tpchgen's
                        # own figures, lineitem SF=10) and decodes for free in the rewrite.
                        "--compression",
                        "UNCOMPRESSED",
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
                    raise RuntimeError(f"{table} part {part}: tpchgen-cli produced no parquet")
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
                id_schema = field_id_schema(tbl.schema(), pq.read_schema(files[0]))

            jobs = []
            for index, path in enumerate(files):
                name = f"part-{part:05d}-{index:03d}-{path.name}"
                jobs.append((path, onelake.relative(cfg, table, name)))
                registered.append(f"{onelake.table_root(cfg, table)}/{name}")
            futures.append(pool.submit(finish_part, jobs, part_dir, id_schema, stats))
            _log(f"  part {part}/{total_parts} generated ({elapsed:.1f}s), queued")

        for future in futures:
            future.result()

        register_started = time.perf_counter()
        # One add_files for the whole table: each call is an Iceberg commit, and one commit per
        # part would be `total_parts` round-trips and `total_parts` snapshots for no benefit.
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
    # One pool thread per disk slot: a part holding a slot is always rewriting or uploading, never
    # queued behind another part. `spawn`, not Linux's default fork: the catalog and the azure
    # clients already have threads running, and forking a threaded process is unsafe.
    with (
        tempfile.TemporaryDirectory() as tmp,
        ProcessPoolExecutor(
            os.cpu_count() or 1, mp_context=multiprocessing.get_context("spawn")
        ) as procs,
        ThreadPoolExecutor(cfg.max_local_parts) as pool,
    ):
        for table, parts in plan.items():
            results[table] = build_table(table, parts, Path(tmp), pool)

    _mark_complete(catalog, cfg)
    elapsed = time.perf_counter() - overall
    _log(
        f"total {elapsed:.1f}s wall | gen {totals['gen_s']:.1f}s | "
        f"field-id wait {totals['ids_s']:.1f}s | upload {totals['upload_s']:.1f}s | "
        f"{totals['bytes'] / 2**30:.2f} GiB"
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
