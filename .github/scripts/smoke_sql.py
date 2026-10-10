"""PHASE 1: can this engine's SQL dialect run the suite's queries at all?

NO CREDENTIALS, NO NETWORK, NO FABRIC. The suite -- TPC-H's 22 statements or TPC-DS's 99, by
BENCH_SUITE (bench/suite.py) -- is generated locally at SF=1, registered as plain parquet, and
the engine is handed the SAME statements the benchmark sends -- byte for byte, through
bench/tpch/queries.py, including the per-engine identifier rewrite.

WHY THIS EXISTS. Until now the only way to discover that an engine cannot parse Q22 was a full
bench.yml dispatch: generate into OneLake, install four engines, attach, ~15 minutes and a Fabric
round trip, to learn something a parser rejects in two seconds. Every expensive failure in this
repo so far -- DuckDB's azure transport, LakeSail's credential fallback, Polars' missing field ids
-- was found at the END of a run that had already paid for everything.

WHAT IT DOES NOT TEST: the catalog, the credential, or the storage path. That is smoke_catalog.py,
and it runs second precisely so that a failure here is unambiguous. Dialect problem, or
credential problem -- never both at once.

THE SECOND RESULT, which is almost more valuable than the first: every engine reads IDENTICAL
local data, so every engine must return the SAME ROW COUNT for the same query. `--compare` checks
that across the artifacts. A disagreement is a correctness bug in one of them, and no timing chart
would ever show it.

REGISTRATION IS THE ONE THING THAT DIFFERS from a real run. The engines' setup() attaches a REST
catalog that does not exist locally, so each adapter below builds the same session setup() would
and registers local parquet under the name that engine's IDENT_STYLE expects. The adapters live
HERE and not in the engine classes on purpose: the engine classes are what the benchmark
measures, and a second code path inside them is a second thing that can drift.

`execute()`, though, IS the real one -- Polars' streaming collect, chDB's JSONCompact, DuckDB's
fetchall. That is where an engine's row counting lives, so the smoke test has to use it.

TWO LOCAL GENERATORS, one per suite, each the same tool the suite's prepare job runs: tpchgen-cli
for TPC-H, DuckDB's dsdgen for TPC-DS. GENERATED ONCE, by smoke.yml's plan job (`--generate`),
and read from the cache by every engine job. Not a shortcut: dsdgen's output differs between
DuckDB builds (the 2.0 nightly the DuckDB engines run and stable 1.5 give the same row counts
and different values), and the first TPC-DS run let each engine job generate its own copy --
so `compare` reported 18 disagreements that were about data, not SQL. The engine jobs fail on
a cache miss rather than generate, so every engine reads identical bytes or nothing.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from bench import scrub
from bench.config import Config
from bench.suite import suite_class
from bench.tpch import queries

# SF=1, not a token scale. The data is small enough to generate in seconds and real enough that
# the row counts are the actual answers -- which is what makes the cross-engine comparison mean
# something. It also keeps TPC-H Q11's `(0.0001 / {SF})` threshold at its correct value.
SMOKE_SF = 1


def selected_queries(n_queries: int) -> list[int]:
    """SMOKE_QUERIES ("3,7,16"), or every query when unset -- a first run on a new engine
    wants a handful of statements in a minute, not the whole suite."""
    raw = os.environ.get("SMOKE_QUERIES", "").strip()
    if not raw:
        return list(range(1, n_queries + 1))
    return sorted({int(q) for q in raw.split(",") if q.strip()})


# Dummy GUIDs. Config wants them, phase 1 never uses them -- nothing here talks to Fabric.
# Config.from_env() would demand the real secrets, and this job deliberately has none.
LOCAL_CFG_IDS = ("00000000-0000-0000-0000-000000000000",) * 2


def local_config(suite: type[Config], engine: str) -> Config:
    workspace, lakehouse = LOCAL_CFG_IDS
    return suite(workspace_id=workspace, lakehouse_id=lakehouse, sf=SMOKE_SF, engine=engine)


def generate(dest: Path, suite: type[Config]) -> dict[str, Path]:
    """One parquet per table, with the suite's own generator. Skips what is already there."""
    dest.mkdir(parents=True, exist_ok=True)
    if suite.TEST == "tpcds":
        return _dsdgen(dest, suite.TABLES)
    return _tpchgen(dest, suite.TABLES)


def _tpchgen(dest: Path, tables: tuple[str, ...]) -> dict[str, Path]:
    """TPC-H via the tpchgen-cli already in requirements/catalog.txt."""
    paths = {}
    for table in tables:
        # EXACT filename, never a glob. `part*.parquet` also matches `partsupp.parquet`, so a
        # glob silently registered partsupp's data as `part` and eight queries died with
        # "Referenced column p_partkey not found" -- which reads like a dialect gap and is not
        # one. The smoke test caught it on its own first run.
        target = dest / f"{table}.parquet"
        if target.exists():
            paths[table] = target
            continue
        if shutil.which("tpchgen-cli") is None:
            raise RuntimeError("tpchgen-cli not on PATH; install requirements/smoke.txt")
        started = time.perf_counter()
        subprocess.run(
            [
                "tpchgen-cli",
                "-s",
                str(SMOKE_SF),
                "--tables",
                table,
                "--output-dir",
                str(dest),
                "--format",
                "parquet",
            ],
            check=True,
            capture_output=True,
        )
        if not target.exists():
            raise RuntimeError(
                f"tpchgen-cli produced no {target.name}; found {[p.name for p in dest.iterdir()]}"
            )
        paths[table] = target
        print(
            f"  generated {table:<9} {target.stat().st_size / 1e6:6.1f} MB "
            f"in {time.perf_counter() - started:.1f}s",
            flush=True,
        )
    return paths


def _dsdgen(dest: Path, tables: tuple[str, ...]) -> dict[str, Path]:
    """TPC-DS via DuckDB's dsdgen -- the generator tpcds.yml's prepare job runs, at SF=1.

    dsdgen builds every table at once, so one call fills an in-memory database and each table is
    then COPYed out to its own parquet, under the exact filename the adapters register.

    The existence check comes BEFORE DuckDB is started, on purpose: the engine jobs call this
    with a full cache and no DuckDB installed (only the plan job has the generator), and the
    first version imported first -- four of six TPC-DS smoke jobs died on ModuleNotFoundError
    with every file already on disk.
    """
    paths = {table: dest / f"{table}.parquet" for table in tables}
    missing = [table for table, path in paths.items() if not path.exists()]
    if not missing:
        return paths

    from bench.duckdb_cli import DuckDBCli
    from bench.tpcds.generate import load_tpcds_extension

    started = time.perf_counter()
    con = DuckDBCli()
    load_tpcds_extension(con)
    con.sql(f"CALL dsdgen(sf = {SMOKE_SF})")
    print(f"  dsdgen SF={SMOKE_SF} in {time.perf_counter() - started:.1f}s", flush=True)
    for table in missing:
        con.sql(f"COPY {table} TO '{paths[table].as_posix()}' (FORMAT parquet)")
        print(f"  generated {table:<22} {paths[table].stat().st_size / 1e6:6.1f} MB", flush=True)
    con.close()
    return paths


# --- per-engine local registration ------------------------------------------------------------
#
# Each returns a constructed engine whose execute() is ready to run. The registered NAME must
# match what bench/tpch/queries.py will ask for:
#
#   dotted      -> a real schema/database, `CH0001.lineitem`
#   backticked  -> ONE identifier whose text contains a dot, `` `CH0001.lineitem` ``
#
# That second shape looks odd until you remember why it exists: neither chDB nor Polars has a
# second namespace level, so the qualified name has to survive as a single quoted identifier.


def _duckdb(cfg: Config, paths: dict[str, Path]):
    from bench.duckdb_cli import DuckDBCli
    from bench.tpch.engines.duckdb_iceberg import DuckDBIceberg

    engine = DuckDBIceberg(cfg)
    engine._conn = DuckDBCli()
    engine._conn.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema}")
    for table, path in paths.items():
        engine._conn.sql(
            f"CREATE OR REPLACE VIEW {cfg.schema}.{table} AS "
            f"SELECT * FROM read_parquet('{path.as_posix()}')"
        )
    return engine


def _chdb(cfg: Config, paths: dict[str, Path]):
    from chdb import session

    from bench.tpch.engines.chdb_iceberg import DB, SEMANTIC_SETTINGS, ChdbIceberg

    engine = ChdbIceberg(cfg)
    # In-memory session: no config file, no filesystem cache. Those exist for OneLake reads.
    engine._session = session.Session()
    # THE SETTINGS THAT CHANGE ANSWERS, applied here too. Without them this check runs ClickHouse
    # defaults while the benchmark runs something else, and the first version of this adapter did
    # exactly that -- it reported Q13 as still broken after the fix, because the fix was in
    # setup() and setup() is not what runs here.
    for statement in SEMANTIC_SETTINGS:
        engine._session.query(statement)
    engine._session.query(f"CREATE DATABASE IF NOT EXISTS {DB}")
    engine._session.query(f"USE {DB}")
    for table, path in paths.items():
        # Mirrors production: inside the attached catalog the table's NAME is literally
        # "CH0001.lineitem", dot included, which is why the query text keeps its backticks.
        engine._session.query(
            f"CREATE VIEW `{cfg.schema}.{table}` AS "
            f"SELECT * FROM file('{path.as_posix()}', Parquet)"
        )
    return engine


def _polars(cfg: Config, paths: dict[str, Path]):
    import polars as pl

    from bench.tpch.engines.polars_iceberg import PolarsIceberg

    engine = PolarsIceberg(cfg)
    engine._ctx = pl.SQLContext()
    for table, path in paths.items():
        # Identical to the real setup() except the source is scan_parquet, not scan_iceberg.
        engine._ctx.register(f"{cfg.schema}.{table}", pl.scan_parquet(path))
    return engine


def _lakesail(cfg: Config, paths: dict[str, Path]):
    from pysail.spark import SparkConnectServer
    from pyspark.sql import SparkSession

    from bench.tpch.engines.lakesail_iceberg import LakesailIceberg

    engine = LakesailIceberg(cfg)
    engine._server = SparkConnectServer()
    engine._server.start()
    _, port = engine._server.listening_address
    engine._spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    engine._spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.schema}")
    for table, path in paths.items():
        # Sail's DDL coverage is narrower than Spark's, so try the declarative form first and
        # fall back to a view over a registered DataFrame. Whichever works, the resulting name
        # is `CH0001.lineitem`, which is what the dotted style asks for.
        location = path.resolve().as_posix()
        try:
            engine._spark.sql(
                f"CREATE TABLE IF NOT EXISTS {cfg.schema}.{table} "
                f"USING parquet LOCATION '{location}'"
            )
        except Exception:  # noqa: BLE001 - the fallback is the point
            engine._spark.read.parquet(location).createOrReplaceTempView(f"_src_{table}")
            engine._spark.sql(
                f"CREATE OR REPLACE VIEW {cfg.schema}.{table} AS SELECT * FROM _src_{table}"
            )
    return engine


def _daft(cfg: Config, paths: dict[str, Path]):
    import daft
    from daft import Session

    from bench.tpch.engines.daft_iceberg import DaftIceberg

    engine = DaftIceberg(cfg)
    engine._sess = Session()
    for table, path in paths.items():
        # Identical to the real setup() except the source is read_parquet, not read_iceberg --
        # so no IOConfig, because nothing here touches Azure.
        engine._sess.create_temp_table(f"{cfg.schema}.{table}", daft.read_parquet(str(path)))
    return engine


def _pyspark(cfg: Config, paths: dict[str, Path], extra: dict[str, str] | None = None):
    from pyspark.sql import SparkSession

    from bench.tpch.engines.pyspark_iceberg import PysparkIceberg

    engine = PysparkIceberg(cfg)
    # No jars, no catalog, no credentials -- the point of phase 1 is the dialect alone. Only
    # the settings that shape EXECUTION are carried over from the real setup().
    builder = (
        SparkSession.builder.master("local[4]")
        .appName("bench-smoke")
        .config("spark.sql.shuffle.partitions", "8")
        # A PARSER setting, so it belongs in the dialect check. The engine module says why the
        # benchmark sets it.
        .config("spark.sql.ansi.doubleQuotedIdentifiers", "true")
    )
    for key, value in (extra or {}).items():
        builder = builder.config(key, value)
    engine._spark = builder.getOrCreate()
    engine._spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.schema}")
    for table, path in paths.items():
        # `USING parquet OPTIONS (path ...)`, NOT a view over a temp view: Spark refuses to
        # create a persistent object that references a temporary one --
        #   [INVALID_TEMP_OBJ_REFERENCE] Cannot create the persistent object
        #   spark_catalog.CH0001.lineitem of the type VIEW because it references to the
        #   temporary object _src_lineitem
        # and a dotted name has to be persistent, because temp views are a flat namespace.
        engine._spark.sql(
            f"CREATE TABLE IF NOT EXISTS {cfg.schema}.{table} "
            f"USING parquet OPTIONS (path '{path.resolve().as_posix()}')"
        )
    return engine


def _pyspark_gluten(cfg: Config, paths: dict[str, Path]):
    # Unlike the file cache, Gluten changes EXECUTION -- a different engine computes every row --
    # so phase 1 loads the plugin, and `compare` checks Velox's answers against everyone else's.
    from bench.tpch.engines.pyspark_gluten_iceberg import gluten_conf

    return _pyspark(cfg, paths, gluten_conf())


def _starrocks(cfg: Config, paths: dict[str, Path]):
    """The benchmark's container with the parquet mounted, one view per table over FILES().

    Views, not tables: an internal table in the single-node allin1 image needs
    `replication_num=1`, and a view reads the same bytes every other engine reads, as they are.
    """
    from bench import starrocks
    from bench.tpch.engines.starrocks_iceberg import StarrocksIceberg

    data = Path(next(iter(paths.values()))).resolve().parent
    starrocks.start(mounts={data: "/smoke"})
    engine = StarrocksIceberg(cfg)
    engine._conn = starrocks.connect()
    engine._version = starrocks.version(engine._conn)
    with engine._conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {cfg.schema}")
        cur.execute(f"USE {cfg.schema}")
        for table, path in paths.items():
            cur.execute(
                f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM "
                f'FILES("path"="file:///smoke/{Path(path).name}", "format"="parquet")'
            )
    return engine


def _trino_type(arrow_type) -> str:
    import pyarrow as pa

    if pa.types.is_int32(arrow_type):
        return "integer"
    if pa.types.is_int64(arrow_type):
        return "bigint"
    if pa.types.is_decimal(arrow_type):
        return f"decimal({arrow_type.precision}, {arrow_type.scale})"
    if pa.types.is_date(arrow_type):
        return "date"
    if pa.types.is_floating(arrow_type):
        return "double"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "varchar"
    raise ValueError(f"no Trino type for {arrow_type}")


def _trino(cfg: Config, paths: dict[str, Path]):
    """The benchmark's container, one Hive table per parquet, under a catalog named `onelake`.

    Trino reads files only as tables, and a Hive table's `external_location` is a DIRECTORY, so
    each file gets its own (a hard link, not a copy). The Hive file metastore sits beside them in
    the same writable mount. The catalog takes the Iceberg one's name so the suites'
    `CH0001.lineitem` resolves through the same default catalog and schema as on OneLake.
    """
    import pyarrow.parquet as pq

    from bench import trino
    from bench.tpch.engines.trino_iceberg import TrinoIceberg

    root = Path(next(iter(paths.values()))).resolve().parent / "trino-smoke"
    shutil.rmtree(root, ignore_errors=True)
    for table, path in paths.items():
        (root / table).mkdir(parents=True)
        os.link(Path(path).resolve(), root / table / Path(path).name)
    (root / "metastore").mkdir()
    for directory in [root, *root.iterdir()]:
        directory.chmod(0o777)  # the container's user is not the runner's
    trino.start(mounts={root: "/smoke"})
    schema = cfg.schema.lower()
    engine = TrinoIceberg(cfg)
    engine._conn = trino.connect(schema=schema)
    engine._version = trino.version(engine._conn)
    trino.create_catalog(
        engine._conn,
        trino.CATALOG,
        "hive",
        {
            "hive.metastore": "file",
            "hive.metastore.catalog.dir": "local:///metastore",
            "fs.native-local.enabled": "true",
            "local.location": "/smoke",
        },
    )
    trino.sql(engine._conn, f"CREATE SCHEMA IF NOT EXISTS {trino.CATALOG}.{schema}")
    for table, path in paths.items():
        columns = ", ".join(
            f'"{field.name}" {_trino_type(field.type)}' for field in pq.read_schema(path)
        )
        trino.sql(
            engine._conn,
            f"CREATE TABLE {trino.CATALOG}.{schema}.{table} ({columns}) "
            f"WITH (external_location = 'local:///{table}', format = 'PARQUET')",
        )
    return engine


ADAPTERS = {
    "duckdb_iceberg": _duckdb,
    "chdb_iceberg": _chdb,
    "polars_iceberg": _polars,
    "lakesail_iceberg": _lakesail,
    "daft_iceberg": _daft,
    "pyspark_iceberg": _pyspark,
    "pyspark_gluten_iceberg": _pyspark_gluten,
    "starrocks_iceberg": _starrocks,
    "trino_iceberg": _trino,
}


def run(engine_name: str, data_dir: Path, out_dir: Path, suite: type[Config]) -> int:
    """Register locally, run every statement, write the JSON. Returns a process exit code.

    THE TWO FAILURE MODES ARE KEPT APART, because they mean opposite things:

      exit 2  registration failed -- this harness is broken, or the engine cannot read plain
              local parquet, which is not a dialect result and must not be reported as one
      exit 1  one or more QUERIES failed -- the dialect gap this script exists to find
    """
    cfg = local_config(suite, engine_name)
    statements = queries.load(engine_name, cfg.schema, cfg.sf, cfg.SQL_PATH, cfg.N_QUERIES)

    adapter = ADAPTERS.get(engine_name)
    if adapter is None:
        print(
            f"::error::no local registration adapter for {engine_name!r}; "
            f"add one to {Path(__file__).name}"
        )
        return 2

    print(
        f"\n{engine_name} | {suite.TITLE} local SF={SMOKE_SF} | schema {cfg.schema} | "
        f"style {queries.style_for(engine_name)}",
        flush=True,
    )

    started = time.perf_counter()
    try:
        engine = adapter(cfg, generate(data_dir, suite))
    except Exception as exc:  # noqa: BLE001 - reporting failures is this script's job
        print(f"::error::{engine_name} local registration failed: {scrub.scrub_exc(exc, 600)}")
        return 2
    print(
        f"  registered {len(suite.TABLES)} tables in {time.perf_counter() - started:.1f}s",
        flush=True,
    )

    numbers = selected_queries(cfg.N_QUERIES)
    rows, failed = [], 0
    try:
        for number in numbers:
            sql = statements[number - 1]
            query_started = time.perf_counter()
            try:
                count = engine.execute(sql)
                elapsed = time.perf_counter() - query_started
                rows.append(
                    {"query": number, "status": "ok", "rows": count, "dur": round(elapsed, 4)}
                )
                print(f"  Q{number:<2} ok    {count:>8,} rows  {elapsed:6.2f}s", flush=True)
            except Exception as exc:  # noqa: BLE001 - a dialect gap IS the result here
                failed += 1
                message = scrub.scrub_exc(exc, 300).replace("\n", " ")
                rows.append({"query": number, "status": "error", "rows": None, "error": message})
                print(f"  Q{number:<2} FAIL  {message}", flush=True)
    finally:
        try:
            engine.close()
        except Exception as exc:  # noqa: BLE001 - teardown must not mask the result
            print(f"  warning: close() failed: {exc}")

    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "engine": engine_name,
        "version": engine.version,
        "suite": suite.TEST,
        "sf": SMOKE_SF,
        "phase": "sql",
        "rows": rows,
    }
    (out_dir / f"{engine_name}.json").write_text(json.dumps(payload, indent=1), encoding="utf-8")

    ok = len(numbers) - failed
    print(f"\n{engine_name} {engine.version}: {ok}/{len(numbers)} queries ran", flush=True)
    if failed:
        print(f"::error::{engine_name} cannot run {failed} of {len(numbers)} {suite.TITLE} queries")
    return 1 if failed else 0


def compare(out_dir: Path, suite: type[Config]) -> int:
    """Every engine saw the same bytes, so every engine must agree on every row count.

    Runs after the matrix, over the downloaded artifacts. Only compares queries that SUCCEEDED
    everywhere -- an engine that failed Q22 has already been reported by its own job, and
    counting that twice would just be noise.
    """
    files = sorted(out_dir.glob("*.json"))
    if len(files) < 2:
        print(f"only {len(files)} engine result(s) in {out_dir}; nothing to compare")
        return 0

    results = {}
    for path in files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        results[payload["engine"]] = {
            row["query"]: row["rows"] for row in payload["rows"] if row["status"] == "ok"
        }
    print(f"comparing row counts across {len(results)}: {', '.join(sorted(results))}")

    disagreements = 0
    for number in range(1, suite.N_QUERIES + 1):
        counts = {e: r[number] for e, r in results.items() if number in r}
        if len(set(counts.values())) > 1:
            disagreements += 1
            detail = ", ".join(f"{e}={c:,}" for e, c in sorted(counts.items()))
            print(f"::error::Q{number} row counts disagree: {detail}")

    if disagreements:
        print(f"\n{disagreements} quer(ies) disagree -- one of these engines is wrong")
        return 1
    print("\nall engines agree on every query they completed")
    return 0


if __name__ == "__main__":
    suite = suite_class()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("engine", nargs="?", help="engine name, e.g. duckdb_iceberg")
    parser.add_argument(
        "--data",
        default=Path(f"{suite.TEST}-local"),
        type=Path,
        help="where the local parquet lives (cached in CI); tpch-local or tpcds-local",
    )
    parser.add_argument("--out", default="smoke", type=Path, help="where to write the JSON")
    parser.add_argument(
        "--compare",
        action="store_true",
        help="compare row counts across every JSON in --out and exit",
    )
    parser.add_argument(
        "--generate",
        action="store_true",
        help="generate the suite's SF=1 parquet into --data and exit (the plan job)",
    )
    args = parser.parse_args()

    if args.generate:
        paths = generate(args.data, suite)
        print(f"{suite.TITLE} SF={SMOKE_SF}: {len(paths)} tables under {args.data}")
        sys.exit(0)
    if args.compare:
        sys.exit(compare(args.out, suite))
    if not args.engine:
        parser.error("an engine name is required unless --compare is given")
    sys.exit(run(args.engine, args.data, args.out, suite))
