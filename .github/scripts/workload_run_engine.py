"""Run the dbt workload on ONE engine and write its part: `python workload_run_engine.py parts`.

The engine is BENCH_ENGINE, a target in workload/profiles.yml: duckdb, polars, gluten or trino.

    1. mint the OneLake bearer and hand it to the profile through the environment
    2. gluten only: start bench's Gluten/Velox SparkSession (the ETL's writing variant, so the
       SAS can create and delete), with the OneLake REST catalog as spark.sql.defaultCatalog --
       dbt-spark's `method: session` then getOrCreate()s it
    3. `dbt run`, then `dbt test`, in-process through dbtRunner, timed; one `dbt retry` after
       either if it failed after writing run_results.json, which heals a transient catalog error
       on a single node
    4. refuse a build that ran NO model: a target matching no folder disables every tree and
       dbt exits 0 having built nothing
    5. `dbt run-operation model_row_counts`, rows per model for the page's DAG (best-effort)
    6. parts/workload_<engine>.json: status, timings per node, peak RSS -- scrubbed and
       leak-checked, and written on failure too (a runner that dies writes nothing: the publish
       job carries that engine forward as "died")

THE FILE NAME MUST NOT START WITH `dbt_`. dbt's plugin manager imports every top-level module
named dbt_* when a command starts, and this script's directory is sys.path[0]: as dbt_*.py it
would be imported as a "plugin" and re-run itself inside dbt's own invoke (fabric-dbt-examples
learned that one as dbt_in_fabric.py).
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path

from bench import auth, scrub
from bench.report import leak_check
from bench.workload.config import BENCH_ENGINE, DEFAULT_PROCESS_LIMIT, ENGINES, WorkloadConfig

REPO = Path(__file__).resolve().parents[2]
PROJECT = REPO / "workload"

# What the part records as the stack, per leg: the adapter and the engine under it.
PACKAGES = {
    "duckdb": ("dbt-core", "dbt-duckdb", "duckdb"),
    "polars": ("dbt-core", "dbt-polars", "polars", "pyiceberg"),
    "gluten": ("dbt-core", "dbt-spark", "pyspark"),
    "trino": ("dbt-core", "dbt-trino", "trino"),
}

# The Hive EXTERNAL tables the trino tree reads the landing zone through (workload/macros/
# trino_read_csv.sql): folder under Files/dbt/csv_raw, and how many VARCHAR columns, c0..c{n-1}.
# 140 clears the widest AEMO record (price, 130); the DUID reference files are under 30.
TRINO_CSV_TABLES = {"daily": 140, "price_today": 140, "scada_today": 140, "duid": 30}
TRINO_ARCHIVE_LOG = (
    "source_type varchar, source_filename varchar, archive_path varchar, archived_at timestamp(3), "
    "row_count bigint, source_url varchar, etag varchar, csv_filename varchar"
)


def versions(engine: str) -> dict[str, str]:
    out = {}
    for name in PACKAGES[engine]:
        try:
            out[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            out[name] = ""
    return out


def start_gluten(cfg: WorkloadConfig):
    """bench's Gluten session, the one the Light ETL writes with. Kept alive for the process."""
    from bench.etl.engines.pyspark_gluten_iceberg import _WritingGluten

    engine = _WritingGluten(cfg)
    engine.setup()
    spark = engine.session
    # dbt-spark's macros are written for stock Spark, where "x" is a string; bench's session reads
    # it as an identifier (pyspark_iceberg.py, for TPC-DS). Nothing in these models double-quotes.
    spark.conf.set("spark.sql.ansi.doubleQuotedIdentifiers", "false")
    return engine


def start_trino(cfg: WorkloadConfig, token: str) -> str:
    """bench's Trino container with the OneLake catalog attached, and a Hive catalog `files` over
    the landing zone -- the Trino ETL's shape (bench/etl/engines/trino_iceberg.py). Returns the
    server version."""
    from bench import trino

    trino.start()
    conn = trino.connect()
    try:
        trino.attach(conn, cfg, token)
        trino.create_catalog(
            conn,
            "files",
            "hive",
            {
                "hive.metastore": "file",
                "hive.metastore.catalog.dir": "local:///trino-metastore",
                "fs.native-local.enabled": "true",
                "local.location": "/tmp",
                **trino.storage_properties(),
            },
        )
        trino.sql(conn, "CREATE SCHEMA IF NOT EXISTS files.dbt")
        for folder, width in TRINO_CSV_TABLES.items():
            columns = ", ".join(f"c{i} varchar" for i in range(width))
            trino.sql(conn, f"DROP TABLE IF EXISTS files.dbt.{folder}")
            trino.sql(
                conn,
                f"CREATE TABLE files.dbt.{folder} ({columns}) WITH ("
                f"external_location = '{cfg.files_abfss}/csv_raw/{folder}', "
                "format = 'CSV', skip_header_line_count = 1)",
            )
        # The archive log is the one parquet file directly under Files/dbt; Hive does not descend
        # into csv_raw/ (hive.recursive-directories is off).
        trino.sql(conn, "DROP TABLE IF EXISTS files.dbt.archive_log")
        trino.sql(
            conn,
            f"CREATE TABLE files.dbt.archive_log ({TRINO_ARCHIVE_LOG}) WITH (external_location = "
            f"'{cfg.files_abfss}', format = 'PARQUET')",
        )
        return f"{trino.version(conn)} ({trino.IMAGE})"
    finally:
        conn.close()


class PeakRss:
    """Peak resident memory of the leg while dbt runs: this process and its children (dbt, the
    engine in-process, Spark's JVM) plus, on Trino, the server's processes in its container --
    sampled twice a second, summed, the maximum kept. A leg the runner kills records nothing."""

    def __init__(self, engine: str):
        import threading

        self.peak = 0
        self._stop = threading.Event()
        self._roots = [os.getpid()]
        if engine == "trino":
            from bench import trino

            pid = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Pid}}", trino.CONTAINER],
                capture_output=True,
                text=True,
                check=False,
            ).stdout.strip()
            if pid.isdigit() and int(pid) > 0:
                self._roots.append(int(pid))
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def _rss(self) -> int:
        import psutil

        total = 0
        for root in self._roots:
            try:
                proc = psutil.Process(root)
                procs = [proc, *proc.children(recursive=True)]
            except psutil.Error:
                continue
            for p in procs:
                with contextlib.suppress(psutil.Error):
                    total += p.memory_info().rss
        return total

    def _sample(self) -> None:
        while not self._stop.wait(0.5):
            self.peak = max(self.peak, self._rss())

    def __enter__(self) -> PeakRss:
        self.peak = self._rss()
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        self._thread.join()

    @property
    def mb(self) -> int:
        return round(self.peak / 2**20)


class Dbt:
    def __init__(self, engine: str):
        self.engine = engine
        self.last_error = ""

    def run(self, *args: str, callbacks=None) -> tuple[bool, float]:
        from dbt.cli.main import dbtRunner  # late: after the environment is set

        scrub.safe_print(f"=== dbt {' '.join(args)} ({self.engine}) ===")
        t0 = time.perf_counter()
        res = dbtRunner(callbacks=callbacks or []).invoke(
            [*args, "--target", self.engine, "--profiles-dir", "."]
        )
        elapsed = time.perf_counter() - t0
        scrub.safe_print(f"=== dbt {args[0]}: success={res.success} in {elapsed:.1f}s ===")
        self.last_error = "" if res.success else scrub.scrub(str(res.exception or ""))[:2000]
        if res.exception:
            scrub.safe_print(res.exception)
        return bool(res.success), elapsed


def node_timings() -> list[dict]:
    path = PROJECT / "target" / "run_results.json"
    if not path.exists():
        return []
    results = json.loads(path.read_text(encoding="utf-8"))["results"]
    return [
        {
            "node": r["unique_id"],
            "status": r["status"],
            "seconds": round(float(r.get("execution_time") or 0.0), 3),
            "message": scrub.scrub(r.get("message") or "")[:500],
        }
        for r in results
    ]


def sql_lines(rel_path: str) -> int:
    """Lines of SQL in a model file, comments and blank lines left out."""
    path = PROJECT / rel_path
    if not path.exists():
        return 0
    text = re.sub(r"\{#.*?#\}", "", path.read_text(encoding="utf-8"), flags=re.S)
    return sum(
        1 for line in text.splitlines() if line.strip() and not line.strip().startswith("--")
    )


def model_dag() -> dict[str, dict]:
    """The project's DAG as dbt itself published it (target/manifest.json): every enabled model,
    the models it reads, and how this leg builds it -- materialization, incremental strategy and
    key, the model file and its size -- plus the description and the `meta.differences` notes the
    shared patch files give it. The Real workload tab draws it and opens it on a click."""
    path = PROJECT / "target" / "manifest.json"
    if not path.exists():
        return {}
    manifest = json.loads(path.read_text(encoding="utf-8"))
    models = {uid: n for uid, n in manifest["nodes"].items() if n["resource_type"] == "model"}
    # The landed files the models read, as dbt sources: drawn as the DAG's roots.
    sources = {uid: n for uid, n in manifest.get("sources", {}).items()}
    names = {uid: n["name"] for uid, n in models.items()} | {
        uid: n["name"] for uid, n in sources.items()
    }
    out = {}
    for n in sources.values():
        meta = n["config"].get("meta") or n.get("meta") or {}
        out[n["name"]] = {
            "materialized": "source",
            "parents": [],
            "description": (n.get("description") or "").strip(),
            "differences": meta.get("differences", []),
        }
    for n in models.values():
        config, meta = n["config"], n["config"].get("meta") or n.get("meta") or {}
        key = config.get("unique_key")
        out[n["name"]] = {
            "materialized": config.get("materialized", ""),
            "parents": sorted(names[p] for p in n["depends_on"]["nodes"] if p in names),
            "strategy": config.get("incremental_strategy") or "",
            "unique_key": key if isinstance(key, list) else [key] if key else [],
            "merge_clauses": bool(config.get("merge_clauses")),
            "path": n["original_file_path"],
            "sql_lines": sql_lines(n["original_file_path"]),
            "description": (n.get("description") or "").strip(),
            "differences": meta.get("differences", []),
        }
    return out


def process_limit(engine: str) -> int:
    """Files each fact model folds this run: PROCESS_LIMITS' entry for this engine
    ("gluten=100,trino=150"), else process_limit, else the default."""
    for item in (os.environ.get("PROCESS_LIMITS") or "").split(","):
        name, _, value = item.strip().partition("=")
        if name == engine and value.strip().isdigit():
            return int(value)
    return int(os.environ.get("process_limit") or DEFAULT_PROCESS_LIMIT)  # noqa: SIM112


def main(parts: Path) -> int:
    cfg = WorkloadConfig.from_env()
    engine = cfg.engine
    if engine not in ENGINES:
        raise SystemExit(f"BENCH_ENGINE must be one of {ENGINES}, not {engine!r}")
    parts.mkdir(parents=True, exist_ok=True)
    started = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    part = {
        "suite": "workload",
        "engine": engine,
        "bench_engine": BENCH_ENGINE[engine],
        "run_id": cfg.run_id,
        "run_url": cfg.run_url,
        "git_sha": cfg.git_sha,
        "started": started,
        # Lower case: the models read env_var('process_limit'), as in the source project.
        "process_limit": process_limit(engine),
        "versions": versions(engine),
        "status": "failed",
    }

    # Late and fresh: the profile bakes this string into DuckDB's ATTACH and pyiceberg's catalog,
    # and nothing renews it. workload.yml's job timeout keeps the leg inside its lifetime.
    token = auth.onelake_token(fresh=True)
    os.environ.update(cfg.dbt_env(token))
    os.environ["process_limit"] = str(part["process_limit"])  # noqa: SIM112
    os.environ.setdefault("DUCKDB_TEMP_DIR", "/tmp/duckdb_spill")
    os.makedirs(os.environ["DUCKDB_TEMP_DIR"], exist_ok=True)
    if engine == "polars":
        # Polars SQL's read_csv()/read_parquet() take no credentials: OneLake only through this.
        os.environ["AZURE_STORAGE_TOKEN"] = token

    spark = None
    try:
        if engine == "gluten":
            spark = start_gluten(cfg)
            part["versions"]["spark"] = spark.version
        if engine == "trino":
            part["versions"]["trino-server"] = start_trino(cfg, token)
        os.chdir(PROJECT)
        dbt = Dbt(engine)

        part["build_seconds"] = 0.0
        part["nodes"] = []

        def step(command: str) -> bool:
            ok, seconds = dbt.run(command)
            part["build_seconds"] = round(part["build_seconds"] + seconds, 3)
            part["nodes"] += node_timings()
            if not ok and (PROJECT / "target" / "run_results.json").exists():
                part.setdefault("first_error", dbt.last_error)
                ok, retry_s = dbt.run("retry")
                part["retry_seconds"] = round(part.get("retry_seconds", 0.0) + retry_s, 3)
                part["retry_nodes"] = part.get("retry_nodes", []) + node_timings()
            return ok

        # Every model, then every test -- not `dbt build`, which runs each test right after its
        # model: assert_fct_scada_grain's GROUP BY over all of fct_scada then ran just before
        # fct_summary, and the polars runner died in fct_summary (run 39564650603).
        with PeakRss(engine) as rss:
            ok = step("run") and step("test")
        part["peak_rss_mb"] = rss.mb
        scrub.safe_print(f"{engine}: peak RSS {rss.mb:,} MB during dbt run and test")
        models = [n for n in part["nodes"] if n["node"].startswith("model.")]
        if ok and not models:
            ok = False
            dbt.last_error = f"dbt built no model for target {engine}: check +enabled gating"
        part["dag"] = model_dag()
        if not ok:
            part["error"] = dbt.last_error
            return 1

        logged: list[str] = []

        def keep_log(event) -> None:
            if event.info.name.startswith("JinjaLog"):
                logged.append(event.info.msg)

        part["status"] = "success"
        # Rows per model, for the Real workload tab's DAG. Best-effort: a count that fails is a
        # missing number on the page, never a failed leg.
        if dbt.run("run-operation", "model_row_counts", callbacks=[keep_log])[0]:
            found = [m for m in logged if m.lstrip().startswith('{"model_rows"')]
            if found:
                counted = json.loads(found[-1])
                part["model_rows"] = counted["model_rows"]
                part["model_files"] = counted.get("model_files", {})
        return 0
    except Exception as exc:  # noqa: BLE001 - the part is written either way
        part["error"] = scrub.scrub_exc(exc)
        scrub.safe_print(f"::error::{part['error']}")
        return 1
    finally:
        part["finished"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        out = parts / f"workload_{engine}.json"
        out.write_text(scrub.scrub(json.dumps(part, indent=2)) + "\n", encoding="utf-8")
        leak_check([out])
        if spark is not None:
            spark.close()


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1] if len(sys.argv) > 1 else "parts").resolve()))
