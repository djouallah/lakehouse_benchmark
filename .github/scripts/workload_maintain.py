"""Table maintenance for the workload's tables: `python workload_maintain.py run|publish <record>`.

Every 2 hours each workload leg merges into its own dbt_<engine>_{landing,mart} tables, and
nothing on that write path folds the small files back together or drops a snapshot. This is the
daily job that does, one table at a time (table_maintenance.yml, in the `workload` queue so it
never overlaps a dbt run -- iceberg_rewrite_data_files commits optimistically, and overlapping a
merge would fail one side):

  1. compact: DuckDB's iceberg_rewrite_data_files, target 128 MiB, a group of at least
     MIN_INPUT_FILES files under the target;
  2. expire: pyiceberg's expire_snapshots, older than EXPIRE_OLDER_THAN_DAYS (branch and tag heads
     kept). It only drops snapshot entries from the metadata -- no file is deleted;
  3. orphans: pyiceberg's remove_orphan_files (apache/iceberg-python #3361, unreleased: the fork
     pin in requirements/maintenance.txt), older than ORPHAN_OLDER_THAN_DAYS. This is the step that
     deletes the files the expired snapshots and the compacted-away files left behind.

Ported from djouallah/analytics-as-code scripts/compact_iceberg.py and expire_snapshots.py.

`run <record>`: does the work and writes the run's record (per table: before/after counts and each
step's result or its exact error) to <record>, after every table, so a job killed by its timeout
still leaves what it did. `publish <record> [--write]`: the step summary, and with --write
results/maintenance/<run_id>.json plus docs/data/maintenance.json, the Real workload tab's copy.
The commit loop re-runs publish after every reset onto main, never the maintenance itself.

Never fails on a table: every step is best-effort and its error is recorded verbatim. `run` exits 1
only when every table errored.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

from bench import auth, scrub
from bench.config import ICEBERG_ENDPOINT, ONELAKE_BLOB
from bench.report import leak_check
from bench.workload.config import BENCH_ENGINE, ENGINES, WorkloadConfig

REPO = Path(__file__).resolve().parents[2]

RESULTS = REPO / "results" / "maintenance"
PAGE_DATA = REPO / "docs" / "data" / "maintenance.json"

LAYERS = ("landing", "mart")
# dbt's own scratch relations: Spark's <model>__stage, Trino's <model>__dbt_tmp.
SCRATCH = ("__stage", "__dbt_tmp")

TARGET_MB = 128
MIN_INPUT_FILES = int(os.environ.get("COMPACT_MIN_INPUT_FILES", "5"))
# Stop starting new tables past this much wall clock, so the job reports what it did instead of
# being killed mid-rewrite. Whatever is skipped is tomorrow's: compaction is incremental.
BUDGET_MINUTES = float(os.environ.get("COMPACT_BUDGET_MINUTES", "70"))
EXPIRE_DAYS = float(os.environ.get("EXPIRE_OLDER_THAN_DAYS", "1"))
# A file younger than this may belong to a commit in flight, so it is never a candidate.
ORPHAN_DAYS = float(os.environ.get("ORPHAN_OLDER_THAN_DAYS", "3"))
# DuckDB and the catalog hold the bearer as a string: re-attach when less than this remains.
REFRESH_SECONDS = 10 * 60

CATALOG = "onelake"
# The REST update expiry issues, as GET /v1/config's `endpoints` list spells it.
UPDATE_TABLE_ENDPOINT = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"


def oneline(exc: BaseException) -> str:
    """The exact error, scrubbed, on one line: DuckDB's carry a SQL echo and a caret ruler."""
    return " ".join(scrub.scrub_exc(exc, 600).split())


# --- connections ---------------------------------------------------------------------------------


def duckdb_connect(cfg: WorkloadConfig, token: str):
    """DuckDB attached to the catalog as the workload's duckdb profile is: an azure secret for the
    blobs, the bearer for the catalog. ACCESS_DELEGATION_MODE 'none': the client brings its own
    storage credential, as analytics-as-code's compaction does on the same OneLake endpoint."""
    import duckdb

    con = duckdb.connect(":memory:")
    for ext in ("azure", "iceberg"):
        con.install_extension(ext)
        con.load_extension(ext)
    # Its default transport fails OneLake's TLS handshake; the workflow sets curl.
    transport = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE", "default")
    con.execute(f"SET GLOBAL azure_transport_option_type = '{transport}'")
    con.execute(
        f"SET GLOBAL temp_directory = '{os.environ.get('DUCKDB_TEMP_DIR', '/tmp/duckdb_spill')}'"
    )
    con.execute(
        f"CREATE SECRET onelake_storage (TYPE azure, PROVIDER access_token, ACCESS_TOKEN '{token}')"
    )
    con.execute(
        f"ATTACH '{cfg.warehouse}' AS {CATALOG} (TYPE iceberg, ENDPOINT '{ICEBERG_ENDPOINT}', "
        f"TOKEN '{token}', ACCESS_DELEGATION_MODE 'none')"
    )
    return con


def iceberg_catalog(cfg: WorkloadConfig, token: str):
    """bench.auth.catalog's RestCatalog, on FsspecFileIO: remove_orphan_files lists the table's
    folder through the FileIO, and adlfs is the one that can."""
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        "onelake",
        **{
            "uri": ICEBERG_ENDPOINT,
            "token": token,
            "warehouse": cfg.warehouse,
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "adls.account-name": "onelake",
            "adls.account-host": ONELAKE_BLOB,
            "adls.token": token,
        },
    )
    allow_table_updates(catalog)
    return catalog


def allow_table_updates(catalog) -> None:
    """Let the catalog, not the client, decide whether it takes a metadata commit: pyiceberg
    refuses client-side when update-table is not in GET /v1/config's endpoints. OneLake
    advertises it (analytics-as-code, 2026-08-25); this only fires if that ever changes."""
    from pyiceberg.catalog import rest

    supported = getattr(catalog, "_supported_endpoints", None)
    if supported is None:
        return
    capability = getattr(getattr(rest, "Capability", None), "V1_UPDATE_TABLE", None)
    if capability is None and hasattr(getattr(rest, "Endpoint", None), "from_string"):
        capability = rest.Endpoint.from_string(UPDATE_TABLE_ENDPOINT)
    if capability is not None and capability not in supported:
        supported.add(capability)
        scrub.safe_print("  update-table is not advertised: the catalog answers for itself")


# --- measures ------------------------------------------------------------------------------------


def state(tbl) -> dict:
    """What the table holds now, without scanning a manifest: the snapshot count, and the data
    files from the manifest list (one Avro file), bytes and rows from the snapshot summary when
    the writer kept its totals."""
    snapshots = tbl.metadata.snapshots or []
    out = {
        "snapshots": len(snapshots),
        "data_files": 0,
        "delete_files": 0,
        "bytes": None,
        "rows": None,
    }
    current = tbl.current_snapshot()
    if current is None:
        return out
    from pyiceberg.manifest import ManifestContent

    for m in current.manifests(tbl.io):
        n = (m.added_files_count or 0) + (m.existing_files_count or 0)
        out["data_files" if m.content == ManifestContent.DATA else "delete_files"] += n
    for key, field in (("total-files-size", "bytes"), ("total-records", "rows")):
        if current.summary and current.summary.get(key) is not None:
            out[field] = int(current.summary[key])
    return out


# --- the three steps -----------------------------------------------------------------------------


def compact(con, name: str) -> dict:
    """{rewritten, added, rewritten_bytes}: what iceberg_rewrite_data_files returned."""
    row = con.execute(
        f"SELECT rewritten_data_files, added_data_files, rewritten_bytes "
        f"FROM iceberg_rewrite_data_files('{CATALOG}.{name}', "
        f"target_file_size_bytes => '{TARGET_MB}MiB', min_input_files => {MIN_INPUT_FILES})"
    ).fetchone()
    if row is None:
        raise RuntimeError("iceberg_rewrite_data_files returned no row")
    rewritten, added, rewritten_bytes = row
    return {
        "rewritten": rewritten or 0,
        "added": added or 0,
        "rewritten_bytes": rewritten_bytes or 0,
    }


def expire(catalog, name: str, cutoff: datetime) -> int:
    """Snapshots expired, counted as ExpireSnapshots.older_than does and confirmed by a re-read:
    a commit that returns is not proof it landed."""
    from pyiceberg.utils.datetime import datetime_to_millis

    tbl = catalog.load_table(name)
    cutoff_ms = datetime_to_millis(cutoff)
    protected = {ref.snapshot_id for ref in tbl.metadata.refs.values()}
    victims = [
        s
        for s in tbl.metadata.snapshots or []
        if s.timestamp_ms < cutoff_ms and s.snapshot_id not in protected
    ]
    if not victims:
        return 0
    before = len(tbl.metadata.snapshots)
    tbl.maintenance.expire_snapshots().older_than(cutoff).commit()
    after = len(catalog.load_table(name).metadata.snapshots or [])
    if after >= before:
        raise RuntimeError(
            f"expiry committed {len(victims)} and the table still has {after} snapshots"
        )
    return before - after


def remove_orphans(catalog, name: str) -> dict:
    """{deleted, failed, bytes}: the files no snapshot or metadata file references, older than
    ORPHAN_DAYS. adlfs lists `abfss://<workspace>/...` where the metadata says
    `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/...`: the same container, the host
    dropped, so the two are declared one authority, from the table's own location."""
    tbl = catalog.load_table(name)
    authority = urlparse(tbl.metadata.location).netloc
    result = (
        tbl.maintenance.remove_orphan_files()
        .older_than(timedelta(days=ORPHAN_DAYS))
        .equal_authorities({authority.split("@")[0]: authority})
        .execute()
    )
    return {
        "deleted": len(result.deleted_files),
        "failed": len(result.failed_to_delete),
        "bytes": int(result.total_bytes or 0),
    }


# --- run -----------------------------------------------------------------------------------------


def tables(catalog) -> list[tuple[str, str]]:
    """(engine, "namespace.table") for every workload table, scratch relations left out."""
    existing = {ns[0] for ns in catalog.list_namespaces()}
    out = []
    for engine in ENGINES:
        for layer in LAYERS:
            namespace = f"dbt_{engine}_{layer}"
            if namespace not in existing:
                continue
            for identifier in sorted(catalog.list_tables(namespace)):
                if not identifier[-1].endswith(SCRATCH):
                    out.append((engine, ".".join(identifier)))
    return out


def run(record_path: Path) -> int:
    import duckdb
    import pyiceberg

    cfg = WorkloadConfig.from_env()
    token = auth.onelake_token(fresh=True)
    con, catalog = duckdb_connect(cfg, token), iceberg_catalog(cfg, token)
    record = {
        "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "run_url": os.environ.get("BENCH_RUN_URL", ""),
        "started": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished": "",
        "settings": {
            "target_mb": TARGET_MB,
            "min_input_files": MIN_INPUT_FILES,
            "expire_days": EXPIRE_DAYS,
            "orphan_days": ORPHAN_DAYS,
        },
        "versions": {
            "duckdb": duckdb.__version__,
            "pyiceberg": getattr(pyiceberg, "__version__", ""),
        },
        "tables": [],
    }

    def save() -> None:
        record_path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")

    todo = tables(catalog)
    scrub.safe_print(
        f"{len(todo)} table(s): compact to {TARGET_MB} MiB (min {MIN_INPUT_FILES} input files), "
        f"expire snapshots older than {EXPIRE_DAYS:g}d, orphan files older than {ORPHAN_DAYS:g}d; "
        f"budget {BUDGET_MINUTES:g} min"
    )
    started = time.monotonic()
    for i, (engine, name) in enumerate(todo, 1):
        entry: dict = {"engine": engine, "table": name}
        record["tables"].append(entry)
        if (time.monotonic() - started) / 60 >= BUDGET_MINUTES:
            entry["skipped"] = f"not attempted ({BUDGET_MINUTES:g} min budget spent)"
            continue
        if auth.token_expires_on() - time.time() < REFRESH_SECONDS:
            token = auth.onelake_token(fresh=True)
            con.close()
            con, catalog = duckdb_connect(cfg, token), iceberg_catalog(cfg, token)
        scrub.safe_print(f"[{i}/{len(todo)}] {name} ...")
        t0 = time.monotonic()
        try:
            entry["before"] = state(catalog.load_table(name))
        except Exception as exc:  # noqa: BLE001 - recorded, the next table still runs
            entry["error"] = {"load": oneline(exc)}
            scrub.safe_print(f"  load: {entry['error']['load']}")
            save()
            continue
        errors = {}
        cutoff = datetime.now(UTC) - timedelta(days=EXPIRE_DAYS)
        steps = (
            ("compact", compact, con),
            ("expired", expire, catalog, cutoff),
            ("orphans", remove_orphans, catalog),
        )
        for step, call, handle, *args in steps:
            try:
                entry[step] = call(handle, name, *args)
            except Exception as exc:  # noqa: BLE001 - recorded, the next step still runs
                errors[step] = oneline(exc)
            scrub.safe_print(f"  {step}: {errors.get(step) or entry[step]}")
        try:
            entry["after"] = state(catalog.load_table(name))
        except Exception as exc:  # noqa: BLE001
            errors["after"] = oneline(exc)
        if errors:
            entry["error"] = errors
        entry["seconds"] = round(time.monotonic() - t0, 1)
        save()
    record["finished"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    save()
    con.close()
    attempted = [t for t in record["tables"] if "skipped" not in t]
    if attempted and all("error" in t for t in attempted):
        print("::error::every table errored")
        return 1
    return 0


# --- publish -------------------------------------------------------------------------------------


def totals(entries: list[dict]) -> dict:
    """One engine's sums over its tables."""

    def total(side: str, field: str):
        values = [t[side][field] for t in entries if t.get(side) and t[side].get(field) is not None]
        return sum(values) if values else None

    return {
        "tables": len(entries),
        "errors": sum("error" in t for t in entries),
        "skipped": sum("skipped" in t for t in entries),
        "files_before": total("before", "data_files"),
        "files_after": total("after", "data_files"),
        "bytes_after": total("after", "bytes"),
        "snapshots_before": total("before", "snapshots"),
        "snapshots_after": total("after", "snapshots"),
        "compacted": sum((t.get("compact") or {}).get("rewritten", 0) for t in entries),
        "compacted_into": sum((t.get("compact") or {}).get("added", 0) for t in entries),
        "compacted_bytes": sum((t.get("compact") or {}).get("rewritten_bytes", 0) for t in entries),
        "expired": sum(t.get("expired") or 0 for t in entries),
        "orphans_deleted": sum((t.get("orphans") or {}).get("deleted", 0) for t in entries),
        "orphans_failed": sum((t.get("orphans") or {}).get("failed", 0) for t in entries),
        "orphan_bytes": sum((t.get("orphans") or {}).get("bytes", 0) for t in entries),
    }


def page_data(record: dict) -> dict:
    """The latest run as the Real workload tab reads it, keyed by bench engine id."""
    engines = {}
    for engine in ENGINES:
        entries = [t for t in record["tables"] if t["engine"] == engine]
        if entries:
            engines[BENCH_ENGINE[engine]] = {
                "totals": totals(entries),
                "tables": {
                    t["table"]: {k: v for k, v in t.items() if k not in ("engine", "table")}
                    for t in entries
                },
            }
    return {
        "run_id": record["run_id"],
        "run_url": record["run_url"],
        "started": record["started"],
        "finished": record["finished"],
        "settings": record["settings"],
        "versions": record["versions"],
        "engines": engines,
    }


def summary(record: dict) -> str:
    s = record["settings"]
    lines = [
        f"## Table maintenance: compact to {s['target_mb']} MiB, expire > {s['expire_days']:g}d, "
        f"orphans > {s['orphan_days']:g}d",
        "",
        "| table | data files | snapshots | compacted | expired | orphans deleted | s | error |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for t in record["tables"]:
        b, a = t.get("before") or {}, t.get("after") or {}
        c, o = t.get("compact") or {}, t.get("orphans") or {}
        err = t.get("skipped") or "; ".join(f"{k}: {v}" for k, v in (t.get("error") or {}).items())
        lines.append(
            f"| `{t['table']}` | {b.get('data_files', '')} → {a.get('data_files', '')} "
            f"| {b.get('snapshots', '')} → {a.get('snapshots', '')} "
            f"| {c.get('rewritten', '')} → {c.get('added', '')} | {t.get('expired', '')} "
            f"| {o.get('deleted', '')} ({o.get('bytes', 0) / 1e9:.2f} GB) | {t.get('seconds', '')} "
            f"| {err.replace('|', '/')} |"
        )
    return "\n".join(lines) + "\n"


def publish(record_path: Path, write: bool) -> int:
    record = json.loads(record_path.read_text(encoding="utf-8"))
    text = summary(record)
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(text)
    if write:
        RESULTS.mkdir(parents=True, exist_ok=True)
        out = RESULTS / f"{record['run_id']}.json"
        out.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        PAGE_DATA.write_text(json.dumps(page_data(record), indent=2) + "\n", encoding="utf-8")
        leak_check([out, PAGE_DATA])
        print(f"wrote {out.relative_to(REPO)} and {PAGE_DATA.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 3 or sys.argv[1] not in ("run", "publish"):
        sys.exit(__doc__)
    path = Path(sys.argv[2])
    sys.exit(run(path) if sys.argv[1] == "run" else publish(path, "--write" in sys.argv))
