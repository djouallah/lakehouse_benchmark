"""TEMPORARY, read-only: did table maintenance's compaction actually drop the old files?

maintenance.json shows data_files before == after on every table, even where DuckDB says 20 files
were compacted into 5. For a few tables this prints the current snapshot three ways: the manifest
list's counts (what workload_maintain.state() reads), the live data files pyiceberg's
inspect.files() returns from the manifests themselves, and DuckDB's iceberg_metadata(); plus the
snapshot history and the row count against the snapshot summary's total-records.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from workload_maintain import CATALOG, duckdb_connect, iceberg_catalog  # noqa: E402

from bench import auth  # noqa: E402
from bench.workload.config import WorkloadConfig  # noqa: E402

TABLES = [
    "dbt_duckdb_landing.fct_price",
    "dbt_duckdb_landing.fct_price_today",
    "dbt_polars_landing.fct_price",
    "dbt_trino_landing.fct_price",
]


def main() -> None:
    from pyiceberg.manifest import ManifestContent

    cfg = WorkloadConfig.from_env()
    token = auth.onelake_token(fresh=True)
    con, catalog = duckdb_connect(cfg, token), iceberg_catalog(cfg, token)
    for name in TABLES:
        print(f"\n===== {name}")
        tbl = catalog.load_table(name)
        cur = tbl.current_snapshot()
        print("snapshots:")
        for s in tbl.metadata.snapshots or []:
            sm = s.summary or {}
            print(
                f"  {s.snapshot_id} parent={s.parent_snapshot_id} op={sm.operation.value if sm else '?'} "
                f"added={sm.get('added-data-files')} deleted={sm.get('deleted-data-files')} "
                f"total={sm.get('total-data-files')} size={sm.get('total-files-size')} "
                f"records={sm.get('total-records')}{'  <- current' if cur and s.snapshot_id == cur.snapshot_id else ''}"
            )
        if cur is None:
            continue
        print("manifest list of current:")
        for m in cur.manifests(tbl.io):
            print(
                f"  {m.manifest_path.rsplit('/', 1)[-1]} {ManifestContent(m.content).name} "
                f"added={m.added_files_count} existing={m.existing_files_count} deleted={m.deleted_files_count} "
                f"snap={m.added_snapshot_id}"
            )
        files = tbl.inspect.files().to_pylist()
        data = [f for f in files if f["content"] == 0]
        print(
            f"pyiceberg inspect.files(): {len(data)} live data files, "
            f"{sum(f['file_size_in_bytes'] for f in data):,} bytes, "
            f"{sum(f['record_count'] for f in data):,} records"
        )
        rows = con.execute(
            f"SELECT status, content, count(*) FROM iceberg_metadata('{CATALOG}.{name}') "
            "GROUP BY ALL ORDER BY ALL"
        ).fetchall()
        print(f"duckdb iceberg_metadata (status, content, files): {rows}")
        n = con.execute(f"SELECT count(*) FROM {CATALOG}.{name}").fetchone()[0]
        print(f"duckdb count(*): {n:,}")


if __name__ == "__main__":
    main()
