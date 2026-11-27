"""Write the site's docs/data/delta.json (the Delta support tab) from results/delta/*.json.

The Iceberg grid's renderer (render_readme.py), on the Delta rows: the same cell rule (yes only if
every probe is yes, otherwise the worst outcome with the reading quoted), the same column order
(supported cells, most first), the same JSON the page draws. delta_capability.yml's publish job
runs this after the probes and commits it with the results.

    python .github/scripts/capability/render_delta.py [results-dir] [site-json]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from render_readme import _one_line, cell, load

# The engines with a SQL path that writes Delta. An engine whose docs say it only reads Delta, or
# whose SQL has no Delta write, has no column. delta-rs and Polars are asked through their Python
# API (delta_capability_delta_rs.py, delta_capability_polars.py); duckrun is DuckDB SQL with
# delta-rs writing (delta_capability_duckrun.py).
ENGINES = ("spark", "sail", "trino", "duckdb", "delta_rs", "duckrun", "polars")
# The site's engine keys: the same as the Iceberg grid's, so each engine keeps its one label and
# colour across the site (bench/charts.py, docs/index.html).
SITE_KEY = {
    "spark": "pyspark_iceberg",
    "sail": "lakesail_iceberg",
    "trino": "trino_iceberg",
    "duckdb": "duckdb_iceberg",
    "delta_rs": "delta_rs",
    "duckrun": "duckrun",
    "polars": "polars_iceberg",
}

# (group, row label, probe key): every engine is asked every row under the same key
# (delta_probe.PROBES). The groups are the Iceberg grid's, so the two formats read side by side,
# plus Tables written by Spark: reading and writing a table Spark wrote with its Delta features
# turned on (their own group so Writes fits a 1080p screen, tests/test_layout.py);
# Basic is the minimum to use the format at all: create, read, append, append safely.
TABLE = [
    ("Basic", "CREATE TABLE at a path", "create_table"),
    ("Basic", "SELECT a table delta-rs wrote", "select_table"),
    ("Basic", "INSERT / append", "insert_into"),
    ("Basic", "INSERT ... SELECT", "insert_select"),
    ("Basic", "Concurrent append: both kept", "race_append"),
    ("Writes", "CREATE TABLE AS SELECT", "ctas"),
    ("Writes", "DELETE", "delete_from"),
    ("Writes", "UPDATE", "update"),
    ("Writes", "MERGE with two actions (UPDATE + INSERT)", "merge_upsert"),
    (
        "Writes",
        "MERGE into a partitioned table reads and rewrites only its partitions",
        "merge_partitions",
    ),
    ("Writes", "INSERT OVERWRITE, whole table", "insert_overwrite"),
    ("Writes", "CREATE OR REPLACE TABLE AS SELECT", "create_or_replace"),
    ("Writes", "TRUNCATE", "truncate"),
    ("Writes", "Overwrite one partition", "overwrite_partition"),
    ("Writes", "Convert Parquet files to Delta", "convert_to_delta"),
    ("Writes", "DELETE writes deletion vectors", "deletion_vectors"),
    ("Writes", "CHECK constraint: a violating INSERT is refused", "check_constraint"),
    ("Writes", "Concurrent writer: DELETE loses nothing", "race_delete"),
    ("Writes", "Concurrent writer: UPDATE loses nothing", "race_update"),
    ("Catalog", "Register a Delta table as catalog.schema.table", "full_identifier"),
    ("Catalog", "CREATE SCHEMA at a folder", "schema_create"),
    ("Catalog", "DROP SCHEMA removes the folder", "schema_drop"),
    ("Catalog", "CREATE SCHEMA IF NOT EXISTS, existing schema", "schema_existing"),
    ("Catalog", "CREATE TABLE IF NOT EXISTS, existing table", "create_if_not_exists"),
    ("Catalog", "DROP TABLE removes the table folder", "drop_table"),
    ("Partitions & types", "Partitioned table", "partitioned"),
    ("Partitions & types", "Generated column as the partition", "generated_partition"),
    ("Partitions & types", "Types: decimal, date, timestamp, timestamptz, binary", "types_scalar"),
    ("Partitions & types", "Nested types: struct, list, map", "types_nested"),
    ("Partitions & types", "VARIANT: write and read back", "variant"),
    ("Schema", "Add column", "add_column"),
    ("Schema", "Drop column (column mapping)", "drop_column"),
    ("Schema", "Rename column (column mapping)", "rename_column"),
    ("Schema", "Type widening (int → long)", "type_widening"),
    ("Schema", "Set table property", "set_property"),
    ("Schema", "Write after a schema change", "write_after_schema_change"),
    ("Reads & history", "File pruning from file stats", "file_pruning"),
    ("Reads & history", "Partition pruning", "partition_pruning"),
    ("Reads & history", "MAX and COUNT from the log, no data file read", "stats_from_log"),
    ("Reads & history", "Time travel by version", "time_travel_version"),
    ("Reads & history", "Time travel by timestamp", "time_travel_timestamp"),
    ("Reads & history", "Table history", "history"),
    ("Reads & history", "DESCRIBE DETAIL", "describe_detail"),
    ("Reads & history", "Change data feed", "change_data_feed"),
    ("Tables written by Spark", "Read, deletion vectors on", "read_deletion_vectors"),
    ("Tables written by Spark", "Read, column mapping on", "read_column_mapping"),
    ("Tables written by Spark", "Read, type widening on", "read_type_widening"),
    ("Tables written by Spark", "Write, deletion vectors on", "write_spark_deletion_vectors"),
    ("Tables written by Spark", "Write, column mapping on", "write_spark_column_mapping"),
    ("Tables written by Spark", "Write, change data feed on", "write_spark_change_data_feed"),
    ("Maintenance", "OPTIMIZE (compaction)", "optimize"),
    ("Maintenance", "OPTIMIZE ... ZORDER BY", "zorder"),
    ("Maintenance", "CLUSTER BY", "liquid_clustering"),
    ("Maintenance", "VACUUM", "vacuum"),
    ("Maintenance", "Checkpoint written", "checkpoint"),
]
ROWS = [(label, key) for _, label, key in TABLE]
GROUPS: dict[str, list[str]] = {}
for _group, _label, _ in TABLE:
    GROUPS.setdefault(_group, []).append(_label)


def site_matrix(data: dict) -> dict:
    """The Delta grid for the site, grouped. Each cell is {"o": outcome, "n": what came back}."""
    engines = [e for e in ENGINES if e in data]
    by_key = {e: {r["key"]: r for r in data[e]["rows"]} for e in engines}
    sources = dict(ROWS)
    rows = []
    for group, labels in GROUPS.items():
        for label in labels:
            cells = {}
            for e in engines:
                outcome, why = cell(by_key[e], [sources[label]])
                note = "; ".join(_one_line(r["detail"], 220) for r in why)
                cells[SITE_KEY[e]] = {"o": outcome, **({"n": note} if note else {})}
            rows.append({"group": group, "label": label, "cells": cells})
    engines = sorted(
        engines, key=lambda e: -sum(r["cells"][SITE_KEY[e]]["o"] == "supported" for r in rows)
    )
    for r in rows:
        r["cells"] = {SITE_KEY[e]: r["cells"][SITE_KEY[e]] for e in engines}
    return {
        "engines": {
            SITE_KEY[e]: {k: data[e][k] for k in ("version", "run", "url", "date")} for e in engines
        },
        "groups": list(GROUPS),
        "rows": rows,
    }


def main() -> int:
    results = Path(sys.argv[1] if len(sys.argv) > 1 else "results/delta")
    site = Path(sys.argv[2] if len(sys.argv) > 2 else "docs/data/delta.json")
    data = load(results)
    site.parent.mkdir(parents=True, exist_ok=True)
    site.write_text(
        json.dumps(site_matrix(data), ensure_ascii=False, indent=1) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"wrote {site} from {', '.join(sorted(data))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
