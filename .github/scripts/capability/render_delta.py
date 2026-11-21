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
# whose SQL has no Delta write, has no column.
ENGINES = ("spark", "sail", "trino", "duckdb")
# The site's engine keys: the same as the Iceberg grid's, so each engine keeps its one label and
# colour across the site (bench/charts.py, docs/index.html).
SITE_KEY = {
    "spark": "pyspark_iceberg",
    "sail": "lakesail_iceberg",
    "trino": "trino_iceberg",
    "duckdb": "duckdb_iceberg",
}

# (row label, probe key): every engine is asked every row under the same key (delta_probe.PROBES).
ROWS = [
    ("Register a Delta table as catalog.schema.table", "full_identifier"),
    ("CREATE TABLE at a path", "create_table"),
    ("SELECT a table delta-rs wrote", "select_table"),
    ("CREATE TABLE AS SELECT", "ctas"),
    ("Concurrent append: both kept", "race_append"),
    ("INSERT / append", "insert_into"),
    ("INSERT ... SELECT", "insert_select"),
    ("DELETE", "delete_from"),
    ("UPDATE", "update"),
    ("INSERT OVERWRITE, whole table", "insert_overwrite"),
    ("CREATE TABLE IF NOT EXISTS, existing table", "create_if_not_exists"),
    ("MERGE with two actions (UPDATE + INSERT)", "merge_upsert"),
    ("TRUNCATE", "truncate"),
    ("Overwrite one partition", "overwrite_partition"),
    ("DELETE writes deletion vectors", "deletion_vectors"),
    ("Partitioned table", "partitioned"),
    ("Generated column as the partition", "generated_partition"),
    ("Types: decimal, date, timestamp, timestamptz, binary", "types_scalar"),
    ("Nested types: struct, list, map", "types_nested"),
    ("Add column", "add_column"),
    ("Drop column (column mapping)", "drop_column"),
    ("Rename column (column mapping)", "rename_column"),
    ("Type widening (int → long)", "type_widening"),
    ("Set table property", "set_property"),
    ("Write after a schema change", "write_after_schema_change"),
    ("File pruning from file stats", "file_pruning"),
    ("Time travel by version", "time_travel_version"),
    ("Time travel by timestamp", "time_travel_timestamp"),
    ("Table history", "history"),
    ("Change data feed", "change_data_feed"),
    ("OPTIMIZE (compaction)", "optimize"),
    ("OPTIMIZE ... ZORDER BY", "zorder"),
    ("Liquid clustering (CLUSTER BY)", "liquid_clustering"),
    ("VACUUM", "vacuum"),
    ("Checkpoint written", "checkpoint"),
]

GROUPS = {
    # The grid opens on Core: the minimum an engine needs to be usable on Delta.
    "Core": [
        "Register a Delta table as catalog.schema.table",
        "CREATE TABLE at a path",
        "SELECT a table delta-rs wrote",
        "CREATE TABLE AS SELECT",
        "INSERT / append",
        "INSERT ... SELECT",
        "DELETE",
        "UPDATE",
        "INSERT OVERWRITE, whole table",
        "Concurrent append: both kept",
    ],
    "Writes": [
        "MERGE with two actions (UPDATE + INSERT)",
        "TRUNCATE",
        "Overwrite one partition",
        "DELETE writes deletion vectors",
        "CREATE TABLE IF NOT EXISTS, existing table",
    ],
    "Partitions & types": [
        "Partitioned table",
        "Generated column as the partition",
        "Types: decimal, date, timestamp, timestamptz, binary",
        "Nested types: struct, list, map",
    ],
    "Schema": [
        "Add column",
        "Drop column (column mapping)",
        "Rename column (column mapping)",
        "Type widening (int → long)",
        "Set table property",
        "Write after a schema change",
    ],
    "Reads & history": [
        "File pruning from file stats",
        "Time travel by version",
        "Time travel by timestamp",
        "Table history",
        "Change data feed",
    ],
    "Maintenance": [
        "OPTIMIZE (compaction)",
        "OPTIMIZE ... ZORDER BY",
        "Liquid clustering (CLUSTER BY)",
        "VACUUM",
        "Checkpoint written",
    ],
}


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
