"""Write the site's docs/data/capability.json (the Iceberg support tab) from
results/capability/*.json. Every byte is generated; capability.yml's publish job runs this after
the probes and commits it together with the results.

A results file is what one probe script saved (Report.save, or isolation_duckdb's own): the
engine, its version, the CI run, the date, and one row per probe with the probe's method name
as `key`. A row of the capability table names, per engine, the probe keys it is read from: yes
only if every one of them is yes, otherwise the worst outcome, with a note quoting what came back.

    python .github/scripts/capability/render_readme.py [results-dir] [site-json]
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

# The column order, most usable first. Spark is the reference: the Iceberg Java library the others
# are read against; Trino is built on the same library, with its own SQL and planning. Polars has
# no SQL writes but supports more than Databend (16 cells to 10, run 39538881239).
ENGINES = ("spark", "trino", "sail", "duckdb", "chdb", "polars", "databend")
# The ones with SQL, for the rows Polars has no operation for.
SQL = ("spark", "trino", "sail", "duckdb", "chdb", "databend")
# The SQL engines asked the DDL and row-level probes that chDB has no statement for.
FULL_SQL = ("spark", "trino", "duckdb", "sail", "databend")

# (row label, {engine: probe keys}); an engine missing from the dict has no such operation.
ROWS = [
    # Polars' CREATE TABLE is sink_iceberg creating the table it writes: a CTAS too.
    (
        "CREATE TABLE",
        {
            "polars": ["sink_create"],
            **{e: ["create_table"] for e in FULL_SQL},
        },
    ),
    ("CREATE TABLE AS SELECT", {"polars": ["sink_create"], **{e: ["ctas"] for e in SQL}}),
    ("CREATE TABLE IF NOT EXISTS, existing table", {e: ["create_if_not_exists"] for e in SQL}),
    (
        "INSERT / append",
        {
            "spark": ["insert_into"],
            "trino": ["insert_into"],
            "databend": ["insert_into"],
            "polars": ["sink_append"],
            "duckdb": ["insert_values"],
            "sail": ["insert_into"],
        },
    ),
    ("INSERT ... SELECT", {e: ["insert_select"] for e in FULL_SQL}),
    ("DELETE", {"polars": ["delete_rows"], **{e: ["delete_from"] for e in FULL_SQL}}),
    ("UPDATE", {e: ["update"] for e in FULL_SQL}),
    # The upsert. Spark and Sail ask it of a merge-on-read table; chDB has no MERGE statement.
    (
        "MERGE with two actions (UPDATE + INSERT)",
        {
            "spark": ["merge_mor"],
            "trino": ["merge_upsert"],
            "databend": ["merge_upsert"],
            "duckdb": ["merge_upsert"],
            "sail": ["merge_mor"],
            "chdb": ["merge_into"],
        },
    ),
    (
        "INSERT OVERWRITE, whole table",
        {
            "spark": ["insert_overwrite"],
            "trino": ["insert_overwrite"],
            "databend": ["insert_overwrite"],
            "duckdb": ["insert_overwrite"],
            "sail": ["insert_overwrite"],
            "chdb": ["insert_overwrite"],
            "polars": ["sink_overwrite"],
        },
    ),
    ("TRUNCATE", {e: ["truncate"] for e in FULL_SQL}),
    (
        "Partitioned table",
        {
            "spark": ["partitioned"],
            "trino": ["partitioned"],
            "databend": ["partitioned"],
            "polars": ["partitioned"],
            "duckdb": ["create_partitioned"],
            "sail": ["partitioned"],
        },
    ),
    ("Partition transform: bucket", {e: ["partition_bucket"] for e in ENGINES}),
    ("Partition transform: truncate", {e: ["partition_truncate"] for e in ENGINES}),
    (
        "Partition transforms: year / month / day / hour",
        {e: ["partition_temporal"] for e in ENGINES},
    ),
    (
        "Types: decimal, date, timestamp, timestamptz, uuid, binary",
        {e: ["types_scalar"] for e in ENGINES},
    ),
    ("Nested types: struct, list, map", {e: ["types_nested"] for e in ENGINES}),
    (
        "Add column",
        {"polars": ["schema_merge_add_column"], **{e: ["add_column"] for e in SQL}},
    ),
    ("Drop column", {e: ["drop_column"] for e in SQL}),
    ("Rename column", {e: ["rename_column"] for e in SQL}),
    ("Type promotion (int → long)", {e: ["type_promotion"] for e in ENGINES}),
    ("Partition evolution", {e: ["partition_evolution"] for e in SQL}),
    (
        "Write after partition evolution",
        {e: ["write_after_evolution"] for e in ENGINES},
    ),
    ("Set table property", {e: ["set_property"] for e in SQL}),
    (
        "Sort order evolution",
        {
            "spark": ["sort_order"],
            "trino": ["sort_order"],
            "databend": ["sort_order"],
            "duckdb": ["create_sorted"],
            "sail": ["sort_order"],
        },
    ),
    ("Time travel", {e: ["time_travel"] for e in ENGINES}),
    (
        "Metadata tables",
        {
            "spark": ["metadata_tables"],
            "trino": ["metadata_tables"],
            "databend": ["metadata_tables"],
            "duckdb": ["metadata_snapshots", "metadata_files"],
            "sail": ["metadata_tables"],
        },
    ),
    # pyiceberg writes four data files with known min/max bounds, then deletes some of them from
    # storage and keeps the metadata. A right answer means the engine never opened them.
    ("File pruning from min/max in the metadata", {e: ["file_pruning"] for e in ENGINES}),
    ("MAX from the metadata, no data file read", {e: ["max_from_metadata"] for e in ENGINES}),
    ("Compaction", {e: ["rewrite_data_files"] for e in FULL_SQL}),
    ("Rewrite manifests", {e: ["rewrite_manifests"] for e in SQL}),
    ("Create branch", {e: ["create_branch"] for e in SQL}),
    ("Create tag", {e: ["create_tag"] for e in FULL_SQL}),
    (
        "Drop table with purge",
        {
            "spark": ["drop_purge"],
            "trino": ["drop_purge"],
            "databend": ["drop_purge"],
            "duckdb": ["drop_table"],
            "sail": ["drop_purge"],
        },
    ),
    (
        "Create / drop namespace",
        {
            "spark": ["namespace"],
            "trino": ["namespace"],
            "databend": ["namespace"],
            "duckdb": ["create_schema", "drop_schema"],
            "sail": ["namespace"],
        },
    ),
    ("CREATE SCHEMA IF NOT EXISTS, existing schema", {e: ["namespace_existing"] for e in SQL}),
    # Writer B (pyiceberg) commits between the engine's read and its commit
    # (bench/capability/race.py).
    # DuckDB's cells are its isolation run's, at Iceberg's default level.
    ("Concurrent append: both kept", {e: ["race_append"] for e in ENGINES}),
    (
        "Concurrent writer: DELETE loses nothing",
        {"polars": ["race_delete"], **{e: ["race_delete"] for e in SQL}},
    ),
    ("Concurrent writer: UPDATE loses nothing", {e: ["race_update"] for e in SQL}),
]

# chDB's probe keys where they differ from the row's shared name; the rows built over ENGINES
# already ask chDB the shared one. chDB's UPDATE is `ALTER TABLE ... UPDATE`.
CHDB = {
    "CREATE TABLE": ["create_table"],
    "INSERT / append": ["insert_into"],
    "INSERT ... SELECT": ["insert_select"],
    "DELETE": ["delete_from", "alter_delete"],
    "UPDATE": ["alter_update"],
    "TRUNCATE": ["truncate"],
    "Partitioned table": ["partitioned"],
    "Sort order evolution": ["sort_order_evolution"],
    "Metadata tables": ["metadata_tables"],
    "Compaction": ["compaction"],
    "Create tag": ["create_tag"],
    "Drop table with purge": ["drop_table"],
    "Create / drop namespace": ["namespace"],
}
for _label, _sources in ROWS:
    if _label in CHDB:
        _sources["chdb"] = CHDB[_label]

# THE SITE'S GRID (docs/data/capability.json, the Iceberg support tab): every row of ROWS, grouped.
GROUPS = {
    # The grid opens on Core: the minimum an engine needs to be usable on Iceberg.
    "Core": [
        "CREATE TABLE",
        "CREATE TABLE AS SELECT",
        "INSERT / append",
        "INSERT ... SELECT",
        "DELETE",
        "UPDATE",
        "MERGE with two actions (UPDATE + INSERT)",
        "INSERT OVERWRITE, whole table",
        "TRUNCATE",
        "Drop table with purge",
        "Create / drop namespace",
        "CREATE SCHEMA IF NOT EXISTS, existing schema",
        "Concurrent append: both kept",
        "Concurrent writer: DELETE loses nothing",
        "Concurrent writer: UPDATE loses nothing",
    ],
    "Tables & types": [
        "CREATE TABLE IF NOT EXISTS, existing table",
        "Partitioned table",
        "Partition transform: bucket",
        "Partition transform: truncate",
        "Partition transforms: year / month / day / hour",
        "Types: decimal, date, timestamp, timestamptz, uuid, binary",
        "Nested types: struct, list, map",
    ],
    "Schema": [
        "Add column",
        "Drop column",
        "Rename column",
        "Type promotion (int → long)",
        "Partition evolution",
        "Write after partition evolution",
        "Set table property",
        "Sort order evolution",
    ],
    "Reading from metadata": [
        "File pruning from min/max in the metadata",
        "MAX from the metadata, no data file read",
    ],
    "Read & refs": [
        "Time travel",
        "Metadata tables",
        "Create branch",
        "Create tag",
    ],
    # The site shows this group on a tab of its own, Maintenance.
    "Maintenance": ["Compaction", "Rewrite manifests"],
}
# The site's engine keys, which are lakehouse_benchmark's.
SITE_KEY = {
    "spark": "pyspark_iceberg",
    "trino": "trino_iceberg",
    "databend": "databend_iceberg",
    "polars": "polars_iceberg",
    "duckdb": "duckdb_iceberg",
    "sail": "lakesail_iceberg",
    "chdb": "chdb_iceberg",
}

# Worst first: the cell shows the worst outcome among the row's probes.
SEVERITY = ["broken", "no", "no-op", "skipped", "supported"]


def load(results: Path) -> dict:
    found = {}
    for path in sorted(results.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        found[data["engine"]] = data
    return found


# A statement that runs but does not do what the row means is a no: (row label, what the reading
# says) -> why. TRUNCATE means metadata-only. It comes from the reading, so it goes when the engine
# fixes it.
FALLS_SHORT = {
    ("TRUNCATE", "NOT metadata-only"): (
        "Runs, but not as a metadata operation. The data files stay, and a delete file is written "
        "for them: slower than dropping the files, and every reader then has to apply the deletes."
    ),
}


# A no the catalog decides, where the reading's own message does not say why: (row label, engine)
# -> why, shown in place of the quoted detail while the cell is a no. Spark's CTAS is an atomic
# staged create, refused with a bare 400 "Malformed request".
CATALOG_NO = {
    ("CREATE TABLE AS SELECT", "spark"): (
        "the OneLake catalog does not support staged create, which Spark's CTAS uses."
    ),
}


def falls_short(rows: dict, keys: list[str], label: str) -> str:
    """Why one engine's yes on one row is really a no, or '' when it is a yes."""
    details = " ".join(str(rows[k]["detail"]) for k in keys if k in rows)
    return next(
        (why for (row, said), why in FALLS_SHORT.items() if row == label and said in details), ""
    )


def cell(rows: dict, keys: list[str]) -> tuple[str, list[dict]]:
    """(outcome, the rows that made it not a yes) for one engine's probes of one table row."""
    picked = [rows[k] for k in keys if k in rows]
    if not picked:
        return "skipped", []
    worst = min(picked, key=lambda r: SEVERITY.index(r["outcome"]))["outcome"]
    return worst, [r for r in picked if r["outcome"] != "supported"]


_MESSAGE = re.compile(r'"message"\s*:\s*"([^"]+)"')
_STATUS = re.compile(r"\b([45]\d\d)\b")


def _one_line(text: str, limit: int = 300) -> str:
    """One line of what came back. A catalog error is quoted by its own status and message,
    which a client wraps in a long prefix that would otherwise eat the whole limit."""
    flat = " ".join(str(text).split()).replace("|", "/")
    message = _MESSAGE.search(flat)
    if message:
        status = _STATUS.search(flat)
        flat = (f"{status.group(1)} " if status else "") + message.group(1)
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def by_engine(data: dict) -> tuple[list[str], dict]:
    """The engines with a reading, and each one's rows by probe key. DuckDB's column also carries
    its isolation run's rows."""
    engines = [e for e in ENGINES if e in data]
    by_key = {e: {r["key"]: r for r in data[e]["rows"]} for e in engines}
    if "duckdb" in by_key:
        isolation_rows = data.get("duckdb_isolation", {}).get("rows", [])
        by_key["duckdb"].update({r["key"]: r for r in isolation_rows})
    return engines, by_key


def site_matrix(data: dict) -> dict:
    """The capability grid for the site, grouped. Each cell is
    {"o": outcome, "n": what came back}."""
    engines, by_key = by_engine(data)
    sources = dict(ROWS)
    rows = []
    for group, labels in GROUPS.items():
        for label in labels:
            cells = {}
            for e in engines:
                if e not in sources[label]:
                    cells[SITE_KEY[e]] = {"o": "na"}
                    continue
                outcome, why = cell(by_key[e], sources[label][e])
                note = "; ".join(_one_line(r["detail"], 220) for r in why)
                if outcome == "supported" and (
                    short := falls_short(by_key[e], sources[label][e], label)
                ):
                    outcome, note = "no", short
                elif outcome == "no" and (label, e) in CATALOG_NO:
                    note = CATALOG_NO[(label, e)]
                cells[SITE_KEY[e]] = {"o": outcome, **({"n": note} if note else {})}
            rows.append({"group": group, "label": label, "cells": cells})
    return {
        "engines": {
            SITE_KEY[e]: {
                "version": data[e]["version"],
                "run": data[e]["run"],
                "url": data[e]["url"],
                "date": data[e]["date"],
            }
            for e in engines
        },
        "groups": list(GROUPS),
        "rows": rows,
    }


def main() -> int:
    results = Path(sys.argv[1] if len(sys.argv) > 1 else "results/capability")
    site = Path(sys.argv[2] if len(sys.argv) > 2 else "docs/data/capability.json")
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
