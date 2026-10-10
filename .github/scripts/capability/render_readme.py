"""Write docs/capability/RESULTS.md and the site's docs/data/capability.json from
results/capability/*.json. Every byte is generated; capability.yml's publish job runs this after
the probes and commits both together with the results.

A results file is what one probe script saved (Report.save, or isolation_duckdb's own): the
engine, its version, the CI run, the date, and one row per probe with the probe's method name
as `key`. A row of the capability table names, per engine, the probe keys it is read from: yes
only if every one of them is yes, otherwise the worst outcome, with a note quoting what came back.

    python .github/scripts/capability/render_readme.py [results-dir] [results-md] [site-json]
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ENGINES = ("polars", "duckdb", "sail", "chdb")
# The ones with SQL, for the rows Polars has no operation for.
SQL = ("duckdb", "sail", "chdb")
TITLE = {"polars": "Polars", "duckdb": "DuckDB", "sail": "Sail", "chdb": "chDB"}
LANGUAGE = {"polars": "Rust", "duckdb": "C++", "sail": "Rust", "chdb": "C++"}
IMPLEMENTATION = {
    "polars": "own parquet reader and writer, pyiceberg for the catalog and commit",
    "duckdb": "own",
    "sail": "own, on DataFusion",
    "chdb": "ClickHouse's own",
}

# (row label, {engine: probe keys}); an engine missing from the dict has no such operation.
ROWS = [
    ("CREATE TABLE", {"duckdb": ["create_table"], "sail": ["create_table"]}),
    (
        "INSERT / append",
        {"polars": ["sink_append"], "duckdb": ["insert_values"], "sail": ["insert_into"]},
    ),
    ("INSERT ... SELECT", {"duckdb": ["insert_select"]}),
    ("DELETE", {"duckdb": ["delete_from"], "sail": ["delete_from"]}),
    ("UPDATE", {"duckdb": ["update"], "sail": ["update"]}),
    (
        "MERGE with one action",
        {
            e: [f"merge_{a}_only" for a in ("update", "delete", "insert", "by_source")]
            for e in ("duckdb", "sail")
        },
    ),
    ("TRUNCATE", {"duckdb": ["truncate"], "sail": ["truncate"]}),
    ("CREATE TABLE AS SELECT", {"duckdb": ["ctas"], "sail": ["ctas"]}),
    (
        "Partitioned table",
        {
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
        {"polars": ["write_after_evolution"], "duckdb": ["write_after_evolution"]},
    ),
    ("Set table property", {e: ["set_property"] for e in SQL}),
    ("Sort order at create", {"duckdb": ["create_sorted_at_create"]}),
    ("Sort order evolution", {"duckdb": ["create_sorted"], "sail": ["sort_order"]}),
    ("Time travel", {e: ["time_travel"] for e in ENGINES}),
    (
        "Metadata tables",
        {"duckdb": ["metadata_snapshots", "metadata_files"], "sail": ["metadata_tables"]},
    ),
    ("Compaction", {"duckdb": ["rewrite_data_files"], "sail": ["rewrite_data_files"]}),
    ("Expire snapshots", {e: ["expire_snapshots"] for e in SQL}),
    ("Create branch", {e: ["create_branch"] for e in SQL}),
    ("Create tag", {"sail": ["create_tag"]}),
    ("Drop table with purge", {"duckdb": ["drop_table"], "sail": ["drop_purge"]}),
    (
        "Create / drop namespace",
        {"duckdb": ["create_schema", "drop_schema"], "sail": ["namespace"]},
    ),
    ("Credential vending", {"duckdb": ["credential_vending"]}),
    ("A commit against a stale snapshot is refused", {"polars": ["stale_assertion"]}),
    # Writer B (pyiceberg) commits between the engine's read and its commit (bench/race.py).
    # DuckDB's cells are its isolation run's, at Iceberg's default level.
    ("Concurrent append: both kept", {e: ["race_append"] for e in ENGINES}),
    ("Concurrent writer: DELETE loses nothing", {e: ["race_delete"] for e in SQL}),
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
    # chDB has no MERGE statement at all: its MERGE INTO probe is the one-action cell's answer.
    "MERGE with one action": ["merge_into"],
    "TRUNCATE": ["truncate"],
    "CREATE TABLE AS SELECT": ["ctas"],
    "Partitioned table": ["partitioned"],
    "Sort order at create": ["sorted_at_create"],
    "Sort order evolution": ["sort_order_evolution"],
    "Metadata tables": ["metadata_tables"],
    "Compaction": ["compaction"],
    "Create tag": ["create_tag"],
    "Drop table with purge": ["drop_table"],
}
for _label, _sources in ROWS:
    if _label in CHDB:
        _sources["chdb"] = CHDB[_label]

# THE SITE'S GRID (docs/data/capability.json, the Capability tab): every row of ROWS, grouped.
GROUPS = {
    "Write": [
        "INSERT / append",
        "INSERT ... SELECT",
        "DELETE",
        "UPDATE",
        "MERGE with one action",
        "TRUNCATE",
    ],
    "Tables & types": [
        "CREATE TABLE",
        "CREATE TABLE AS SELECT",
        "Partitioned table",
        "Partition transform: bucket",
        "Partition transform: truncate",
        "Partition transforms: year / month / day / hour",
        "Types: decimal, date, timestamp, timestamptz, uuid, binary",
        "Nested types: struct, list, map",
        "Drop table with purge",
        "Create / drop namespace",
    ],
    "Schema": [
        "Add column",
        "Drop column",
        "Rename column",
        "Type promotion (int → long)",
        "Partition evolution",
        "Write after partition evolution",
        "Set table property",
        "Sort order at create",
        "Sort order evolution",
    ],
    "Concurrency": [
        "A commit against a stale snapshot is refused",
        "Concurrent append: both kept",
        "Concurrent writer: DELETE loses nothing",
        "Concurrent writer: UPDATE loses nothing",
    ],
    "Read & maintenance": [
        "Time travel",
        "Metadata tables",
        "Compaction",
        "Expire snapshots",
        "Create branch",
        "Create tag",
        "Credential vending",
    ],
}
# The site's engine keys, which are lakehouse_benchmark's.
SITE_KEY = {
    "polars": "polars_iceberg",
    "duckdb": "duckdb_iceberg",
    "sail": "lakesail_iceberg",
    "chdb": "chdb_iceberg",
}
# Readings saved before the probes moved here carry no `url`; they ran in iceberg-probe-native.
RUN_URL = "https://github.com/djouallah/iceberg-probe-native/actions/runs/{}"

# Worst first: the cell shows the worst outcome among the row's probes.
SEVERITY = ["broken", "no", "no-op", "skipped", "supported"]
MARK = {"supported": "yes", "no": "no", "no-op": "no-op", "skipped": "—", "broken": "?"}
SUPERSCRIPT = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹")


def load(results: Path) -> dict:
    found = {}
    for path in sorted(results.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        found[data["engine"]] = data
    return found


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


def capability(data: dict) -> tuple[list[str], list[str]]:
    engines, by_key = by_engine(data)
    head = ["| Operation | " + " | ".join(TITLE[e] for e in engines) + " |"]
    head.append("|---|" + "---|" * len(engines))
    head.append("| Version | " + " | ".join(data[e]["version"] for e in engines) + " |")
    head.append("| Language | " + " | ".join(LANGUAGE[e] for e in engines) + " |")
    head.append(
        "| Iceberg implementation | " + " | ".join(IMPLEMENTATION[e] for e in engines) + " |"
    )
    notes: list[str] = []
    for label, sources in ROWS:
        cells = []
        for e in engines:
            if e not in sources:
                cells.append("na")
                continue
            outcome, why = cell(by_key[e], sources[e])
            mark = MARK[outcome]
            if why and outcome != "skipped":
                notes.append(
                    f"{TITLE[e]}, {label}: "
                    + "; ".join(f"{r['question']}: `{_one_line(r['detail'])}`" for r in why)
                )
                mark += " " + str(len(notes)).translate(SUPERSCRIPT)
            cells.append(mark)
        head.append(f"| {label} | " + " | ".join(cells) + " |")
    return head, [f"{i}. {n}" for i, n in enumerate(notes, 1)]


LEVEL_TITLES = {
    "insert": "INSERT, B appends",
    "delete": "DELETE a row, B appends",
    "update_other": "UPDATE another row, B appends",
    "merge_other": "MERGE on another row, B appends",
    "delete_vs_delete": "DELETE a row, B deletes another row",
}


def isolation(data: dict) -> list[str]:
    iso = data.get("duckdb_isolation")
    if not iso:
        return []
    levels = iso["levels"]
    names = list(iso["configs"])
    out = [
        "## DuckDB: isolation levels and transactions",
        "",
        "DuckDB is the only one with transactions. Writer B (pyiceberg) commits between DuckDB's",
        "read and DuckDB's commit; the race is injected at the REST commit through a local proxy",
        "(`bench/capability/race.py`), so it is deterministic. Every DuckDB connection runs",
        "`SET iceberg_use_metadata_log = false`",
        "([duckdb-iceberg#1475](https://github.com/duckdb/duckdb-iceberg/issues/1475)).",
        "",
        "`refused` DuckDB's commit fails and B's change stands · `retried` both changes kept ·",
        "`lost` B's change is gone · `broken` the race could not be run",
        "",
        "| DuckDB writes, B commits in between | " + " | ".join(names) + " |",
        "|---|" + "---|" * len(names),
    ]
    for key, title in LEVEL_TITLES.items():
        cells = [levels.get(f"{key}/{n}", {}).get("outcome", "—") for n in names]
        if any(c != "—" for c in cells):
            out.append(f"| {title} | " + " | ".join(cells) + " |")
    out += [
        "",
        "Columns are table properties: "
        + "; ".join(
            f"`{n}` " + (", ".join(f"`{k} = {v}`" for k, v in p.items()) or "nothing set")
            for n, p in iso["configs"].items()
        )
        + ".",
        "",
        "| Transaction (`BEGIN ... COMMIT`), B commits in the middle | Outcome | What came back |",
        "|---|---|---|",
    ]
    out += [
        f"| {t['title']} | {t['outcome']} | `{_one_line(t['detail'], 200)}` |"
        for t in iso["transactions"]
    ]
    out += [
        "",
        "| Statements in one `BEGIN ... COMMIT`, no concurrent writer | Outcome | What came back |",
        "|---|---|---|",
    ]
    out += [
        f"| {c['title']} | {c['outcome']} | `{_one_line(c['detail'], 200)}` |"
        for c in iso["combos"]
    ]
    return out + [""]


def provenance(data: dict) -> list[str]:
    lines = [
        "## Where these readings come from",
        "",
        "The OneLake Iceberg REST catalog in production, read by CI "
        "(`.github/workflows/capability.yml`), which writes this file. Every cell is a reading "
        "taken by sending the request, not a property of the product: re-run rather than trust it.",
        "",
    ]
    for name in (*ENGINES, "duckdb_isolation"):
        if name in data:
            d = data[name]
            url = d.get("url") or RUN_URL.format(d["run"])
            lines.append(f"- {name}: {d['version']}, [run {d['run']}]({url}), {d['date']}")
    return lines + [""]


def render(data: dict) -> str:
    table, notes = capability(data)
    out = [
        "# Iceberg support: what each engine can do against the OneLake Iceberg REST catalog",
        "",
        "Polars, DuckDB, Sail and chDB: engines with their own Iceberg implementation, no JVM.",
        "",
        "`yes` works · `no` refused · `no-op` accepted but not applied · `na` the engine has no "
        "such operation · `—` not probed · `?` the probe could not ask",
        "",
        *table,
        "",
    ]
    if notes:
        out += ["## Notes", "", *notes, ""]
    out += isolation(data)
    out += provenance(data)
    return "\n".join(out)


def site_matrix(data: dict) -> dict:
    """The capability grid for the site: RESULTS.md's cells, grouped. Each cell is
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
                cells[SITE_KEY[e]] = {"o": outcome, **({"n": note} if note else {})}
            rows.append({"group": group, "label": label, "cells": cells})
    return {
        "engines": {
            SITE_KEY[e]: {
                "version": data[e]["version"],
                "run": data[e]["run"],
                "url": data[e].get("url") or RUN_URL.format(data[e]["run"]),
                "date": data[e]["date"],
            }
            for e in engines
        },
        "groups": list(GROUPS),
        "rows": rows,
    }


def main() -> int:
    results = Path(sys.argv[1] if len(sys.argv) > 1 else "results/capability")
    path = Path(sys.argv[2] if len(sys.argv) > 2 else "docs/capability/RESULTS.md")
    site = Path(sys.argv[3] if len(sys.argv) > 3 else "docs/data/capability.json")
    data = load(results)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render(data), encoding="utf-8", newline="\n")
    site.parent.mkdir(parents=True, exist_ok=True)
    site.write_text(
        json.dumps(site_matrix(data), ensure_ascii=False, indent=1) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"wrote {path} and {site} from {', '.join(sorted(data))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
