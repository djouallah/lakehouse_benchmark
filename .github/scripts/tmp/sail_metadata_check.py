"""TEMPORARY, read-only: why DuckDB 2.0.1 refuses every Sail workload table with "Iceberg metadata
has a negative 'last-partition-id'" (table maintenance, since 44826936006 on 2026-11-20).

For each table of the Sail leg, and one DuckDB leg table to compare: the REST catalog's loadTable
answer (last-partition-id, partition specs, format version), then the metadata JSON files
themselves, the current one and its metadata-log, read from OneLake, to show whether the stored
file carries the negative value or only the REST answer does, and since which metadata version.
Writes nothing. Deleted once the cause is reported."""

from __future__ import annotations

import json
import urllib.parse
import urllib.request

from bench import auth
from bench.config import ICEBERG_ENDPOINT, ONELAKE_DFS
from bench.scrub import safe_print
from bench.workload.config import WorkloadConfig

cfg = WorkloadConfig.from_env()
token = auth.onelake_token()


def get(url: str) -> bytes:
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {token}", "x-ms-version": "2023-11-03"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def rest(path: str, **query: str) -> dict:
    q = f"?{urllib.parse.urlencode(query)}" if query else ""
    return json.loads(get(f"{ICEBERG_ENDPOINT}/v1/{path}{q}"))


def stored(location: str) -> dict:
    """A metadata JSON file, abfss://ws@onelake.dfs.../lh/... read over HTTPS."""
    parsed = urllib.parse.urlparse(location)
    ws = parsed.netloc.split("@", 1)[0]
    return json.loads(get(f"https://{ONELAKE_DFS}/{ws}{parsed.path}"))


def summary(meta: dict) -> dict:
    snap = next(
        (
            s
            for s in meta.get("snapshots", [])
            if s["snapshot-id"] == meta.get("current-snapshot-id")
        ),
        {},
    )
    return {
        "format-version": meta.get("format-version"),
        "last-partition-id": meta.get("last-partition-id"),
        "default-spec-id": meta.get("default-spec-id"),
        "partition-specs": meta.get("partition-specs"),
        "last-column-id": meta.get("last-column-id"),
        "last-updated-ms": meta.get("last-updated-ms"),
        "snapshot-summary": snap.get("summary"),
        "properties": meta.get("properties"),
    }


prefix = rest("config", warehouse=cfg.warehouse).get("overrides", {}).get("prefix", "")
base = f"{prefix}/" if prefix else ""
namespaces = [ns[0] for ns in rest(f"{base}namespaces")["namespaces"]]
sail = [ns for ns in namespaces if ns.startswith("dbt_sail")]
safe_print("namespaces:", namespaces)

targets = []
for ns in sail:
    for ident in rest(f"{base}namespaces/{ns}/tables")["identifiers"]:
        targets.append((ns, ident["name"]))
targets.append(("dbt_duckdb_mart", "fct_summary"))  # a leg DuckDB 2.0.1 reads fine

for ns, name in targets:
    safe_print(f"\n===== {ns}.{name}")
    try:
        loaded = rest(f"{base}namespaces/{ns}/tables/{name}")
    except Exception as e:  # noqa: BLE001
        safe_print("loadTable failed:", type(e).__name__, e)
        continue
    meta = loaded["metadata"]
    safe_print("REST loadTable:", json.dumps(summary(meta), indent=1))
    location = loaded.get("metadata-location")
    if not location:
        safe_print("no metadata-location")
        continue
    try:
        safe_print(
            "stored current file last-partition-id:", stored(location).get("last-partition-id")
        )
    except Exception as e:  # noqa: BLE001
        safe_print("current file read failed:", type(e).__name__, e)
    log = meta.get("metadata-log", [])
    safe_print(f"metadata-log: {len(log)} entries")
    # The first, the last 5, and a spread between, to see where the value changed.
    picks = sorted(
        {0, *range(max(0, len(log) - 5), len(log)), *range(0, len(log), max(1, len(log) // 10))}
    )
    for i in picks:
        entry = log[i]
        try:
            old = stored(entry["metadata-file"])
            safe_print(
                f"  log[{i}] ts={entry['timestamp-ms']}"
                f" last-partition-id={old.get('last-partition-id')}"
                f" specs={json.dumps(old.get('partition-specs'))}"
            )
        except Exception as e:  # noqa: BLE001
            safe_print(f"  log[{i}] read failed:", type(e).__name__, e)
