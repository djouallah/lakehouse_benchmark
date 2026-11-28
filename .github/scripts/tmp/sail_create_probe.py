"""TEMPORARY: where the last-partition-id -1 on every Sail table comes from (DuckDB 2.0.1 refuses
it; every other leg's tables carry 999 -- tmp/sail_metadata_check.py, run 46092514546).

Sail main (86da637b4f) still creates tables with -1 (run 46094202665, CREATE + INSERT and CTAS).
Its createTable request leaves `partition-spec` out for an unpartitioned table, where pyiceberg and
Spark send an empty spec. This sends OneLake's REST catalog the same createTable twice, raw: with
`partition-spec: {"spec-id": 0, "fields": []}` and without it, reads each table's
last-partition-id, and drops both. Deleted once reported."""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request

from bench import auth
from bench.config import ICEBERG_ENDPOINT, Config
from bench.scrub import safe_print

cfg = Config.from_env()
token = auth.onelake_token()
NS = "_bench_probe"
RUN = os.environ.get("GITHUB_RUN_ID", "local")


def call(method: str, url: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
    return json.loads(raw) if raw else {}


q = urllib.parse.quote(cfg.warehouse)
prefix = call("GET", f"{ICEBERG_ENDPOINT}/v1/config?warehouse={q}").get("overrides", {})
prefix = prefix.get("prefix", "")
tables = f"{ICEBERG_ENDPOINT}/v1/{prefix + '/' if prefix else ''}namespaces/{NS}/tables"
schema = {
    "type": "struct",
    "schema-id": 0,
    "fields": [
        {"id": 1, "name": "id", "required": False, "type": "int"},
        {"id": 2, "name": "s", "required": False, "type": "string"},
    ],
}
cases = {
    f"lpid_{RUN}_with_spec": {"partition-spec": {"spec-id": 0, "fields": []}},
    f"lpid_{RUN}_no_spec": {},
}
try:
    for name, extra in cases.items():
        body = {"name": name, "schema": schema, "stage-create": False, **extra}
        meta = call("POST", tables, body)["metadata"]
        safe_print(
            f"{'with' if extra else 'without'} partition-spec:"
            f" last-partition-id={meta.get('last-partition-id')}"
            f" specs={json.dumps(meta.get('partition-specs'))}"
        )
finally:
    for name in cases:
        try:
            call("DELETE", f"{tables}/{name}?purgeRequested=true")
        except Exception as e:  # noqa: BLE001
            safe_print(f"drop {name} failed:", type(e).__name__, e)
