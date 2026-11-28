"""TEMPORARY: does Sail main (86da637b4f) still create an Iceberg table whose metadata has
last-partition-id -1, which DuckDB 2.0.1 refuses? Every Sail workload table carries -1 since it
was created on 2026-10-21/22; every other leg's carries 999 (tmp/sail_metadata_check.py, run
46092514546).

Creates three small unpartitioned tables in _bench_probe the ways the dbt-sail leg can (CREATE +
INSERT, CTAS, CREATE OR REPLACE AS on an existing table), reads each one's last-partition-id from
the REST catalog and from the stored metadata file, and drops them. Deleted once reported."""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request

from bench import auth
from bench.capability.engines.lakesail_iceberg import LakesailIceberg
from bench.config import ICEBERG_ENDPOINT, ONELAKE_DFS, Config
from bench.scrub import safe_print

cfg = Config.from_env()
token = auth.onelake_token()
NS = "_bench_probe"
RUN = os.environ.get("GITHUB_RUN_ID", "local")


def get(url: str) -> bytes:
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {token}", "x-ms-version": "2023-11-03"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


prefix = (
    json.loads(get(f"{ICEBERG_ENDPOINT}/v1/config?warehouse={urllib.parse.quote(cfg.warehouse)}"))
    .get("overrides", {})
    .get("prefix", "")
)
base = f"{ICEBERG_ENDPOINT}/v1/{prefix}/" if prefix else f"{ICEBERG_ENDPOINT}/v1/"


def report(table: str, how: str) -> None:
    loaded = json.loads(get(f"{base}namespaces/{NS}/tables/{table}"))
    meta = loaded["metadata"]
    parsed = urllib.parse.urlparse(loaded["metadata-location"])
    ws = parsed.netloc.split("@", 1)[0]
    on_disk = json.loads(get(f"https://{ONELAKE_DFS}/{ws}{parsed.path}"))
    safe_print(
        f"{how}: REST last-partition-id={meta.get('last-partition-id')}"
        f" stored={on_disk.get('last-partition-id')}"
        f" specs={json.dumps(meta.get('partition-specs'))}"
        f" format-version={meta.get('format-version')}"
    )


engine = LakesailIceberg(cfg)
engine.setup()
spark = engine.session
names = [f"sail_lpid_{RUN}_{i}" for i in range(3)]
try:
    a, b, c = (f"onelake.{NS}.{n}" for n in names)
    spark.sql(f"CREATE TABLE {a} (id INT, s STRING) USING iceberg").collect()
    spark.sql(f"INSERT INTO {a} VALUES (1, 'x')").collect()
    report(names[0], "CREATE TABLE + INSERT")

    spark.sql(f"CREATE TABLE {b} USING iceberg AS SELECT 1 AS id, 'x' AS s").collect()
    report(names[1], "CTAS")

    spark.sql(f"CREATE TABLE {c} (id INT, s STRING) USING iceberg").collect()
    spark.sql(f"CREATE OR REPLACE TABLE {c} USING iceberg AS SELECT 2 AS id, 'y' AS s").collect()
    report(names[2], "CREATE, then CREATE OR REPLACE AS")
finally:
    for n in names:
        try:
            spark.sql(f"DROP TABLE IF EXISTS onelake.{NS}.{n}").collect()
        except Exception as e:  # noqa: BLE001
            safe_print(f"drop {n} failed:", type(e).__name__, e)
    engine.close()
