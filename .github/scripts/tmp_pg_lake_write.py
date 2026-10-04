"""TEMPORARY (tmp-pg-lake-write.yml): what exactly fails when pg_lake writes Iceberg to OneLake.

1. `CREATE TABLE ... USING iceberg` on an abfss:// location fails with "404 The specified path does
   not exist". pg_lake first runs `pg_lake_list_files('<location>/**')` (ErrorIfLocationIsNotEmpty,
   pg_lake_table/src/ddl/create_table.c) on a location that does not exist yet. Glob it on dfs and
   on blob, a missing folder and an existing one.
2. The same CTAS on an az:// location succeeds and OneLake picks the table up, but pyiceberg then
   fails "Unrecognized filesystem type in URI: https". Print the paths pg_lake wrote and the ones
   the REST catalog serves.

Delete once read.
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.parse
import urllib.request

from bench import auth, onelake, pg_lake, scrub
from bench.config import ICEBERG_ENDPOINT, ONELAKE_BLOB, ONELAKE_DFS
from bench.suite import suite_class

NS = "candidate"
TABLE = "pg_lake_paths"


def say(text: object) -> None:
    print(scrub.scrub(str(text)), flush=True)


def duck(statement: str) -> None:
    """Through psql inside the pgduck container: psycopg's C extension segfaulted the Python
    process on the first blob listing in run 37187539188. These statements carry no token."""
    out = subprocess.run(
        ["docker", "exec", pg_lake.DUCK_CONTAINER, "psql", "-h", pg_lake.INSIDE[pg_lake.SOCKET_DIR],
         "-p", str(pg_lake.DUCK_PORT), "-U", "postgres", "-At", "-c", statement],
        capture_output=True,
        text=True,
        check=False,
    )  # fmt: skip
    status = "PASS" if out.returncode == 0 else "FAIL"
    say(f"{status}  {statement}\n      -> {(out.stdout + out.stderr).strip()[:800]}")


def rest(path: str, token: str) -> dict:
    request = urllib.request.Request(
        f"{ICEBERG_ENDPOINT}{path}", headers={"Authorization": f"Bearer {token}"}
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read())


def clear(cfg) -> None:
    catalog = auth.catalog(cfg)
    try:
        if catalog.table_exists(f"{NS}.{TABLE}"):
            catalog.drop_table(f"{NS}.{TABLE}")
    except Exception as exc:  # noqa: BLE001
        say(f"  catalog drop: {scrub.scrub_exc(exc, 300)}")
    directory = onelake.file_system(cfg).get_directory_client(
        f"{cfg.lakehouse_id}/Tables/{NS}/{TABLE}"
    )
    if directory.exists():
        directory.delete_directory()


def main() -> None:
    cfg = suite_class("tpch").from_env()
    ws, lh = cfg.workspace_id, cfg.lakehouse_id
    pg_lake.start()
    token = auth.onelake_token()
    pg_lake.storage_secret(token)

    say("\n[1] the listing pg_lake runs before CREATE TABLE, on dfs and on blob")
    roots = {"dfs": f"abfss://{ws}@{ONELAKE_DFS}/{lh}", "blob": f"az://{ONELAKE_BLOB}/{ws}/{lh}"}
    for endpoint, root in roots.items():
        say(f"-- {endpoint}")
        duck(f"SELECT * FROM pg_lake_list_files('{root}/Tables/{NS}/no_such_table/**')")
        duck(f"SELECT * FROM glob('{root}/Tables/{NS}/no_such_table/**')")
        duck(f"SELECT count(*) FROM glob('{root}/Tables/{cfg.schema}/nation/**')")

    say("\n[2] az:// CTAS: what pg_lake writes, what the catalog serves")
    clear(cfg)
    conn = pg_lake.connect()
    location = f"az://{ONELAKE_BLOB}/{ws}/{lh}/Tables/{NS}/{TABLE}"
    pg_lake.sql(conn, f"CREATE SCHEMA IF NOT EXISTS {NS}")
    pg_lake.sql(conn, f"DROP TABLE IF EXISTS {NS}.{TABLE}")
    pg_lake.sql(
        conn,
        f"CREATE TABLE {NS}.{TABLE} USING iceberg WITH (location = '{location}') "
        "AS SELECT g AS id FROM generate_series(1, 25) g",
    )
    written = pg_lake.sql(
        conn,
        "SELECT metadata_location FROM iceberg_tables "
        f"WHERE table_namespace = '{NS}' AND table_name = '{TABLE}'",
    )[0][0]
    say(f"pg_lake metadata_location: {written}")
    metadata = json.loads(pg_lake.sql(conn, f"SELECT lake_iceberg.metadata('{written}')")[0][0])
    say(f"pg_lake metadata.location: {metadata.get('location')}")
    for snapshot in metadata.get("snapshots", []):
        say(f"pg_lake manifest-list: {snapshot.get('manifest-list')}")
    try:
        files = pg_lake.sql(conn, f"SELECT * FROM lake_iceberg.files('{written}') LIMIT 3")
        say(f"pg_lake data files: {files}")
    except Exception as exc:  # noqa: BLE001
        say(f"lake_iceberg.files: {scrub.scrub_exc(exc, 400)}")

    config = rest(f"/v1/config?warehouse={urllib.parse.quote(cfg.warehouse, safe='')}", token)
    prefix = (config.get("overrides") or {}).get("prefix") or cfg.warehouse
    say(f"catalog prefix: {prefix}")
    path = f"/v1/{urllib.parse.quote(prefix, safe='')}/namespaces/{NS}/tables/{TABLE}"
    served = None
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            served = rest(path, token)
            break
        except urllib.error.HTTPError as exc:
            say(f"  loadTable: HTTP {exc.code}, waiting for OneLake to pick the table up")
            time.sleep(20)
    if served:
        meta = served.get("metadata", {})
        say(f"served metadata-location: {served.get('metadata-location')}")
        say(f"served metadata.location: {meta.get('location')}")
        for snapshot in meta.get("snapshots", []):
            say(f"served manifest-list: {snapshot.get('manifest-list')}")
        say(f"served config: {sorted((served.get('config') or {}).keys())}")
    try:
        table = auth.catalog(cfg).load_table(f"{NS}.{TABLE}")
        say(f"pyiceberg rows: {table.scan().to_arrow().num_rows}")
    except Exception as exc:  # noqa: BLE001
        say(f"pyiceberg: {type(exc).__name__}: {scrub.scrub_exc(exc, 600)}")

    pg_lake.sql(conn, f"DROP TABLE IF EXISTS {NS}.{TABLE}")
    clear(cfg)


if __name__ == "__main__":
    main()
