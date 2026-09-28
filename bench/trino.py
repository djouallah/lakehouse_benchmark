"""Trino against OneLake: the container, the connection, the catalog. Shared by the query engine
(bench/tpch/engines/trino_iceberg.py) and the smoke test's local phase.

A SERVER, NOT A LIBRARY, like StarRocks: the official image runs one JVM that is both coordinator
and worker, and Python talks to it over Trino's HTTP protocol. `start()` brings the container up
and waits until the coordinator answers a query -- it says SERVER_STARTING_UP until it has
registered itself as a worker. The workflow pulls the image before the engine is timed.

HOW IT READS ONELAKE, each line found by a failed candidate_engine.yml run (2026-09-27):

* CATALOG: created in SQL (`catalog.management=dynamic`), the REST catalog with the bearer as a
  fixed `oauth2.token`. It cannot be refreshed in place, so `attach()` re-creates the catalog on a
  fresh bearer between statements when it runs low (`needs_refresh`), as StarRocks does.
* CASE: Trino folds identifiers to lower case and OneLake's namespaces are CH0010;
  `case-insensitive-name-matching` maps `ch0010` back. Its mapping cache lives
  CATALOG_CACHE_SECONDS, like every engine's catalog cache.
* STORAGE: Trino's native Azure filesystem with `azure.auth-type=DEFAULT` -- the Azure SDK's
  DefaultAzureCredential, whose workload-identity leg reads AZURE_FEDERATED_TOKEN_FILE: the GitHub
  OIDC assertion, mounted and rewritten every 4 minutes, so it refreshes for any run length.
* `azure.endpoint=fabric.microsoft.com`, WITHOUT WHICH NO DATA FILE OPENS. The filesystem checks
  each location's host against `<account>.dfs.<endpoint>`, default `core.windows.net`, and
  OneLake's host is onelake.dfs.fabric.microsoft.com: "Location does not match configured Azure
  endpoint" (run 36326766643). The catalog attaches and lists either way.

MEMORY. The image sizes the heap at 80% of the container's 15 GB: 12 GB. Queries may use 9 GB of
it, and 2 GB stays as headroom for everything else in the JVM. Spill is off by default in Trino,
as in StarRocks, and on here: a join or aggregation that outgrows its 9 GB spills rather than
failing, as it does in DuckDB, Gluten and StarRocks.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path

from bench import auth, scrub
from bench.config import (
    CATALOG_CACHE_SECONDS,
    ICEBERG_ENDPOINT,
    TOKEN_MIN_LIFETIME_SECONDS,
    Config,
)

IMAGE = os.environ.get("TRINO_IMAGE", "trinodb/trino:483")
CONTAINER = "trino"
CATALOG = "onelake"
PORT = 8080
# The container's memory ceiling, of the runner's 15.6 GB; the same cgroup as StarRocks'.
CONTAINER_MEMORY = "15g"

CONFIG = "\n".join(
    [
        "coordinator=true",
        "node-scheduler.include-coordinator=true",
        f"http-server.http.port={PORT}",
        f"discovery.uri=http://localhost:{PORT}",
        "catalog.management=dynamic",
        "catalog.store=memory",
        "query.max-memory=9GB",
        "query.max-memory-per-node=9GB",
        "memory.heap-headroom-per-node=2GB",
        "spill-enabled=true",
        "spiller-spill-path=/tmp/trino-spill",
    ]
)

ASSERTION_DIR = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "trino-oidc"
ASSERTION_IN_CONTAINER = "/var/run/trino-oidc/assertion"
ASSERTION_REFRESH_S = 240

_refresher: threading.Thread | None = None


def _write_assertion() -> None:
    target = ASSERTION_DIR / "assertion"
    target.write_text(auth._github_oidc_assertion(), encoding="utf-8")
    target.chmod(0o644)  # the container's user is not the runner's


def _keep_assertion_fresh() -> None:
    """Write the OIDC assertion now and every 4 minutes; it lives about five.

    Only in a job that can mint one (`id-token: write`). The smoke test's local phase has no
    credentials by design and reads mounted parquet, so it runs with the directory empty.
    """
    global _refresher
    ASSERTION_DIR.mkdir(parents=True, exist_ok=True)
    if not os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL"):
        return
    _write_assertion()
    if _refresher is not None:
        return

    def loop() -> None:
        while True:
            time.sleep(ASSERTION_REFRESH_S)
            try:
                _write_assertion()
            except Exception as exc:  # noqa: BLE001 - a failed refresh must not kill the run
                scrub.safe_print(f"  warning: assertion refresh failed: {exc}")

    _refresher = threading.Thread(target=loop, daemon=True)
    _refresher.start()


def _running() -> bool:
    out = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER],
        capture_output=True,
        text=True,
        check=False,
    )
    return out.stdout.strip() == "true"


def start(timeout_s: int = 300, mounts: dict[Path, str] | None = None) -> None:
    """The container, up and answering queries. Idempotent: a running container is reused.

    `mounts` maps host directories to WRITABLE paths in the container -- the smoke test's Hive
    file metastore lives beside its parquet.
    """
    _keep_assertion_fresh()
    if not _running():
        subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)
        config = ASSERTION_DIR.parent / "trino-config.properties"
        config.write_text(CONFIG + "\n", encoding="utf-8")
        config.chmod(0o644)
        volumes = [
            f"{ASSERTION_DIR}:{Path(ASSERTION_IN_CONTAINER).parent}:ro",
            f"{config}:/etc/trino/config.properties:ro",
            *[f"{host}:{inside}" for host, inside in (mounts or {}).items()],
        ]
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                CONTAINER,
                *[arg for volume in volumes for arg in ("-v", volume)],
                "-e",
                f"AZURE_CLIENT_ID={os.environ.get('AZURE_CLIENT_ID', '')}",
                "-e",
                f"AZURE_TENANT_ID={os.environ.get('AZURE_TENANT_ID', '')}",
                "-e",
                f"AZURE_FEDERATED_TOKEN_FILE={ASSERTION_IN_CONTAINER}",
                # A ceiling under the runner's RAM, as for StarRocks: past it the kernel kills a
                # process in the container and the statement fails, rather than the runner dying.
                "--memory",
                CONTAINER_MEMORY,
                "--memory-swap",
                CONTAINER_MEMORY,
                "-p",
                f"127.0.0.1:{PORT}:{PORT}",
                IMAGE,
            ],
            check=True,
            capture_output=True,
        )
    deadline = time.time() + timeout_s
    while True:
        try:
            conn = connect()
            sql(conn, "SELECT 1")
            conn.close()
            return
        except Exception:  # noqa: BLE001 - not up yet
            pass
        if time.time() > deadline:
            raise RuntimeError(f"Trino not answering after {timeout_s}s")
        time.sleep(3)


def connect(schema: str | None = None):
    import trino

    return trino.dbapi.connect(
        host="127.0.0.1",
        port=PORT,
        user="bench",
        catalog=CATALOG,
        schema=schema,
        request_timeout=None,
    )


def sql(conn, statement: str) -> list[tuple]:
    cur = conn.cursor()
    cur.execute(statement)
    return [tuple(row) for row in cur.fetchall()]


def version(conn) -> str:
    return str(sql(conn, "SELECT version()")[0][0])


def storage_properties() -> dict[str, str]:
    """Trino's native Azure filesystem on OneLake (see the module docstring)."""
    return {
        "fs.native-azure.enabled": "true",
        "azure.auth-type": "DEFAULT",
        "azure.endpoint": "fabric.microsoft.com",
    }


def create_catalog(conn, name: str, connector: str, properties: dict[str, str]) -> None:
    body = ", ".join(f"\"{key}\" = '{value}'" for key, value in properties.items())
    sql(conn, f"DROP CATALOG IF EXISTS {name}")
    sql(conn, f"CREATE CATALOG {name} USING {connector} WITH ({body})")


def attach(conn, cfg: Config, token: str) -> None:
    """(Re)create the OneLake catalog on `token`."""
    create_catalog(
        conn,
        CATALOG,
        "iceberg",
        {
            "iceberg.catalog.type": "rest",
            "iceberg.rest-catalog.uri": ICEBERG_ENDPOINT,
            "iceberg.rest-catalog.warehouse": cfg.warehouse,
            "iceberg.rest-catalog.security": "OAUTH2",
            "iceberg.rest-catalog.oauth2.token": token,
            "iceberg.rest-catalog.vended-credentials-enabled": "false",
            "iceberg.rest-catalog.case-insensitive-name-matching": "true",
            "iceberg.rest-catalog.case-insensitive-name-matching.cache-ttl": (
                f"{CATALOG_CACHE_SECONDS}s"
            ),
            **storage_properties(),
        },
    )


def needs_refresh(expires: float) -> bool:
    return expires - time.time() < TOKEN_MIN_LIFETIME_SECONDS
