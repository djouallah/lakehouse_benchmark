"""Databend against OneLake: the container, the connection, the catalog. Shared by the query engine
(bench/tpch/engines/databend_iceberg.py) and the smoke test.

A SERVER, NOT A LIBRARY, like StarRocks: the all-in-one image runs databend-meta and databend-query
in one container, a Rust engine end to end, and Python talks to it over the MySQL protocol on 3307.
The image is pulled by the workflow before the engine is timed, so the setup row measures a cold
start, not a download.

THE IMAGE IS OUR BUILD OF MAIN, for now. Added 2026-10-17 after a build of databendlabs/databend
main passed candidate_engine.yml (TPC-H 22/22, Files CSV, Iceberg write; run 39015860524). The
decimal fix it needs (databend#20594, closes #20588: FIXED_LEN_BYTE_ARRAY decimals failed with
"Not enough bytes to decode", TPC-H 5/22) is on main only; v1.2.952-nightly lacks it. So
databend_build.yml builds databend-query with Databend's own release profile and databend_image.yml
copies it into the v1.2.952-nightly image, in this repo's GHCR. Once a nightly carries the fix,
IMAGE becomes that nightly and both workflows go.

HOW IT READS ONELAKE (candidate_engine.py, Databend):

* CATALOG: Databend's fork of iceberg-rust, the REST catalog with the bearer as a fixed `token`.
  It cannot be refreshed in place, so `attach()` is re-run on a fresh bearer between statements
  when it runs low (`needs_refresh`), as for StarRocks and Trino.
* STORAGE: opendal's Azdls service, with a user delegation SAS (`adls.sas-token`) on the lakehouse
  directory. The workload identity in the AZURE_* environment does NOT reach Azdls -- its signer
  goes to IMDS and fails (run 39015860524) -- so the SAS is the credential. It lives about 55
  minutes, so it is renewed with the bearer: `needs_refresh` watches whichever expires first.
  Opendal's own `filesystem` and `endpoint` keys pass through raw and point Azdls at OneLake.
* IDENTIFIERS: unquoted identifiers fold to lower case by default and OneLake's namespace is
  CH0010, so `unquoted_ident_case_sensitive` is set GLOBAL at start-up.

SPILL IS DATABEND'S DEFAULT: `query_out_of_memory_behavior` is `spilling`, and joins,
aggregations, sorts and windows spill past 60% of the query's memory. `max_threads` defaults to the
core count, 4 here, what every other engine is told. `start()` logs them, so a result records what
it ran with.

BUT THE MEMORY CEILING IS OURS. Databend does not read the cgroup: it sized itself from the
runner's RAM ("Memory: limit: unlimited", max_memory_usage 13.4 GB) inside a 15 GB container, and
TPC-H SF=100 Q4 lost the server mid-query (run 39025777931) instead of spilling. SERVER_MEMORY
caps the process at 12 GiB -- `max_server_memory_usage`, enforced only with
`max_memory_limit_enabled` -- and the per-query defaults follow from it (half each, spill at 60%).
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path

from bench import auth, scrub
from bench.config import ICEBERG_ENDPOINT, ONELAKE_DFS, TOKEN_MIN_LIFETIME_SECONDS, Config

IMAGE = os.environ.get("DATABEND_IMAGE", "ghcr.io/djouallah/databend:main-8b1dc20714")
CONTAINER = "databend"
CATALOG = "onelake"
PORT = 3307
USER = "databend"
# The container's memory ceiling, of the runner's 15.6 GB; the cgroup StarRocks and Trino get.
CONTAINER_MEMORY = "15g"
# Databend's own process ceiling, under the container's, with room for databend-meta (docstring).
SERVER_MEMORY = 12 * 1024**3
# What start() reads back and logs: the settings that decide memory, spill and parallelism.
LOGGED_SETTINGS = (
    "max_threads",
    "max_memory_usage",
    "max_query_memory_usage",
    "query_out_of_memory_behavior",
    "join_spilling_memory_ratio",
    "aggregate_spilling_memory_ratio",
    "sort_spilling_memory_ratio",
)

ASSERTION_DIR = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "databend-oidc"
ASSERTION_IN_CONTAINER = "/var/run/databend-oidc/assertion"
ASSERTION_REFRESH_S = 240

_refresher: threading.Thread | None = None


def _write_assertion() -> None:
    target = ASSERTION_DIR / "assertion"
    target.write_text(auth._github_oidc_assertion(), encoding="utf-8")
    target.chmod(0o644)  # the container's user is not the runner's


def _keep_assertion_fresh() -> None:
    """Write the OIDC assertion now and every 4 minutes, for the AZURE_* workload identity.

    The Iceberg reads use the SAS; this is for Databend's azblob reader (raw files), which does
    take the environment credential. Only in a job that can mint one (`id-token: write`).
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
    """The container, up and answering. Idempotent: a running container is reused.

    `mounts` maps host directories to read-only paths in the container, for `fs://` reads.
    """
    _keep_assertion_fresh()
    if not _running():
        subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True, check=False)
        volumes = {ASSERTION_DIR: str(Path(ASSERTION_IN_CONTAINER).parent), **(mounts or {})}
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                CONTAINER,
                *[arg for host, inside in volumes.items() for arg in ("-v", f"{host}:{inside}:ro")],
                # A ceiling under the runner's RAM, as for StarRocks: past it the kernel kills a
                # process in the container, the statement fails with a reason and the job lives.
                "--memory",
                CONTAINER_MEMORY,
                "--memory-swap",
                CONTAINER_MEMORY,
                "-e",
                f"QUERY_MAX_SERVER_MEMORY_USAGE={SERVER_MEMORY}",
                "-e",
                "QUERY_MAX_MEMORY_LIMIT_ENABLED=true",
                "-e",
                f"QUERY_DEFAULT_USER={USER}",
                "-e",
                f"QUERY_DEFAULT_PASSWORD={USER}",
                "-e",
                f"AZURE_CLIENT_ID={os.environ.get('AZURE_CLIENT_ID', '')}",
                "-e",
                f"AZURE_TENANT_ID={os.environ.get('AZURE_TENANT_ID', '')}",
                "-e",
                f"AZURE_FEDERATED_TOKEN_FILE={ASSERTION_IN_CONTAINER}",
                # `fs://` is "insecure storage" to Databend, refused unless `storage.allow_insecure`
                # (serfig reads it from the environment). Only for the smoke test's mounted parquet.
                *(["-e", "STORAGE_ALLOW_INSECURE=true"] if mounts else []),
                "-p",
                f"127.0.0.1:{PORT}:3307",
                IMAGE,
            ],
            check=True,
            capture_output=True,
        )
    deadline = time.time() + timeout_s
    while True:
        try:
            conn = connect()
            with conn.cursor() as cur:
                # GLOBAL, so every later connection folds no case either.
                cur.execute("SET GLOBAL unquoted_ident_case_sensitive = 1")
            conn.close()
            break
        except Exception:  # noqa: BLE001 - not up yet
            pass
        if time.time() > deadline:
            raise RuntimeError(f"Databend not answering after {timeout_s}s")
        time.sleep(3)
    conn = connect()
    try:
        scrub.safe_print(f"  databend settings: {settings(conn)}")
    finally:
        conn.close()


def connect():
    import pymysql

    return pymysql.connect(
        host="127.0.0.1", port=PORT, user=USER, password=USER, autocommit=True, read_timeout=None
    )


def version(conn) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        return str(cur.fetchone()[0])


def settings(conn) -> str:
    """LOGGED_SETTINGS as `name=value`, read back from the server."""
    names = ", ".join(f"'{n}'" for n in LOGGED_SETTINGS)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT name, value FROM system.settings WHERE name IN ({names}) ORDER BY name"
        )
        return " ".join(f"{name}={value}" for name, value in cur.fetchall())


def sas(cfg: Config, write: bool = False) -> tuple[str, float]:
    """A user delegation SAS on the lakehouse directory and its expiry (module docstring)."""
    from bench.tpch.engines.pyspark_gluten_iceberg import onelake_sas

    return onelake_sas(cfg.workspace_id, cfg.lakehouse_id, write=write)


def attach(conn, cfg: Config, token: str, sas_token: str) -> None:
    """(Re)create the OneLake catalog on `token` and `sas_token` and make it the current catalog."""
    props = {
        "token": token,
        "filesystem": cfg.workspace_id,
        "endpoint": f"https://{ONELAKE_DFS}",
        "adls.sas-token": sas_token,
    }
    body = " ".join(f"\"{k}\"='{v}'" for k, v in props.items())
    with conn.cursor() as cur:
        cur.execute(f"DROP CATALOG IF EXISTS {CATALOG}")
        cur.execute(
            f"CREATE CATALOG {CATALOG} TYPE=ICEBERG CONNECTION=(TYPE='rest' "
            f"ADDRESS='{ICEBERG_ENDPOINT}' WAREHOUSE='{cfg.warehouse}' {body})"
        )
        cur.execute(f"USE CATALOG {CATALOG}")


def needs_refresh(expires: float) -> bool:
    return expires - time.time() < TOKEN_MIN_LIFETIME_SECONDS
