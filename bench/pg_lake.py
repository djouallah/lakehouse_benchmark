"""pg_lake against OneLake: the two containers, the token shim, the catalog. Shared by the query
engine and the candidate probe.

TWO SERVERS. Postgres with the pg_lake extensions plans every statement and hands what it can
vectorise to pgduck_server, a DuckDB process that speaks the Postgres protocol on a Unix socket.
Both images come from .github/workflows/pg_lake_image.yml (pg_lake publishes none). The socket
directory and Postgres' temp directory are host directories mounted into both containers, as
upstream's docker-compose shares them, and Python reaches pgduck_server through the same socket.

CATALOG AUTH: A TOKEN SHIM. pg_lake (3.5) talks to a REST catalog only through the OAuth2
client-credentials grant -- a Basic-auth POST of client_id:client_secret to `oauth_endpoint` -- and
has no static-bearer option (Snowflake-Labs/pg_lake#209). The bench has no client secret, only
workload identity. So `oauth_endpoint` points at a small HTTP server in this process that answers
every grant with the bench's own storage bearer (`auth.onelake_token`); the user mapping's id and
secret are placeholders it never checks. pg_lake re-runs the grant when the token nears expiry or
the catalog answers 401, so the catalog side refreshes itself, for any run length.

STORAGE AUTH is a DuckDB Azure secret inside pgduck_server, `PROVIDER access_token`, replaced in
place when the token turns over -- what bench/tpch/engines/duckdb_iceberg.py does in its own
process. OneLake's host is not in pg_lake's default Azure allowlist, so
`pg_lake.allowed_azure_host_suffixes` adds it.

THE FILE CACHE, every engine's where it has one. pgduck_server keeps what it reads in `--cache_dir`
and pg_lake's cache manager bounds it with `pg_lake_engine.max_cache_size`; sized like Velox's and
Trino's, half the free disk at start, since spill writes to the same disk.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from bench import auth, scrub
from bench.config import ONELAKE_DFS, TOKEN_MIN_LIFETIME_SECONDS

TAG = "v3.5.3-pg18"
IMAGE_PG = os.environ.get("PG_LAKE_IMAGE", f"ghcr.io/djouallah/pg_lake_postgres:{TAG}")
IMAGE_DUCK = os.environ.get("PGDUCK_IMAGE", f"ghcr.io/djouallah/pgduck_server:{TAG}")
PG_CONTAINER = "pg_lake"
DUCK_CONTAINER = "pgduck_server"
PG_MAJOR = "18"
PORT = 5432
DUCK_PORT = 5332
# The Postgres database. Named after the catalog so three-part names (`onelake.CH0010.nation`)
# resolve: Postgres accepts a database qualifier when it names the current database.
DATABASE = "onelake"
# The iceberg_catalog server for OneLake's REST endpoint.
SERVER = "onelake"
# pgduck_server's memory, of the runner's 15.6 GB; the container ceiling sits above it, under
# the runner, as for Trino and StarRocks. Postgres itself only plans and stitches results.
DUCK_MEMORY_LIMIT = "11GB"
DUCK_CONTAINER_MEMORY = "13g"

HOME = "/home/postgres"
ROOT = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "pg_lake"
SOCKET_DIR = ROOT / "socket"
TMP_DIR = ROOT / "pgsql_tmp"
CACHE_DIR = ROOT / "cache"
SPILL_DIR = ROOT / "spill"
INSIDE = {
    SOCKET_DIR: f"{HOME}/pgduck_socket_dir",
    TMP_DIR: f"{HOME}/pgsql-{PG_MAJOR}/data/base/pgsql_tmp",
    CACHE_DIR: f"{HOME}/cache",
    SPILL_DIR: f"{HOME}/spill",
}
CACHE_FREE_FRACTION = 0.5
CACHE_MIN_GIB = 8

# Upstream's default allowlist plus OneLake's dfs and blob hosts.
AZURE_HOST_SUFFIXES = ",".join(
    [
        ".dfs.core.windows.net",
        ".blob.core.windows.net",
        "." + ONELAKE_DFS.split(".", 1)[1],
        ".blob.fabric.microsoft.com",
    ]
)


# --- the token shim -------------------------------------------------------------------------


class _Grant(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - http.server's name
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        token = auth.onelake_token(skew=TOKEN_MIN_LIFETIME_SECONDS)
        expires_in = max(60, int(auth.token_expires_on() - time.time()))
        body = json.dumps(
            {"access_token": token, "token_type": "bearer", "expires_in": expires_in}
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:  # the request line is all it would say
        pass


_shim: ThreadingHTTPServer | None = None


def token_endpoint() -> str:
    """The shim's URL, started on first call. Postgres runs on the host network, so loopback."""
    global _shim
    if _shim is None:
        _shim = ThreadingHTTPServer(("127.0.0.1", 0), _Grant)
        threading.Thread(target=_shim.serve_forever, daemon=True).start()
    return f"http://127.0.0.1:{_shim.server_address[1]}/oauth/tokens"


# --- the containers -------------------------------------------------------------------------


def _running(name: str) -> bool:
    out = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        capture_output=True,
        text=True,
        check=False,
    )
    return out.stdout.strip() == "true"


def cache_size_mb() -> int:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    free_gib = shutil.disk_usage(CACHE_DIR).free / 2**30
    size_gib = max(CACHE_MIN_GIB, int(free_gib * CACHE_FREE_FRACTION))
    scrub.safe_print(f"  pg_lake file cache {size_gib}GB ({free_gib:.0f}GB free) at {CACHE_DIR}")
    return size_gib * 1024


def _volumes(mounts: dict[Path, str] | None) -> list[str]:
    pairs = {**{host: inside for host, inside in INSIDE.items()}, **(mounts or {})}
    return [arg for host, inside in pairs.items() for arg in ("-v", f"{host}:{inside}")]


def start(timeout_s: int = 300, mounts: dict[Path, str] | None = None) -> None:
    """Both containers up, the extension created. Idempotent: running containers are reused.

    `mounts` maps extra host directories into both containers (the smoke test's parquet).
    """
    for directory in INSIDE:
        directory.mkdir(parents=True, exist_ok=True)
        directory.chmod(0o777)  # the containers' user is not necessarily the runner's
    if not _running(DUCK_CONTAINER):
        subprocess.run(["docker", "rm", "-f", DUCK_CONTAINER], capture_output=True, check=False)
        init = ROOT / "pgduck-init.sql"
        # curl transport: DuckDB's default Azure transport fails OneLake's TLS handshake on Linux
        # (see bench.config.azure_transport). Spill to the mounted runner disk.
        init.write_text(
            "SET GLOBAL azure_transport_option_type = 'curl';\n"
            f"SET GLOBAL temp_directory = '{INSIDE[SPILL_DIR]}';\n",
            encoding="utf-8",
        )
        init.chmod(0o644)
        subprocess.run(
            [
                "docker", "run", "-d", "--name", DUCK_CONTAINER, "--network", "host",
                *_volumes(mounts),
                "-v", f"{init}:/pgduck-init.sql:ro",
                "--memory", DUCK_CONTAINER_MEMORY, "--memory-swap", DUCK_CONTAINER_MEMORY,
                IMAGE_DUCK,
                "pgduck_server",
                "--unix_socket_directory", INSIDE[SOCKET_DIR],
                "--unix_socket_permissions", "0777",
                "--port", str(DUCK_PORT),
                "--memory_limit", DUCK_MEMORY_LIMIT,
                "--cache_dir", INSIDE[CACHE_DIR],
                "--init_file_path", "/pgduck-init.sql",
            ],
            check=True,
            capture_output=True,
        )  # fmt: skip
    if not _running(PG_CONTAINER):
        subprocess.run(["docker", "rm", "-f", PG_CONTAINER], capture_output=True, check=False)
        settings = {
            "listen_addresses": "127.0.0.1",
            "port": str(PORT),
            "shared_preload_libraries": "pg_extension_base",
            "pg_lake_engine.host": f"host={INSIDE[SOCKET_DIR]} port={DUCK_PORT}",
            "pg_lake.allowed_azure_host_suffixes": AZURE_HOST_SUFFIXES,
            "pg_lake_engine.enable_cache_manager": "on",
            "pg_lake_engine.max_cache_size": str(cache_size_mb()),
            "shared_buffers": "1GB",
            "work_mem": "256MB",
        }
        subprocess.run(
            [
                "docker", "run", "-d", "--name", PG_CONTAINER, "--network", "host",
                *_volumes(mounts),
                IMAGE_PG,
                "postgres", "-D", f"{HOME}/pgsql-{PG_MAJOR}/data",
                *[arg for key, value in settings.items() for arg in ("-c", f"{key}={value}")],
            ],
            check=True,
            capture_output=True,
        )  # fmt: skip
    deadline = time.time() + timeout_s
    while True:
        try:
            with connect("postgres") as conn:
                if not sql(conn, f"SELECT 1 FROM pg_database WHERE datname = '{DATABASE}'"):
                    sql(conn, f"CREATE DATABASE {DATABASE}")
            with connect() as conn:
                sql(conn, "CREATE EXTENSION IF NOT EXISTS pg_lake CASCADE")
            return
        except Exception as exc:  # noqa: BLE001 - not up yet
            last = exc
        if time.time() > deadline:
            raise RuntimeError(f"pg_lake not answering after {timeout_s}s: {last}")
        time.sleep(3)


def connect(database: str = DATABASE):
    """Postgres, autocommit: each statement its own transaction, as the other engines run."""
    import psycopg

    return psycopg.connect(
        host="127.0.0.1", port=PORT, user="postgres", dbname=database, autocommit=True
    )


def connect_duck():
    """pgduck_server itself, through the mounted socket: DuckDB statements, not Postgres ones.

    A client-side cursor, so statements travel in the simple query protocol, the one psql uses.
    """
    import psycopg

    return psycopg.connect(
        host=str(SOCKET_DIR),
        port=DUCK_PORT,
        user="postgres",
        dbname="postgres",
        autocommit=True,
        cursor_factory=psycopg.ClientCursor,
    )


def sql(conn, statement: str) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(statement)
        return [tuple(row) for row in cur.fetchall()] if cur.description else []


def version(conn) -> str:
    return str(sql(conn, "SELECT extversion FROM pg_extension WHERE extname = 'pg_lake'")[0][0])


# --- OneLake --------------------------------------------------------------------------------


def storage_secret(token: str) -> None:
    """(Re)place pgduck_server's OneLake secret on `token`.

    Temporary, so the token never lands on disk; DuckDB's secret manager is per instance, so
    every pgduck_server connection pg_lake opens sees it.
    """
    with connect_duck() as duck:
        sql(
            duck,
            "CREATE OR REPLACE SECRET onelake_storage "
            f"(TYPE azure, PROVIDER access_token, ACCESS_TOKEN '{token}')",
        )


def attach(conn, endpoint: str, options: dict[str, str] | None = None) -> None:
    """The iceberg_catalog server for OneLake, logging in through the token shim."""
    body = ", ".join(
        f"{key} '{value}'"
        for key, value in {
            "rest_endpoint": endpoint,
            "oauth_endpoint": token_endpoint(),
            "enable_vended_credentials": "false",
            **(options or {}),
        }.items()
    )
    sql(conn, f"DROP SERVER IF EXISTS {SERVER} CASCADE")
    sql(conn, f"CREATE SERVER {SERVER} TYPE 'rest' FOREIGN DATA WRAPPER iceberg_catalog "
              f"OPTIONS ({body})")  # fmt: skip
    # Placeholders: the shim answers any grant (see the module docstring).
    sql(conn, f"CREATE USER MAPPING FOR PUBLIC SERVER {SERVER} "
              "OPTIONS (client_id 'bench', client_secret 'unused')")  # fmt: skip


def read_only_table(namespace: str, table: str, catalog_name: str | None) -> str:
    """The DDL that attaches one existing catalog table, its columns taken from the metadata."""
    named = f", catalog_name = '{catalog_name}'" if catalog_name else ""
    return (
        f'CREATE TABLE "{namespace.lower()}"."{table}" () USING iceberg WITH '
        f"(catalog = '{SERVER}', read_only = true{named}, "
        f"catalog_namespace = '{namespace}', catalog_table_name = '{table}')"
    )


def cache_usage() -> str:
    """How much the cache holds on disk: the proof it engaged, read at close."""
    total = sum(p.stat().st_size for p in CACHE_DIR.rglob("*") if p.is_file())
    return f"{total / 2**30:.1f} GiB in {CACHE_DIR}"
