"""Can a candidate engine join the bench? candidate_engine.yml runs this; README says the rules.

THREE REQUIREMENTS, all against the real lakehouse, and a candidate needs every one:

  1. SQL          the 22 TPC-H statements run as SQL (bench.tpch.queries, the bench's own text)
  2. read Azure   the OneLake Iceberg tables through the REST catalog -- nation must count 25,
                  which takes real data-file reads, not just metadata -- AND a raw CSV in the
                  lakehouse Files section, where the ETL lands its input
  3. write        CTAS an Iceberg table through the same catalog, read it back in the engine AND
                  in pyiceberg -- a table only the writer can read is not Iceberg

Every probe runs regardless of the others and the SUMMARY is the result: which layer fails
(catalog auth, storage auth, dialect, Iceberg write) is the finding. Where the right credential
form is undocumented for OneLake, the candidate lists VARIANTS and each one's error is printed.

Statements carry the bearer or a SAS in their text and engines quote statements back in errors,
so everything printed goes through `scrub`.

A new candidate is a subclass with `start`, `sql`, and the variant lists, plus a registry entry.
"""

from __future__ import annotations

import csv
import io
import os
import subprocess
import sys
import threading
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

from bench import auth, onelake, scrub
from bench.config import ICEBERG_ENDPOINT, ONELAKE_DFS
from bench.etl.config import EtlConfig
from bench.etl.data import csv_names
from bench.etl.schema import COLUMNS, FILTER
from bench.suite import suite_class
from bench.tpch import queries
from bench.tpch.engines.pyspark_gluten_iceberg import onelake_sas

CONTAINER = "candidate"
WRITE_NS = "candidate"
NATION_ROWS = 25
# The widest AEMO record in the landed files, from StarRocks' own inference error in run
# 36213949272 ("Schema column count: 120").
CSV_MAX_WIDTH = 120
# The GitHub OIDC assertion, on the host and as the container sees it. Hadoop's
# WorkloadIdentityTokenProvider re-reads the file on every token refresh, so a thread rewrites it
# well inside the assertion's ~5-minute life -- the scheme bench/tpch/engines/pyspark_iceberg.py
# uses for Spark-OSS.
ASSERTION_DIR = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "candidate-oidc"
ASSERTION_IN_CONTAINER = "/var/run/candidate-oidc/assertion"
ASSERTION_REFRESH_S = 240


def _keep_assertion_fresh() -> None:
    ASSERTION_DIR.mkdir(parents=True, exist_ok=True)
    target = ASSERTION_DIR / "assertion"

    def write() -> None:
        target.write_text(auth._github_oidc_assertion(), encoding="utf-8")
        target.chmod(0o644)  # the container's user is not the runner's

    write()

    def loop() -> None:
        while True:
            time.sleep(ASSERTION_REFRESH_S)
            try:
                write()
            except Exception as exc:  # noqa: BLE001 - a failed refresh must not kill the run
                _say(f"  warning: assertion refresh failed: {exc}")

    threading.Thread(target=loop, daemon=True).start()


def _say(text: str) -> None:
    print(scrub.scrub(text), flush=True)


class Candidate:
    name = ""
    default_image = ""
    list_namespaces = "SHOW DATABASES FROM onelake"

    def __init__(self, cfg) -> None:
        self.cfg = cfg
        self.image = os.environ.get("CANDIDATE_IMAGE") or self.default_image

    def start(self) -> None: ...
    def sql(self, statement: str) -> list[tuple]: ...
    def version(self) -> str: ...
    def attach_variants(self, token: str, sas: str) -> dict[str, list[str]]: ...
    def files_variants(self, path: str, sas: str) -> dict[str, str | list[str]]: ...
    def write_variants(self, table: str, source: str) -> dict[str, list[str]]: ...
    def use_catalog(self) -> list[str]: ...


class StarRocks(Candidate):
    """StarRocks allin1: the Java FE (planner, catalog) and C++ BE (execution) in one container.

    Catalog properties are the documented REST ones (docs/en/data_source/catalog/iceberg/
    iceberg.md). Storage goes through hadoop-azure, keyed by the HOST of the abfss URI.

    NEVER `azure.adls2.storage_account`: StarRocks turns it into `<account>.dfs.core.windows.net`
    (AzureStorageCloudCredential), so "onelake" configures the wrong host and every read of
    onelake.dfs.fabric.microsoft.com falls back to SharedKey -- `fs.azure.account.key` null, the
    error of run 36214160572. Left empty, the OAuth keys are set unscoped, and
    `azure.adls2.endpoint` scopes a SAS to OneLake's host instead.
    """

    name = "starrocks"
    default_image = "starrocks/allin1-ubuntu:4.1-latest"
    # The bench engine's spelling of the TPC-H table names (bench.tpch.queries.IDENT_STYLE).
    dialect = "starrocks_iceberg"

    def start(self) -> None:
        _keep_assertion_fresh()
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                CONTAINER,
                "-v",
                f"{ASSERTION_DIR}:{Path(ASSERTION_IN_CONTAINER).parent}:ro",
                "-p",
                "127.0.0.1:9030:9030",
                "-p",
                "127.0.0.1:8030:8030",
                "-p",
                "127.0.0.1:8040:8040",
                self.image,
            ],
            check=True,
        )
        digest = subprocess.run(
            ["docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", self.image],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        _say(f"image {self.image}  {digest}")
        # The FE answers SELECT 1 before the BE has registered; a query needs a live BE.
        deadline = time.time() + 300
        while True:
            try:
                alive = [r for r in self.sql("SHOW BACKENDS") if "true" in map(str, r)]
                if alive:
                    return
            except Exception:  # noqa: BLE001 - not up yet
                pass
            if time.time() > deadline:
                raise RuntimeError("StarRocks BE not alive after 300s")
            time.sleep(5)

    def _conn(self):
        import pymysql

        if getattr(self, "_c", None) is None:
            self._c = pymysql.connect(
                host="127.0.0.1",
                port=9030,
                user="root",
                password="",
                autocommit=True,
                read_timeout=3600,
            )
        return self._c

    def sql(self, statement: str) -> list[tuple]:
        try:
            with self._conn().cursor() as cur:
                cur.execute(statement)
                return list(cur.fetchall())
        except Exception:
            # A connection the server dropped must not poison every later probe.
            if getattr(self, "_c", None) is not None and not self._c.open:
                self._c = None
            raise

    def version(self) -> str:
        return str(self.sql("SELECT current_version()")[0][0])

    def storage_variants(self, sas: str) -> dict[str, str]:
        """hadoop-azure credentials for OneLake, as StarRocks property lists."""
        return {
            # Spark-OSS's credential: the OIDC assertion file, exchanged by hadoop-azure itself,
            # refreshable for any run length.
            "workload identity": (
                f'"azure.adls2.oauth2_token_file"="{ASSERTION_IN_CONTAINER}", '
                f'"azure.adls2.oauth2_tenant_id"="{os.environ.get("AZURE_TENANT_ID", "")}", '
                f'"azure.adls2.oauth2_client_id"="{os.environ.get("AZURE_CLIENT_ID", "")}"'
            ),
            # Gluten's credential: a user-delegation SAS, scoped to OneLake's dfs host. One hour.
            "SAS on the OneLake endpoint": (
                f'"azure.adls2.endpoint"="{ONELAKE_DFS}", "azure.adls2.sas_token"="{sas}"'
            ),
        }

    def attach_variants(self, token: str, sas: str) -> dict[str, list[str]]:
        base = (
            '"type"="iceberg", "iceberg.catalog.type"="rest", '
            f'"iceberg.catalog.uri"="{ICEBERG_ENDPOINT}", '
            f'"iceberg.catalog.warehouse"="{self.cfg.warehouse}"'
        )
        oauth = f'"iceberg.catalog.security"="oauth2", "iceberg.catalog.oauth2.token"="{token}"'

        def ddl(props: str) -> list[str]:
            return [
                "DROP CATALOG IF EXISTS onelake",
                f"CREATE EXTERNAL CATALOG onelake PROPERTIES ({base}, {props})",
            ]

        no_vending = '"iceberg.catalog.vended-credentials-enabled"="false"'
        variants = {
            f"oauth2 token + {label}": ddl(f"{oauth}, {no_vending}, {props}")
            for label, props in self.storage_variants(sas).items()
        }
        # Vended alone attaches and lists, but in run 36214160572 every data read still fell back
        # to SharedKey: whatever OneLake vends does not reach hadoop-azure. Kept as a check.
        variants["oauth2 token + vended credentials"] = ddl(
            f'{oauth}, "iceberg.catalog.vended-credentials-enabled"="true"'
        )
        return variants

    def use_catalog(self) -> list[str]:
        return ["SET CATALOG onelake", "SET query_timeout = 3600"]

    def files_variants(self, path: str, sas: str) -> dict[str, str]:
        """The ETL's actual read of one AEMO file: parse, filter, cast -- not just reach it.

        AEMO "daily" files are RAGGED: a `C` header line, then `I`/`D` rows of several record
        types, each its own width. Default inference fails on that ("Schema column count: 120
        doesn't match source value column count: 10", run 36213949272). The ETL's engines read it
        with the 53-column DUNIT layout (bench/etl/schema.py), short rows padded with NULL, then
        filter to DUNIT v3 -- so that is the read asked for here. `schema` is FILES() from 4.1.2.
        """
        return {
            label: (
                f"SELECT count(*), sum(CAST({cols['TOTALCLEARED']} AS DOUBLE)) FROM {source} "
                f"WHERE {' AND '.join(f'{cols[c]} = {v!r}' for c, v in FILTER)}"
            )
            for label, (source, cols) in self._csv_sources(path, sas).items()
        }

    def files_diagnostics(self, path: str, sas: str) -> dict[str, str]:
        """What each read actually sees -- printed, not gated."""
        out = {}
        for label, (source, cols) in self._csv_sources(path, sas).items():
            first = ", ".join(cols[c] for c in COLUMNS[:6])
            out[f"{label}: I/UNIT/VERSION"] = (
                f"SELECT {cols['I']}, {cols['UNIT']}, {cols['VERSION']}, count(*) FROM {source} "
                "GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 12"
            )
            # The first six fields of a few data rows, as this read splits them.
            out[f"{label}: first fields of D rows"] = (
                f"SELECT {first} FROM {source} WHERE {cols['I']} = 'D' LIMIT 3"
            )
        return out

    def _csv_sources(self, path: str, sas: str) -> dict[str, tuple[str, dict[str, str]]]:
        """Each read as (FILES() source, ETL column name -> expression in that source).

        STRING, NEVER BARE `VARCHAR`. In StarRocks a `VARCHAR` with no length is VARCHAR(1), and a
        CSV value that does not fit loads as NULL. Declared that way, only one-character fields
        survived -- `D`, `1` -- while `DUNIT`, `DISPATCH` and every timestamp came back NULL, so
        the DUNIT filter matched nothing (runs 36219398717, 36221536704, 36221803419; it looked
        like a column-name problem until the raw fields were printed).

        The line-split read is the fallback that avoids FILES()'s column mapping: each line is
        one STRING (a separator that never occurs) and SQL `split_part` takes the fields. AEMO
        quotes only its timestamps, and none contains a comma.
        """
        storage = self.storage_variants(sas)["workload identity"]
        csv = (
            '"format"="csv", "csv.column_separator"=",", "csv.enclose"=\'"\', "csv.skip_header"="1"'
        )
        named = {c: c for c in COLUMNS}

        def files(width: int) -> tuple[str, dict[str, str]]:
            declared = [f"{c} STRING" for c in COLUMNS]
            # The widest record in these files is 120 fields; declaring that width means no row
            # is ever LONGER than the schema, only shorter.
            declared += [f"c{i} STRING" for i in range(len(COLUMNS), width)]
            schema = f'"schema"="{", ".join(declared)}", "fill_mismatch_column_with"="null"'
            return f'FILES("path"="{path}", {csv}, {schema}, {storage})', named

        lines = (
            f'FILES("path"="{path}", "format"="csv", "csv.column_separator"="|~|", '
            f'"csv.skip_header"="1", "schema"="line STRING", {storage})'
        )
        split = {c: f"split_part(line, ',', {i + 1})" for i, c in enumerate(COLUMNS)}
        return {
            "53 STRING columns, short rows NULL-padded": files(len(COLUMNS)),
            "120 STRING columns, short rows NULL-padded": files(CSV_MAX_WIDTH),
            "one STRING per line, split_part": (lines, split),
        }

    def write_variants(self, table: str, source: str) -> dict[str, list[str]]:
        location = f"{self.cfg.base_path}/Tables/{WRITE_NS}/{table}"
        prep = [
            f"CREATE DATABASE IF NOT EXISTS {WRITE_NS}",
            f"DROP TABLE IF EXISTS {WRITE_NS}.{table}",
        ]
        return {
            # OneLake wants tables under Tables/<ns>/<table>, which pyiceberg and Spark had to
            # pass explicitly (bench/etl/iceberg.py, bench/etl/engines/pyspark_iceberg.py).
            "CTAS with location": prep
            + [
                f'CREATE TABLE {WRITE_NS}.{table} PROPERTIES ("location"="{location}") '
                f"AS SELECT * FROM {source}"
            ],
            "CTAS": prep + [f"CREATE TABLE {WRITE_NS}.{table} AS SELECT * FROM {source}"],
            "CREATE + INSERT": prep
            + [
                f"CREATE TABLE {WRITE_NS}.{table} (n_nationkey INT, n_name VARCHAR(25), "
                f"n_regionkey INT, n_comment VARCHAR(152))",
                f"INSERT INTO {WRITE_NS}.{table} SELECT * FROM {source}",
            ],
        }


class Trino(Candidate):
    """Trino: one JVM, coordinator and worker in the same process, in the official image.

    CATALOGS ARE CREATED IN SQL. `catalog.management=dynamic` (mounted config.properties) enables
    `CREATE CATALOG ... USING iceberg WITH (...)`, so each variant is a statement list like
    StarRocks' rather than a properties file and a restart.

    CASE. Trino folds unquoted identifiers to lower case, and OneLake's namespaces are CH0010:
    `iceberg.rest-catalog.case-insensitive-name-matching` maps `ch0010` back to the real name.

    STORAGE is Trino's native Azure filesystem. `azure.auth-type=DEFAULT` is the Azure SDK's
    DefaultAzureCredential, whose workload-identity leg reads AZURE_FEDERATED_TOKEN_FILE -- the
    same refreshable OIDC assertion file Spark-OSS and StarRocks read, re-read on every refresh.
    """

    name = "trino"
    default_image = "trinodb/trino:latest"
    # Plain schema-qualified SQL; the connection's default catalog resolves `CH0010.lineitem`.
    dialect = "duckdb_iceberg"
    list_namespaces = "SHOW SCHEMAS FROM onelake"

    CONFIG = "\n".join(
        [
            "coordinator=true",
            "node-scheduler.include-coordinator=true",
            "http-server.http.port=8080",
            "discovery.uri=http://localhost:8080",
            "catalog.management=dynamic",
            "catalog.store=memory",
        ]
    )

    PROXY_PORT = 8181

    def _start_rest_proxy(self) -> None:
        """A pass-through between Trino and OneLake's REST catalog that logs every write.

        OneLake answers Trino's create commit with a bare 400 "Malformed request" and no detail,
        while Spark's staged CTAS commits fine; the difference is in the request body, which only
        a man in the middle sees. GETs pass silently; every other call prints its body and the
        reply. Auth headers are forwarded, never printed, and output goes through `scrub`.
        """
        import http.server
        from urllib.parse import urlsplit

        import requests

        upstream = "{0.scheme}://{0.netloc}".format(urlsplit(ICEBERG_ENDPOINT))
        skip = {"content-length", "content-encoding", "transfer-encoding", "connection"}

        class Handler(http.server.BaseHTTPRequestHandler):
            def forward(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else None
                headers = {
                    k: v
                    for k, v in self.headers.items()
                    if k.lower() not in {"host", "content-length", "accept-encoding"}
                }
                resp = requests.request(
                    self.command, upstream + self.path, headers=headers, data=body, timeout=600
                )
                if self.command != "GET":
                    _say(
                        f"    [rest] {self.command} {self.path} -> {resp.status_code}\n"
                        f"      request:  {(body or b'').decode(errors='replace')[:12000]}\n"
                        f"      response: {resp.text[:4000]}"
                    )
                self.send_response(resp.status_code)
                for k, v in resp.headers.items():
                    if k.lower() not in skip:
                        self.send_header(k, v)
                self.send_header("Content-Length", str(len(resp.content)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(resp.content)

            do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = forward

            def log_message(self, *args) -> None:
                pass

        server = http.server.ThreadingHTTPServer(("0.0.0.0", self.PROXY_PORT), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()

    def start(self) -> None:
        _keep_assertion_fresh()
        self._start_rest_proxy()
        config = ASSERTION_DIR.parent / "candidate-trino.properties"
        config.write_text(self.CONFIG + "\n", encoding="utf-8")
        config.chmod(0o644)
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                CONTAINER,
                "-v",
                f"{ASSERTION_DIR}:{Path(ASSERTION_IN_CONTAINER).parent}:ro",
                "-v",
                f"{config}:/etc/trino/config.properties:ro",
                "-e",
                f"AZURE_CLIENT_ID={os.environ.get('AZURE_CLIENT_ID', '')}",
                "-e",
                f"AZURE_TENANT_ID={os.environ.get('AZURE_TENANT_ID', '')}",
                "-e",
                f"AZURE_FEDERATED_TOKEN_FILE={ASSERTION_IN_CONTAINER}",
                # The REST logging proxy runs on the runner, outside the container.
                "--add-host=host.docker.internal:host-gateway",
                "-p",
                "127.0.0.1:8080:8080",
                self.image,
            ],
            check=True,
        )
        digest = subprocess.run(
            ["docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", self.image],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        _say(f"image {self.image}  {digest}")
        # The coordinator answers SERVER_STARTING_UP until it has registered itself as a worker.
        deadline = time.time() + 300
        while True:
            try:
                self.sql("SELECT 1")
                return
            except Exception:  # noqa: BLE001 - not up yet
                self._c = None
            if time.time() > deadline:
                raise RuntimeError("Trino not answering after 300s")
            time.sleep(5)

    def _conn(self):
        import trino

        if getattr(self, "_c", None) is None:
            self._c = trino.dbapi.connect(
                host="127.0.0.1",
                port=8080,
                user="bench",
                catalog="onelake",
                schema=self.cfg.schema.lower(),
                request_timeout=3600,
            )
        return self._c

    def sql(self, statement: str) -> list[tuple]:
        from trino.exceptions import TrinoQueryError

        cur = self._conn().cursor()
        try:
            cur.execute(statement)
            return [tuple(r) for r in cur.fetchall()]
        except TrinoQueryError as exc:
            # The message is only the outermost wrapper ("Error processing metadata for table");
            # the reason is in the cause chain of failureInfo.
            chain, info = [], exc.failure_info or {}
            while info:
                frame = (info.get("stack") or [""])[0]
                chain.append(f"{info.get('type')}: {info.get('message')} @ {frame}")
                info = info.get("cause") or {}
            raise RuntimeError(" <- ".join(chain) or str(exc)) from exc

    def version(self) -> str:
        return str(self.sql("SELECT version()")[0][0])

    def attach_variants(self, token: str, sas: str) -> dict[str, list[str]]:
        base = {
            "iceberg.catalog.type": "rest",
            "iceberg.rest-catalog.uri": ICEBERG_ENDPOINT,
            "iceberg.rest-catalog.warehouse": self.cfg.warehouse,
            "iceberg.rest-catalog.security": "OAUTH2",
            "iceberg.rest-catalog.oauth2.token": token,
            "iceberg.rest-catalog.case-insensitive-name-matching": "true",
            "fs.native-azure.enabled": "true",
        }

        def ddl(props: dict[str, str]) -> list[str]:
            body = ", ".join(f"\"{k}\" = '{v}'" for k, v in (base | props).items())
            return [
                "DROP CATALOG IF EXISTS onelake",
                f"CREATE CATALOG onelake USING iceberg WITH ({body})",
            ]

        no_vending = {"iceberg.rest-catalog.vended-credentials-enabled": "false"}
        proxied = urlsplit(ICEBERG_ENDPOINT).path
        return {
            # First, so it is the catalog every later gate uses: the passing storage settings,
            # with the catalog reached through the logging proxy (`_start_rest_proxy`).
            "via REST logging proxy + workload identity + azure.endpoint": ddl(
                no_vending
                | self.AZURE
                | {
                    "iceberg.rest-catalog.uri": (
                        f"http://host.docker.internal:{self.PROXY_PORT}{proxied}"
                    )
                }
            ),
            "oauth2 token + workload identity (azure.auth-type DEFAULT)": ddl(
                no_vending | {"azure.auth-type": "DEFAULT"}
            ),
            # OneLake's host is onelake.dfs.fabric.microsoft.com, not <acct>.dfs.core.windows.net.
            "oauth2 token + workload identity + azure.endpoint fabric.microsoft.com": ddl(
                no_vending
                | {"azure.auth-type": "DEFAULT", "azure.endpoint": "fabric.microsoft.com"}
            ),
            "oauth2 token + vended credentials": ddl(
                {"iceberg.rest-catalog.vended-credentials-enabled": "true"}
            ),
        }

    def use_catalog(self) -> list[str]:
        return []

    # The storage properties the passing attach variant needed (run 36326766643).
    AZURE = {
        "fs.native-azure.enabled": "true",
        "azure.auth-type": "DEFAULT",
        "azure.endpoint": "fabric.microsoft.com",
    }

    def files_variants(self, path: str, sas: str) -> dict[str, list[str]]:
        """The ETL's read of one AEMO file, through a Hive EXTERNAL CSV table over Files/csv.

        Trino has no path-based file reader: a CSV is a Hive table whose `external_location` is
        the folder. Every column VARCHAR (Trino's CSV format allows nothing else), 120 of them so
        no row is longer than the schema; a short row reads NULL past its end. `"$path"` picks
        the one file out of the folder. The Hive catalog needs a metastore for that one table
        definition: in the container, or in the lakehouse.
        """
        folder, name = path.rsplit("/", 1)
        declared = [c.lower() for c in COLUMNS] + [
            f"c{i}" for i in range(len(COLUMNS), CSV_MAX_WIDTH)
        ]
        columns = ", ".join(f'"{c}" varchar' for c in declared)
        where = " AND ".join(f"\"{c.lower()}\" = '{v}'" for c, v in FILTER)
        query = [
            "CREATE SCHEMA IF NOT EXISTS files.etl",
            "DROP TABLE IF EXISTS files.etl.aemo",
            f"CREATE TABLE files.etl.aemo ({columns}) WITH (external_location = '{folder}', "
            "format = 'CSV', skip_header_line_count = 1)",
            'SELECT count(*), sum(CAST("totalcleared" AS double)) FROM files.etl.aemo '
            f"WHERE {where} AND \"$path\" LIKE '%/{name}'",
        ]

        def catalog(metastore: dict[str, str]) -> list[str]:
            props = {"hive.metastore": "file"} | metastore | self.AZURE
            body = ", ".join(f"\"{k}\" = '{v}'" for k, v in props.items())
            return [
                "DROP CATALOG IF EXISTS files",
                f"CREATE CATALOG files USING hive WITH ({body})",
            ]

        return {
            "hive CSV table, metastore in the container": catalog(
                {
                    "hive.metastore.catalog.dir": "local:///trino-metastore",
                    "fs.native-local.enabled": "true",
                    "local.location": "/tmp",
                }
            )
            + query,
            "hive CSV table, metastore in the lakehouse": catalog(
                {"hive.metastore.catalog.dir": f"{self.cfg.base_path}/Files/_trino_metastore"}
            )
            + query,
        }

    def files_diagnostics(self, path: str, sas: str) -> dict[str, str]:
        return {
            "rows per record type in that file": (
                'SELECT "i", "unit", "version", count(*) FROM files.etl.aemo '
                f"WHERE \"$path\" LIKE '%/{path.rsplit('/', 1)[1]}' "
                "GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 12"
            )
        }

    def _clear(self, table: str) -> None:
        directory = onelake.file_system(self.cfg).get_directory_client(
            f"{self.cfg.lakehouse_id}/Tables/{WRITE_NS}/{table}"
        )
        if directory.exists():
            directory.delete_directory()

    def write_diagnostics(self) -> None:
        """What sits under Tables/candidate after the write gate: what Trino called non-empty."""
        fs = onelake.file_system(self.cfg)
        prefix = f"{self.cfg.lakehouse_id}/Tables/{WRITE_NS}"
        try:
            paths = [p.name[len(prefix) :] for p in fs.get_paths(prefix, recursive=True)]
            _say(f"    Tables/{WRITE_NS} holds {len(paths)} paths: {paths[:40]}")
        except Exception as exc:  # noqa: BLE001
            _say(f"    listing Tables/{WRITE_NS} failed: {scrub.scrub_exc(exc, 500)}")

    def _commit_bisect(self) -> None:
        """Which update in Trino's staged-create commit does OneLake call "Malformed request"?

        Replays the exact commit the REST proxy captured (run 36329873292) straight against
        OneLake, minus the snapshot (so no data files are needed), dropping one suspect at a time.
        Each attempt stages its own table first, as Trino does, and is dropped after.
        """
        import requests

        _say("\n[staged-create commit bisect]")
        headers = {"Authorization": f"Bearer {auth.onelake_token()}"}
        config = requests.get(
            f"{ICEBERG_ENDPOINT}/v1/config",
            params={"warehouse": self.cfg.warehouse},
            headers=headers,
            timeout=60,
        ).json()
        prefix = (config.get("overrides") or {}).get("prefix") or (
            config.get("defaults") or {}
        ).get("prefix")
        tables = f"{ICEBERG_ENDPOINT}/v1/{prefix}/namespaces/{WRITE_NS}/tables"
        schema = {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "n_nationkey", "required": False, "type": "long"},
                {"id": 2, "name": "n_name", "required": False, "type": "string"},
                {"id": 3, "name": "n_regionkey", "required": False, "type": "long"},
                {"id": 4, "name": "n_comment", "required": False, "type": "string"},
            ],
        }
        trino_props = {
            "format-version": "2",
            "write.format.default": "PARQUET",
            "write.parquet.compression-codec": "",
        }

        def updates(meta: dict, drop=(), props=None) -> list[dict]:
            every = [
                {"action": "assign-uuid", "uuid": meta["table-uuid"]},
                {"action": "upgrade-format-version", "format-version": 2},
                {"action": "add-schema", "schema": schema, "last-column-id": 4},
                {"action": "set-current-schema", "schema-id": -1},
                {"action": "add-spec", "spec": {"spec-id": 0, "fields": []}},
                {"action": "set-default-spec", "spec-id": -1},
                {"action": "add-sort-order", "sort-order": {"order-id": 0, "fields": []}},
                {"action": "set-default-sort-order", "sort-order-id": -1},
                {"action": "set-location", "location": meta["location"]},
                {"action": "set-properties", "updates": trino_props if props is None else props},
                {
                    "action": "remove-properties",
                    "removals": ["write.parquet.compression-codec", "format-version"],
                },
            ]
            return [u for u in every if u["action"] not in drop]

        no_version = {"write.format.default": "PARQUET", "write.parquet.compression-codec": ""}
        variants = {
            "trino's commit (no snapshot)": {},
            "without remove-properties": {"drop": {"remove-properties"}},
            "set-properties without format-version": {"props": no_version},
            "set-properties without the empty codec": {
                "props": {"format-version": "2", "write.format.default": "PARQUET"}
            },
            "neither format-version nor remove-properties": {
                "props": no_version,
                "drop": {"remove-properties"},
            },
            "without set-location": {"drop": {"set-location"}},
            "without set-properties and remove-properties": {
                "drop": {"set-properties", "remove-properties"}
            },
            "without upgrade-format-version": {"drop": {"upgrade-format-version"}},
        }
        for index, (label, change) in enumerate(variants.items()):
            name = f"bisect{index}"
            requests.delete(
                f"{tables}/{name}", params={"purgeRequested": "true"}, headers=headers, timeout=120
            )
            self._clear(name)
            staged = requests.post(
                tables,
                json={
                    "name": name,
                    "schema": schema,
                    "partition-spec": {"spec-id": 0, "fields": []},
                    "write-order": {"order-id": 0, "fields": []},
                    "properties": trino_props,
                    "stage-create": True,
                },
                headers=headers,
                timeout=120,
            )
            if not staged.ok:
                _say(f"    stage {staged.status_code}  {label}  {staged.text[:300]}")
                continue
            commit = requests.post(
                f"{tables}/{name}",
                json={
                    "requirements": [{"type": "assert-create"}],
                    "updates": updates(staged.json()["metadata"], **change),
                },
                headers=headers,
                timeout=120,
            )
            _say(f"    commit {commit.status_code}  {label}  {commit.text[:200]}")
            requests.delete(
                f"{tables}/{name}", params={"purgeRequested": "true"}, headers=headers, timeout=120
            )
            self._clear(name)

    def _unstaged_probe(self, source: str) -> None:
        """Can Trino create WITHOUT stage-create on OneLake?

        TrinoRestCatalog.newCreateTableTransaction (483) stages the create whenever it has a
        location, and it has one whenever the namespace reports a `location` property
        (`defaultTableLocation`); with none it calls `create()` -- a plain, unstaged create -- and
        also skips the empty-location check. So: what location does OneLake report per namespace,
        and does a namespace Trino creates without one take the unstaged path?
        """
        ns = "trino_unstaged"
        _say("\n[unstaged create probe]")
        for schema in (WRITE_NS, self.cfg.schema.lower()):
            _try(self, f"namespace {schema}", [f"SHOW CREATE SCHEMA onelake.{schema}"])
        directory = onelake.file_system(self.cfg).get_directory_client(
            f"{self.cfg.lakehouse_id}/Tables/{ns}"
        )
        if directory.exists():
            directory.delete_directory()
        _try(self, "drop leftover", [f"DROP TABLE IF EXISTS onelake.{ns}.nation"])
        _try(self, f"create namespace {ns}", [f"CREATE SCHEMA IF NOT EXISTS onelake.{ns}"])
        _try(self, f"namespace {ns}", [f"SHOW CREATE SCHEMA onelake.{ns}"])
        created, _ = _try(
            self,
            f"CTAS into {ns}, no location",
            [f"CREATE TABLE onelake.{ns}.nation AS SELECT * FROM {source}"],
        )
        if created:
            _try(self, "read back", [f"SELECT count(*) FROM onelake.{ns}.nation"])
            _try(self, "table location", [f"SHOW CREATE TABLE onelake.{ns}.nation"])
            _try(self, "drop", [f"DROP TABLE onelake.{ns}.nation"])

    def write_variants(self, table: str, source: str) -> dict[str, list[str]]:
        # Trino refuses to create a table on a non-empty location (run 36326766643), and a failed
        # attempt leaves files that DROP TABLE cannot reach. Each variant gets its own cleared
        # location, and the listing afterwards says what made a location non-empty.
        inserted = f"{table}_ci"
        for name in (table, inserted):
            self._clear(name)
        self.write_diagnostics()
        self._commit_bisect()

        def at(name: str) -> str:
            return f"{self.cfg.base_path}/Tables/{WRITE_NS}/{name}"

        prep = [
            f"CREATE SCHEMA IF NOT EXISTS onelake.{WRITE_NS}",
            f"DROP TABLE IF EXISTS onelake.{WRITE_NS}.{table}",
        ]
        return {
            # Trino skips its empty-location check only for a REPLACE (IcebergMetadata.
            # beginCreateTable, 483: `!replace && listFiles(location).hasNext()`), and OneLake's
            # staged create has already written metadata/00000-*.metadata.json there by the time
            # it checks -- every plain CREATE failed on that file (run 36327806785).
            # Trino commits the table's Puffin statistics with the data (`set-statistics`), which
            # Spark's CTAS never sends; with them the commit came back 400 "Malformed request".
            "CREATE OR REPLACE TABLE AS, no extended statistics": [
                "SET SESSION onelake.collect_extended_statistics_on_write = false",
                f"CREATE SCHEMA IF NOT EXISTS onelake.{WRITE_NS}",
                f"CREATE OR REPLACE TABLE onelake.{WRITE_NS}.{table} AS SELECT * FROM {source}",
            ],
            "CREATE OR REPLACE TABLE AS": [
                f"CREATE SCHEMA IF NOT EXISTS onelake.{WRITE_NS}",
                f"CREATE OR REPLACE TABLE onelake.{WRITE_NS}.{table} AS SELECT * FROM {source}",
            ],
            "CTAS with location": prep
            + [
                f"CREATE TABLE onelake.{WRITE_NS}.{table} WITH (location = '{at(table)}') "
                f"AS SELECT * FROM {source}"
            ],
            "CREATE + INSERT with location": prep
            + [
                f"CREATE TABLE onelake.{WRITE_NS}.{table} (n_nationkey bigint, n_name varchar, "
                f"n_regionkey bigint, n_comment varchar) WITH (location = '{at(inserted)}')",
                f"INSERT INTO onelake.{WRITE_NS}.{table} SELECT * FROM {source}",
            ],
            "CTAS": prep + [f"CREATE TABLE onelake.{WRITE_NS}.{table} AS SELECT * FROM {source}"],
        }


CANDIDATES = {"starrocks": StarRocks, "trino": Trino}


def _try(engine: Candidate, label: str, statements: list[str]) -> tuple[bool, list[tuple]]:
    started = time.perf_counter()
    rows: list[tuple] = []
    try:
        for statement in statements:
            rows = engine.sql(statement)
    except Exception as exc:  # noqa: BLE001 - reporting failures is this script's job
        took = time.perf_counter() - started
        _say(f"    FAIL  {label}  ({took:.1f}s)  {scrub.scrub_exc(exc, 1500)}")
        return False, []
    _say(f"    PASS  {label}  ({time.perf_counter() - started:.1f}s)  {str(rows[:5])[:300]}")
    return True, rows


def _reference(etl: EtlConfig, name: str) -> tuple[int, float]:
    """DUNIT v3 row count and sum(TOTALCLEARED) of one CSV, by Python's csv module alone.

    Same rules as the ETL: skip the first line, filter by position on bench/etl/schema.py's
    FILTER, pad short rows. Independent of every engine, so a match means the parse is right.
    """
    raw = (
        onelake.file_system(etl)
        .get_file_client(f"{etl.csv_relative}/{name}")
        .download_file()
        .readall()
        .decode("utf-8", errors="replace")
    )
    position = {c: i for i, c in enumerate(COLUMNS)}
    cleared = position["TOTALCLEARED"]
    rows, total = 0, 0.0
    widths: Counter[int] = Counter()
    for record in csv.reader(io.StringIO(raw).readlines()[1:]):
        if all(len(record) > position[c] and record[position[c]] == v for c, v in FILTER):
            rows += 1
            widths[len(record)] += 1
            if len(record) > cleared and record[cleared]:
                total += float(record[cleared])
    # The field count of the rows kept: what a schema-by-position reader has to cope with.
    _say(f"  reference: DUNIT v3 row widths {dict(widths)} (COLUMNS has {len(COLUMNS)})")
    return rows, total


def _first_passing(
    engine: Candidate, gate: str, variants: dict[str, list[str]], check
) -> str | None:
    _say(f"\n[{gate}]")
    for label, statements in variants.items():
        ok, rows = _try(engine, label, statements)
        if ok and check(rows):
            return label
        if ok:
            _say(f"          {label}: ran, but the check failed on {str(rows[:5])[:300]}")
    return None


def main() -> int:
    name = sys.argv[1] if len(sys.argv) > 1 else "starrocks"
    cfg = suite_class("tpch").from_env()
    engine = CANDIDATES[name](cfg)
    engine.start()
    _say(f"{name} {engine.version()}")

    token = auth.onelake_token()
    sas, _ = onelake_sas(cfg.workspace_id, cfg.lakehouse_id)
    results: dict[str, bool] = {}

    # Gate 0: attach. A variant counts only when it lists the namespace AND reads nation's data
    # files: listing alone passed in run 36214160572 with storage auth entirely broken.
    attached = None
    _say("\n[attach REST catalog]")
    for label, statements in engine.attach_variants(token, sas).items():
        ok, _ = _try(engine, label, statements)
        if not ok:
            continue
        listed, rows = _try(engine, f"{label}: list namespaces", [engine.list_namespaces])
        # Case-insensitive: Trino lists OneLake's CH0010 as ch0010.
        if not (listed and any(cfg.schema.lower() in str(r).lower() for r in rows)):
            continue
        read, rows = _try(
            engine, f"{label}: nation", [f"SELECT count(*) FROM onelake.{cfg.schema}.nation"]
        )
        if read and rows and int(rows[0][0]) == NATION_ROWS:
            attached = label
            break
    results["attach catalog"] = attached is not None
    # Even with no variant listing the namespace, carry on: each later gate's error is diagnosis.
    for statement in engine.use_catalog():
        _try(engine, statement, [statement])

    # Gate 1: read Iceberg from OneLake.
    ok, rows = _try(engine, "nation count", [f"SELECT count(*) FROM onelake.{cfg.schema}.nation"])
    results["read iceberg (nation = 25)"] = bool(ok and rows and int(rows[0][0]) == NATION_ROWS)

    # Gate 1b: parse a raw AEMO CSV from the Files section, as the ETL does, and match a count
    # and a sum computed here in plain Python from the same bytes.
    files = None
    try:
        etl = EtlConfig.from_env()
        names, _ = csv_names(etl, 1)
        path = f"{etl.csv_abfss}/{names[0]}"
        want_rows, want_sum = _reference(etl, names[0])
        _say(f"\nfile {path}\n  reference (python csv): {want_rows} DUNIT v3 rows, sum {want_sum}")

        def matches(rows) -> bool:
            if not rows or rows[0][0] is None:
                return False
            got_rows, got_sum = int(rows[0][0]), float(rows[0][1] or 0)
            return got_rows == want_rows > 0 and abs(got_sum - want_sum) <= 1e-6 * max(
                1.0, abs(want_sum)
            )

        files = _first_passing(
            engine,
            "parse Files/csv (ragged AEMO, DUNIT v3)",
            {
                label: stmt if isinstance(stmt, list) else [stmt]
                for label, stmt in engine.files_variants(path, sas).items()
            },
            matches,
        )
        # Always, pass or fail: the declared-schema reads' behaviour is a finding either way.
        _say("\n[parse Files/csv: what each read sees]")
        for label, statement in engine.files_diagnostics(path, sas).items():
            _try(engine, label, [statement])
    except Exception as exc:  # noqa: BLE001 - no CSV landed, or no SAS: the gate fails, loudly
        _say(f"\n[parse Files/csv]\n    FAIL  setup  {scrub.scrub_exc(exc, 1500)}")
    results["parse Files csv (count + sum = python)"] = files is not None

    # Gate 2: SQL, the whole TPC-H suite.
    _say(f"\n[TPC-H SF={cfg.sf}, {queries.N_QUERIES} statements]")
    passed = 0
    dialect = getattr(engine, "dialect", name)
    for index, statement in enumerate(queries.load(dialect, cfg.schema, cfg.sf), start=1):
        ok, rows = _try(engine, f"Q{index}", [statement])
        if ok:
            _say(f"          Q{index}: {len(rows)} rows")
        passed += ok
    results[f"SQL (TPC-H {passed}/{queries.N_QUERIES})"] = passed == queries.N_QUERIES

    # Gate 3: write Iceberg, then read it back in the engine AND in pyiceberg.
    table = name
    written = _first_passing(
        engine,
        "write Iceberg",
        engine.write_variants(table, f"{cfg.schema}.nation"),
        lambda rows: True,
    )
    readback = False
    if written:
        ok, rows = _try(engine, "read back", [f"SELECT count(*) FROM {WRITE_NS}.{table}"])
        try:
            arrow = auth.catalog(cfg).load_table(f"{WRITE_NS}.{table}").scan().to_arrow()
            _say(f"    pyiceberg read-back: {arrow.num_rows} rows, schema {arrow.schema}")
            readback = (
                bool(ok and rows)
                and int(rows[0][0]) == NATION_ROWS
                and arrow.num_rows == NATION_ROWS
            )
        except Exception as exc:  # noqa: BLE001
            _say(f"    FAIL  pyiceberg read-back  {scrub.scrub_exc(exc, 1500)}")
        _try(engine, "drop", [f"DROP TABLE IF EXISTS {WRITE_NS}.{table}"])
    if hasattr(engine, "write_diagnostics"):
        engine.write_diagnostics()
    results["write Iceberg (+ pyiceberg read-back = 25)"] = readback

    _say(f"\nSUMMARY  {name}  {engine.image}")
    _say(f"  attach variant: {attached}   files variant: {files}   write variant: {written}")
    for label, ok in results.items():
        _say(f"  {'PASS' if ok else 'FAIL'}  {label}")
    requirements = all(results.values())
    _say(
        f"\n  {'QUALIFIES' if requirements else 'DOES NOT QUALIFY'}: SQL, read Azure, write Iceberg"
    )
    return 0 if requirements else 1


if __name__ == "__main__":
    sys.exit(main())
