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


def _create_unstaged(cfg, table: str) -> None:
    """`candidate.<table>` with nation's schema, created through the catalog in one request.

    The unstaged create bench/etl/iceberg.py `recreate` makes for the ETL -- the way around the
    catalog's refusal of an engine's own create (staged, or field ids from 0). Drops any previous
    table and its folder first.
    """
    from bench.tpch.generate import _all_optional

    catalog = auth.catalog(cfg)
    identifier = f"{WRITE_NS}.{table}"
    if catalog.table_exists(identifier):
        catalog.drop_table(identifier)
    directory = onelake.file_system(cfg).get_directory_client(
        f"{cfg.lakehouse_id}/Tables/{WRITE_NS}/{table}"
    )
    if directory.exists():
        directory.delete_directory()
    catalog.create_table(
        identifier,
        schema=_all_optional(catalog.load_table(f"{cfg.schema}.nation").schema().as_arrow()),
        location=f"{cfg.base_path}/Tables/{WRITE_NS}/{table}",
    )


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

    def recover(self) -> None:
        """After a failed read: bring a dead worker back so the next variant is a real test."""


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
    # Extra `docker run` arguments, before the image.
    run_args: tuple[str, ...] = ()

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
                *self.run_args,
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
        self._wait_for_backend()

    def _wait_for_backend(self) -> None:
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
                raise RuntimeError(f"{self.name} BE not alive after 300s")
            time.sleep(5)

    # BE log files inside the container, printed when the BE dies.
    be_logs: tuple[str, ...] = ()
    be_log_lines = 60

    def containers(self) -> tuple[str, ...]:
        """Every container the engine runs in; the BE's is the last."""
        return (CONTAINER,)

    def be_crash_log(self) -> None:
        for log in self.be_logs:
            tail = subprocess.run(
                [
                    "docker",
                    "exec",
                    self.containers()[-1],
                    "tail",
                    "-n",
                    str(self.be_log_lines),
                    log,
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            _say(f"    --- {log}")
            _say((tail.stdout + tail.stderr)[-20000:])

    def recover(self) -> None:
        try:
            if any("true" in map(str, r) for r in self.sql("SHOW BACKENDS")):
                return
        except Exception:  # noqa: BLE001 - FE down too: restart below
            pass
        _say("    BE not alive: its log, then a container restart")
        self.be_crash_log()
        subprocess.run(["docker", "restart", *self.containers()], check=False)
        self._c = None
        self._wait_for_backend()

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

    def start(self) -> None:
        _keep_assertion_fresh()
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
        return {
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

    def write_diagnostics(self) -> None:
        """What sits under Tables/candidate after the write gate: what Trino called non-empty."""
        fs = onelake.file_system(self.cfg)
        prefix = f"{self.cfg.lakehouse_id}/Tables/{WRITE_NS}"
        try:
            paths = [p.name[len(prefix) :] for p in fs.get_paths(prefix, recursive=True)]
            _say(f"    Tables/{WRITE_NS} holds {len(paths)} paths: {paths[:40]}")
        except Exception as exc:  # noqa: BLE001
            _say(f"    listing Tables/{WRITE_NS} failed: {scrub.scrub_exc(exc, 500)}")

    def write_variants(self, table: str, source: str) -> dict[str, list[str]]:
        """The catalog does not support staged creates, and Trino stages every create.

        Trino (483) stages a CREATE TABLE or CTAS whenever the table has a location, and it takes
        one from the namespace, which this catalog always reports. The stage is accepted; the
        commit that finishes it is refused with a bare 400 "Malformed request". So a NEW table
        is created unstaged through the catalog -- bench/etl/iceberg.py `recreate`, the create
        chDB and Polars already use -- and Trino fills it with one INSERT, one commit, with its
        extended statistics off (below).
        """
        _create_unstaged(self.cfg, table)
        return {
            # With extended statistics off. Trino's default writes Puffin statistics into the
            # same commit, and the catalog refuses that commit with a bare 400 (run 36362007273);
            # without them the commit is add-snapshot + set-snapshot-ref and is accepted (run
            # 36362665394). The same 400 hit CREATE OR REPLACE on the existing table.
            "catalog create + Trino INSERT, no extended statistics": [
                "SET SESSION onelake.collect_extended_statistics_on_write = false",
                f"INSERT INTO onelake.{WRITE_NS}.{table} SELECT * FROM {source}",
            ],
        }


class Doris(StarRocks):
    """Apache Doris all-in-one: one FE (Java: planner, catalog) and one BE (C++) in one container.

    Same shape and protocol as StarRocks (MySQL on 9030, user root), which forked from it in 2020.

    ONELAKE IS A DOCUMENTED PATH (docs: lakehouse/best-practices/doris-onelake, 3.1.4+), but only
    with a client secret, and this app registration has none. Doris routes
    `abfss://...dfs.fabric.microsoft.com` to hadoop-azure over JNI and hands it the catalog's raw
    `fs.*` properties, the user's last so they win (AzureFileSystemProperties
    .oauth2BackendProperties). So the storage credential is hadoop-azure's workload identity, the
    one Spark-OSS and StarRocks use, set as raw `fs.azure.*` keys scoped to OneLake's host. The
    client-secret keys Doris insists on for `azure.auth_type=OAuth2` are then scoped to a host
    nothing reads -- on the FE they are written AFTER the raw keys, so on OneLake's host they
    would win there.

    NATIVE AZURE WILL NOT COVER ONELAKE. apache/doris#68103 (native Azure credentials, vended SAS)
    keeps "genuine Fabric OneLake locations" on this Hadoop path, so the BE crash with workload
    identity / SAS on that path is the blocker, not a missing feature.

    4.1.4.x ships only FE and BE component images (no all-in-one), so the default runs two
    containers on their own network. An `all-in-one-*` CANDIDATE_IMAGE runs the single container
    StarRocks does.
    """

    name = "doris"
    default_image = "apache/doris:fe-4.1.4.1"
    list_namespaces = "SHOW DATABASES FROM onelake"
    be_logs = tuple(
        f"/opt/apache-doris/be/log/{name}" for name in ("be.out", "be.WARNING", "be.INFO")
    )
    # be.out holds the crash header and stack; 60 lines cut the stack off (run 36401357218).
    be_log_lines = 250
    NETWORK = "doris"
    SUBNET = "172.20.80"
    FE_SERVERS = f"fe1:{SUBNET}.2:9010"
    BE_CONTAINER = f"{CONTAINER}-be"

    @property
    def all_in_one(self) -> bool:
        return ":all-in-one" in self.image

    @property
    def run_args(self) -> tuple[str, ...]:
        # The all-in-one image is tuned for CI fixtures (BE mem_limit 40%); the runner is 16 GB.
        return ("-e", "BE_CONFIG_EXTRA=mem_limit = 80%") if self.all_in_one else ()

    def containers(self) -> tuple[str, ...]:
        return (CONTAINER,) if self.all_in_one else (CONTAINER, self.BE_CONTAINER)

    def start(self) -> None:
        if self.all_in_one:
            super().start()
            return
        _keep_assertion_fresh()
        # The BE refuses to start with a small max_map_count or with swap on.
        subprocess.run(["sudo", "sysctl", "-w", "vm.max_map_count=2000000"], check=True)
        subprocess.run(["sudo", "swapoff", "-a"], check=True)
        subprocess.run(
            ["docker", "network", "create", "--subnet", f"{self.SUBNET}.0/24", self.NETWORK],
            check=True,
        )
        mount = f"{ASSERTION_DIR}:{Path(ASSERTION_IN_CONTAINER).parent}:ro"
        be_image = self.image.replace(":fe-", ":be-")
        for name, ip, image, env, ports in (
            (
                CONTAINER,
                f"{self.SUBNET}.2",
                self.image,
                ("FE_ID=1",),
                ("9030:9030", "8030:8030"),
            ),
            (
                self.BE_CONTAINER,
                f"{self.SUBNET}.3",
                be_image,
                (f"BE_ADDR={self.SUBNET}.3:9050",),
                ("8040:8040",),
            ),
        ):
            subprocess.run(
                [
                    "docker",
                    "run",
                    "-d",
                    "--name",
                    name,
                    "--network",
                    self.NETWORK,
                    "--ip",
                    ip,
                    "-v",
                    mount,
                    "-e",
                    f"FE_SERVERS={self.FE_SERVERS}",
                    # The images' JDK 17 dies reading the runner's cgroup v2 ("anyController is
                    # null" in CgroupV2Subsystem, FE never up, run 38040345248). The BE embeds a
                    # JVM too (hadoop-azure over JNI).
                    "-e",
                    "JAVA_TOOL_OPTIONS=-XX:-UseContainerSupport",
                    *(a for e in env for a in ("-e", e)),
                    *(a for p in ports for a in ("-p", f"127.0.0.1:{p}")),
                    image,
                ],
                check=True,
            )
            digest = subprocess.run(
                ["docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", image],
                capture_output=True,
                text=True,
                check=False,
            ).stdout.strip()
            _say(f"image {image}  {digest}")
        self._wait_for_backend()

    def be_crash_log(self) -> None:
        super().be_crash_log()
        # A SIGSEGV inside the JNI hadoop-azure call may leave a JVM crash report instead.
        report = subprocess.run(
            [
                "docker",
                "exec",
                self.containers()[-1],
                "sh",
                "-c",
                "for f in $(find /opt/apache-doris -name 'hs_err_pid*.log' 2>/dev/null); "
                'do echo "--- $f"; head -n 120 "$f"; done',
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if report.stdout.strip():
            _say(report.stdout[-8000:])

    HOST = ONELAKE_DFS
    UNUSED_HOST = "unused.dfs.core.windows.net"

    def version(self) -> str:
        return str(self.sql("SELECT @@version_comment")[0][0])

    def _workload_identity(self, host: str) -> dict[str, str]:
        return {
            f"fs.azure.account.auth.type.{host}": "OAuth",
            f"fs.azure.account.oauth.provider.type.{host}": (
                "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider"
            ),
            f"fs.azure.account.oauth2.msi.tenant.{host}": os.environ.get("AZURE_TENANT_ID", ""),
            f"fs.azure.account.oauth2.client.id.{host}": os.environ.get("AZURE_CLIENT_ID", ""),
            f"fs.azure.account.oauth2.token.file.{host}": ASSERTION_IN_CONTAINER,
        }

    def storage(self, sas: str) -> dict[str, dict[str, str]]:
        """Azure storage for OneLake, as Doris catalog / TVF properties."""
        tenant = os.environ.get("AZURE_TENANT_ID", "")
        # What Doris validates for OAuth2, pointed at a host no path uses.
        oauth2_unused = {
            "fs.azure.support": "true",
            "azure.endpoint": f"https://{ONELAKE_DFS}",
            "azure.auth_type": "OAuth2",
            "azure.oauth2_account_host": self.UNUSED_HOST,
            "azure.oauth2_server_uri": f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
            "azure.oauth2_client_id": os.environ.get("AZURE_CLIENT_ID", ""),
            "azure.oauth2_client_secret": "unused",
        }
        return {
            "workload identity (raw fs.azure keys) + OAuth2 on an unused host": oauth2_unused
            | self._workload_identity(self.HOST),
            "workload identity (raw fs.azure keys) alone": self._workload_identity(self.HOST),
            # Gluten's credential: a user-delegation SAS on OneLake's host. One hour.
            "fixed SAS (raw fs.azure keys) + OAuth2 on an unused host": oauth2_unused
            | {
                f"fs.azure.account.auth.type.{self.HOST}": "SAS",
                f"fs.azure.sas.fixed.token.{self.HOST}": sas,
            },
        }

    @staticmethod
    def _props(props: dict[str, str]) -> str:
        return ", ".join(f"'{k}' = '{v}'" for k, v in props.items())

    def attach_variants(self, token: str, sas: str) -> dict[str, list[str]]:
        base = {
            "type": "iceberg",
            "iceberg.catalog.type": "rest",
            "uri": ICEBERG_ENDPOINT,
            "warehouse": self.cfg.warehouse,
            "iceberg.rest.security.type": "oauth2",
            "iceberg.rest.oauth2.token": token,
        }

        def ddl(props: dict[str, str]) -> list[str]:
            return [
                "DROP CATALOG IF EXISTS onelake",
                f"CREATE CATALOG onelake PROPERTIES ({self._props(base | props)})",
            ]

        no_vending = {"iceberg.rest.vended-credentials-enabled": "false"}
        variants = {
            f"oauth2 token + {label}": ddl(no_vending | props)
            for label, props in self.storage(sas).items()
        }
        variants["oauth2 token + vended credentials"] = ddl(
            {"iceberg.rest.vended-credentials-enabled": "true"}
        )
        return variants

    def use_catalog(self) -> list[str]:
        return ["SWITCH onelake", "SET query_timeout = 3600"]

    def _csv_sources(self, path: str, sas: str) -> dict[str, tuple[str, dict[str, str]]]:
        """Each read as (hdfs() TVF source, ETL column name -> expression in that source).

        OneLake paths go through hadoop-azure, so the TVF is `hdfs()`, not `s3()`/Azure Blob.
        Doris names TVF CSV columns c1..cN unless `csv_schema` declares them.
        """
        # Not the OAuth2 set: hdfs() refuses it ("OAuth2 auth type is only supported for iceberg
        # rest catalog", run 36400638699).
        storage = self.storage(sas)["workload identity (raw fs.azure keys) alone"]
        fs = path.split("/", 3)
        common = {
            "uri": path,
            "fs.defaultFS": f"{fs[0]}//{fs[2]}",
            "format": "csv",
            "skip_lines": "1",
        } | storage

        def tvf(props: dict[str, str]) -> str:
            return f"hdfs({self._props(common | props)})"

        named = {c: c.lower() for c in COLUMNS}

        def declared(width: int) -> tuple[str, dict[str, str]]:
            names = [c.lower() for c in COLUMNS] + [f"c{i}" for i in range(len(COLUMNS), width)]
            schema = ";".join(f"{n}:string" for n in names)
            props = {"column_separator": ",", "enclose": '"', "csv_schema": schema}
            return tvf(props), named

        lines = tvf({"column_separator": "|~|", "csv_schema": "line:string"})
        split = {c: f"split_part(line, ',', {i + 1})" for i, c in enumerate(COLUMNS)}
        return {
            "53 string columns (csv_schema)": declared(len(COLUMNS)),
            "120 string columns (csv_schema)": declared(CSV_MAX_WIDTH),
            "one string per line, split_part": (lines, split),
        }

    def write_variants(self, table: str, source: str) -> dict[str, list[str]]:
        """Doris' own CREATE TABLE and CTAS are refused with the catalog's bare 400 "Malformed
        request" (run 36400638699), as Spark's and Trino's are. So the create goes through the
        catalog and Doris fills the table with one INSERT."""
        _create_unstaged(self.cfg, table)
        return {
            "catalog create + Doris INSERT": [
                f"REFRESH DATABASE {WRITE_NS}",
                f"INSERT INTO {WRITE_NS}.{table} SELECT * FROM {source}",
            ],
        }


class Databend(Candidate):
    """Databend all-in-one: meta and query in one container, a Rust engine end to end.

    NO ICEBERG DATA READ ON AZURE in v1.2.949: Databend builds iceberg-rust with `storage-all`,
    which there means memory/fs/s3/gcs, not `storage-azdls` -- every table load fails with
    "Constructing file io from scheme: azdls not supported now" (run 36516949156). The catalog
    itself attaches and lists namespaces with the bearer.

    The Iceberg catalog is iceberg-rust (Databend's fork) and its FileIO is opendal: an abfss://
    location goes to opendal's Azdls service (src/common/storage/src/operator.rs IcebergFileIO).
    Catalog properties reach both, `adls.sas-token` mapped to opendal's `sas_token` and every key
    it does not know passed through raw -- so opendal's own `filesystem`/`endpoint` can be set.
    With the fix (djouallah/databend fix/iceberg-azure-file-io, run 36520116389) the SAS reads
    nation; the AZURE_* environment does NOT reach Azdls -- its signer goes to IMDS and fails --
    though it does reach the azblob reader below.

    Files/csv: Databend's azblob:// location takes only an account key, which OneLake has none
    of, so the reads are azblob with the environment credential. An https:// URL carrying the SAS
    is no way round it: Databend drops the query string and OneLake answers 401 (run 36516949156).

    Speaks MySQL on 3307. The core is Apache-2.0; the Elastic-2.0 enterprise features are not used.
    """

    name = "databend"
    default_image = "datafuselabs/databend:v1.2.949-nightly"
    dialect = "starrocks_iceberg"
    list_namespaces = "SHOW DATABASES FROM onelake"
    USER = "databend"

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
                "-e",
                f"QUERY_DEFAULT_USER={self.USER}",
                "-e",
                f"QUERY_DEFAULT_PASSWORD={self.USER}",
                "-e",
                f"AZURE_CLIENT_ID={os.environ.get('AZURE_CLIENT_ID', '')}",
                "-e",
                f"AZURE_TENANT_ID={os.environ.get('AZURE_TENANT_ID', '')}",
                "-e",
                f"AZURE_FEDERATED_TOKEN_FILE={ASSERTION_IN_CONTAINER}",
                "-p",
                "127.0.0.1:3307:3307",
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
        deadline = time.time() + 300
        while True:
            try:
                # Unquoted identifiers fold to lower case by default; OneLake's namespace is
                # CH0010. GLOBAL, so a reconnect keeps it.
                self.sql("SET GLOBAL unquoted_ident_case_sensitive = 1")
                return
            except Exception:  # noqa: BLE001 - not up yet
                self._c = None
            if time.time() > deadline:
                raise RuntimeError("Databend not answering after 300s")
            time.sleep(5)

    def _conn(self):
        import pymysql

        if getattr(self, "_c", None) is None:
            self._c = pymysql.connect(
                host="127.0.0.1",
                port=3307,
                user=self.USER,
                password=self.USER,
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
            if getattr(self, "_c", None) is not None and not self._c.open:
                self._c = None
            raise

    def version(self) -> str:
        return str(self.sql("SELECT version()")[0][0])

    def attach_variants(self, token: str, sas: str) -> dict[str, list[str]]:
        base = {"token": token}

        def ddl(props: dict[str, str]) -> list[str]:
            body = " ".join(f"\"{k}\"='{v}'" for k, v in (base | props).items())
            return [
                "DROP CATALOG IF EXISTS onelake",
                f"CREATE CATALOG onelake TYPE=ICEBERG CONNECTION=(TYPE='rest' "
                f"ADDRESS='{ICEBERG_ENDPOINT}' WAREHOUSE='{self.cfg.warehouse}' {body})",
            ]

        azdls = {"filesystem": self.cfg.workspace_id, "endpoint": f"https://{ONELAKE_DFS}"}
        # The same SAS signs the INSERT's data files, so it needs create/write, not read+list.
        write_sas, _ = onelake_sas(self.cfg.workspace_id, self.cfg.lakehouse_id, write=True)
        return {
            "oauth2 token + workload identity (AZURE_* env)": ddl(azdls),
            "oauth2 token + SAS (adls.sas-token)": ddl(azdls | {"adls.sas-token": write_sas}),
            "oauth2 token + vended credentials": ddl(
                {"header.X-Iceberg-Access-Delegation": "vended-credentials"}
            ),
        }

    def use_catalog(self) -> list[str]:
        return ["USE CATALOG onelake"]

    FORMATS = (
        # Ragged AEMO rows: short ones NULL-padded, long ones cut, rather than an error.
        "CREATE OR REPLACE FILE FORMAT aemo_csv TYPE = CSV SKIP_HEADER = 1 "
        "FIELD_DELIMITER = ',' QUOTE = '\"' ERROR_ON_COLUMN_COUNT_MISMATCH = false",
        # One field per line: a delimiter and a quote that never occur.
        "CREATE OR REPLACE FILE FORMAT aemo_lines TYPE = CSV SKIP_HEADER = 1 "
        "FIELD_DELIMITER = '|' QUOTE = '`' ERROR_ON_COLUMN_COUNT_MISMATCH = false",
    )

    def _csv_sources(self, path: str, sas: str) -> dict[str, tuple[str, str, dict[str, str]]]:
        """Each read as (location, file format, ETL column name -> expression in that source)."""
        # abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Files/csv/<file>
        relative = path.split("/", 3)[3]
        blob = ONELAKE_DFS.replace(".dfs.", ".blob.")
        azblob = (
            f"'azblob://{self.cfg.workspace_id}/{relative}' "
            f"(CONNECTION => (ENDPOINT_URL = 'https://{blob}'), FILE_FORMAT => '{{fmt}}')"
        )
        by_position = {c: f"${i + 1}" for i, c in enumerate(COLUMNS)}
        split = {c: f"split_part($1, ',', {i + 1})" for i, c in enumerate(COLUMNS)}
        return {
            "azblob, env credential, positional columns": (azblob, "aemo_csv", by_position),
            "azblob, env credential, one field per line, split_part": (azblob, "aemo_lines", split),
        }

    def files_variants(self, path: str, sas: str) -> dict[str, list[str]]:
        out = {}
        for label, (source, fmt, cols) in self._csv_sources(path, sas).items():
            where = " AND ".join(f"{cols[c]} = '{v}'" for c, v in FILTER)
            out[label] = [
                *self.FORMATS,
                f"SELECT count(*), sum(TRY_CAST({cols['TOTALCLEARED']} AS DOUBLE)) "
                f"FROM {source.format(fmt=fmt)} WHERE {where}",
            ]
        return out

    def files_diagnostics(self, path: str, sas: str) -> dict[str, str]:
        out = {}
        for label, (source, fmt, cols) in self._csv_sources(path, sas).items():
            out[f"{label}: I/UNIT/VERSION"] = (
                f"SELECT {cols['I']}, {cols['UNIT']}, {cols['VERSION']}, count(*) "
                f"FROM {source.format(fmt=fmt)} GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 12"
            )
        return out

    def write_variants(self, table: str, source: str) -> dict[str, list[str]]:
        """Created through the catalog unstaged, filled by one Databend INSERT, as for Trino and
        Doris: the catalog refuses staged creates."""
        _create_unstaged(self.cfg, table)
        return {
            "catalog create + Databend INSERT": [
                f"INSERT INTO onelake.{WRITE_NS}.{table} SELECT * FROM {source}",
            ],
        }


CANDIDATES = {
    "starrocks": StarRocks,
    "trino": Trino,
    "doris": Doris,
    "databend": Databend,
}


def _try(engine: Candidate, label: str, statements: list) -> tuple[bool, list[tuple]]:
    """Run a variant's steps in order: SQL strings through the engine, callables in Python."""
    started = time.perf_counter()
    rows: list[tuple] = []
    try:
        for statement in statements:
            rows = (statement() or []) if callable(statement) else engine.sql(statement)
    except Exception as exc:  # noqa: BLE001 - reporting failures is this script's job
        took = time.perf_counter() - started
        _say(f"    FAIL  {label}  ({took:.1f}s)  {scrub.scrub_exc(exc, 6000)}")
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
        engine.recover()
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
