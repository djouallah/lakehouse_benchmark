"""What the OneLake Iceberg REST catalog lets Databend do.

Databend's Iceberg tables are its fork of iceberg-rust: the REST catalog, opendal's Azdls for the
files, and iceberg-rust's Transaction API for commits. Every statement is Databend's own SQL and
planning (src/query/storages/iceberg). From its source: CREATE TABLE is iceberg-rust's
`create_table`, one unstaged request with no location, so the catalog assigns it; PARTITION BY
takes identity columns only; the one write it commits is `fast_append`.

THE SERVER IS bench.databend's: the all-in-one image, the catalog on the bearer, the files on a
user delegation SAS that can also write, since the probes' INSERTs write data files. A second
catalog, `race`, points at the race proxy for the concurrency probes; the container reaches the
runner's proxy through host.docker.internal. Databend caches a loaded Iceberg table for 10
minutes, so a probe that changes a table through pyiceberg and then writes through Databend asks
what Databend does with the table it already holds.

EVERY PROBE CHECKS ITS EFFECT through pyiceberg, which reads what the CATALOG now says rather
than what Databend cached. Accepted with no effect is a no-op, not a yes. Every error Databend
raises is a `no` carrying its full message, so the log says whose limit it is.
"""

from __future__ import annotations

import os
import sys

from harness import (
    BROKEN,
    EVOLVE_FIRST,
    EVOLVE_THEN,
    MAX_EXPECTED,
    NAMESPACE,
    NESTED_TYPES,
    NOOP,
    PRUNE_EXPECTED,
    PRUNE_ID,
    REFUSED,
    SCALAR_TYPES,
    SKIPPED,
    SUPPORTED,
    TEMPORAL,
    TRANSFORM_ROWS,
    NoOp,
    Refused,
    Report,
    Skip,
    any_wanted,
    asked,
    evolution_check,
    evolve_spec,
    iceberg_schema,
    partition_result,
    promotion_check,
    read_without_files,
    remove_data_files,
    rows,
    transform_case,
    truncate_kind,
    type_result,
    verdict,
    wanted,
    write_stats_files,
)
from isolation_duckdb import race_probe

from bench import auth, databend, scrub
from bench.capability.race import RaceProxy
from bench.config import Config

CATALOG = databend.CATALOG
SEED = [(1, 10), (2, 20), (3, 30)]


def _one_line(text: object, limit: int = 700) -> str:
    flat = " ".join(scrub.scrub(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _say(text: object) -> None:
    print(scrub.scrub(text), flush=True)


# Column type and literal for each TYPE_EXPECTED case, in Databend's spelling.
TYPES = {
    "decimal": ("DECIMAL(10, 2)", "CAST('12.34' AS DECIMAL(10, 2))"),
    "date": ("DATE", "CAST('2026-01-02' AS DATE)"),
    "timestamp": ("TIMESTAMP", "CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP)"),
    "timestamptz": ("TIMESTAMP_TZ", "CAST('2026-01-02 03:04:05.123456 +00:00' AS TIMESTAMP_TZ)"),
    "uuid": ("UUID", "CAST('6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab' AS UUID)"),
    "binary": ("BINARY", "FROM_HEX('0102')"),
    "struct": ("TUPLE(a BIGINT, b VARCHAR)", "CAST((1, 'x') AS TUPLE(a BIGINT, b VARCHAR))"),
    "list": ("ARRAY(BIGINT)", "CAST([1, 2, 3] AS ARRAY(BIGINT))"),
    "map": ("MAP(VARCHAR, BIGINT)", "CAST({'k': 1} AS MAP(VARCHAR, BIGINT))"),
}


def _union(rows_: list[str]) -> str:
    return " UNION ALL ".join(f"SELECT {r}" for r in rows_)


def _select(pairs) -> str:
    """A SELECT yielding BIGINT (id, v) rows, so no insert depends on implicit casts."""
    return _union([f"CAST({i} AS BIGINT) AS id, CAST({v} AS BIGINT) AS v" for i, v in pairs])


def _transform_sql(kind: str) -> tuple[str, str, str]:
    """(columns, PARTITION BY expression, INSERT's SELECT) for one transform_case."""
    if kind == "bucket":
        columns, part = "id BIGINT, x BIGINT", "bucket(4, id)"
        pairs = TRANSFORM_ROWS["bucket"]
        select = _union([f"CAST({i} AS BIGINT), CAST({v} AS BIGINT)" for i, v in pairs])
    elif kind == "truncate":
        columns, part = "id BIGINT, x VARCHAR", "truncate(2, x)"
        select = _union([f"CAST({i} AS BIGINT), '{v}'" for i, v in TRANSFORM_ROWS["truncate"]])
    else:
        columns, part = "id BIGINT, x TIMESTAMP", f"{kind}(x)"
        pairs = TRANSFORM_ROWS["temporal"]
        select = _union([f"CAST({i} AS BIGINT), CAST('{v}' AS TIMESTAMP)" for i, v in pairs])
    return columns, part, select


class DatabendCapability:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.conn = None
        self.report = Report(reason=lambda exc: _one_line(scrub.scrub_exc(exc, 2000), 600))
        self.run = "{}_{}".format(
            os.environ.get("GITHUB_RUN_ID", "local"),
            os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
        )
        self.keep = os.environ.get("CAPABILITY_KEEP") == "1"
        self.ns = NAMESPACE
        self.version = "no session"
        self.can_create = False
        self.created: list[str] = []
        self.namespaces: list[str] = []
        self._catalog = None
        self.proxy = None

    # -- helpers -----------------------------------------------------------------------------

    def sql(self, statement: str) -> list[tuple]:
        """One statement. Databend's own `no` becomes `Refused`, with its code and message."""
        _say(f"       > {_one_line(statement, 300)}")
        try:
            with self.conn.cursor() as cur:
                cur.execute(statement)
                return [tuple(row) for row in cur.fetchall()]
        except Exception as exc:  # noqa: BLE001 - every Databend error is an answer
            raise Refused(_one_line(f"{type(exc).__name__}: {exc}")) from None

    def name(self, what: str) -> str:
        return f"db_{self.run}_{what}"

    def t(self, table: str, ns: str | None = None, catalog: str = CATALOG) -> str:
        return f'{catalog}."{ns or self.ns}"."{table}"'

    def t2(self, table: str) -> str:
        """database.table in the current catalog (onelake): OPTIMIZE takes no catalog part."""
        return f'"{self.ns}"."{table}"'

    def pyiceberg(self):
        if self._catalog is None:
            self._catalog = auth.catalog(self.cfg)
        return self._catalog

    def iceberg(self, table: str):
        return self.pyiceberg().load_table((self.ns, table))

    def _snapshots(self, table: str) -> list:
        return sorted(
            self.iceberg(table).metadata.snapshots,
            key=lambda s: (s.sequence_number or 0, s.timestamp_ms),
        )

    def _py_rows(self, table: str, cols=("id", "v")):
        try:
            arrow = self.iceberg(table).scan(selected_fields=cols).to_arrow()
            return sorted(tuple(row[c] for c in cols) for row in arrow.to_pylist())
        except Exception as exc:  # noqa: BLE001 - a cross-check must not decide the answer
            return f"pyiceberg could not read it: {_one_line(scrub.scrub_exc(exc, 300), 300)}"

    def _own_rows(self, table: str, cols: str = "id, v"):
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT {cols} FROM {self.t(table)} ORDER BY id")
                return [tuple(r) for r in cur.fetchall()]
        except Exception as exc:  # noqa: BLE001 - a read-back must not decide the answer
            return f"Databend could not read it back: {_one_line(exc, 300)}"

    def _expect(self, table: str, expected: list[tuple], what: str, cols=("id", "v")) -> str:
        """pyiceberg decides, because it reads the catalog; Databend's own read is reported."""
        py = self._py_rows(table, cols)
        own = self._own_rows(table, ", ".join(cols))
        if py != expected:
            raise NoOp(
                f"{what} returned success; expected {expected}, pyiceberg reads {py}, "
                f"Databend reads {own}"
            )
        agree = "Databend agrees" if own == py else f"Databend reads {own}"
        return f"{what}: pyiceberg reads {py}; {agree}"

    def _delete_files(self, table: str) -> str:
        try:
            tbl = self.iceberg(table)
            kinds: dict[str, int] = {}
            for manifest in tbl.current_snapshot().manifests(tbl.io):
                for entry in manifest.fetch_manifest_entry(tbl.io, discard_deleted=True):
                    kind = entry.data_file.content.name.lower()
                    if kind != "data":
                        kinds[kind] = kinds.get(kind, 0) + 1
            return ", ".join(f"{n} {k}" for k, n in sorted(kinds.items())) or "no delete files"
        except Exception as exc:  # noqa: BLE001 - informational only
            return f"delete files unreadable: {_one_line(exc, 200)}"

    def _create(self, what: str, columns: str = "id BIGINT, v BIGINT", tail: str = "") -> str:
        table = self.name(what)
        self.sql(f"CREATE TABLE {self.t(table)} ({columns}) ENGINE = ICEBERG {tail}")
        self.created.append(table)
        return table

    def _fresh(self, what: str, tail: str = "") -> str:
        """A table holding SEED, made the way the create probe proved works."""
        if not self.can_create:
            raise Skip("no table could be created, so there is nothing to write to")
        try:
            table = self._create(what, tail=tail)
            self.sql(f"INSERT INTO {self.t(table)} {_select(SEED)}")
        except Refused as exc:
            raise Skip(f"could not seed the table: {exc}") from None
        return table

    # -- session -----------------------------------------------------------------------------

    def session(self) -> str:
        self.proxy = RaceProxy(bind="0.0.0.0").start()
        databend.start()
        self.conn = databend.connect()
        token = auth.onelake_token()
        sas, _ = databend.sas(self.cfg, write=True)
        databend.attach(self.conn, self.cfg, token, sas)
        try:
            databend.attach(
                self.conn,
                self.cfg,
                token,
                sas,
                name="race",
                address=self.proxy.endpoint.replace("127.0.0.1", "host.docker.internal"),
                use=False,
            )
            race = "race catalog attached"
        except Exception as exc:  # noqa: BLE001 - the race probes say so on their own
            race = f"race catalog refused: {_one_line(exc, 300)}"
        self.version = f"{databend.version(self.conn)} ({databend.IMAGE})"
        listed = (self.ns,) in self.pyiceberg().list_namespaces()
        seen = self.sql(f"SHOW DATABASES FROM {CATALOG} LIKE '{self.ns}'")
        return (
            f"Databend {self.version}; {NAMESPACE} {'listed' if listed else 'NOT listed'}, "
            f"{'seen' if seen else 'NOT seen'} by Databend; {race}"
        )

    # -- create ------------------------------------------------------------------------------

    def create_table(self) -> str:
        table = self._create("base")
        self.can_create = True
        tbl = self.iceberg(table)
        ids = [f.field_id for f in tbl.schema().fields]
        location = tbl.metadata.location
        return (
            f"field ids {ids}, format-version {tbl.metadata.format_version}, "
            f"at the catalog's location `…{location[location.find('/Tables/') :]}`"
        )

    def create_if_not_exists(self) -> str:
        table = self._fresh("ine")
        self.sql(
            f"CREATE TABLE IF NOT EXISTS {self.t(table)} (id BIGINT, v BIGINT, extra BIGINT) "
            "ENGINE = ICEBERG"
        )
        names = [f.name for f in self.iceberg(table).schema().fields]
        rows_ = self._py_rows(table)
        if "extra" in names or rows_ != SEED:
            raise NoOp(f"IF NOT EXISTS replaced the existing table: columns {names}, rows {rows_}")
        return f"accepted, the existing table untouched: columns {names}, rows {rows_}"

    def ctas(self) -> str:
        table = self.name("ctas")
        self.created.append(table)
        self.sql(f"CREATE TABLE {self.t(table)} ENGINE = ICEBERG AS {_select(SEED)}")
        return self._expect(table, SEED, "CTAS")

    def partitioned(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create("part", "id BIGINT, v BIGINT, p VARCHAR", "PARTITION BY (p)")
        self.sql(
            f"INSERT INTO {self.t(table)} "
            + _union(
                [
                    "CAST(1 AS BIGINT), CAST(10 AS BIGINT), 'a'",
                    "CAST(2 AS BIGINT), CAST(20 AS BIGINT), 'a'",
                    "CAST(3 AS BIGINT), CAST(30 AS BIGINT), 'b'",
                ]
            )
        )
        spec = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("PARTITION BY returned success and the spec is empty")
        files = len(list(self.iceberg(table).scan().plan_files()))
        return f"spec {spec}; 3 rows over 2 partitions wrote {files} data file(s)"

    def _transform(self, kind: str) -> tuple[str, str, str]:
        columns, part, select = _transform_sql(kind)
        _, _, parts = transform_case(kind)
        try:
            table = self._create(f"tr_{kind}", columns, f"PARTITION BY ({part})")
            self.sql(f"INSERT INTO {self.t(table)} {select}")
        except Refused as exc:
            return kind, REFUSED, str(exc)
        return partition_result(self.iceberg(table), kind, parts)

    def _transforms(self, kinds) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        return verdict([self._transform(kind) for kind in kinds])

    def partition_bucket(self) -> str:
        return self._transforms(["bucket"])

    def partition_truncate(self) -> str:
        return self._transforms(["truncate"])

    def partition_temporal(self) -> str:
        return self._transforms(TEMPORAL)

    def _typed(self, name: str) -> tuple[str, str, str]:
        column, literal = TYPES[name]
        try:
            table = self._create(f"type_{name}", f"id BIGINT, x {column}")
            self.sql(f"INSERT INTO {self.t(table)} SELECT CAST(1 AS BIGINT), {literal}")
        except Refused as exc:
            return name, REFUSED, str(exc)
        return type_result(self.iceberg(table), name)

    def _types(self, names) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        return verdict([self._typed(name) for name in names])

    def types_scalar(self) -> str:
        return self._types(SCALAR_TYPES)

    def types_nested(self) -> str:
        return self._types(NESTED_TYPES)

    def sort_order(self) -> str:
        table = self._fresh("sorted")
        self.sql(f"ALTER TABLE {self.t(table)} CLUSTER BY (id)")
        if not self.iceberg(table).sort_order().fields:
            raise NoOp("CLUSTER BY returned success and the table has no sort order")
        return "sort order set"

    # -- write -------------------------------------------------------------------------------

    def insert_into(self) -> str:
        table = self._fresh("insert")
        return self._expect(table, SEED, "INSERT INTO") + (
            f"; {len(self._snapshots(table))} commit(s)"
        )

    def insert_select(self) -> str:
        table = self._fresh("insertsel")
        self.sql(f"INSERT INTO {self.t(table)} SELECT id + 10, v FROM {self.t(table)}")
        return self._expect(table, SEED + [(11, 10), (12, 20), (13, 30)], "INSERT INTO ... SELECT")

    def delete_from(self) -> str:
        table = self._fresh("delete")
        self.sql(f"DELETE FROM {self.t(table)} WHERE id = 1")
        return f"{self._expect(table, SEED[1:], 'DELETE')}; {self._delete_files(table)}"

    def update(self) -> str:
        table = self._fresh("update")
        self.sql(f"UPDATE {self.t(table)} SET v = 999 WHERE id = 1")
        return self._expect(table, [(1, 999), (2, 20), (3, 30)], "UPDATE")

    def merge_upsert(self) -> str:
        table = self._fresh("merge")
        self.sql(
            f"MERGE INTO {self.t(table)} AS t USING ({_select([(1, 777), (9, 90)])}) AS s "
            f"ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v "
            f"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
        return (
            self._expect(table, [(1, 777), (2, 20), (3, 30), (9, 90)], "MERGE, UPDATE + INSERT")
            + f"; {len(self._snapshots(table)) - 1} commit(s) after the seed; "
            + self._delete_files(table)
        )

    def truncate(self) -> str:
        table = self._fresh("truncate")
        self.sql(f"TRUNCATE TABLE {self.t(table)}")
        result = self._expect(table, [], "TRUNCATE")
        return f"{result}; {truncate_kind(self.iceberg(table))}"

    def insert_overwrite(self) -> str:
        """Databend's whole-table overwrite is INSERT OVERWRITE; an append instead is a no-op."""
        table = self._fresh("overwrite")
        uuid = self.iceberg(table).metadata.table_uuid
        before = len(self._snapshots(table))
        self.sql(f"INSERT OVERWRITE {self.t(table)} {_select([(7, 70), (8, 80)])}")
        snapshots = self._snapshots(table)
        summary = snapshots[-1].summary
        operation = summary.operation.value if summary is not None else "?"
        same = "same table" if self.iceberg(table).metadata.table_uuid == uuid else "a NEW table"
        result = self._expect(table, [(7, 70), (8, 80)], "INSERT OVERWRITE")
        return (
            f"{result}; {same}, {len(snapshots) - before} snapshot(s) added, operation {operation}"
        )

    # -- schema ------------------------------------------------------------------------------

    def add_column(self) -> str:
        table = self._fresh("addcol")
        self.sql(f"ALTER TABLE {self.t(table)} ADD COLUMN w BIGINT")
        names = [f.name for f in self.iceberg(table).schema().fields]
        if "w" not in names:
            raise NoOp(f"returned success and the schema is {names}")
        return f"schema {names}"

    def drop_column(self) -> str:
        table = self._fresh("dropcol")
        self.sql(f"ALTER TABLE {self.t(table)} DROP COLUMN v")
        names = [f.name for f in self.iceberg(table).schema().fields]
        if "v" in names:
            raise NoOp(f"returned success and the schema is still {names}")
        return f"schema now {names}"

    def rename_column(self) -> str:
        table = self._fresh("renamecol")
        self.sql(f"ALTER TABLE {self.t(table)} RENAME COLUMN v TO v2")
        names = [f.name for f in self.iceberg(table).schema().fields]
        if "v2" not in names:
            raise NoOp(f"returned success and the schema is still {names}")
        return self._expect(table, SEED, "renamed, data kept", ("id", "v2"))

    def type_promotion(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create("promote", "id BIGINT, c INT")
        self.sql(f"INSERT INTO {self.t(table)} SELECT CAST(1 AS BIGINT), CAST(7 AS INT)")
        self.sql(f"ALTER TABLE {self.t(table)} MODIFY COLUMN c BIGINT")
        return promotion_check(self.iceberg(table))

    def partition_evolution(self) -> str:
        table = self._fresh("specevo")
        self.sql(f"ALTER TABLE {self.t(table)} PARTITION BY (id)")
        spec = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("returned success and the table is still unpartitioned")
        return f"spec now {spec}"

    def write_after_evolution(self) -> str:
        """pyiceberg adds bucket(4, id), as for every engine; Databend writes before and after."""
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create("evowrite")
        self.sql(f"INSERT INTO {self.t(table)} {_select(EVOLVE_FIRST)}")
        evolve_spec(self.iceberg(table))
        self.sql(f"INSERT INTO {self.t(table)} {_select(EVOLVE_THEN)}")
        count = self.sql(f"SELECT count(*) FROM {self.t(table)}")[0][0]
        return evolution_check(self.iceberg(table), count)

    def set_property(self) -> str:
        table = self._fresh("props")
        self.sql(f"ALTER TABLE {self.t(table)} SET OPTIONS (probed_at = '{self.run}')")
        if self.iceberg(table).properties.get("probed_at") != self.run:
            raise NoOp("returned success and the property is not on the table")
        return "the property is on the table"

    # -- read --------------------------------------------------------------------------------

    def time_travel(self) -> str:
        table = self._fresh("travel")
        seeded = self._snapshots(table)[-1].snapshot_id
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        found = self.sql(f"SELECT count(*) FROM {self.t(table)} AT (SNAPSHOT => '{seeded}')")
        if found[0][0] != 3:
            raise NoOp(f"AT (SNAPSHOT => the seed's) reads {found[0][0]} rows, not 3")
        return "AT (SNAPSHOT => the seed's snapshot) reads 3 rows (the table has 4)"

    def metadata_tables(self) -> str:
        table = self._fresh("inspect")
        found = self.sql(f"SELECT count(*) FROM iceberg_snapshot('{self.ns}', '{table}')")
        return f"iceberg_snapshot() reads {found[0][0]} snapshot(s)"

    def _stats_table(self, what: str) -> str:
        """harness.STATS_FILES, written by pyiceberg: four files with known min/max bounds."""
        table = self.name(what)
        self.pyiceberg().create_table((self.ns, table), iceberg_schema())
        self.created.append(table)
        write_stats_files(self.iceberg(table))
        return table

    def select_table(self) -> str:
        """pyiceberg creates and seeds the table; the engine reads it by its catalog name."""
        table = self.name("select")
        self.pyiceberg().create_table((self.ns, table), schema=iceberg_schema())
        self.created.append(table)
        self.pyiceberg().load_table((self.ns, table)).append(rows(SEED))
        found = sorted(
            tuple(int(x) for x in r) for r in self.sql(f"SELECT id, v FROM {self.t(table)}")
        )
        if found != SEED:
            raise NoOp(f"reads {found}, pyiceberg wrote {SEED}")
        return f"SELECT id, v FROM the catalog table reads the {len(SEED)} rows pyiceberg wrote"

    def file_pruning(self) -> str:
        table = self._stats_table("prune")
        gone = remove_data_files(self.iceberg(table), keep_id=PRUNE_ID)
        return read_without_files(
            f"SELECT v WHERE id = {PRUNE_ID}",
            lambda: self.sql(f"SELECT v FROM {self.t(table)} WHERE id = {PRUNE_ID}"),
            PRUNE_EXPECTED,
            lambda: self.sql(f"SELECT sum(v) FROM {self.t(table)}"),
            gone,
        )

    def max_from_metadata(self) -> str:
        table = self._stats_table("max")
        gone = remove_data_files(self.iceberg(table))
        return read_without_files(
            "SELECT max(id)",
            lambda: self.sql(f"SELECT max(id) FROM {self.t(table)}"),
            MAX_EXPECTED,
            lambda: self.sql(f"SELECT sum(v) FROM {self.t(table)}"),
            gone,
        )

    # -- refs --------------------------------------------------------------------------------

    def create_branch(self) -> str:
        table = self._fresh("branch")
        self.sql(f"ALTER TABLE {self.t(table)} CREATE BRANCH probe_branch")
        refs = sorted(self.iceberg(table).metadata.refs)
        if "probe_branch" not in refs:
            raise NoOp(f"returned success and the refs are {refs}")
        return f"refs now {refs}"

    def create_tag(self) -> str:
        table = self._fresh("tag")
        self.sql(f"ALTER TABLE {self.t(table)} CREATE TAG probe_tag")
        refs = sorted(self.iceberg(table).metadata.refs)
        if "probe_tag" not in refs:
            raise NoOp(f"returned success and the refs are {refs}")
        return f"refs now {refs}"

    # -- maintenance -------------------------------------------------------------------------

    def expire_snapshots(self) -> str:
        """Databend's history clean-up is OPTIMIZE ... PURGE."""
        table = self._fresh("expire")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(5, 50)])}")
        before = len(self._snapshots(table))
        self.sql(f"OPTIMIZE TABLE {self.t2(table)} PURGE")
        after = len(self._snapshots(table))
        if after >= before:
            raise NoOp(f"returned success and history went {before} -> {after}")
        return f"history {before} -> {after} snapshot(s)"

    def rewrite_data_files(self) -> str:
        table = self._fresh("compact")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        before = len(list(self.iceberg(table).scan().plan_files()))
        self.sql(f"OPTIMIZE TABLE {self.t2(table)} COMPACT")
        after = len(list(self.iceberg(table).scan().plan_files()))
        if after >= before:
            raise NoOp(f"returned success and data files went {before} -> {after}")
        return self._expect(table, SEED + [(4, 40)], f"{before} -> {after} data file(s), data kept")

    def rewrite_manifests(self) -> str:
        table = self._fresh("manifests")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        before = len(self._snapshots(table))
        self.sql(f"OPTIMIZE TABLE {self.t2(table)} COMPACT SEGMENT")
        if len(self._snapshots(table)) == before:
            raise NoOp("returned success and no new snapshot was committed")
        return "a new snapshot rewrote the manifests"

    # -- catalog -----------------------------------------------------------------------------

    def namespace_create(self) -> str:
        ns = self.name("ns")
        self.sql(f'CREATE DATABASE {CATALOG}."{ns}"')
        self.namespaces.append(ns)
        if (ns,) not in self.pyiceberg().list_namespaces():
            raise NoOp("CREATE DATABASE returned success and pyiceberg does not list it")
        return f"`{ns}` created; pyiceberg lists it"

    def namespace_drop(self) -> str:
        if not self.namespaces:
            raise Skip("no namespace was created to drop")
        ns = self.namespaces[-1]
        self.sql(f'DROP DATABASE {CATALOG}."{ns}"')
        if (ns,) in self.pyiceberg().list_namespaces():
            raise NoOp("DROP DATABASE returned success and pyiceberg still lists it")
        self.namespaces.remove(ns)
        return "the empty namespace is gone"

    def namespace_existing(self) -> str:
        self.sql(f'CREATE DATABASE IF NOT EXISTS {CATALOG}."{self.ns}"')
        return f"accepted on the existing `{self.ns}`"

    def drop_purge(self) -> str:
        """Databend's DROP TABLE on an Iceberg table, then whether its files are gone."""
        table = self._fresh("droppable")
        tbl = self.iceberg(table)
        data = [task.file.file_path for task in tbl.scan().plan_files()]
        self.sql(f"DROP TABLE {self.t(table)}")
        if self.pyiceberg().table_exists((self.ns, table)):
            raise NoOp("returned success and the catalog still has it")
        self.created.remove(table)
        try:
            left = sum(1 for path in data if tbl.io.new_input(path).exists())
        except Exception as exc:  # noqa: BLE001 - informational
            return f"gone from the catalog; could not check storage: {_one_line(exc, 200)}"
        if left:
            raise NoOp(f"gone from the catalog, and {left} of {len(data)} data file(s) remain")
        return f"gone from the catalog; 0 of {len(data)} data file(s) still in storage"

    # -- concurrency -------------------------------------------------------------------------

    def _race(self, key: str, statement: str) -> str:
        """pyiceberg makes and seeds the table; Databend writes through the `race` catalog, whose
        commit bench.capability.race holds until pyiceberg's has landed."""
        if self.conn is None:
            raise Skip("no Databend session")
        table = self.name(key)
        self.pyiceberg().create_table((self.ns, table), schema=iceberg_schema())
        self.created.append(table)
        self.iceberg(table).append(rows(SEED))
        raced = self.t(table, catalog="race")
        write = lambda: self.sql(statement.format(t=raced))  # noqa: E731
        try:
            return race_probe(key, self.pyiceberg(), self.proxy, table, write)
        except Exception as exc:  # noqa: BLE001 - re-raised unless it is Databend's own no
            # No commit was tried because the statement itself is refused on Iceberg tables:
            # that is the engine's `no`, not a probe that could not ask.
            if "commits [none]" in str(exc) and "does not support" in str(exc):
                raise Refused(str(exc)) from None
            raise

    def race_append(self) -> str:
        return self._race("race_append", "INSERT INTO {t} " + _select([(5, 50)]))

    def race_delete(self) -> str:
        return self._race("race_delete", "DELETE FROM {t} WHERE id = 1")

    def race_update(self) -> str:
        return self._race("race_update", "UPDATE {t} SET v = v + 1 WHERE id = 2")

    # -- teardown ----------------------------------------------------------------------------

    def drop_everything(self) -> None:
        if not (self.created or self.namespaces):
            return
        if self.keep:
            print(f"\nCAPABILITY_KEEP=1; left {len(self.created)} table(s) behind")
            return
        dropped, left = 0, []
        for table in self.created:
            try:
                if self.pyiceberg().table_exists((self.ns, table)):
                    self.pyiceberg().purge_table((self.ns, table))
                dropped += 1
            except Exception as exc:  # noqa: BLE001 - teardown is best effort, by design
                left.append(f"{table} ({_one_line(exc, 80)})")
        for ns in self.namespaces:
            try:
                self.pyiceberg().drop_namespace(ns)
            except Exception as exc:  # noqa: BLE001
                left.append(f"namespace {ns} ({_one_line(exc, 80)})")
        print(f"\ndropped {dropped} probe table(s)" + (f"; left behind {left}" if left else ""))


PROBES = [
    ("session", "Databend starts and attaches the REST catalog", "session"),
    ("create", "CREATE TABLE ENGINE = ICEBERG (the catalog assigns the location)", "create_table"),
    ("create", "CREATE TABLE IF NOT EXISTS on an existing table", "create_if_not_exists"),
    ("create", "CREATE TABLE ... AS SELECT", "ctas"),
    ("create", "PARTITION BY (p), then INSERT", "partitioned"),
    ("create", "ALTER TABLE CLUSTER BY (sort order)", "sort_order"),
    ("create", "PARTITION BY (bucket(4, id)), then INSERT", "partition_bucket"),
    ("create", "PARTITION BY (truncate(2, x)), then INSERT", "partition_truncate"),
    ("create", "PARTITION BY year / month / day / hour, then INSERT", "partition_temporal"),
    ("create", "types: decimal, date, timestamp, timestamptz, uuid, binary", "types_scalar"),
    ("create", "nested types: struct, list, map", "types_nested"),
    ("write", "INSERT INTO", "insert_into"),
    ("write", "INSERT INTO ... SELECT", "insert_select"),
    ("write", "DELETE FROM", "delete_from"),
    ("write", "UPDATE", "update"),
    ("write", "MERGE, two actions: MATCHED UPDATE + NOT MATCHED INSERT", "merge_upsert"),
    ("write", "TRUNCATE TABLE", "truncate"),
    ("write", "INSERT OVERWRITE (whole-table overwrite)", "insert_overwrite"),
    ("schema", "ALTER TABLE ADD COLUMN", "add_column"),
    ("schema", "ALTER TABLE DROP COLUMN", "drop_column"),
    ("schema", "ALTER TABLE RENAME COLUMN", "rename_column"),
    ("schema", "ALTER TABLE MODIFY COLUMN c BIGINT (int -> long)", "type_promotion"),
    ("schema", "ALTER TABLE PARTITION BY (partition evolution)", "partition_evolution"),
    ("schema", "INSERT, pyiceberg adds bucket(4, id), INSERT", "write_after_evolution"),
    ("schema", "ALTER TABLE SET OPTIONS", "set_property"),
    ("read", "time travel, AT (SNAPSHOT => ...)", "time_travel"),
    ("read", "metadata tables (iceberg_snapshot)", "metadata_tables"),
    ("read", "SELECT a table by its catalog name", "select_table"),
    ("read", "WHERE id = 22 with the other data files deleted (file pruning)", "file_pruning"),
    ("read", "max(id) with every data file deleted (min/max from metadata)", "max_from_metadata"),
    ("refs", "ALTER TABLE CREATE BRANCH", "create_branch"),
    ("refs", "ALTER TABLE CREATE TAG", "create_tag"),
    ("maintenance", "OPTIMIZE TABLE PURGE (expire snapshots)", "expire_snapshots"),
    ("maintenance", "OPTIMIZE TABLE COMPACT (compaction)", "rewrite_data_files"),
    ("maintenance", "OPTIMIZE TABLE COMPACT SEGMENT (rewrite manifests)", "rewrite_manifests"),
    ("catalog", "CREATE DATABASE (namespace)", "namespace_create"),
    ("catalog", "DROP DATABASE (namespace)", "namespace_drop"),
    ("catalog", "CREATE DATABASE IF NOT EXISTS, existing namespace", "namespace_existing"),
    ("catalog", "DROP TABLE, its files removed", "drop_purge"),
    ("concurrency", "INSERT INTO; B appends between Databend's read and commit", "race_append"),
    ("concurrency", "DELETE id 1; B appends", "race_delete"),
    ("concurrency", "UPDATE v = v + 1 on id 2; B appends", "race_update"),
]


def write_step_summary(probe: DatabendCapability) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    counts = probe.report.tally()
    lines = [
        "## OneLake Iceberg REST catalog, asked by Databend",
        "",
        f"{counts[SUPPORTED]} supported · {counts[REFUSED]} refused · "
        f"{counts[NOOP]} accepted then ignored · "
        f"{counts[SKIPPED]} skipped · {counts[BROKEN]} could not be asked",
        "",
        *probe.report.markdown(),
        "",
        f"Databend `{probe.version}` · namespace `{probe.ns}` · run `{probe.run}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    if not any_wanted(PROBES):
        print(f"this run asks none of {', '.join(sorted(asked()))} of this client")
        return 0
    cfg = Config.from_env()
    probe = DatabendCapability(cfg)
    print(f"workspace {cfg.workspace_id}  lakehouse {cfg.lakehouse_id}  namespace {NAMESPACE}")

    try:
        for group, question, method in PROBES:
            if not wanted(method):
                continue
            ok = probe.report.run(group, question, getattr(probe, method))
            if not ok and method == "session":
                print("\nstopped: no Databend session, so nothing below could be asked")
                write_step_summary(probe)
                return 1
    finally:
        try:
            probe.drop_everything()
        except Exception as exc:  # noqa: BLE001 - teardown must not mask the findings
            print(f"  warning: teardown failed: {_one_line(exc, 200)}")
        if probe.conn is not None:
            probe.conn.close()
        if probe.proxy is not None:
            probe.proxy.close()

    counts = probe.report.tally()
    print("\n" + "\n".join(probe.report.markdown()))
    print(
        f"\n{counts[SUPPORTED]} supported, {counts[REFUSED]} refused, "
        f"{counts[NOOP]} accepted then ignored, "
        f"{counts[SKIPPED]} skipped, {counts[BROKEN]} could not be asked"
    )
    write_step_summary(probe)
    probe.report.save("databend", probe.version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
