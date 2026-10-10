"""What the OneLake Iceberg REST catalog lets DuckDB do.

WHAT DUCKDB CAN WRITE HERE, AND WHOSE LIMIT EACH `no` IS. DuckDB's iceberg extension attaches this
endpoint as a REST catalog and plans, writes and commits Iceberg itself. It tracks the NIGHTLY
CLI (capability.yml installs it; requirements/capability_duckdb.txt has no duckdb), because that
is where the extension's Iceberg write support moves. This script sends it the Iceberg write and
DDL vocabulary one statement at a time and checks what each statement DID.

A `no` carrying the catalog's REST error is the catalog's; any other `no` is DuckDB's. Where
DuckDB has no statement for an operation at all, the probe asks what the parser says rather than
skipping, so the `no` carries DuckDB's own words.

THE ATTACH is bench/capability/engines/duckdb_iceberg.py: the curl transport, an access-token
storage secret, ACCESS_DELEGATION_MODE 'none', and the two OneLake write flags. DuckDB has no
per-table location on CREATE, so every table here takes the location the catalog assigns.

EVERY PROBE CHECKS ITS EFFECT, twice where it is cheap: read back in DuckDB, and again through
pyiceberg, which reads what the CATALOG now says rather than what DuckDB cached. Accepted with no
effect is a no-op, not a yes.

Every error DuckDB raises is a `no` carrying its full message. The OneLake token is in the ATTACH,
so everything printed goes through bench.scrub.

"""

from __future__ import annotations

import contextlib
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
    evolution_check,
    iceberg_schema,
    partition_result,
    promotion_check,
    read_without_files,
    remove_data_files,
    transform_case,
    type_result,
    verdict,
    write_stats_files,
)

from bench import auth, scrub
from bench.capability.engines.duckdb_iceberg import CATALOG, attach, connect
from bench.capability.engines.duckdb_iceberg import version as duckdb_version
from bench.config import ICEBERG_ENDPOINT, Config, azure_transport

SEED = [(1, 10), (2, 20), (3, 30)]
SEED_SELECT = "SELECT * FROM (VALUES (1::BIGINT, 10::BIGINT), (2, 20), (3, 30)) AS s(id, v)"


def _one_line(text: object, limit: int = 700) -> str:
    flat = " ".join(scrub.scrub(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _say(text: object) -> None:
    print(scrub.scrub(text), flush=True)


# Column type and literal for each TYPE_EXPECTED case.
TYPES = {
    "decimal": ("DECIMAL(10, 2)", "12.34"),
    "date": ("DATE", "DATE '2026-01-02'"),
    "timestamp": ("TIMESTAMP", "TIMESTAMP '2026-01-02 03:04:05.123456'"),
    "timestamptz": ("TIMESTAMPTZ", "TIMESTAMPTZ '2026-01-02 03:04:05.123456+00'"),
    "uuid": ("UUID", "UUID '6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab'"),
    "binary": ("BLOB", "'\\x01\\x02'::BLOB"),
    "struct": ("STRUCT(a BIGINT, b VARCHAR)", "{'a': 1, 'b': 'x'}"),
    "list": ("BIGINT[]", "[1, 2, 3]"),
    "map": ("MAP(VARCHAR, BIGINT)", "MAP {'k': 1}"),
}


def _transform_sql(kind: str) -> tuple[str, str, str]:
    """(columns, partition transform, VALUES) for one transform_case."""
    if kind == "bucket":
        columns, part = "id BIGINT, x BIGINT", "bucket(4, id)"
        values = [f"({i}, {v})" for i, v in TRANSFORM_ROWS["bucket"]]
    elif kind == "truncate":
        columns, part = "id BIGINT, x VARCHAR", "truncate(2, x)"
        values = [f"({i}, '{v}')" for i, v in TRANSFORM_ROWS["truncate"]]
    else:
        columns, part = "id BIGINT, x TIMESTAMP", f"{kind}(x)"
        values = [f"({i}, TIMESTAMP '{v}')" for i, v in TRANSFORM_ROWS["temporal"]]
    return columns, part, ", ".join(values)


class DuckDBCapability:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.report = Report(reason=lambda exc: _one_line(scrub.scrub_exc(exc, 2000), 600))
        self.run = "{}_{}".format(
            os.environ.get("GITHUB_RUN_ID", "local"),
            os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
        )
        self.keep = os.environ.get("CAPABILITY_KEEP") == "1"
        self.ns = NAMESPACE
        self.version = "no session"
        self.attached = False
        self.can_create = False
        # (namespace, table, kind) for the teardown; kind is "table".
        self.created: list[tuple[str, str, str]] = []
        self.namespaces: list[str] = []
        self._c = None
        self._token = ""
        self._catalog = None

    # -- the connection ----------------------------------------------------------------------

    def _connect(self):
        conn = connect()
        attach(conn, self.cfg, self._token)
        return conn

    def sql(self, statement: str, echo: bool = True) -> list[tuple]:
        """One statement. DuckDB's own `no` becomes `Refused`."""
        if echo:
            _say(f"       > {_one_line(statement, 300)}")
        try:
            return list(self._c.execute(statement).fetchall())
        except RuntimeError as exc:
            raise Refused(_one_line(str(exc))) from None

    # -- naming ------------------------------------------------------------------------------

    def name(self, what: str) -> str:
        return f"dk_{self.run}_{what}"

    def t(self, table: str, ns: str | None = None) -> str:
        return f'{CATALOG}."{ns or self.ns}"."{table}"'

    # -- pyiceberg, for what the catalog says ------------------------------------------------

    def pyiceberg(self):
        if self._catalog is None:
            self._catalog = auth.catalog(self.cfg)
        return self._catalog

    def iceberg(self, table: str, ns: str | None = None):
        return self.pyiceberg().load_table((ns or self.ns, table))

    def _snapshots(self, table: str) -> list:
        return sorted(
            self.iceberg(table).metadata.snapshots,
            key=lambda s: (s.sequence_number or 0, s.timestamp_ms),
        )

    def _exists(self, table: str, ns: str | None = None) -> bool:
        return self.pyiceberg().table_exists((ns or self.ns, table))

    def _columns(self, table: str) -> list[str]:
        return [f.name for f in self.iceberg(table).schema().fields]

    # -- reading back ------------------------------------------------------------------------

    def _dk_rows(self, table: str, cols: str = "id, v"):
        # A fresh connection: what the catalog holds now, not this session's cache.
        conn = self._connect()
        try:
            return [
                tuple(r)
                for r in conn.execute(f"SELECT {cols} FROM {self.t(table)} ORDER BY id").fetchall()
            ]
        except Exception as exc:  # noqa: BLE001 - a read-back must not decide the answer
            return f"DuckDB could not read it back: {_one_line(scrub.scrub_exc(exc, 300), 300)}"
        finally:
            conn.close()

    def _py_rows(self, table: str, cols: tuple[str, ...] = ("id", "v"), snapshot_id=None):
        try:
            arrow = (
                self.iceberg(table).scan(snapshot_id=snapshot_id, selected_fields=cols).to_arrow()
            )
            return sorted(tuple(row[c] for c in cols) for row in arrow.to_pylist())
        except Exception as exc:  # noqa: BLE001 - a cross-check must not decide the answer
            return f"pyiceberg could not read it: {_one_line(scrub.scrub_exc(exc, 300), 300)}"

    def _expect(self, table: str, expected: list[tuple], what: str, cols=("id", "v")) -> str:
        dk = self._dk_rows(table, ", ".join(cols))
        py = self._py_rows(table, tuple(cols))
        if expected not in (dk, py):
            raise NoOp(
                f"{what} returned success; expected {expected}, DuckDB reads {dk}, "
                f"pyiceberg reads {py}"
            )
        agree = "pyiceberg agrees" if py == dk else f"pyiceberg reads {py}"
        return f"{what}: DuckDB reads {dk}; {agree}"

    # -- making tables -----------------------------------------------------------------------

    def _create(self, what: str, columns: str = "id BIGINT, v BIGINT", tail: str = "") -> str:
        table = self.name(what)
        self.sql(f"CREATE TABLE {self.t(table)} ({columns}) {tail}")
        self.created.append((self.ns, table, "table"))
        return table

    def _fresh(self, what: str) -> str:
        """A table holding SEED, made the way the create probe proved works."""
        if not self.can_create:
            raise Skip("no table could be created, so there is nothing to write to")
        try:
            table = self._create(what)
            self.sql(f"INSERT INTO {self.t(table)} VALUES (1, 10), (2, 20), (3, 30)")
        except Refused as exc:
            raise Skip(f"could not seed the table: {exc}") from None
        return table

    def _forget(self, table: str, ns: str | None = None) -> None:
        self.created = [c for c in self.created if (c[0], c[1]) != (ns or self.ns, table)]

    # -- session -----------------------------------------------------------------------------

    def session(self) -> str:
        self.version = duckdb_version()
        self._token = auth.onelake_token()
        self._c = self._connect()
        self.attached = True
        ext = self.sql(
            "SELECT extension_name, extension_version FROM duckdb_extensions() "
            "WHERE extension_name IN ('iceberg', 'azure', 'httpfs') AND loaded",
            echo=False,
        )
        schemas = [
            r[0]
            for r in self.sql(
                f"SELECT schema_name FROM duckdb_schemas() WHERE database_name = '{CATALOG}'",
                echo=False,
            )
        ]
        seen = "listed" if NAMESPACE in schemas else "NOT listed"
        exts = ", ".join(f"{n} {v}" for n, v in ext)
        return f"DuckDB {self.version} ({exts}); {len(schemas)} namespace(s), {NAMESPACE} {seen}"

    # -- probes: namespaces ------------------------------------------------------------------

    def namespace_existing(self) -> str:
        self.sql(f'CREATE SCHEMA IF NOT EXISTS {CATALOG}."{NAMESPACE}"')
        return f"accepted on the existing `{NAMESPACE}`"

    def create_schema(self) -> str:
        ns = self.name("ns")
        self.sql(f'CREATE SCHEMA {CATALOG}."{ns}"')
        self.namespaces.append(ns)
        if (ns,) not in self.pyiceberg().list_namespaces():
            raise NoOp("CREATE SCHEMA returned success and pyiceberg does not list it")
        return f"`{ns}` created; pyiceberg lists it"

    def drop_schema(self) -> str:
        if not self.namespaces:
            raise Skip("no namespace was created to drop")
        ns = self.namespaces[-1]
        self.sql(f'DROP SCHEMA {CATALOG}."{ns}"')
        self.namespaces.remove(ns)
        if (ns,) in self.pyiceberg().list_namespaces():
            raise NoOp("DROP SCHEMA returned success and pyiceberg still lists it")
        return "the empty namespace is gone"

    # -- probes: create ----------------------------------------------------------------------

    def create_table(self) -> str:
        table = self._create("base")
        self.can_create = True
        tbl = self.iceberg(table)
        ids = [f.field_id for f in tbl.schema().fields]
        tail = tbl.location().split(self.cfg.lakehouse_id, 1)[-1]
        return (
            f"created at the catalog's location `…{tail}`; field ids {ids}, "
            f"format-version {tbl.metadata.format_version}"
        )

    def create_if_not_exists(self) -> str:
        table = self._fresh("ine")
        self.sql(f"CREATE TABLE IF NOT EXISTS {self.t(table)} (id BIGINT, v BIGINT, extra BIGINT)")
        names = self._columns(table)
        rows = self._py_rows(table)
        if "extra" in names or rows != SEED:
            raise NoOp(f"IF NOT EXISTS replaced the existing table: columns {names}, rows {rows}")
        return f"accepted, the existing table untouched: columns {names}, rows {rows}"

    def ctas(self) -> str:
        table = self.name("ctas")
        self.sql(f"CREATE TABLE {self.t(table)} AS {SEED_SELECT}")
        self.created.append((self.ns, table, "table"))
        return self._expect(table, SEED, "CTAS")

    def create_partitioned(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create(
            "part", columns="id BIGINT, v BIGINT, p VARCHAR", tail="PARTITIONED BY (p)"
        )
        spec = [f"{f.transform}" for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("PARTITIONED BY returned success and the spec is empty")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b')")
        files = len(list(self.iceberg(table).scan().plan_files()))
        return f"spec {spec}; 3 rows over 2 partitions wrote {files} data file(s)"

    def _transform(self, kind: str) -> tuple[str, str, str]:
        columns, part, values = _transform_sql(kind)
        _, _, parts = transform_case(kind)
        try:
            table = self._create(f"tr_{kind}", columns, f"PARTITIONED BY ({part})")
            self.sql(f"INSERT INTO {self.t(table)} VALUES {values}")
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
            self.sql(f"INSERT INTO {self.t(table)} VALUES (1, {literal})")
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

    def create_sorted(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create("sorted")
        self.sql(f"ALTER TABLE {self.t(table)} SET SORTED BY (id)")
        order = self.iceberg(table).sort_order()
        if not order.fields:
            raise NoOp("SET SORTED BY returned success and the table has no sort order")
        return f"sort order {[str(f) for f in order.fields]}"

    def create_sorted_at_create(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create("sortedcreate", tail="SORTED BY (id)")
        order = self.iceberg(table).sort_order()
        if not order.fields:
            raise NoOp(
                "CREATE TABLE ... SORTED BY returned success and the table has no sort order"
            )
        return f"sort order {[str(f) for f in order.fields]}"

    # -- probes: write -----------------------------------------------------------------------

    def insert_values(self) -> str:
        table = self._fresh("insert")
        return self._expect(table, SEED, "INSERT INTO ... VALUES") + (
            f"; {len(self._snapshots(table))} commit(s)"
        )

    def insert_select(self) -> str:
        table = self._fresh("insertsel")
        self.sql(f"INSERT INTO {self.t(table)} SELECT id + 10, v FROM {self.t(table)}")
        return self._expect(table, SEED + [(11, 10), (12, 20), (13, 30)], "INSERT INTO ... SELECT")

    def _delete_files(self, table: str) -> int:
        return sum(len(task.delete_files) for task in self.iceberg(table).scan().plan_files())

    def delete_from(self) -> str:
        table = self._fresh("delete")
        self.sql(f"DELETE FROM {self.t(table)} WHERE id = 1")
        how = self._delete_files(table)
        return self._expect(table, SEED[1:], "DELETE") + (
            f"; {how} delete file(s) in the scan"
            + (" (merge-on-read)" if how else " (copy-on-write)")
        )

    def update(self) -> str:
        table = self._fresh("update")
        self.sql(f"UPDATE {self.t(table)} SET v = 999 WHERE id = 1")
        return self._expect(table, [(1, 999), (2, 20), (3, 30)], "UPDATE")

    def _merge(self, what: str, clauses: str, expected: list[tuple], label: str) -> str:
        """One MERGE of (1, 777), (9, 90) into SEED, with the given WHEN clauses; the detail
        says how many snapshots its one commit carried."""
        table = self._fresh(what)
        before = len(self._snapshots(table))
        self.sql(
            f"MERGE INTO {self.t(table)} AS tg "
            f"USING (SELECT * FROM (VALUES (1::BIGINT, 777::BIGINT), (9, 90)) AS s(id, v)) AS s "
            f"ON tg.id = s.id {clauses}"
        )
        added = len(self._snapshots(table)) - before
        return self._expect(table, expected, label) + f"; {added} snapshot(s) in the commit"

    def merge_update_only(self) -> str:
        return self._merge(
            "mergeupd",
            "WHEN MATCHED THEN UPDATE SET v = s.v",
            [(1, 777), (2, 20), (3, 30)],
            "MERGE, UPDATE only",
        )

    def merge_delete_only(self) -> str:
        return self._merge(
            "mergedel", "WHEN MATCHED THEN DELETE", [(2, 20), (3, 30)], "MERGE, DELETE only"
        )

    def merge_insert_only(self) -> str:
        return self._merge(
            "mergeins",
            "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
            SEED + [(9, 90)],
            "MERGE, INSERT only",
        )

    def merge_by_source_only(self) -> str:
        return self._merge(
            "mergesrconly",
            "WHEN NOT MATCHED BY SOURCE THEN DELETE",
            [(1, 10)],
            "MERGE, NOT MATCHED BY SOURCE DELETE only",
        )

    def merge_update_by_source(self) -> str:
        return self._merge(
            "mergeupdsrc",
            "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED BY SOURCE THEN DELETE",
            [(1, 777)],
            "MERGE, UPDATE + NOT MATCHED BY SOURCE DELETE",
        )

    def merge_three(self) -> str:
        return self._merge(
            "mergethree",
            "WHEN MATCHED THEN UPDATE SET v = s.v "
            "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v) "
            "WHEN NOT MATCHED BY SOURCE THEN DELETE",
            [(1, 777), (9, 90)],
            "MERGE, UPDATE + INSERT + NOT MATCHED BY SOURCE DELETE",
        )

    def _transaction(self, what: str, statements: list[str], expected: list[tuple]) -> str:
        """Several writes to one table in BEGIN ... COMMIT: one commit, one snapshot each."""
        table = self._fresh(what)
        before = len(self._snapshots(table))
        self.sql("BEGIN TRANSACTION")
        try:
            for statement in statements:
                self.sql(statement.format(t=self.t(table)))
            self.sql("COMMIT")
        except Refused:
            with contextlib.suppress(Refused):
                self.sql("ROLLBACK", echo=False)
            raise
        added = len(self._snapshots(table)) - before
        label = " + ".join(st.split()[0] for st in statements) + " in one transaction"
        return self._expect(table, expected, label) + f"; {added} snapshot(s) in the commit"

    def truncate(self) -> str:
        table = self._fresh("truncate")
        self.sql(f"TRUNCATE {self.t(table)}")
        result = self._expect(table, [], "TRUNCATE")
        # Metadata-only means the snapshot drops the data files and writes nothing new; the other
        # way is a delete file per data file, which every reader then has to apply.
        summary = self.iceberg(table).current_snapshot().summary
        counts = {
            k: summary.get(k, "0")
            for k in ("deleted-data-files", "added-data-files", "added-delete-files")
        }
        kind = (
            "metadata-only"
            if counts["added-data-files"] == "0" and counts["added-delete-files"] == "0"
            else "NOT metadata-only"
        )
        return f"{result}; {kind}: {summary.operation.value} snapshot, " + ", ".join(
            f"{k} {v}" for k, v in counts.items()
        )

    # -- probes: schema ----------------------------------------------------------------------

    def add_column(self) -> str:
        table = self._fresh("addcol")
        self.sql(f"ALTER TABLE {self.t(table)} ADD COLUMN w BIGINT")
        cols = self._columns(table)
        if "w" not in cols:
            raise NoOp(f"returned success and the schema is {cols}")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (4, 40, 400)")
        return f"schema {cols}; " + self._expect(
            table, [(1, None), (2, None), (3, None), (4, 400)], "new column written", ("id", "w")
        )

    def drop_column(self) -> str:
        table = self._fresh("dropcol")
        self.sql(f"ALTER TABLE {self.t(table)} DROP COLUMN v")
        cols = self._columns(table)
        if "v" in cols:
            raise NoOp(f"returned success and the schema is {cols}")
        return f"schema now {cols}"

    def write_after_evolution(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create("evowrite")
        first = ", ".join(f"({i}, {v})" for i, v in EVOLVE_FIRST)
        then = ", ".join(f"({i}, {v})" for i, v in EVOLVE_THEN)
        self.sql(f"INSERT INTO {self.t(table)} VALUES {first}")
        self.sql(f"ALTER TABLE {self.t(table)} SET PARTITIONED BY (bucket(4, id))")
        self.sql(f"INSERT INTO {self.t(table)} VALUES {then}")
        count = self.sql(f"SELECT count(*) FROM {self.t(table)}")[0][0]
        return evolution_check(self.iceberg(table), count)

    def type_promotion(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create("promote", "id BIGINT, c INTEGER")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (1, 7)")
        self.sql(f"ALTER TABLE {self.t(table)} ALTER COLUMN c TYPE BIGINT")
        return promotion_check(self.iceberg(table))

    def rename_column(self) -> str:
        table = self._fresh("renamecol")
        self.sql(f"ALTER TABLE {self.t(table)} RENAME COLUMN v TO v2")
        cols = self._columns(table)
        if "v2" not in cols:
            raise NoOp(f"returned success and the schema is {cols}")
        return f"schema now {cols}; " + self._expect(table, SEED, "data kept", ("id", "v2"))

    def rename_table(self) -> str:
        table = self._fresh("rename")
        new = self.name("renamed")
        self.sql(f'ALTER TABLE {self.t(table)} RENAME TO "{new}"')
        self._forget(table)
        self.created.append((self.ns, new, "table"))
        if not self._exists(new) or self._exists(table):
            self.created.append((self.ns, table, "table"))
            raise NoOp("returned success and the catalog still has the old name")
        # Is what the metadata points at still there? A rename that moves the folder but keeps
        # the absolute paths in the metadata leaves a table the catalog lists and nobody reads.
        tbl = self.iceberg(new)
        io = tbl.io

        def tail(path: str) -> str:
            return "…" + path.split(self.cfg.lakehouse_id, 1)[-1]

        def state(path: str) -> str:
            return "exists" if io.new_input(path).exists() else "is GONE"

        manifest_list = tbl.current_snapshot().manifest_list
        where = (
            f"location `{tail(tbl.location())}`; metadata `{tail(tbl.metadata_location)}` "
            f"{state(tbl.metadata_location)}; manifest list `{tail(manifest_list)}` "
            f"{state(manifest_list)}"
        )
        dk, py = self._dk_rows(new), self._py_rows(new)
        if SEED not in (dk, py):
            raise NoOp(
                f"renamed in the catalog, data unreadable: {where}; "
                f"pyiceberg: {_one_line(py, 160)}; DuckDB: {_one_line(dk, 160)}"
            )
        return f"renamed; {where}; " + self._expect(new, SEED, "data kept")

    def set_property(self) -> str:
        table = self._fresh("props")
        self.sql(f"CALL set_iceberg_table_properties({self.t(table)}, {{'probe.duckdb': '1'}})")
        props = self.iceberg(table).properties
        if props.get("probe.duckdb") != "1":
            raise NoOp(f"returned success and the property is not there: {sorted(props)}")
        return "the property is on the table"

    def partition_evolution(self) -> str:
        table = self._fresh("evolve")
        self.sql(f"ALTER TABLE {self.t(table)} SET PARTITIONED BY (bucket(4, id))")
        spec = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("returned success and the spec is still empty")
        return f"spec now {spec}"

    # -- probes: read ------------------------------------------------------------------------

    def time_travel(self) -> str:
        table = self._fresh("travel")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (4, 40)")
        first = self._snapshots(table)[0].snapshot_id
        rows = self.sql(f"SELECT count(*) FROM {self.t(table)} AT (VERSION => {first})")
        if rows[0][0] != 3:
            raise NoOp(f"AT (VERSION => first snapshot) reads {rows[0][0]} rows, not 3")
        return "AT (VERSION => <first snapshot>) reads 3 rows (the table has 4)"

    def metadata_snapshots(self) -> str:
        table = self._fresh("metasnap")
        rows = self.sql(f"SELECT count(*) FROM iceberg_snapshots({self.t(table)})")
        return f"iceberg_snapshots() reads {rows[0][0]} snapshot(s); pyiceberg sees " + str(
            len(self._snapshots(table))
        )

    def metadata_files(self) -> str:
        table = self._fresh("metafiles")
        rows = self.sql(f"SELECT count(*) FROM iceberg_metadata({self.t(table)})")
        return f"iceberg_metadata() reads {rows[0][0]} manifest entr(ies)"

    def _stats_table(self, what: str):
        """harness.STATS_FILES, written by pyiceberg: four files with known min/max bounds."""
        table = self.name(what)
        self.pyiceberg().create_table((self.ns, table), iceberg_schema())
        self.created.append((self.ns, table, "table"))
        write_stats_files(self.iceberg(table))
        return table

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

    # -- probes: refs and maintenance, where DuckDB has syntax at all ------------------------

    def create_branch(self) -> str:
        table = self._fresh("branch")
        self.sql(f"ALTER TABLE {self.t(table)} CREATE BRANCH probe_branch")
        refs = self.iceberg(table).metadata.refs
        if "probe_branch" not in refs:
            raise NoOp(f"returned success and the refs are {sorted(refs)}")
        return f"refs now {sorted(refs)}"

    def expire_snapshots(self) -> str:
        table = self._fresh("expire")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (4, 40)")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (5, 50)")
        before = len(self._snapshots(table))
        self.sql(f"CALL iceberg_expire_snapshots({self.t(table)}, retain_last => 1)")
        after = len(self._snapshots(table))
        if after >= before:
            raise NoOp(f"returned success and history is still {after} snapshot(s)")
        return f"history {before} -> {after} snapshot(s)"

    def _name_arg(self, table: str) -> str:
        """The dotted name the maintenance functions take as a string argument."""
        return f"'{CATALOG}.{self.ns}.{table}'"

    def rewrite_data_files(self) -> str:
        """Compaction, duckdb-iceberg's own: one `replace` snapshot over the small files. Its
        planner skips a bucket under min_input_files (run 36227802079 rewrote 0 of 3), so
        rewrite_all."""
        table = self._fresh("compact")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (4, 40)")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (5, 50)")
        before = len(list(self.iceberg(table).scan().plan_files()))
        result = self.sql(
            f"CALL iceberg_rewrite_data_files({self._name_arg(table)}, rewrite_all => true)"
        )
        tbl = self.iceberg(table)
        after = len(list(tbl.scan().plan_files()))
        if after >= before:
            raise NoOp(f"returned {result} and the table still has {after} data file(s)")
        op = tbl.current_snapshot().summary.operation.value
        return f"{before} -> {after} data file(s), commit `{op}`; " + self._expect(
            table, SEED + [(4, 40), (5, 50)], "data kept"
        )

    # -- probes: drops -----------------------------------------------------------------------

    def drop_table(self) -> str:
        table = self._fresh("drop")
        tbl = self.iceberg(table)
        data = [task.file.file_path for task in tbl.scan().plan_files()]
        self.sql(f"DROP TABLE {self.t(table)}")
        if self._exists(table):
            raise NoOp("returned success and the catalog still has it")
        self._forget(table)
        try:
            left = sum(1 for path in data if tbl.io.new_input(path).exists())
            files = f"{left} of {len(data)} data file(s) still in storage"
        except Exception as exc:  # noqa: BLE001 - informational
            files = f"could not check storage: {_one_line(scrub.scrub_exc(exc, 200), 200)}"
        return f"gone from the catalog; {files}"

    def credential_vending(self) -> str:
        """ACCESS_DELEGATION_MODE 'vended_credentials' and NO storage secret, on a connection of
        its own: every byte of storage goes through the SAS token the catalog vends for the
        table, so a read and a write that land prove the vended credential carries both."""
        table = self._fresh("vended")
        conn = connect()
        try:
            conn.execute(f"SET GLOBAL azure_transport_option_type = '{azure_transport()}'")
            conn.execute(
                f"ATTACH '{self.cfg.warehouse}' AS {CATALOG} (TYPE ICEBERG, "
                f"ENDPOINT '{ICEBERG_ENDPOINT}', TOKEN '{self._token}', "
                f"ACCESS_DELEGATION_MODE 'vended_credentials', STAGE_CREATE_TABLES false, "
                f"SKIP_CREATE_TABLE_METADATA_UPDATES true)"
            )
            read = conn.execute(f"SELECT count(*) FROM {self.t(table)}").fetchall()[0][0]
            conn.execute(f"INSERT INTO {self.t(table)} VALUES (4, 40)")
        except RuntimeError as exc:
            raise Refused(_one_line(str(exc))) from None
        finally:
            conn.close()
        return self._expect(
            table,
            [(1, 10), (2, 20), (3, 30), (4, 40)],
            f"no storage secret, read {read} rows then INSERT",
        )

    # -- teardown ----------------------------------------------------------------------------

    def drop_everything(self) -> None:
        if not self.attached or not (self.created or self.namespaces):
            return
        if self.keep:
            print(f"\nCAPABILITY_KEEP=1; left {len(self.created)} object(s) behind")
            return
        dropped, left = 0, []
        for ns, name, _kind in reversed(self.created):
            try:
                if self.pyiceberg().table_exists((ns, name)):
                    self.pyiceberg().purge_table((ns, name))
                dropped += 1
            except Exception as exc:  # noqa: BLE001 - teardown is best effort, by design
                left.append(f"{ns}.{name} ({_one_line(exc, 80)})")
        for ns in self.namespaces:
            try:
                self.pyiceberg().drop_namespace(ns)
            except Exception as exc:  # noqa: BLE001
                left.append(f"namespace {ns} ({_one_line(exc, 80)})")
        print(f"\ndropped {dropped} probe object(s)" + (f"; left behind {left}" if left else ""))


PROBES = [
    ("session", "DuckDB attaches the REST catalog", "session"),
    (
        "catalog",
        "CREATE SCHEMA IF NOT EXISTS on the existing `_bench_capability`",
        "namespace_existing",
    ),
    ("create", "CREATE TABLE (the catalog assigns the location)", "create_table"),
    ("create", "CREATE TABLE IF NOT EXISTS on an existing table", "create_if_not_exists"),
    ("create", "CREATE TABLE ... AS SELECT", "ctas"),
    ("create", "PARTITIONED BY (identity), then write", "create_partitioned"),
    ("create", "CREATE TABLE ... SORTED BY (sort order at create)", "create_sorted_at_create"),
    ("create", "ALTER TABLE ... SET SORTED BY (sort order)", "create_sorted"),
    ("create", "PARTITIONED BY (bucket(4, id)), then write", "partition_bucket"),
    ("create", "PARTITIONED BY (truncate(2, x)), then write", "partition_truncate"),
    ("create", "PARTITIONED BY year / month / day / hour, then write", "partition_temporal"),
    ("create", "types: decimal, date, timestamp, timestamptz, uuid, binary", "types_scalar"),
    ("create", "nested types: struct, list, map", "types_nested"),
    ("write", "INSERT INTO ... VALUES", "insert_values"),
    ("write", "INSERT INTO ... SELECT", "insert_select"),
    ("write", "DELETE FROM ... WHERE", "delete_from"),
    ("write", "UPDATE", "update"),
    ("write", "MERGE, one action: WHEN MATCHED UPDATE", "merge_update_only"),
    ("write", "MERGE, one action: WHEN MATCHED DELETE", "merge_delete_only"),
    ("write", "MERGE, one action: WHEN NOT MATCHED INSERT", "merge_insert_only"),
    ("write", "MERGE, one action: WHEN NOT MATCHED BY SOURCE DELETE", "merge_by_source_only"),
    (
        "write",
        "MERGE, two actions: MATCHED UPDATE + NOT MATCHED BY SOURCE DELETE",
        "merge_update_by_source",
    ),
    ("write", "MERGE, three actions: UPDATE + INSERT + BY SOURCE DELETE", "merge_three"),
    ("write", "TRUNCATE", "truncate"),
    ("schema", "ALTER TABLE ADD COLUMN", "add_column"),
    ("schema", "ALTER TABLE DROP COLUMN", "drop_column"),
    ("schema", "ALTER TABLE RENAME COLUMN", "rename_column"),
    ("schema", "ALTER COLUMN c TYPE BIGINT (int -> long)", "type_promotion"),
    ("schema", "ALTER TABLE ... RENAME TO (table)", "rename_table"),
    ("schema", "set_iceberg_table_properties", "set_property"),
    ("schema", "ALTER TABLE ... SET PARTITIONED BY (partition evolution)", "partition_evolution"),
    ("schema", "INSERT after SET PARTITIONED BY, read across both specs", "write_after_evolution"),
    ("read", "time travel, AT (VERSION => <snapshot>)", "time_travel"),
    ("read", "iceberg_snapshots()", "metadata_snapshots"),
    ("read", "iceberg_metadata()", "metadata_files"),
    ("read", "WHERE id = 22 with the other data files deleted (file pruning)", "file_pruning"),
    ("read", "max(id) with every data file deleted (min/max from metadata)", "max_from_metadata"),
    ("refs", "ALTER TABLE ... CREATE BRANCH", "create_branch"),
    ("maintenance", "iceberg_expire_snapshots", "expire_snapshots"),
    ("maintenance", "iceberg_rewrite_data_files (compaction)", "rewrite_data_files"),
    ("catalog", "DROP TABLE", "drop_table"),
    (
        "catalog",
        "ACCESS_DELEGATION_MODE 'vended_credentials', no storage secret",
        "credential_vending",
    ),
    ("catalog", "CREATE SCHEMA (namespace)", "create_schema"),
    ("catalog", "DROP SCHEMA (namespace)", "drop_schema"),
]


def write_step_summary(probe: DuckDBCapability) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    counts = probe.report.tally()
    lines = [
        "## OneLake Iceberg REST catalog, asked by DuckDB",
        "",
        f"{counts[SUPPORTED]} supported · {counts[REFUSED]} refused · "
        f"{counts[NOOP]} accepted then ignored · "
        f"{counts[SKIPPED]} skipped · {counts[BROKEN]} could not be asked",
        "",
        *probe.report.markdown(),
        "",
        f"DuckDB `{probe.version}` · namespace `{probe.ns}` · run `{probe.run}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    cfg = Config.from_env()
    probe = DuckDBCapability(cfg)
    print(f"workspace {cfg.workspace_id}  lakehouse {cfg.lakehouse_id}  namespace {NAMESPACE}")

    try:
        for group, question, method in PROBES:
            ok = probe.report.run(group, question, getattr(probe, method))
            if not ok and method == "session":
                print("\nstopped: no DuckDB session, so nothing below could be asked")
                write_step_summary(probe)
                return 1
    finally:
        try:
            probe.drop_everything()
        except Exception as exc:  # noqa: BLE001 - teardown must not mask the findings
            print(f"  warning: teardown failed: {_one_line(exc, 200)}")

    counts = probe.report.tally()
    print("\n" + "\n".join(probe.report.markdown()))
    print(
        f"\n{counts[SUPPORTED]} supported, {counts[REFUSED]} refused, "
        f"{counts[NOOP]} accepted then ignored, "
        f"{counts[SKIPPED]} skipped, {counts[BROKEN]} could not be asked"
    )
    write_step_summary(probe)
    probe.report.save("duckdb", probe.version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
