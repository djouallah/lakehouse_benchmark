"""What the OneLake Iceberg REST catalog lets chDB (ClickHouse in-process) do.

chDB's Iceberg support is ClickHouse's own C++ implementation, so like DuckDB and Sail it is an
independent witness: a `no` it shares with the others is very likely the catalog's.

THE SESSION IS bench.capability.engines.chdb_iceberg's: a `DataLakeCatalog` database of
catalog_type 'onelake', with the engine's own bearer token signing both the catalog and storage.
Writes are ClickHouse's experimental Iceberg writer, behind allow_experimental_insert_into_iceberg.

WHERE CLICKHOUSE HAS NO STATEMENT for an operation, the probe sends the Spark form anyway, so the
`no` carries ClickHouse's own parser message and the log says whose limit it is.

IF chDB CANNOT CREATE A TABLE, the write, schema and maintenance probes still run, on a table
pyiceberg creates; their detail says so.

EVERY PROBE CHECKS ITS EFFECT through pyiceberg, which reads what the CATALOG now says rather than
what chDB cached. Accepted with no effect is a no-op, not a yes.

"""

from __future__ import annotations

import os
import sys
from datetime import UTC, datetime

from harness import (
    BROKEN,
    NAMESPACE,
    NESTED_TYPES,
    NOOP,
    REFUSED,
    SCALAR_TYPES,
    SKIPPED,
    SUPPORTED,
    TEMPORAL,
    TRANSFORM_ROWS,
    Broken,
    NoOp,
    Refused,
    Report,
    Skip,
    iceberg_schema,
    partition_result,
    promotion_check,
    promotion_schema,
    rows,
    transform_case,
    type_result,
    type_schema,
    verdict,
)
from isolation_duckdb import race_probe

from bench import auth, scrub
from bench.capability.engines.chdb_iceberg import DB, ChdbIceberg
from bench.capability.race import RaceProxy
from bench.config import Config

SEED = [(1, 10), (2, 20), (3, 30)]

# The VALUES literal chDB writes for each TYPE_EXPECTED case, into a table pyiceberg made.
TYPES = {
    "decimal": "12.34",
    "date": "'2026-01-02'",
    "timestamp": "'2026-01-02 03:04:05.123456'",
    "timestamptz": "'2026-01-02 03:04:05.123456'",
    "uuid": "'6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab'",
    "binary": "unhex('0102')",
    "struct": "tuple(1, 'x')",
    "list": "[1, 2, 3]",
    "map": "map('k', 1)",
}


def _transform_values(kind: str) -> str:
    family = "temporal" if kind in TEMPORAL else kind
    quote = "{}" if kind == "bucket" else "'{}'"
    return ", ".join(f"({i}, {quote.format(v)})" for i, v in TRANSFORM_ROWS[family])


def _one_line(text: object, limit: int = 700) -> str:
    flat = " ".join(scrub.scrub(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _say(text: object) -> None:
    print(scrub.scrub(text), flush=True)


def _values(pairs) -> str:
    return ", ".join(f"({i}, {v})" for i, v in pairs)


def _select(pairs) -> str:
    """A SELECT yielding Int64 (id, v) rows, so no insert depends on implicit casts."""
    return f"SELECT * FROM values('id Int64, v Int64', {_values(pairs)})"


class ChdbCapability:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.engine = ChdbIceberg(cfg)
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
        self.by_pyiceberg: set[str] = set()
        self._catalog = None
        self.proxy = None
        self.race_error = ""

    # -- helpers -----------------------------------------------------------------------------

    def sql(self, statement: str) -> list[tuple]:
        """One statement. chDB's own `no` becomes `Refused`, carrying its message."""
        _say(f"       > {_one_line(statement, 300)}")
        try:
            return self.engine.query(statement)
        except Exception as exc:  # noqa: BLE001 - every chDB error is an answer
            raise Refused(_one_line(f"{type(exc).__name__}: {exc}")) from None

    def name(self, what: str) -> str:
        return f"ch_{self.run}_{what}"

    def t(self, table: str) -> str:
        """The catalog's `ns.table` is one table name inside the attached database."""
        return f"{DB}.`{self.ns}.{table}`"

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

    def _ch_rows(self, table: str, cols: str = "id, v"):
        try:
            return self.engine.query(f"SELECT {cols} FROM {self.t(table)} ORDER BY id")
        except Exception as exc:  # noqa: BLE001 - a read-back must not decide the answer
            return f"chDB could not read it back: {_one_line(exc, 300)}"

    def _expect(self, table: str, expected: list[tuple], what: str, cols=("id", "v")) -> str:
        """pyiceberg decides, because it reads the catalog; chDB's own read is reported beside."""
        py = self._py_rows(table, cols)
        ch = self._ch_rows(table, ", ".join(cols))
        made = "; table made by pyiceberg" if table in self.by_pyiceberg else ""
        if py != expected:
            raise NoOp(
                f"{what} returned success; expected {expected}, pyiceberg reads {py}, "
                f"chDB reads {ch}{made}"
            )
        agree = "chDB agrees" if ch == py else f"chDB reads {ch}"
        return f"{what}: pyiceberg reads {py}; {agree}{made}"

    def _create(self, what: str, columns: str = "id Int64, v Int64", tail: str = "") -> str:
        table = self.name(what)
        self.sql(f"CREATE TABLE {self.t(table)} ({columns}) {tail}".rstrip())
        self.created.append(table)
        return table

    def _py_create(self, what: str, partitioned: bool = False, schema=None, spec=None) -> str:
        """The same table made by pyiceberg, for when chDB cannot create one itself."""
        table = self.name(what)
        schema = schema or iceberg_schema()
        if partitioned:
            from pyiceberg.partitioning import PartitionField, PartitionSpec
            from pyiceberg.schema import Schema
            from pyiceberg.transforms import IdentityTransform
            from pyiceberg.types import NestedField, StringType

            schema = Schema(*schema.fields, NestedField(3, "p", StringType(), required=False))
            spec = PartitionSpec(PartitionField(3, 1000, IdentityTransform(), "p"))
        kwargs = {"partition_spec": spec} if spec else {}
        self.pyiceberg().create_table((self.ns, table), schema, **kwargs)
        self.created.append(table)
        self.by_pyiceberg.add(table)
        return table

    def _empty(self, what: str) -> str:
        """An empty (id, v) table: chDB's if it can create one, otherwise pyiceberg's."""
        return self._create(what) if self.can_create else self._py_create(what)

    def _fresh(self, what: str) -> str:
        """A table holding SEED, written by chDB."""
        table = self._empty(what)
        try:
            self.sql(f"INSERT INTO {self.t(table)} VALUES {_values(SEED)}")
        except Refused as exc:
            raise Skip(f"could not seed the table: {exc}") from None
        return table

    def _partitioned(self, what: str) -> str:
        if self.can_create:
            table = self._create(what, "id Int64, v Int64, p String", "PARTITION BY p")
        else:
            table = self._py_create(what, partitioned=True)
        try:
            self.sql(f"INSERT INTO {self.t(table)} VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b')")
        except Refused as exc:
            raise Skip(f"could not seed the table: {exc}") from None
        return table

    # -- session -----------------------------------------------------------------------------

    def session(self) -> str:
        self.engine.setup()
        # A second database, `race`, on the race proxy, for the concurrency probes only.
        self.proxy = RaceProxy().start()
        try:
            self.engine.attach("race", self.proxy.endpoint)
        except Exception as exc:  # noqa: BLE001 - only the concurrency probes depend on it
            self.race_error = _one_line(f"{type(exc).__name__}: {exc}", 300)
        self.version = self.engine.version
        listed = self.sql(
            f"SELECT count() FROM system.tables WHERE database = '{DB}' "
            f"SETTINGS show_data_lake_catalogs_in_system_tables = 1"
        )
        return f"chdb {self.version}; {listed[0][0]} catalog table(s) listed"

    # -- create ------------------------------------------------------------------------------

    def create_table(self) -> str:
        table = self._create("base")
        self.can_create = True
        tbl = self.iceberg(table)
        fields = [(f.field_id, f.name, str(f.field_type)) for f in tbl.schema().fields]
        return f"fields {fields}, format-version {tbl.metadata.format_version}"

    def ctas(self) -> str:
        table = self.name("ctas")
        self.created.append(table)
        self.sql(f"CREATE TABLE {self.t(table)} AS {_select(SEED)}")
        return self._expect(table, SEED, "CTAS")

    def partitioned(self) -> str:
        if self.can_create:
            table = self._create("part", "id Int64, v Int64, p String", "PARTITION BY p")
        else:
            table = self._py_create("part", partitioned=True)
        self.sql(f"INSERT INTO {self.t(table)} VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b')")
        spec = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("PARTITION BY returned success and the spec is empty")
        files = list(self.iceberg(table).scan().plan_files())
        parts = {str(task.file.partition) for task in files}
        made = "; table made by pyiceberg" if table in self.by_pyiceberg else ""
        return f"spec {spec}; 3 rows over {len(parts)} partition(s), {len(files)} file(s){made}"

    def _transform(self, kind: str) -> tuple[str, str, str]:
        schema, spec, parts = transform_case(kind)
        table = self._py_create(f"tr_{kind}", schema=schema, spec=spec)
        try:
            self.sql(f"INSERT INTO {self.t(table)} VALUES {_transform_values(kind)}")
        except Refused as exc:
            return kind, REFUSED, str(exc)
        return partition_result(self.iceberg(table), kind, parts)

    def partition_bucket(self) -> str:
        return verdict([self._transform("bucket")])

    def partition_truncate(self) -> str:
        return verdict([self._transform("truncate")])

    def partition_temporal(self) -> str:
        return verdict([self._transform(kind) for kind in TEMPORAL])

    def _typed(self, name: str) -> tuple[str, str, str]:
        table = self._py_create(f"type_{name}", schema=type_schema(name))
        try:
            self.sql(f"INSERT INTO {self.t(table)} VALUES (1, {TYPES[name]})")
        except Refused as exc:
            return name, REFUSED, str(exc)
        return type_result(self.iceberg(table), name)

    def types_scalar(self) -> str:
        return verdict([self._typed(name) for name in SCALAR_TYPES])

    def types_nested(self) -> str:
        return verdict([self._typed(name) for name in NESTED_TYPES])

    def sorted_at_create(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create("sorted", tail="ORDER BY id")
        order = self.iceberg(table).sort_order()
        if not order.fields:
            raise NoOp("ORDER BY returned success and the table has no sort order")
        return f"sort order {order}"

    # -- write -------------------------------------------------------------------------------

    def insert_into(self) -> str:
        table = self._empty("insert")
        self.sql(f"INSERT INTO {self.t(table)} VALUES {_values(SEED)}")
        return self._expect(table, SEED, "INSERT INTO") + (
            f"; {len(self._snapshots(table))} commit(s)"
        )

    def insert_select(self) -> str:
        table = self._fresh("insertsel")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        return self._expect(table, SEED + [(4, 40)], "INSERT ... SELECT")

    def delete_from(self) -> str:
        table = self._fresh("delete")
        self.sql(f"DELETE FROM {self.t(table)} WHERE id = 1")
        return self._expect(table, SEED[1:], "DELETE FROM")

    def alter_delete(self) -> str:
        table = self._fresh("altdelete")
        self.sql(f"ALTER TABLE {self.t(table)} DELETE WHERE id = 1")
        return self._expect(table, SEED[1:], "ALTER TABLE ... DELETE")

    def update(self) -> str:
        table = self._fresh("update")
        self.sql(f"UPDATE {self.t(table)} SET v = 999 WHERE id = 1")
        return self._expect(table, [(1, 999), (2, 20), (3, 30)], "UPDATE")

    def alter_update(self) -> str:
        table = self._fresh("altupdate")
        self.sql(f"ALTER TABLE {self.t(table)} UPDATE v = 999 WHERE id = 1")
        return self._expect(table, [(1, 999), (2, 20), (3, 30)], "ALTER TABLE ... UPDATE")

    def merge_into(self) -> str:
        table = self._fresh("merge")
        self.sql(
            f"MERGE INTO {self.t(table)} t USING ({_select([(1, 777), (9, 90)])}) s "
            f"ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v "
            f"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
        return self._expect(table, [(1, 777), (2, 20), (3, 30), (9, 90)], "MERGE INTO")

    def truncate(self) -> str:
        table = self._fresh("truncate")
        self.sql(f"TRUNCATE TABLE {self.t(table)}")
        return self._expect(table, [], "TRUNCATE")

    # -- schema ------------------------------------------------------------------------------

    def add_column(self) -> str:
        table = self._fresh("addcol")
        self.sql(f"ALTER TABLE {self.t(table)} ADD COLUMN w Nullable(Int64)")
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
        table = self._py_create("promote", schema=promotion_schema())
        self.sql(f"INSERT INTO {self.t(table)} VALUES (1, 7)")
        self.sql(f"ALTER TABLE {self.t(table)} MODIFY COLUMN c Nullable(Int64)")
        return promotion_check(self.iceberg(table)) + "; table made by pyiceberg"

    def partition_evolution(self) -> str:
        table = self._fresh("specevo")
        self.sql(f"ALTER TABLE {self.t(table)} ADD PARTITION FIELD bucket(4, id)")
        spec = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("returned success and the table is still unpartitioned")
        return f"spec now {spec}"

    def set_property(self) -> str:
        table = self._fresh("props")
        self.sql(f"ALTER TABLE {self.t(table)} SET TBLPROPERTIES ('probed-at' = '{self.run}')")
        if self.iceberg(table).properties.get("probed-at") != self.run:
            raise NoOp("returned success and the property is not on the table")
        return "the property is on the table"

    def sort_order_evolution(self) -> str:
        table = self._fresh("sortevo")
        self.sql(f"ALTER TABLE {self.t(table)} MODIFY ORDER BY id")
        order = self.iceberg(table).sort_order()
        if not order.fields:
            raise NoOp("MODIFY ORDER BY returned success and the table has no sort order")
        return f"sort order now {order}"

    # -- read --------------------------------------------------------------------------------

    def time_travel(self) -> str:
        table = self._fresh("travel")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (4, 40)")
        first = self._snapshots(table)[0].snapshot_id
        rows_then = self.sql(
            f"SELECT count() FROM {self.t(table)} SETTINGS iceberg_snapshot_id = {first}"
        )
        if rows_then[0][0] != 3:
            raise NoOp(f"iceberg_snapshot_id of the first snapshot reads {rows_then[0][0]} rows")
        return "SETTINGS iceberg_snapshot_id of the first snapshot reads 3 rows (the table has 4)"

    def metadata_tables(self) -> str:
        table = self._fresh("inspect")
        found = self.sql(
            f"SELECT count() FROM system.iceberg_history "
            f"WHERE database = '{DB}' AND table = '{self.ns}.{table}'"
        )
        if found[0][0] == 0:
            raise NoOp("system.iceberg_history has no row for the table")
        return f"system.iceberg_history reads {found[0][0]} snapshot(s)"

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
        table = self._fresh("expire")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (4, 40)")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (5, 50)")
        before = len(self._snapshots(table))
        now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
        self.sql(
            f"ALTER TABLE {self.t(table)} EXECUTE expire_snapshots('{now}') "
            f"SETTINGS allow_experimental_expire_snapshots = 1"
        )
        after = len(self._snapshots(table))
        if after >= before:
            raise NoOp(f"returned success and history went {before} -> {after}")
        return f"history {before} -> {after} snapshot(s)"

    def compaction(self) -> str:
        table = self._fresh("compact")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (4, 40)")
        before = len(list(self.iceberg(table).scan().plan_files()))
        self.sql(
            f"OPTIMIZE TABLE {self.t(table)} SETTINGS allow_experimental_iceberg_compaction = 1"
        )
        after = len(list(self.iceberg(table).scan().plan_files()))
        if after >= before:
            raise NoOp(f"returned success and data files went {before} -> {after}")
        return self._expect(table, SEED + [(4, 40)], f"{before} -> {after} data file(s), data kept")

    # -- catalog -----------------------------------------------------------------------------

    def rename_table(self) -> str:
        table = self._fresh("rename")
        target = self.name("renamed")
        self.sql(f"RENAME TABLE {self.t(table)} TO {self.t(target)}")
        self.created = [target if c == table else c for c in self.created]
        found = self._py_rows(target)
        if found != SEED:
            raise NoOp(f"renamed, and the data is unreadable under the new name: {found}")
        return "renamed, and its 3 rows read back"

    # -- concurrency -------------------------------------------------------------------------

    def _race(self, key: str, statement: str) -> str:
        """pyiceberg makes and seeds the table; chDB writes through the `race` database, whose
        commit bench.capability.race holds until pyiceberg's has landed."""
        if self.proxy is None:
            raise Skip("no chDB session")
        if self.race_error:
            raise Broken(f"the `race` database on the proxy did not attach: {self.race_error}")
        table = self.name(key)
        self.pyiceberg().create_table((self.ns, table), schema=iceberg_schema())
        self.created.append(table)
        self.iceberg(table).append(rows(SEED))
        raced = f"race.`{self.ns}.{table}`"
        return race_probe(
            key, self.pyiceberg(), self.proxy, table, lambda: self.sql(statement.format(t=raced))
        )

    def race_append(self) -> str:
        return self._race("race_append", "INSERT INTO {t} VALUES (5, 50)")

    def race_delete(self) -> str:
        return self._race("race_delete", "DELETE FROM {t} WHERE id = 1")

    def race_update(self) -> str:
        return self._race("race_update", "ALTER TABLE {t} UPDATE v = v + 1 WHERE id = 2")

    # -- teardown ----------------------------------------------------------------------------

    def drop_everything(self) -> None:
        if not self.created:
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
        print(f"\ndropped {dropped} probe table(s)" + (f"; left behind {left}" if left else ""))


PROBES = [
    ("session", "chDB attaches the catalog (DataLakeCatalog, catalog_type 'onelake')", "session"),
    ("create", "CREATE TABLE", "create_table"),
    ("create", "CREATE TABLE ... AS SELECT", "ctas"),
    ("create", "PARTITION BY p, then INSERT", "partitioned"),
    ("create", "ORDER BY at create (sort order)", "sorted_at_create"),
    ("create", "INSERT into bucket(4, id) partitions", "partition_bucket"),
    ("create", "INSERT into truncate(2, x) partitions", "partition_truncate"),
    ("create", "INSERT into year / month / day / hour partitions", "partition_temporal"),
    ("create", "types: decimal, date, timestamp, timestamptz, uuid, binary", "types_scalar"),
    ("create", "nested types: struct, list, map", "types_nested"),
    ("write", "INSERT INTO ... VALUES", "insert_into"),
    ("write", "INSERT INTO ... SELECT", "insert_select"),
    ("write", "DELETE FROM", "delete_from"),
    ("write", "ALTER TABLE ... DELETE WHERE", "alter_delete"),
    ("write", "UPDATE ... SET", "update"),
    ("write", "ALTER TABLE ... UPDATE", "alter_update"),
    ("write", "MERGE INTO", "merge_into"),
    ("write", "TRUNCATE TABLE", "truncate"),
    ("schema", "ALTER TABLE ADD COLUMN", "add_column"),
    ("schema", "ALTER TABLE DROP COLUMN", "drop_column"),
    ("schema", "ALTER TABLE RENAME COLUMN", "rename_column"),
    ("schema", "MODIFY COLUMN c Int64 (int -> long)", "type_promotion"),
    ("schema", "ALTER TABLE ADD PARTITION FIELD (partition evolution)", "partition_evolution"),
    ("schema", "ALTER TABLE SET TBLPROPERTIES", "set_property"),
    ("schema", "ALTER TABLE MODIFY ORDER BY (sort order evolution)", "sort_order_evolution"),
    ("read", "time travel, SETTINGS iceberg_snapshot_id", "time_travel"),
    ("read", "metadata (system.iceberg_history)", "metadata_tables"),
    ("refs", "ALTER TABLE ... CREATE BRANCH", "create_branch"),
    ("refs", "ALTER TABLE ... CREATE TAG", "create_tag"),
    ("maintenance", "ALTER TABLE ... EXECUTE expire_snapshots", "expire_snapshots"),
    ("maintenance", "OPTIMIZE TABLE (compaction)", "compaction"),
    ("catalog", "RENAME TABLE, then read", "rename_table"),
    ("concurrency", "INSERT INTO; B appends between chDB's read and commit", "race_append"),
    ("concurrency", "DELETE id 1; B appends", "race_delete"),
    ("concurrency", "ALTER TABLE ... UPDATE v = v + 1 on id 2; B appends", "race_update"),
]


def write_step_summary(probe: ChdbCapability) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    counts = probe.report.tally()
    lines = [
        "## OneLake Iceberg REST catalog, asked by chDB",
        "",
        f"{counts[SUPPORTED]} supported · {counts[REFUSED]} refused · "
        f"{counts[NOOP]} accepted then ignored · "
        f"{counts[SKIPPED]} skipped · {counts[BROKEN]} could not be asked",
        "",
        *probe.report.markdown(),
        "",
        f"chdb `{probe.version}` · namespace `{probe.ns}` · run `{probe.run}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    cfg = Config.from_env()
    probe = ChdbCapability(cfg)
    print(f"workspace {cfg.workspace_id}  lakehouse {cfg.lakehouse_id}  namespace {NAMESPACE}")

    try:
        for group, question, method in PROBES:
            ok = probe.report.run(group, question, getattr(probe, method))
            if not ok and method == "session":
                print("\nstopped: no chDB session, so nothing below could be asked")
                write_step_summary(probe)
                return 1
    finally:
        try:
            probe.drop_everything()
        except Exception as exc:  # noqa: BLE001 - teardown must not mask the findings
            print(f"  warning: teardown failed: {_one_line(exc, 200)}")
        probe.engine.close()
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
    probe.report.save("chdb", probe.version)
    return 0


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    # The embedded ClickHouse server may leave threads behind; a plain exit could then hang until
    # the job timeout. The findings are already printed and summarised.
    os._exit(code)
