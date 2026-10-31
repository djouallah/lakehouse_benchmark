"""What the OneLake Iceberg REST catalog lets Polars do, asked in SQL.

SQL ONLY, AS EVERY OTHER ENGINE. Each cell is a statement sent through Polars' SQLContext with the
REST catalog attached by `register_catalog` (Polars 2.1.0, pola-rs/polars#30113): the grid compares
what each engine's SQL can do. Polars' Python API (`sink_iceberg`, `pl.delete_iceberg`,
`scan_iceberg`) is not asked, the way no other engine is credited for a library call beside its SQL.

THE STATEMENTS ARE THE ONES THE OTHER SQL ENGINES TAKE, in the generic form, except a partition
transform, where Polars has its own `PARTITION BY (bucket(4, id))` (pola-rs/polars#30403). Where it
has no statement of its own it is sent the common one (Spark's, for ALTER ... and CALL), and its
error is the answer, quoted in full.

THE TABLES A STATEMENT WRITES INTO ARE MADE BY PYICEBERG, created and seeded: this way each
statement is asked on its own. The create probes ask Polars' own CREATE TABLE (#30260).

EVERY PROBE CHECKS ITS EFFECT through pyiceberg, which reads what the CATALOG now says. Accepted
with no effect is a no-op, not a yes.

THE STORAGE CREDENTIAL is `{"bearer_token": ...}`, the same one lakehouse_benchmark's Polars
engines give `scan_iceberg`: register_catalog passes it to every scan.
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
    Broken,
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
    promotion_schema,
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

from bench import auth, scrub
from bench.capability.race import RaceProxy
from bench.config import Config
from bench.polars_build import polars_version

CATALOG = "lake"
SEED = [(1, 10), (2, 20), (3, 30)]

# Column type and literal for each TYPE_EXPECTED case, in Polars' SQL.
TYPES = {
    "decimal": ("DECIMAL(10, 2)", "CAST('12.34' AS DECIMAL(10, 2))"),
    "date": ("DATE", "CAST('2026-01-02' AS DATE)"),
    "timestamp": ("TIMESTAMP", "CAST('2026-01-02 03:04:05.123456' AS TIMESTAMP)"),
    "timestamptz": ("TIMESTAMPTZ", "CAST('2026-01-02 03:04:05.123456+00:00' AS TIMESTAMPTZ)"),
    "uuid": ("UUID", "CAST('6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab' AS UUID)"),
    "binary": ("BINARY", "X'0102'"),
    "struct": ("STRUCT<a BIGINT, b VARCHAR>", "STRUCT(1 AS a, 'x' AS b)"),
    "list": ("BIGINT[]", "[1, 2, 3]"),
    "map": ("MAP<VARCHAR, BIGINT>", "MAP {'k': 1}"),
}


def _one_line(text: object, limit: int = 700) -> str:
    flat = " ".join(scrub.scrub(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _say(text: object) -> None:
    print(scrub.scrub(text), flush=True)


def _values(pairs) -> str:
    return ", ".join(f"({i}, {v})" for i, v in pairs)


def _transform_sql(kind: str) -> tuple[str, str, str]:
    """(columns, partition transform, VALUES) for one transform_case."""
    if kind == "bucket":
        values = _values(TRANSFORM_ROWS["bucket"])
        return "id BIGINT, x BIGINT", "bucket(4, id)", values
    if kind == "truncate":
        values = ", ".join(f"({i}, '{v}')" for i, v in TRANSFORM_ROWS["truncate"])
        return "id BIGINT, x VARCHAR", "truncate(2, x)", values
    values = ", ".join(f"({i}, CAST('{v}' AS TIMESTAMP))" for i, v in TRANSFORM_ROWS["temporal"])
    return "id BIGINT, x TIMESTAMP", f"{kind}(x)", values


class PolarsCapability:
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
        self.catalog = None
        self.storage: dict[str, str] = {}
        self.ctx = None
        self.created: list[str] = []
        self.namespaces: list[str] = []
        self.proxy = None

    # -- helpers -----------------------------------------------------------------------------

    def sql(self, statement: str) -> list[tuple]:
        """One statement through the SQLContext. Polars' own `no` becomes `Refused`, carrying
        its message."""
        _say(f"       > {_one_line(statement, 300)}")
        try:
            return self.ctx.execute(statement, eager=True).rows()
        except Exception as exc:  # noqa: BLE001 - every Polars error is an answer
            message = f"{type(exc).__name__}: {scrub.scrub_exc(exc, 2000)}"
            raise Refused(_one_line(message)) from None

    def name(self, what: str) -> str:
        return f"pl_{self.run}_{what}"

    def t(self, table: str, ns: str | None = None, catalog: str = CATALOG) -> str:
        return f"{catalog}.{ns or self.ns}.{table}"

    def iceberg(self, table: str):
        return self.catalog.load_table((self.ns, table))

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
            return sorted(
                self.ctx.execute(f"SELECT {cols} FROM {self.t(table)}", eager=True).rows()
            )
        except Exception as exc:  # noqa: BLE001 - a read-back must not decide the answer
            return f"Polars could not read it back: {_one_line(exc, 300)}"

    def _expect(self, table: str, expected: list[tuple], what: str, cols=("id", "v")) -> str:
        """pyiceberg decides, because it reads the catalog; Polars' own read is reported beside."""
        py = self._py_rows(table, cols)
        own = self._own_rows(table, ", ".join(cols))
        if py != expected:
            raise NoOp(
                f"{what} returned success; expected {expected}, pyiceberg reads {py}, "
                f"Polars reads {own}"
            )
        agree = "Polars agrees" if own == py else f"Polars reads {own}"
        return f"{what}: pyiceberg reads {py}; {agree}"

    def _created(self, table: str) -> bool:
        if self.catalog.table_exists((self.ns, table)):
            self.created.append(table)
            return True
        return False

    def _create(self, what: str, columns: str = "id BIGINT, v BIGINT", tail: str = "") -> str:
        """Polars' own CREATE TABLE; a success that leaves no table in the catalog is a no-op."""
        table = self.name(what)
        self.sql(f"CREATE TABLE {self.t(table)} ({columns}){tail}")
        if not self._created(table):
            raise NoOp("CREATE TABLE returned success and the catalog has no such table")
        return table

    def _empty(self, what: str, schema=None, **kwargs) -> str:
        """A table pyiceberg creates (module docstring)."""
        if self.catalog is None:
            raise Skip("the catalog never loaded")
        table = self.name(what)
        self.catalog.create_table((self.ns, table), schema=schema or iceberg_schema(), **kwargs)
        self.created.append(table)
        return table

    def _fresh(self, what: str, **kwargs) -> str:
        """A table holding SEED, created and appended by pyiceberg."""
        table = self._empty(what, **kwargs)
        try:
            self.iceberg(table).append(rows(SEED))
        except Exception as exc:  # noqa: BLE001 - no seed, no question
            raise Skip(f"pyiceberg could not seed the table: {_one_line(exc, 300)}") from None
        return table

    # -- session -----------------------------------------------------------------------------

    def session(self) -> str:
        # Polars sizes its thread pool at import time, so this goes first.
        os.environ.setdefault("POLARS_MAX_THREADS", "4")
        import polars as pl

        self.version = polars_version()
        self.catalog = auth.catalog(self.cfg)
        self.storage = {"bearer_token": auth.onelake_token()}
        self.catalog.create_namespace_if_not_exists(self.ns)
        self.ctx = pl.SQLContext().register_catalog(
            CATALOG, self.catalog, storage_options=self.storage
        )
        return f"polars {self.version}; the REST catalog attached as `{CATALOG}`"

    # -- create ------------------------------------------------------------------------------

    def create_table(self) -> str:
        table = self._create("base")
        tbl = self.iceberg(table)
        return f"field ids {[f.field_id for f in tbl.schema().fields]}"

    def create_if_not_exists(self) -> str:
        table = self._fresh("ine")
        self.sql(f"CREATE TABLE IF NOT EXISTS {self.t(table)} (id BIGINT, v BIGINT, extra BIGINT)")
        names = [f.name for f in self.iceberg(table).schema().fields]
        rows_ = self._py_rows(table)
        if "extra" in names or rows_ != SEED:
            raise NoOp(f"IF NOT EXISTS replaced the existing table: columns {names}, rows {rows_}")
        return f"accepted, the existing table untouched: columns {names}, rows {rows_}"

    def ctas(self) -> str:
        table = self.name("ctas")
        self.sql(
            f"CREATE TABLE {self.t(table)} AS SELECT * FROM (VALUES {_values(SEED)}) AS s(id, v)"
        )
        if not self._created(table):
            raise NoOp("CTAS returned success and the catalog has no such table")
        return self._expect(table, SEED, "CTAS")

    def partitioned(self) -> str:
        table = self._create("part", "id BIGINT, v BIGINT, p VARCHAR", " PARTITIONED BY (p)")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b')")
        spec = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("PARTITIONED BY returned success and the spec is empty")
        files = len(list(self.iceberg(table).scan().plan_files()))
        return f"spec {spec}; 3 rows over 2 partitions wrote {files} data file(s)"

    def _transform(self, kind: str) -> tuple[str, str, str]:
        columns, part, values = _transform_sql(kind)
        _, _, parts = transform_case(kind)
        try:
            table = self._create(f"tr_{kind}", columns, f" PARTITION BY ({part})")
            self.sql(f"INSERT INTO {self.t(table)} VALUES {values}")
        except (Refused, NoOp) as exc:
            return kind, REFUSED, str(exc)
        return partition_result(self.iceberg(table), kind, parts)

    def partition_bucket(self) -> str:
        return verdict([self._transform("bucket")])

    def partition_truncate(self) -> str:
        return verdict([self._transform("truncate")])

    def partition_temporal(self) -> str:
        return verdict([self._transform(kind) for kind in TEMPORAL])

    def _typed(self, name: str) -> tuple[str, str, str]:
        column, literal = TYPES[name]
        try:
            table = self._create(f"type_{name}", f"id BIGINT, x {column}")
            self.sql(f"INSERT INTO {self.t(table)} SELECT CAST(1 AS BIGINT), {literal}")
        except (Refused, NoOp) as exc:
            return name, REFUSED, str(exc)
        return type_result(self.iceberg(table), name)

    def types_scalar(self) -> str:
        return verdict([self._typed(name) for name in SCALAR_TYPES])

    def types_nested(self) -> str:
        return verdict([self._typed(name) for name in NESTED_TYPES])

    def sort_order(self) -> str:
        table = self._fresh("sorted")
        self.sql(f"ALTER TABLE {self.t(table)} WRITE ORDERED BY id")
        if not self.iceberg(table).sort_order().fields:
            raise NoOp("returned success and the table has no sort order")
        return "sort order set"

    # -- write -------------------------------------------------------------------------------

    def insert_into(self) -> str:
        table = self._empty("insert")
        self.sql(f"INSERT INTO {self.t(table)} VALUES {_values(SEED)}")
        return self._expect(table, SEED, "INSERT INTO")

    def insert_select(self) -> str:
        table = self._fresh("insertsel")
        self.sql(f"INSERT INTO {self.t(table)} SELECT id + 10, v FROM {self.t(table)}")
        return self._expect(table, SEED + [(11, 10), (12, 20), (13, 30)], "INSERT INTO ... SELECT")

    def delete_from(self) -> str:
        table = self._fresh("delete")
        self.sql(f"DELETE FROM {self.t(table)} WHERE id = 1")
        return self._expect(table, SEED[1:], "DELETE")

    def update(self) -> str:
        table = self._fresh("update")
        self.sql(f"UPDATE {self.t(table)} SET v = 999 WHERE id = 1")
        return self._expect(table, [(1, 999), (2, 20), (3, 30)], "UPDATE")

    def merge_upsert(self) -> str:
        table = self._fresh("merge")
        self.sql(
            f"MERGE INTO {self.t(table)} t "
            f"USING (SELECT * FROM (VALUES (1, 777), (9, 90)) AS s(id, v)) s "
            f"ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v "
            f"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
        return self._expect(table, [(1, 777), (2, 20), (3, 30), (9, 90)], "MERGE, UPDATE + INSERT")

    def truncate(self) -> str:
        table = self._fresh("truncate")
        self.sql(f"TRUNCATE TABLE {self.t(table)}")
        result = self._expect(table, [], "TRUNCATE")
        return f"{result}; {truncate_kind(self.iceberg(table))}"

    def insert_overwrite(self) -> str:
        table = self._fresh("overwrite")
        self.sql(f"INSERT OVERWRITE TABLE {self.t(table)} VALUES (7, 70), (8, 80)")
        return self._expect(table, [(7, 70), (8, 80)], "INSERT OVERWRITE")

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
        import pyarrow as pa
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        schema = promotion_schema()
        table = self._empty("promote", schema=schema)
        self.iceberg(table).append(
            pa.Table.from_pylist([{"id": 1, "c": 7}], schema=schema_to_pyarrow(schema))
        )
        self.sql(f"ALTER TABLE {self.t(table)} ALTER COLUMN c SET DATA TYPE BIGINT")
        return promotion_check(self.iceberg(table))

    def partition_evolution(self) -> str:
        table = self._fresh("specevo")
        self.sql(f"ALTER TABLE {self.t(table)} ADD PARTITION FIELD bucket(4, id)")
        spec = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("returned success and the table is still unpartitioned")
        return f"spec now {spec}"

    def write_after_evolution(self) -> str:
        """pyiceberg adds bucket(4, id), as for every engine; Polars writes before and after."""
        table = self._empty("evowrite")
        self.sql(f"INSERT INTO {self.t(table)} VALUES {_values(EVOLVE_FIRST)}")
        evolve_spec(self.iceberg(table))
        self.sql(f"INSERT INTO {self.t(table)} VALUES {_values(EVOLVE_THEN)}")
        count = self.sql(f"SELECT count(*) FROM {self.t(table)}")[0][0]
        return evolution_check(self.iceberg(table), count)

    def set_property(self) -> str:
        table = self._fresh("props")
        self.sql(f"ALTER TABLE {self.t(table)} SET TBLPROPERTIES ('probed-at' = '{self.run}')")
        if self.iceberg(table).properties.get("probed-at") != self.run:
            raise NoOp("returned success and the property is not on the table")
        return "the property is on the table"

    # -- read --------------------------------------------------------------------------------

    def time_travel(self) -> str:
        table = self._fresh("travel")
        seeded = self.iceberg(table).current_snapshot().snapshot_id
        self.iceberg(table).append(rows([(4, 40)]))
        found = self.sql(f"SELECT count(*) FROM {self.t(table)} FOR VERSION AS OF {seeded}")
        if found[0][0] != 3:
            raise NoOp(f"FOR VERSION AS OF the seed's snapshot reads {found[0][0]} rows, not 3")
        return "FOR VERSION AS OF the seed's snapshot reads 3 rows (the table has 4)"

    def metadata_tables(self) -> str:
        table = self._fresh("inspect")
        found = self.sql(f"SELECT count(*) FROM {self.t(table)}.snapshots")
        return f"t.snapshots reads {found[0][0]} snapshot(s)"

    def _stats_table(self, what: str) -> str:
        """harness.STATS_FILES, written by pyiceberg: four files with known min/max bounds."""
        table = self._empty(what)
        write_stats_files(self.iceberg(table))
        return table

    def select_table(self) -> str:
        """pyiceberg creates and seeds the table; the engine reads it by its catalog name."""
        table = self.name("select")
        self.catalog.create_table((self.ns, table), schema=iceberg_schema())
        self.created.append(table)
        self.catalog.load_table((self.ns, table)).append(rows(SEED))
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

    def _ref(self, kind: str) -> str:
        table = self._fresh(kind)
        self.sql(f"ALTER TABLE {self.t(table)} CREATE {kind.upper()} probe_{kind}")
        refs = sorted(self.iceberg(table).metadata.refs)
        if f"probe_{kind}" not in refs:
            raise NoOp(f"returned success and the refs are {refs}")
        return f"refs now {refs}"

    def create_branch(self) -> str:
        return self._ref("branch")

    def create_tag(self) -> str:
        return self._ref("tag")

    # -- maintenance -------------------------------------------------------------------------

    def rewrite_data_files(self) -> str:
        table = self._fresh("compact")
        self.iceberg(table).append(rows([(4, 40)]))
        before = len(list(self.iceberg(table).scan().plan_files()))
        self.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{self.ns}.{table}')")
        after = len(list(self.iceberg(table).scan().plan_files()))
        if after >= before:
            raise NoOp(f"returned success and data files went {before} -> {after}")
        return self._expect(table, SEED + [(4, 40)], f"{before} -> {after} data file(s), data kept")

    def rewrite_manifests(self) -> str:
        table = self._fresh("manifests")
        self.iceberg(table).append(rows([(4, 40)]))
        before = len(self._snapshots(table))
        self.sql(f"CALL {CATALOG}.system.rewrite_manifests(table => '{self.ns}.{table}')")
        if len(self._snapshots(table)) == before:
            raise NoOp("returned success and no new snapshot was committed")
        return "a new snapshot rewrote the manifests"

    # -- catalog -----------------------------------------------------------------------------

    def namespace_create(self) -> str:
        ns = self.name("ns")
        self.sql(f"CREATE SCHEMA {CATALOG}.{ns}")
        if (ns,) not in self.catalog.list_namespaces():
            raise NoOp("CREATE SCHEMA returned success and pyiceberg does not list it")
        self.namespaces.append(ns)
        return f"`{ns}` created; pyiceberg lists it"

    def namespace_drop(self) -> str:
        """An empty namespace pyiceberg makes, so the drop is asked even without CREATE SCHEMA."""
        ns = self.name("nsdrop")
        self.catalog.create_namespace(ns)
        self.namespaces.append(ns)
        self.sql(f"DROP SCHEMA {CATALOG}.{ns}")
        if (ns,) in self.catalog.list_namespaces():
            raise NoOp("DROP SCHEMA returned success and pyiceberg still lists it")
        self.namespaces.remove(ns)
        return "the empty namespace is gone"

    def namespace_existing(self) -> str:
        self.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{self.ns}")
        return f"accepted on the existing `{self.ns}`"

    def drop_purge(self) -> str:
        """DROP TABLE ... PURGE (pola-rs/polars#30247)."""
        table = self._fresh("droppable")
        tbl = self.iceberg(table)
        data = [task.file.file_path for task in tbl.scan().plan_files()]
        self.sql(f"DROP TABLE {self.t(table)} PURGE")
        if self.catalog.table_exists((self.ns, table)):
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
        """pyiceberg makes and seeds the table; Polars writes through a second catalog, `race`,
        on the race proxy, whose commit bench.capability.race holds until pyiceberg's has landed."""
        if self.ctx is None:
            raise Skip("no Polars session")
        if self.proxy is None:
            self.proxy = RaceProxy().start()
            self.ctx.register_catalog(
                "race", auth.catalog(self.cfg, self.proxy.endpoint), storage_options=self.storage
            )
        table = self._fresh(key)
        raced = self.t(table, catalog="race")
        refused: list[Refused] = []

        def write() -> None:
            try:
                self.sql(statement.format(t=raced))
            except Refused as exc:
                refused.append(exc)
                raise

        try:
            return race_probe(key, self.catalog, self.proxy, table, write)
        except Broken:
            # Refused before any commit was sent: nothing raced, the statement is the answer.
            if refused and not self.proxy.commits.get((self.ns, table)):
                raise refused[0] from None
            raise

    def race_append(self) -> str:
        return self._race("race_append", "INSERT INTO {t} VALUES (5, 50)")

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
                if self.catalog.table_exists((self.ns, table)):
                    self.catalog.purge_table((self.ns, table))
                dropped += 1
            except Exception as exc:  # noqa: BLE001 - teardown is best effort, by design
                left.append(f"{table} ({_one_line(exc, 80)})")
        for ns in self.namespaces:
            try:
                self.catalog.drop_namespace(ns)
            except Exception as exc:  # noqa: BLE001
                left.append(f"namespace {ns} ({_one_line(exc, 80)})")
        print(f"\ndropped {dropped} probe table(s)" + (f"; left behind {left}" if left else ""))


PROBES = [
    ("session", "Polars attaches the REST catalog with register_catalog", "session"),
    ("create", "CREATE TABLE", "create_table"),
    ("create", "CREATE TABLE IF NOT EXISTS on an existing table", "create_if_not_exists"),
    ("create", "CREATE TABLE ... AS SELECT", "ctas"),
    ("create", "PARTITIONED BY (p), then INSERT", "partitioned"),
    ("create", "ALTER TABLE ... WRITE ORDERED BY (sort order)", "sort_order"),
    ("create", "PARTITIONED BY (bucket(4, id)), then INSERT", "partition_bucket"),
    ("create", "PARTITIONED BY (truncate(2, x)), then INSERT", "partition_truncate"),
    ("create", "PARTITIONED BY years / months / days / hours, then INSERT", "partition_temporal"),
    ("create", "types: decimal, date, timestamp, timestamptz, uuid, binary", "types_scalar"),
    ("create", "nested types: struct, list, map", "types_nested"),
    ("write", "INSERT INTO", "insert_into"),
    ("write", "INSERT INTO ... SELECT", "insert_select"),
    ("write", "DELETE FROM", "delete_from"),
    ("write", "UPDATE", "update"),
    ("write", "MERGE, two actions: MATCHED UPDATE + NOT MATCHED INSERT", "merge_upsert"),
    ("write", "TRUNCATE TABLE", "truncate"),
    ("write", "INSERT OVERWRITE, whole table", "insert_overwrite"),
    ("schema", "ALTER TABLE ADD COLUMN", "add_column"),
    ("schema", "ALTER TABLE DROP COLUMN", "drop_column"),
    ("schema", "ALTER TABLE RENAME COLUMN", "rename_column"),
    ("schema", "ALTER COLUMN c SET DATA TYPE BIGINT (int -> long)", "type_promotion"),
    ("schema", "ALTER TABLE ADD PARTITION FIELD (partition evolution)", "partition_evolution"),
    ("schema", "INSERT, pyiceberg adds bucket(4, id), INSERT", "write_after_evolution"),
    ("schema", "ALTER TABLE SET TBLPROPERTIES", "set_property"),
    ("read", "time travel, FOR VERSION AS OF", "time_travel"),
    ("read", "metadata tables (t.snapshots)", "metadata_tables"),
    ("read", "SELECT a table by its catalog name", "select_table"),
    ("read", "WHERE id = 22 with the other data files deleted (file pruning)", "file_pruning"),
    ("read", "max(id) with every data file deleted (min/max from metadata)", "max_from_metadata"),
    ("refs", "ALTER TABLE ... CREATE BRANCH", "create_branch"),
    ("refs", "ALTER TABLE ... CREATE TAG", "create_tag"),
    ("maintenance", "CALL system.rewrite_data_files (compaction)", "rewrite_data_files"),
    ("maintenance", "CALL system.rewrite_manifests", "rewrite_manifests"),
    ("catalog", "CREATE SCHEMA (namespace)", "namespace_create"),
    ("catalog", "DROP SCHEMA (namespace)", "namespace_drop"),
    ("catalog", "CREATE SCHEMA IF NOT EXISTS, existing namespace", "namespace_existing"),
    ("catalog", "DROP TABLE ... PURGE", "drop_purge"),
    ("concurrency", "INSERT INTO; B appends between Polars' read and commit", "race_append"),
    ("concurrency", "DELETE id 1; B appends", "race_delete"),
    ("concurrency", "UPDATE v = v + 1 on id 2; B appends", "race_update"),
]


def write_step_summary(probe: PolarsCapability) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    counts = probe.report.tally()
    lines = [
        "## OneLake Iceberg REST catalog, asked by Polars SQL",
        "",
        f"{counts[SUPPORTED]} supported · {counts[REFUSED]} refused · "
        f"{counts[NOOP]} accepted then ignored · "
        f"{counts[SKIPPED]} skipped · {counts[BROKEN]} could not be asked",
        "",
        *probe.report.markdown(),
        "",
        f"polars `{probe.version}` · namespace `{NAMESPACE}` · run `{probe.run}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    if not any_wanted(PROBES):
        print(f"this run asks none of {', '.join(sorted(asked()))} of this client")
        return 0
    cfg = Config.from_env()
    probe = PolarsCapability(cfg)
    print(f"workspace {cfg.workspace_id}  lakehouse {cfg.lakehouse_id}  namespace {NAMESPACE}")

    try:
        for group, question, method in PROBES:
            if not wanted(method):
                continue
            ok = probe.report.run(group, question, getattr(probe, method))
            if not ok and method == "session":
                print("\nstopped: no catalog, so nothing below could be asked")
                write_step_summary(probe)
                return 1
    finally:
        try:
            probe.drop_everything()
        except Exception as exc:  # noqa: BLE001 - teardown must not mask the findings
            print(f"  warning: teardown failed: {_one_line(exc, 200)}")
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
    probe.report.save("polars", probe.version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
