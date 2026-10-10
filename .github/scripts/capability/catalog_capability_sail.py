"""What the OneLake Iceberg REST catalog lets LakeSail (Sail) do.

Sail speaks Spark SQL but its Iceberg support is its own Rust implementation, so like DuckDB it
is an independent witness: a `no` it shares with pyiceberg and DuckDB is very likely the
catalog's.

THE SESSION IS bench.capability.engines.lakesail_iceberg's: an in-process Spark
Connect server, the OneLake catalog in SAIL_CATALOG__LIST, and storage signed with the engine's
own token in AZURE_STORAGE_TOKEN -- Sail logs that it does not implement vended credentials. The
session time zone is set to UTC, the known fix for Sail's timestamptz writer.

EVERY PROBE CHECKS ITS EFFECT through pyiceberg, which reads what the CATALOG now says rather than
what Sail cached. Accepted with no effect is a no-op, not a yes. Every error Sail raises is a `no`
carrying its full message, so the log says whose limit it is.

"""

from __future__ import annotations

import os
import sys

from harness import (
    BROKEN,
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
    iceberg_schema,
    partition_result,
    promotion_check,
    read_without_files,
    remove_data_files,
    rows,
    transform_case,
    type_result,
    verdict,
    write_stats_files,
)
from isolation_duckdb import race_probe

from bench import auth, scrub
from bench.capability.engines.lakesail_iceberg import LakesailIceberg
from bench.capability.race import RaceProxy
from bench.config import Config

CATALOG = "onelake"
SEED = [(1, 10), (2, 20), (3, 30)]
# Sail writes DELETE and MERGE only as merge-on-read, and says so for copy-on-write tables.
MOR = (
    "TBLPROPERTIES ('write.delete.mode' = 'merge-on-read', "
    "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read')"
)


def _one_line(text: object, limit: int = 700) -> str:
    flat = " ".join(scrub.scrub(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _say(text: object) -> None:
    print(scrub.scrub(text), flush=True)


# Column type and literal for each TYPE_EXPECTED case. Spark SQL has no UUID type; the create
# carries `UUID` anyway, so the answer is the parser's own.
TYPES = {
    "decimal": ("DECIMAL(10, 2)", "CAST('12.34' AS DECIMAL(10, 2))"),
    "date": ("DATE", "DATE '2026-01-02'"),
    "timestamp": ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"),
    "timestamptz": ("TIMESTAMP", "TIMESTAMP '2026-01-02 03:04:05.123456'"),
    "uuid": ("UUID", "'6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab'"),
    "binary": ("BINARY", "X'0102'"),
    "struct": ("STRUCT<a: BIGINT, b: STRING>", "named_struct('a', CAST(1 AS BIGINT), 'b', 'x')"),
    "list": ("ARRAY<BIGINT>", "CAST(array(1, 2, 3) AS ARRAY<BIGINT>)"),
    "map": ("MAP<STRING, BIGINT>", "map('k', CAST(1 AS BIGINT))"),
}


def _transform_sql(kind: str) -> tuple[str, str, str]:
    """(columns, partition transform, INSERT's SELECT) for one transform_case."""
    if kind == "bucket":
        columns, part, cast = "id BIGINT, x BIGINT", "bucket(4, id)", "CAST(x AS BIGINT)"
        values = [f"({i}, {v})" for i, v in TRANSFORM_ROWS["bucket"]]
    elif kind == "truncate":
        columns, part, cast = "id BIGINT, x STRING", "truncate(2, x)", "x"
        values = [f"({i}, '{v}')" for i, v in TRANSFORM_ROWS["truncate"]]
    else:
        columns, part, cast = (
            "id BIGINT, x TIMESTAMP_NTZ",
            f"{kind}s(x)",
            "CAST(x AS TIMESTAMP_NTZ)",
        )
        values = [f"({i}, '{v}')" for i, v in TRANSFORM_ROWS["temporal"]]
    select = (
        f"SELECT CAST(id AS BIGINT) AS id, {cast} AS x FROM VALUES {', '.join(values)} AS s(id, x)"
    )
    return columns, part, select


def _select(pairs) -> str:
    """A SELECT yielding BIGINT (id, v) rows, so no insert depends on implicit casts."""
    values = ", ".join(f"({i}, {v})" for i, v in pairs)
    return (
        f"SELECT CAST(id AS BIGINT) AS id, CAST(v AS BIGINT) AS v FROM VALUES {values} AS s(id, v)"
    )


class SailCapability:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.engine = LakesailIceberg(cfg)
        self.spark = None
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
        """One statement. Sail's own `no` becomes `Refused`, carrying its message."""
        _say(f"       > {_one_line(statement, 300)}")
        try:
            return [tuple(row) for row in self.spark.sql(statement).collect()]
        except Exception as exc:  # noqa: BLE001 - every Sail error is an answer
            raise Refused(_one_line(f"{type(exc).__name__}: {exc}")) from None

    def name(self, what: str) -> str:
        return f"sl_{self.run}_{what}"

    def t(self, table: str, ns: str | None = None) -> str:
        return f"{CATALOG}.`{ns or self.ns}`.`{table}`"

    def ident(self, table: str) -> str:
        """The `ns.table` string Iceberg's CALL procedures take."""
        return f"{self.ns}.{table}"

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

    def _sail_rows(self, table: str, cols: str = "id, v"):
        try:
            return [
                tuple(r)
                for r in self.spark.sql(f"SELECT {cols} FROM {self.t(table)} ORDER BY id").collect()
            ]
        except Exception as exc:  # noqa: BLE001 - a read-back must not decide the answer
            return f"Sail could not read it back: {_one_line(exc, 300)}"

    def _expect(self, table: str, expected: list[tuple], what: str, cols=("id", "v")) -> str:
        """pyiceberg decides, because it reads the catalog; Sail's own read is reported beside."""
        py = self._py_rows(table, cols)
        sail = self._sail_rows(table, ", ".join(cols))
        if py != expected:
            raise NoOp(
                f"{what} returned success; expected {expected}, pyiceberg reads {py}, "
                f"Sail reads {sail}"
            )
        agree = "Sail agrees" if sail == py else f"Sail reads {sail}"
        return f"{what}: pyiceberg reads {py}; {agree}"

    def _delete_files(self, table: str) -> str:
        """What kind of delete files the current snapshot carries, read from its manifests."""
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
        self.sql(f"CREATE TABLE {self.t(table)} ({columns}) USING iceberg {tail}")
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

    def _partitioned(self, what: str) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create(what, "id BIGINT, v BIGINT, p STRING", "PARTITIONED BY (p)")
        self.sql(
            f"INSERT INTO {self.t(table)} SELECT CAST(id AS BIGINT), CAST(v AS BIGINT), p "
            f"FROM VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b') AS s(id, v, p)"
        )
        return table

    # -- session -----------------------------------------------------------------------------

    def session(self) -> str:
        # A second catalog, `race`, on the race proxy, for the concurrency probes.
        self.proxy = RaceProxy().start()
        self.engine.setup(race_endpoint=self.proxy.endpoint)
        self.spark = self.engine.session
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")
        self.version = self.engine.version
        listed = (self.ns,) in self.pyiceberg().list_namespaces()
        return f"pysail {self.version}; {NAMESPACE} {'listed' if listed else 'NOT listed'}"

    # -- create ------------------------------------------------------------------------------

    def create_table(self) -> str:
        table = self._create("base")
        self.can_create = True
        tbl = self.iceberg(table)
        ids = [f.field_id for f in tbl.schema().fields]
        return f"field ids {ids}, format-version {tbl.metadata.format_version}"

    def ctas(self) -> str:
        table = self.name("ctas")
        self.created.append(table)
        self.sql(f"CREATE TABLE {self.t(table)} USING iceberg AS {_select(SEED)}")
        return self._expect(table, SEED, "CTAS")

    def partitioned(self) -> str:
        table = self._partitioned("part")
        spec = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("PARTITIONED BY returned success and the spec is empty")
        files = len(list(self.iceberg(table).scan().plan_files()))
        return f"spec {spec}; 3 rows over 2 partitions wrote {files} data file(s)"

    def _transform(self, kind: str) -> tuple[str, str, str]:
        columns, part, select = _transform_sql(kind)
        _, _, parts = transform_case(kind)
        try:
            table = self._create(f"tr_{kind}", columns, f"PARTITIONED BY ({part})")
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
            self.sql(f"INSERT INTO {self.t(table)} SELECT CAST(1 AS BIGINT) AS id, {literal} AS x")
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
        self.sql(f"ALTER TABLE {self.t(table)} WRITE ORDERED BY id")
        if not self.iceberg(table).sort_order().fields:
            raise NoOp("WRITE ORDERED BY returned success and the table has no sort order")
        return "sort order set"

    # -- write -------------------------------------------------------------------------------

    def insert_into(self) -> str:
        table = self._fresh("insert")
        return self._expect(table, SEED, "INSERT INTO") + (
            f"; {len(self._snapshots(table))} commit(s)"
        )

    def delete_from(self) -> str:
        table = self._fresh("delete")
        self.sql(f"DELETE FROM {self.t(table)} WHERE id = 1")
        return self._expect(table, SEED[1:], "DELETE")

    def delete_mor(self) -> str:
        table = self._fresh("delmor", MOR)
        # BIGINT literal: Sail's delete compares `id = 1` as Int64 == Int32 and refuses.
        self.sql(f"DELETE FROM {self.t(table)} WHERE id = CAST(1 AS BIGINT)")
        return (
            f"{self._expect(table, SEED[1:], 'merge-on-read DELETE')}; {self._delete_files(table)}"
        )

    def merge_delete_mor(self) -> str:
        """A DELETE written as a MERGE, which Sail plans through its position-delete writer."""
        table = self._fresh("mrgdelmor", MOR)
        self.sql(
            f"MERGE INTO {self.t(table)} t USING ({_select([(1, 0)])}) s "
            f"ON t.id = s.id WHEN MATCHED THEN DELETE"
        )
        return (
            f"{self._expect(table, SEED[1:], 'merge-on-read MERGE ... WHEN MATCHED THEN DELETE')}; "
            f"{self._delete_files(table)}"
        )

    def merge_mor(self) -> str:
        table = self._fresh("mergemor", MOR)
        self.sql(
            f"MERGE INTO {self.t(table)} t USING ({_select([(1, 777), (9, 90)])}) s "
            f"ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v "
            f"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
        return (
            self._expect(table, [(1, 777), (2, 20), (3, 30), (9, 90)], "merge-on-read MERGE INTO")
            + f"; {self._delete_files(table)}"
        )

    def update(self) -> str:
        table = self._fresh("update")
        self.sql(f"UPDATE {self.t(table)} SET v = 999 WHERE id = 1")
        return self._expect(table, [(1, 999), (2, 20), (3, 30)], "UPDATE")

    def _merge(self, what: str, action: str, expected: list, label: str) -> str:
        """One MERGE with a single action, source (1, 777), (9, 90): DuckDB's same four."""
        table = self._fresh(what)
        self.sql(
            f"MERGE INTO {self.t(table)} t USING ({_select([(1, 777), (9, 90)])}) s "
            f"ON t.id = s.id {action}"
        )
        return self._expect(table, expected, label)

    def merge_update_only(self) -> str:
        return self._merge(
            "mergeupd",
            "WHEN MATCHED THEN UPDATE SET t.v = s.v",
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

    def truncate(self) -> str:
        table = self._fresh("truncate")
        self.sql(f"TRUNCATE TABLE {self.t(table)}")
        return self._expect(table, [], "TRUNCATE")

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
        self.sql(f"INSERT INTO {self.t(table)} SELECT CAST(1 AS BIGINT) AS id, CAST(7 AS INT) AS c")
        self.sql(f"ALTER TABLE {self.t(table)} ALTER COLUMN c TYPE BIGINT")
        return promotion_check(self.iceberg(table))

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

    # -- read --------------------------------------------------------------------------------

    def time_travel(self) -> str:
        table = self._fresh("travel")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        first = self._snapshots(table)[0].snapshot_id
        rows = self.sql(f"SELECT count(*) FROM {self.t(table)} VERSION AS OF {first}")
        if rows[0][0] != 3:
            raise NoOp(f"VERSION AS OF the first snapshot reads {rows[0][0]} rows, not 3")
        return "VERSION AS OF the first snapshot reads 3 rows (the table has 4)"

    def metadata_tables(self) -> str:
        table = self._fresh("inspect")
        rows = self.sql(f"SELECT count(*) FROM {self.t(table)}.snapshots")
        return f"t.snapshots reads {rows[0][0]} snapshot(s)"

    def _stats_table(self, what: str) -> str:
        """harness.STATS_FILES, written by pyiceberg: four files with known min/max bounds."""
        table = self.name(what)
        self.pyiceberg().create_table((self.ns, table), iceberg_schema())
        self.created.append(table)
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
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(5, 50)])}")
        before = len(self._snapshots(table))
        self.sql(
            f"CALL {CATALOG}.system.expire_snapshots(table => '{self.ident(table)}', "
            f"older_than => current_timestamp(), retain_last => 1)"
        )
        after = len(self._snapshots(table))
        if after >= before:
            raise NoOp(f"returned success and history went {before} -> {after}")
        return f"history {before} -> {after} snapshot(s)"

    def rewrite_data_files(self) -> str:
        table = self._fresh("compact")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        before = len(list(self.iceberg(table).scan().plan_files()))
        self.sql(
            f"CALL {CATALOG}.system.rewrite_data_files(table => '{self.ident(table)}', "
            f"options => map('rewrite-all', 'true'))"
        )
        after = len(list(self.iceberg(table).scan().plan_files()))
        if after >= before:
            raise NoOp(f"returned success and data files went {before} -> {after}")
        return self._expect(table, SEED + [(4, 40)], f"{before} -> {after} data file(s), data kept")

    def rewrite_manifests(self) -> str:
        table = self._fresh("manifests")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        before = len(self._snapshots(table))
        self.sql(f"CALL {CATALOG}.system.rewrite_manifests('{self.ident(table)}')")
        if len(self._snapshots(table)) == before:
            raise NoOp("returned success and no new snapshot was committed")
        return "a new snapshot rewrote the manifests"

    # -- catalog -----------------------------------------------------------------------------

    def namespace(self) -> str:
        ns = self.name("ns")
        self.sql(f"CREATE DATABASE {CATALOG}.`{ns}`")
        self.namespaces.append(ns)
        if (ns,) not in self.pyiceberg().list_namespaces():
            raise NoOp("CREATE DATABASE returned success and pyiceberg does not list it")
        self.sql(f"DROP DATABASE {CATALOG}.`{ns}`")
        if (ns,) in self.pyiceberg().list_namespaces():
            raise NoOp("DROP DATABASE returned success and pyiceberg still lists it")
        self.namespaces.remove(ns)
        return f"`{ns}` created, listed, dropped"

    def rename_table(self) -> str:
        """Renamed AND readable. OneLake moves the folder on a rename; every engine so far found
        the manifest list still pointing at the old one."""
        table = self._fresh("rename")
        target = self.name("renamed")
        self.sql(f"ALTER TABLE {self.t(table)} RENAME TO {self.t(target)}")
        self.created = [target if c == table else c for c in self.created]
        rows = self._py_rows(target)
        if rows != SEED:
            raise NoOp(f"renamed, and the data is unreadable under the new name: {rows}")
        return "renamed, and its 3 rows read back"

    def drop_purge(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create("droppable")
        self.sql(f"DROP TABLE {self.t(table)} PURGE")
        if self.pyiceberg().table_exists((self.ns, table)):
            raise NoOp("returned success and the catalog still has it")
        self.created.remove(table)
        return "gone from the catalog"

    # -- concurrency -------------------------------------------------------------------------

    def _race(self, key: str, statement: str) -> str:
        """pyiceberg makes and seeds the table; Sail writes through the `race` catalog, whose
        commit bench.capability.race holds until pyiceberg's has landed."""
        if self.spark is None:
            raise Skip("no Sail session")
        table = self.name(key)
        self.pyiceberg().create_table((self.ns, table), schema=iceberg_schema())
        self.created.append(table)
        self.iceberg(table).append(rows(SEED))
        raced = f"race.`{self.ns}`.`{table}`"
        return race_probe(
            key, self.pyiceberg(), self.proxy, table, lambda: self.sql(statement.format(t=raced))
        )

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
    ("session", "Sail starts and attaches the REST catalog", "session"),
    ("create", "CREATE TABLE ... USING iceberg", "create_table"),
    ("create", "CREATE TABLE ... AS SELECT", "ctas"),
    ("create", "PARTITIONED BY (p), then INSERT", "partitioned"),
    ("create", "ALTER TABLE ... WRITE ORDERED BY (sort order)", "sort_order"),
    ("create", "PARTITIONED BY (bucket(4, id)), then INSERT", "partition_bucket"),
    ("create", "PARTITIONED BY (truncate(2, x)), then INSERT", "partition_truncate"),
    ("create", "PARTITIONED BY years / months / days / hours, then INSERT", "partition_temporal"),
    ("create", "types: decimal, date, timestamp, timestamptz, uuid, binary", "types_scalar"),
    ("create", "nested types: struct, list, map", "types_nested"),
    ("write", "INSERT INTO", "insert_into"),
    ("write", "DELETE FROM", "delete_from"),
    ("write", "DELETE FROM, merge-on-read table", "delete_mor"),
    ("write", "UPDATE", "update"),
    ("write", "MERGE, UPDATE only", "merge_update_only"),
    ("write", "MERGE, DELETE only", "merge_delete_only"),
    ("write", "MERGE, INSERT only", "merge_insert_only"),
    ("write", "MERGE, NOT MATCHED BY SOURCE DELETE only", "merge_by_source_only"),
    ("write", "MERGE INTO, merge-on-read table", "merge_mor"),
    ("write", "MERGE ... WHEN MATCHED THEN DELETE, merge-on-read table", "merge_delete_mor"),
    ("write", "TRUNCATE TABLE", "truncate"),
    ("schema", "ALTER TABLE ADD COLUMN", "add_column"),
    ("schema", "ALTER TABLE DROP COLUMN", "drop_column"),
    ("schema", "ALTER TABLE RENAME COLUMN", "rename_column"),
    ("schema", "ALTER COLUMN c TYPE BIGINT (int -> long)", "type_promotion"),
    ("schema", "ALTER TABLE ADD PARTITION FIELD (partition evolution)", "partition_evolution"),
    ("schema", "ALTER TABLE SET TBLPROPERTIES", "set_property"),
    ("read", "time travel, VERSION AS OF", "time_travel"),
    ("read", "metadata tables (t.snapshots)", "metadata_tables"),
    ("read", "WHERE id = 22 with the other data files deleted (file pruning)", "file_pruning"),
    ("read", "max(id) with every data file deleted (min/max from metadata)", "max_from_metadata"),
    ("refs", "ALTER TABLE ... CREATE BRANCH", "create_branch"),
    ("refs", "ALTER TABLE ... CREATE TAG", "create_tag"),
    ("maintenance", "CALL system.expire_snapshots", "expire_snapshots"),
    ("maintenance", "CALL system.rewrite_data_files (compaction)", "rewrite_data_files"),
    ("maintenance", "CALL system.rewrite_manifests", "rewrite_manifests"),
    ("catalog", "CREATE DATABASE, then DROP DATABASE", "namespace"),
    ("catalog", "ALTER TABLE ... RENAME TO, then read", "rename_table"),
    ("catalog", "DROP TABLE ... PURGE", "drop_purge"),
    ("concurrency", "INSERT INTO; B appends between Sail's read and commit", "race_append"),
    ("concurrency", "DELETE id 1; B appends", "race_delete"),
    ("concurrency", "UPDATE v = v + 1 on id 2; B appends", "race_update"),
]


def write_step_summary(probe: SailCapability) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    counts = probe.report.tally()
    lines = [
        "## OneLake Iceberg REST catalog, asked by Sail",
        "",
        f"{counts[SUPPORTED]} supported · {counts[REFUSED]} refused · "
        f"{counts[NOOP]} accepted then ignored · "
        f"{counts[SKIPPED]} skipped · {counts[BROKEN]} could not be asked",
        "",
        *probe.report.markdown(),
        "",
        f"pysail `{probe.version}` · namespace `{probe.ns}` · run `{probe.run}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    cfg = Config.from_env()
    probe = SailCapability(cfg)
    print(f"workspace {cfg.workspace_id}  lakehouse {cfg.lakehouse_id}  namespace {NAMESPACE}")

    try:
        for group, question, method in PROBES:
            ok = probe.report.run(group, question, getattr(probe, method))
            if not ok and method == "session":
                print("\nstopped: no Sail session, so nothing below could be asked")
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
    probe.report.save("sail", probe.version)
    return 0


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    # Sail's gRPC server runs on non-daemon threads; if close() left one behind, a plain exit
    # would hang until the job timeout. The findings are already printed and summarised.
    os._exit(code)
