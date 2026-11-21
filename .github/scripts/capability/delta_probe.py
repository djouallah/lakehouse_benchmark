"""What every Delta support probe shares: the probes, their checks, and the run.

THE SAME APPROACH AS THE ICEBERG GRID (harness.py), on Delta tables written by path into OneLake:
every engine is asked every row in SQL, one statement at a time, and every effect is checked
outside the engine (bench.capability.delta.DeltaOracle: DuckDB for the rows, the _delta_log
for the rest). Accepted with no effect is a no-op; every error the engine raises is a `no`
carrying its own words.

ONE CLASS, ONE DIALECT PER ENGINE. `DeltaProbe` holds every probe and writes Spark SQL, which is
Delta's own dialect: Spark's and Sail's scripts use it byte for byte. DuckDB and Trino
override the statements they spell differently. Where an engine has no statement for an
operation, it is sent the closest one it has, or Spark's, so the `no` is in its own words.

    from delta_probe import run
    run(SparkDelta)
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
import sys
import time

from harness import (
    BROKEN,
    NESTED_TYPES,
    NOOP,
    PRUNE_EXPECTED,
    PRUNE_ID,
    REFUSED,
    SKIPPED,
    STATS_FILES,
    SUPPORTED,
    TYPE_EXPECTED,
    NoOp,
    Refused,
    Report,
    Skip,
    any_wanted,
    asked,
    normalize,
    read_without_files,
    verdict,
    wanted,
)

from bench import scrub
from bench.capability.delta import NAMESPACE, DeltaOracle, keep
from bench.config import Config

SEED = [(1, 10), (2, 20), (3, 30)]
NEW_ROWS = [(7, 70), (8, 80)]
# Delta has no UUID type, so the scalar row asks the five it has.
SCALAR_TYPES = ("decimal", "date", "timestamp", "timestamptz", "binary")
CHECKPOINT_COMMITS = 11


def one_line(text: object, limit: int = 600) -> str:
    flat = " ".join(scrub.scrub(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def say(text: object) -> None:
    print(scrub.scrub(text), flush=True)


class DeltaProbe:
    CLIENT = "spark"
    TITLE = "Spark"
    PREFIX = "sp"

    BIGINT, INT, STRING, DATE, TIMESTAMP = "BIGINT", "INT", "STRING", "DATE", "TIMESTAMP"
    # Column type and literal for each TYPE_EXPECTED case, in Spark SQL.
    TYPES = {
        "decimal": ("DECIMAL(10, 2)", "CAST('12.34' AS DECIMAL(10, 2))"),
        "date": ("DATE", "DATE '2026-01-02'"),
        "timestamp": ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"),
        "timestamptz": ("TIMESTAMP", "TIMESTAMP '2026-01-02 03:04:05.123456'"),
        "binary": ("BINARY", "X'0102'"),
        "struct": (
            "STRUCT<a: BIGINT, b: STRING>",
            "named_struct('a', CAST(1 AS BIGINT), 'b', 'x')",
        ),
        "list": ("ARRAY<BIGINT>", "CAST(array(1, 2, 3) AS ARRAY<BIGINT>)"),
        "map": ("MAP<STRING, BIGINT>", "map('k', CAST(1 AS BIGINT))"),
    }

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.oracle = DeltaOracle(cfg)
        self.report = Report(reason=lambda exc: one_line(scrub.scrub_exc(exc, 2000)))
        self.run = "{}_{}".format(
            os.environ.get("GITHUB_RUN_ID", "local"), os.environ.get("GITHUB_RUN_ATTEMPT", "1")
        )
        self.version = "no session"
        self.can_create = False
        self.created: list[str] = []

    # == the engine: what a subclass provides ================================================

    def start(self) -> str:
        """Start the engine; its version."""
        raise NotImplementedError

    def close(self) -> None:
        pass

    def execute(self, statement: str) -> list[tuple]:
        """One statement through the engine, its rows. Any exception is the engine's answer."""
        raise NotImplementedError

    def ref(self, name: str) -> str:
        """How a statement names the probe table."""
        return f"`{name}`"

    def select(self, pairs) -> str:
        """A SELECT yielding BIGINT (id, v) rows, so no insert depends on implicit casts."""
        values = ", ".join(f"({i}, {v})" for i, v in pairs)
        return (
            f"SELECT CAST(id AS BIGINT) AS id, CAST(v AS BIGINT) AS v "
            f"FROM VALUES {values} AS s(id, v)"
        )

    def values(self, rows) -> str:
        """`VALUES (...), (...)` for an INSERT."""
        return "VALUES " + ", ".join("(" + ", ".join(map(str, r)) + ")" for r in rows)

    @staticmethod
    def _props(props: dict | None, keyword: str = "TBLPROPERTIES") -> str:
        if not props:
            return ""
        body = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        return f" {keyword} ({body})"

    def create_sql(
        self,
        name: str,
        columns: str,
        *,
        partition: list[str] | None = None,
        props: dict | None = None,
        cluster: list[str] | None = None,
        if_not_exists: bool = False,
    ) -> list[str]:
        """The statements that create an empty Delta table at the probe table's location."""
        ine = "IF NOT EXISTS " if if_not_exists else ""
        part = f" PARTITIONED BY ({', '.join(partition)})" if partition else ""
        clus = f" CLUSTER BY ({', '.join(cluster)})" if cluster else ""
        return [
            f"CREATE TABLE {ine}{self.ref(name)} ({columns}) USING delta{part}{clus} "
            f"LOCATION '{self.oracle.uri(name)}'{self._props(props)}"
        ]

    def ctas_sql(self, name: str, select: str) -> list[str]:
        return [
            f"CREATE TABLE {self.ref(name)} USING delta "
            f"LOCATION '{self.oracle.uri(name)}' AS {select}"
        ]

    def register_sql(self, name: str) -> list[str]:
        """Make a table delta-rs wrote readable by name."""
        return [f"CREATE TABLE {self.ref(name)} USING delta LOCATION '{self.oracle.uri(name)}'"]

    def generated_sql(self, name: str) -> list[str]:
        return [
            f"CREATE TABLE {self.ref(name)} (id BIGINT, ts TIMESTAMP_NTZ, "
            f"d DATE GENERATED ALWAYS AS (CAST(ts AS DATE))) USING delta PARTITIONED BY (d) "
            f"LOCATION '{self.oracle.uri(name)}'"
        ]

    def generated_insert(self, t: str) -> str:
        return f"INSERT INTO {t} (id, ts) VALUES (1, TIMESTAMP_NTZ '2026-01-02 03:04:05')"

    def delete_sql(self, t: str, where: str) -> str:
        return f"DELETE FROM {t} WHERE {where}"

    def update_sql(self, t: str, assign: str, where: str) -> str:
        return f"UPDATE {t} SET {assign} WHERE {where}"

    def merge_sql(self, t: str, source: str) -> str:
        return (
            f"MERGE INTO {t} AS tg USING ({source}) AS s ON tg.id = s.id "
            "WHEN MATCHED THEN UPDATE SET v = s.v "
            "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )

    def truncate_sql(self, t: str) -> str:
        return f"TRUNCATE TABLE {t}"

    def overwrite_sql(self, t: str, select: str) -> str:
        return f"INSERT OVERWRITE {t} {select}"

    def overwrite_partition_sql(self, t: str) -> str:
        return (
            f"INSERT OVERWRITE {t} PARTITION (p = 'a') "
            "SELECT CAST(7 AS BIGINT) AS id, CAST(70 AS BIGINT) AS v"
        )

    def add_column_sql(self, t: str) -> str:
        return f"ALTER TABLE {t} ADD COLUMN w {self.BIGINT}"

    def drop_column_sql(self, t: str) -> str:
        return f"ALTER TABLE {t} DROP COLUMN v"

    def rename_column_sql(self, t: str) -> str:
        return f"ALTER TABLE {t} RENAME COLUMN v TO v2"

    def widen_sql(self, t: str) -> str:
        return f"ALTER TABLE {t} ALTER COLUMN c TYPE BIGINT"

    def set_property_sql(self, t: str) -> str:
        return f"ALTER TABLE {t} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"

    def version_sql(self, t: str, version: int) -> str:
        return f"SELECT id, v FROM {t} VERSION AS OF {version} ORDER BY id"

    def timestamp_sql(self, t: str, at: str) -> str:
        """`at` is UTC, 'YYYY-MM-DD HH:MM:SS'."""
        return f"SELECT id, v FROM {t} TIMESTAMP AS OF '{at}' ORDER BY id"

    def history_sql(self, name: str) -> str:
        return f"DESCRIBE HISTORY {self.ref(name)}"

    def changes_sql(self, name: str, since: int) -> str:
        """id, v, _change_type from version `since` on."""
        return (
            f"SELECT id, v, _change_type FROM table_changes('{name}', {since}) "
            "ORDER BY id, _change_type"
        )

    def optimize_sql(self, t: str) -> str:
        return f"OPTIMIZE {t}"

    def zorder_sql(self, t: str) -> str:
        return f"OPTIMIZE {t} ZORDER BY (id)"

    def vacuum_sql(self, t: str) -> str:
        return f"VACUUM {t} RETAIN 0 HOURS"

    def checkpoint_sql(self, t: str) -> str | None:
        """An explicit checkpoint statement, for an engine that writes one only when asked."""
        return None

    def drop_sql(self, name: str) -> str | None:
        """Forget the table in the engine's own catalog before its files are purged."""
        return f"DROP TABLE IF EXISTS {self.ref(name)}"

    # == helpers ==============================================================================

    def sql(self, statement: str) -> list[tuple]:
        say(f"       > {one_line(statement, 300)}")
        try:
            return [tuple(r) for r in self.execute(statement)]
        except Exception as exc:  # noqa: BLE001 - every engine error is an answer
            raise Refused(one_line(f"{type(exc).__name__}: {scrub.scrub_exc(exc, 2000)}")) from None

    def run_all(self, statements: list[str]) -> None:
        for statement in statements:
            self.sql(statement)

    def name(self, what: str) -> str:
        return f"{self.PREFIX}_{self.run}_{what}".lower()

    def _claim(self, what: str) -> str:
        name = self.name(what)
        self.created.append(name)
        return name

    def _cols(self, *extra: str) -> str:
        return ", ".join([f"id {self.BIGINT}", f"v {self.BIGINT}", *extra])

    def _create(self, what: str, columns: str | None = None, **kw) -> str:
        name = self._claim(what)
        self.run_all(self.create_sql(name, columns or self._cols(), **kw))
        return name

    def _fresh(self, what: str, **kw) -> str:
        """A table holding SEED, made the way the create probe proved works."""
        if not self.can_create:
            raise Skip("no table could be created, so there is nothing to write to")
        try:
            name = self._create(what, **kw)
            self.sql(f"INSERT INTO {self.ref(name)} {self.select(SEED)}")
        except Refused as exc:
            if kw:
                raise Refused(
                    f"the table this asks for could not be created and seeded: {exc}"
                ) from None
            raise Skip(f"could not seed the table: {exc}") from None
        return name

    def _seeded_by_delta_rs(self, what: str, files=None) -> str:
        """A table delta-rs writes (one file per batch), made readable to the engine."""
        name = self._claim(what)
        for batch in files or [SEED]:
            self.oracle.write(name, batch)
        try:
            self.run_all(self.register_sql(name))
        except Refused as exc:
            raise Refused(f"a Delta table written by delta-rs could not be opened: {exc}") from None
        return name

    def _engine_rows(self, name: str, cols: str = "id, v"):
        try:
            return [
                tuple(r) for r in self.execute(f"SELECT {cols} FROM {self.ref(name)} ORDER BY id")
            ]
        except Exception as exc:  # noqa: BLE001 - a read-back must not decide the answer
            return f"the engine could not read it back: {one_line(exc, 300)}"

    def _expect(self, name: str, expected: list[tuple], what: str, cols=("id", "v")) -> str:
        """DuckDB decides, reading the log afresh; the engine's own read is reported beside."""
        found = self.oracle.rows(name, cols)
        engine = self._engine_rows(name, ", ".join(cols))
        if found != expected:
            raise NoOp(
                f"{what} returned success; expected {expected}, DuckDB reads {found}, "
                f"{self.TITLE} reads {engine}"
            )
        agree = f"{self.TITLE} agrees" if _same(engine, found) else f"{self.TITLE} reads {engine}"
        return f"{what}: DuckDB reads {found}; {agree}; {self._ops(name)}"

    def _ops(self, name: str) -> str:
        try:
            return f"log: {' > '.join(self.oracle.operations(name))}"
        except Exception as exc:  # noqa: BLE001 - informational only
            return f"log unreadable: {one_line(exc, 120)}"

    # == the probes ============================================================================

    def session(self) -> str:
        self.version = self.start()
        return f"{self.TITLE} {self.version}; tables under Tables/{NAMESPACE}"

    # -- core ----------------------------------------------------------------------------------

    def create_table(self) -> str:
        name = self._create("base")
        schema = self.oracle.schema(name)
        if schema != {"id": "long", "v": "long"}:
            raise NoOp(f"CREATE returned success; the log's schema is {schema}")
        self.can_create = True
        proto = self.oracle.protocol(name)
        return (
            f"version {self.oracle.latest(name)}, schema {schema}, protocol "
            f"reader {proto.get('minReaderVersion')} / writer {proto.get('minWriterVersion')}"
        )

    def select_table(self) -> str:
        name = self._seeded_by_delta_rs("select")
        found = self.sql(f"SELECT id, v FROM {self.ref(name)} ORDER BY id")
        if found != SEED:
            raise NoOp(f"reads {found}, delta-rs wrote {SEED}")
        return f"reads the {len(found)} rows delta-rs wrote"

    def ctas(self) -> str:
        name = self._claim("ctas")
        self.run_all(self.ctas_sql(name, self.select(SEED)))
        return self._expect(name, SEED, "CTAS")

    def create_if_not_exists(self) -> str:
        name = self._fresh("ine")
        self.run_all(self.create_sql(name, self._cols(f"extra {self.BIGINT}"), if_not_exists=True))
        schema = self.oracle.schema(name)
        found = self.oracle.rows(name)
        if "extra" in schema or found != SEED:
            raise NoOp(
                f"IF NOT EXISTS replaced the existing table: columns {list(schema)}, rows {found}"
            )
        return f"accepted, the existing table untouched: columns {list(schema)}, rows {found}"

    def insert_into(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        name = self._create("insert")
        self.sql(f"INSERT INTO {self.ref(name)} {self.values(SEED)}")
        return self._expect(name, SEED, "INSERT INTO ... VALUES")

    def insert_select(self) -> str:
        name = self._fresh("insertsel")
        t = self.ref(name)
        self.sql(f"INSERT INTO {t} SELECT id + 10, v FROM {t}")
        return self._expect(name, SEED + [(11, 10), (12, 20), (13, 30)], "INSERT INTO ... SELECT")

    def delete_from(self) -> str:
        name = self._fresh("delete")
        self.sql(self.delete_sql(self.ref(name), "id = 1"))
        return self._expect(name, SEED[1:], "DELETE")

    def update(self) -> str:
        name = self._fresh("update")
        self.sql(self.update_sql(self.ref(name), "v = 999", "id = 1"))
        return self._expect(name, [(1, 999), (2, 20), (3, 30)], "UPDATE")

    def insert_overwrite(self) -> str:
        name = self._fresh("overwrite")
        self.sql(self.overwrite_sql(self.ref(name), self.select(NEW_ROWS)))
        return self._expect(name, NEW_ROWS, "INSERT OVERWRITE")

    # -- writes --------------------------------------------------------------------------------

    def merge_upsert(self) -> str:
        name = self._fresh("merge")
        self.sql(self.merge_sql(self.ref(name), self.select([(1, 777), (9, 90)])))
        return self._expect(name, [(1, 777), (2, 20), (3, 30), (9, 90)], "MERGE")

    def truncate(self) -> str:
        name = self._fresh("truncate")
        self.sql(self.truncate_sql(self.ref(name)))
        result = self._expect(name, [], "TRUNCATE")
        added = self.oracle.actions(name, "add")
        if added:
            raise NoOp(f"TRUNCATE emptied the table by writing {len(added)} new file(s): {result}")
        return result

    def overwrite_partition(self) -> str:
        name = self._partitioned("ovpart")
        self.sql(self.overwrite_partition_sql(self.ref(name)))
        return self._expect(
            name, [(3, 30, "b"), (7, 70, "a")], "overwrite of partition p = 'a'", ("id", "v", "p")
        )

    def deletion_vectors(self) -> str:
        # 20 rows in one INSERT, so the deleted row shares its data file with others: a file
        # holding only deleted rows is dropped whole, and needs no deletion vector.
        many = [(i, i * 10) for i in range(1, 21)]
        if not self.can_create:
            raise Skip("no table could be created")
        try:
            name = self._create("dv", props={"delta.enableDeletionVectors": "true"})
            self.sql(f"INSERT INTO {self.ref(name)} {self.select(many)}")
        except Refused as exc:
            raise Refused(f"a table with deletion vectors on could not be created: {exc}") from None
        self.sql(self.delete_sql(self.ref(name), "id = 1"))
        engine = self._engine_rows(name)
        if not _same(engine, many[1:]):
            raise NoOp(f"DELETE returned success; {self.TITLE} reads {engine}")
        dvs = [a for a in self.oracle.actions(name, "add") if a.get("deletionVector")]
        if not dvs:
            added = len(self.oracle.actions(name, "add"))
            raise NoOp(
                f"the table has deletion vectors on and DELETE rewrote the data instead: "
                f"{added} new file(s), no deletion vector; {self._ops(name)}"
            )
        return f"DELETE wrote {len(dvs)} deletion vector(s); features {self.oracle.features(name)}"

    # -- partitions & types --------------------------------------------------------------------

    def _partitioned(self, what: str) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        name = self._create(what, self._cols(f"p {self.STRING}"), partition=["p"])
        self.sql(
            f"INSERT INTO {self.ref(name)} "
            + self.values([(1, 10, "'a'"), (2, 20, "'a'"), (3, 30, "'b'")])
        )
        return name

    def partitioned(self) -> str:
        name = self._partitioned("part")
        meta = self.oracle.metadata(name)
        if meta.get("partitionColumns") != ["p"]:
            raise NoOp(
                f"PARTITIONED BY returned success; partitionColumns {meta.get('partitionColumns')}"
            )
        parts = {str(a.get("partitionValues")) for a in self.oracle.active_files(name)}
        if len(parts) != 2:
            raise NoOp(f"partitionColumns ['p'], and the files carry partition values {parts}")
        return f"partitionColumns ['p']; 3 rows over {len(parts)} partitions"

    def generated_partition(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        name = self._claim("generated")
        self.run_all(self.generated_sql(name))
        self.sql(self.generated_insert(self.ref(name)))
        meta = self.oracle.metadata(name)
        fields = {f["name"]: f for f in json.loads(meta["schemaString"])["fields"]}
        expr = fields.get("d", {}).get("metadata", {}).get("delta.generationExpression")
        values = [a.get("partitionValues", {}).get("d") for a in self.oracle.active_files(name)]
        if not expr or meta.get("partitionColumns") != ["d"] or values != ["2026-01-02"]:
            raise NoOp(
                f"generation expression {expr!r}, partitionColumns {meta.get('partitionColumns')}, "
                f"file partition values {values}"
            )
        return f"d GENERATED ALWAYS AS ({expr}), partition d = {values[0]} derived on write"

    def _typed(self, kind: str) -> tuple[str, str, str]:
        column, literal = self.TYPES[kind]
        try:
            name = self._create(f"type_{kind}", f"id {self.BIGINT}, x {column}")
            self.sql(
                f"INSERT INTO {self.ref(name)} "
                f"SELECT CAST(1 AS {self.BIGINT}) AS id, {literal} AS x"
            )
        except Refused as exc:
            return kind, REFUSED, str(exc)
        try:
            found = self.oracle.column(name)
        except Exception as exc:  # noqa: BLE001 - the read-back failing is the answer
            return kind, NOOP, f"written; DuckDB cannot read it: {one_line(exc, 200)}"
        try:
            got = normalize(kind, found[0]) if len(found) == 1 else f"{len(found)} rows"
        except Exception:  # noqa: BLE001 - a value of another type reads back as itself
            got = repr(found[0])
        if got != TYPE_EXPECTED[kind]:
            return kind, NOOP, f"expected {TYPE_EXPECTED[kind]}, reads {got}"
        return kind, SUPPORTED, got

    def _types(self, kinds) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        return verdict([self._typed(k) for k in kinds])

    def types_scalar(self) -> str:
        return self._types(SCALAR_TYPES)

    def types_nested(self) -> str:
        return self._types(NESTED_TYPES)

    # -- schema --------------------------------------------------------------------------------

    MAPPED = {"delta.columnMapping.mode": "name"}

    def add_column(self) -> str:
        name = self._fresh("addcol")
        self.sql(self.add_column_sql(self.ref(name)))
        schema = self.oracle.schema(name)
        if "w" not in schema:
            raise NoOp(f"ADD COLUMN returned success; the log's schema is {schema}")
        return f"schema {schema}"

    def drop_column(self) -> str:
        name = self._fresh("dropcol", props=self.MAPPED)
        self.sql(self.drop_column_sql(self.ref(name)))
        schema = self.oracle.schema(name)
        if "v" in schema:
            raise NoOp(f"DROP COLUMN returned success; the log's schema is {schema}")
        return f"column-mapped table; schema {schema}; {self._ops(name)}"

    def rename_column(self) -> str:
        name = self._fresh("renamecol", props=self.MAPPED)
        self.sql(self.rename_column_sql(self.ref(name)))
        schema = self.oracle.schema(name)
        if "v2" not in schema or "v" in schema:
            raise NoOp(f"RENAME COLUMN returned success; the log's schema is {schema}")
        return self._expect(name, SEED, "RENAME COLUMN v TO v2", ("id", "v2"))

    def type_widening(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        try:
            name = self._create(
                "widen",
                f"id {self.BIGINT}, c {self.INT}",
                props={"delta.enableTypeWidening": "true"},
            )
            self.sql(f"INSERT INTO {self.ref(name)} {self.values([(1, 7)])}")
        except Refused as exc:
            raise Refused(
                f"a table with type widening on could not be created and seeded: {exc}"
            ) from None
        self.sql(self.widen_sql(self.ref(name)))
        kind = self.oracle.schema(name).get("c")
        if kind != "long":
            raise NoOp(f"returned success and c is still {kind}")
        read = self._expect(name, [(1, 7)], "ALTER COLUMN c TYPE BIGINT", ("id", "c"))
        return f"c is long in the log; {read}; features {self.oracle.features(name)}"

    def set_property(self) -> str:
        name = self._fresh("prop")
        self.sql(self.set_property_sql(self.ref(name)))
        conf = self.oracle.metadata(name).get("configuration", {})
        if conf.get("delta.enableChangeDataFeed") != "true":
            raise NoOp(f"returned success; the table's configuration is {conf}")
        return f"configuration {conf}"

    def write_after_schema_change(self) -> str:
        name = self._fresh("evolve")
        t = self.ref(name)
        self.sql(self.add_column_sql(t))
        self.sql(f"INSERT INTO {t} {self.values([(4, 40, 400)])}")
        expected = [(1, 10, None), (2, 20, None), (3, 30, None), (4, 40, 400)]
        return self._expect(name, expected, "ADD COLUMN w, then INSERT", ("id", "v", "w"))

    # -- reads & history -----------------------------------------------------------------------

    def file_pruning(self) -> str:
        name = self._seeded_by_delta_rs("prune", STATS_FILES)
        files = self.oracle.active_files(name)
        keep_, gone = [], []
        for add in files:
            stats = json.loads(add.get("stats") or "{}")
            low, high = stats.get("minValues", {}).get("id"), stats.get("maxValues", {}).get("id")
            (keep_ if low is not None and low <= PRUNE_ID <= high else gone).append(add["path"])
        if len(files) != len(STATS_FILES) or len(keep_) != 1:
            raise Skip(f"expected {len(STATS_FILES)} files with stats, one holding id {PRUNE_ID}")
        for path in gone:
            self.oracle.delete_file(name, path)
        if any(self.oracle.file_exists(name, p) for p in gone):
            raise Skip("data files still in storage after the delete")
        t = self.ref(name)
        return read_without_files(
            f"WHERE id = {PRUNE_ID}",
            lambda: self.sql(f"SELECT v FROM {t} WHERE id = {PRUNE_ID}"),
            PRUNE_EXPECTED,
            lambda: self.sql(f"SELECT sum(v) FROM {t}"),
            f"{len(gone)} of {len(files)} data files deleted",
        )

    def _history_table(self, what: str, gap: float = 0) -> tuple[str, int, str]:
        """SEED, then two more rows: (name, the seeded version, a UTC time between the two)."""
        name = self._fresh(what)
        seeded = self.oracle.latest(name)
        at = ""
        if gap:
            time.sleep(gap)
            at = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
            time.sleep(gap)
        self.sql(f"INSERT INTO {self.ref(name)} {self.select(NEW_ROWS)}")
        return name, seeded, at

    def time_travel_version(self) -> str:
        name, seeded, _ = self._history_table("ttv")
        found = self.sql(self.version_sql(self.ref(name), seeded))
        if found != SEED:
            raise NoOp(f"version {seeded} reads {found}, expected {SEED} (the table has 5 rows)")
        return f"version {seeded} reads the 3 seeded rows (the table has 5)"

    def time_travel_timestamp(self) -> str:
        name, _, at = self._history_table("tts", gap=5)
        found = self.sql(self.timestamp_sql(self.ref(name), at))
        if found != SEED:
            raise NoOp(f"as of {at} UTC reads {found}, expected {SEED} (the table has 5 rows)")
        return f"as of {at} UTC reads the 3 seeded rows (the table has 5)"

    def history(self) -> str:
        name, _, _ = self._history_table("history")
        found = self.sql(self.history_sql(name))
        versions = len(self.oracle.versions(name))
        if len(found) != versions:
            raise NoOp(f"{len(found)} history row(s); the log has {versions} commits")
        return f"{len(found)} rows, one per commit"

    def change_data_feed(self) -> str:
        name = self._fresh("cdf", props={"delta.enableChangeDataFeed": "true"})
        since = self.oracle.latest(name) + 1
        self.sql(self.update_sql(self.ref(name), "v = 999", "id = 1"))
        found = self.sql(self.changes_sql(name, since))
        kinds = sorted({str(r[-1]) for r in found})
        if not {"update_preimage", "update_postimage"} <= set(kinds):
            raise NoOp(f"the changes since version {since} read {found}")
        return f"changes since version {since}: {found}"

    # -- maintenance ---------------------------------------------------------------------------

    def optimize(self) -> str:
        name = self._seeded_by_delta_rs("optimize", STATS_FILES)
        before = len(self.oracle.active_files(name))
        self.sql(self.optimize_sql(self.ref(name)))
        after = len(self.oracle.active_files(name))
        expected = sorted(p for batch in STATS_FILES for p in batch)
        if after >= before:
            raise NoOp(f"returned success; {before} data files before and {after} after")
        return f"{before} data files into {after}; " + self._expect(name, expected, "OPTIMIZE")

    def zorder(self) -> str:
        name = self._seeded_by_delta_rs("zorder", STATS_FILES)
        self.sql(self.zorder_sql(self.ref(name)))
        info = self.oracle.actions(name, "commitInfo")
        params = info[0].get("operationParameters", {}) if info else {}
        if (
            not info
            or info[0].get("operation") != "OPTIMIZE"
            or "id" not in str(params.get("zOrderBy"))
        ):
            raise NoOp(f"returned success; the last commit is {info[0] if info else 'none'}")
        return f"OPTIMIZE commit, zOrderBy {params.get('zOrderBy')}"

    def liquid_clustering(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        name = self._create("cluster", cluster=["id"])
        domains = [d.get("domain") for d in self.oracle.all_actions(name, "domainMetadata")]
        features = self.oracle.features(name)
        if "clustering" not in features or "delta.clustering" not in domains:
            raise NoOp(f"CLUSTER BY returned success; features {features}, domains {domains}")
        return f"features {features}; domain delta.clustering"

    def vacuum(self) -> str:
        name = self._claim("vacuum")
        self.oracle.write(name, SEED)
        old = [a["path"] for a in self.oracle.active_files(name)]
        self.oracle.write(name, NEW_ROWS, mode="overwrite")
        try:
            self.run_all(self.register_sql(name))
        except Refused as exc:
            raise Refused(f"a Delta table written by delta-rs could not be opened: {exc}") from None
        if not all(self.oracle.file_exists(name, p) for p in old):
            raise Skip("the overwritten file is already gone before VACUUM")
        self.sql(self.vacuum_sql(self.ref(name)))
        left = [p for p in old if self.oracle.file_exists(name, p)]
        if left:
            raise NoOp(
                f"returned success; the {len(left)} file(s) the overwrite removed "
                "are still in storage"
            )
        return f"the {len(old)} removed file(s) deleted; " + self._expect(name, NEW_ROWS, "VACUUM")

    def checkpoint(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        name = self._create("checkpoint")
        t = self.ref(name)
        try:
            for i in range(CHECKPOINT_COMMITS):
                self.sql(f"INSERT INTO {t} {self.values([(i, i)])}")
        except Refused as exc:
            raise Skip(f"could not write the {CHECKPOINT_COMMITS} commits: {exc}") from None
        explicit = self.checkpoint_sql(t)
        if explicit:
            self.sql(explicit)
        if not self.oracle.checkpointed(name):
            raise NoOp(f"{len(self.oracle.versions(name))} commits and no checkpoint in _delta_log")
        how = f" ({explicit})" if explicit else ""
        return f"checkpoint written after {len(self.oracle.versions(name))} commits{how}"

    # == teardown =============================================================================

    def drop_everything(self) -> None:
        if keep():
            print(f"\nkeeping {len(self.created)} probe table(s) under Tables/{NAMESPACE}")
            return
        dropped, left = 0, []
        for name in self.created:
            statement = self.drop_sql(name)
            if statement:
                # The files go below either way.
                with contextlib.suppress(Exception):
                    self.execute(statement)
            try:
                self.oracle.purge(name)
                dropped += 1
            except Exception as exc:  # noqa: BLE001 - teardown is best effort
                if "PathNotFound" not in str(exc) and "not exist" not in str(exc).lower():
                    left.append(f"{name} ({one_line(exc, 80)})")
        print(f"\npurged {dropped} probe table(s)" + (f"; left behind {left}" if left else ""))


def _same(engine, oracle) -> bool:
    """The engine's rows against the oracle's, ignoring tuple vs list."""
    if isinstance(engine, str) or isinstance(oracle, str):
        return False
    return [tuple(r) for r in engine] == [tuple(r) for r in oracle]


# (group, question, probe key). The grid's rows are render_delta.py's.
PROBES = [
    ("session", "the engine starts", "session"),
    ("create", "CREATE TABLE at a path", "create_table"),
    ("read", "SELECT a Delta table delta-rs wrote", "select_table"),
    ("create", "CREATE TABLE ... AS SELECT", "ctas"),
    ("create", "CREATE TABLE IF NOT EXISTS on an existing table", "create_if_not_exists"),
    ("write", "INSERT INTO ... VALUES", "insert_into"),
    ("write", "INSERT INTO ... SELECT", "insert_select"),
    ("write", "DELETE", "delete_from"),
    ("write", "UPDATE", "update"),
    ("write", "INSERT OVERWRITE, whole table", "insert_overwrite"),
    ("write", "MERGE, UPDATE + INSERT", "merge_upsert"),
    ("write", "TRUNCATE", "truncate"),
    ("write", "overwrite one partition", "overwrite_partition"),
    ("write", "DELETE on a table with deletion vectors on", "deletion_vectors"),
    ("create", "PARTITIONED BY (p)", "partitioned"),
    ("create", "generated column as the partition", "generated_partition"),
    ("create", "types: decimal, date, timestamp, timestamptz, binary", "types_scalar"),
    ("create", "nested types: struct, list, map", "types_nested"),
    ("schema", "ADD COLUMN", "add_column"),
    ("schema", "DROP COLUMN, column-mapped table", "drop_column"),
    ("schema", "RENAME COLUMN, column-mapped table", "rename_column"),
    ("schema", "int -> long, type widening on", "type_widening"),
    ("schema", "set a table property", "set_property"),
    ("schema", "ADD COLUMN, then INSERT", "write_after_schema_change"),
    ("read", "WHERE id = 22 with the other data files deleted (file pruning)", "file_pruning"),
    ("read", "time travel by version", "time_travel_version"),
    ("read", "time travel by timestamp", "time_travel_timestamp"),
    ("read", "table history", "history"),
    ("read", "change data feed", "change_data_feed"),
    ("maintenance", "OPTIMIZE (compaction)", "optimize"),
    ("maintenance", "OPTIMIZE ... ZORDER BY", "zorder"),
    ("maintenance", "CLUSTER BY (liquid clustering)", "liquid_clustering"),
    ("maintenance", "VACUUM", "vacuum"),
    ("maintenance", f"checkpoint after {CHECKPOINT_COMMITS} commits", "checkpoint"),
]


def write_step_summary(probe: DeltaProbe) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    counts = probe.report.tally()
    lines = [
        f"## Delta on OneLake, asked by {probe.TITLE}",
        "",
        f"{counts[SUPPORTED]} supported · {counts[REFUSED]} refused · "
        f"{counts[NOOP]} accepted then ignored · {counts[SKIPPED]} skipped · "
        f"{counts[BROKEN]} could not be asked",
        "",
        *probe.report.markdown(),
        "",
        f"{probe.TITLE} `{probe.version}` · Tables/{NAMESPACE} · run `{probe.run}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main(cls) -> int:
    if not any_wanted(PROBES):
        print(f"this run asks none of {', '.join(sorted(asked()))} of this client")
        return 0
    cfg = Config.from_env()
    probe = cls(cfg)
    print(f"workspace {cfg.workspace_id}  lakehouse {cfg.lakehouse_id}  Tables/{NAMESPACE}")
    try:
        for group, question, method in PROBES:
            if not wanted(method):
                continue
            ok = probe.report.run(group, question, getattr(probe, method))
            if not ok and method == "session":
                print(f"\nstopped: no {probe.TITLE} session, so nothing below could be asked")
                write_step_summary(probe)
                return 1
    finally:
        try:
            probe.drop_everything()
        except Exception as exc:  # noqa: BLE001 - teardown must not mask the findings
            print(f"  warning: teardown failed: {one_line(exc, 200)}")
        try:
            probe.close()
        except Exception as exc:  # noqa: BLE001
            print(f"  warning: close failed: {one_line(exc, 200)}")

    counts = probe.report.tally()
    print("\n" + "\n".join(probe.report.markdown()))
    print(
        f"\n{counts[SUPPORTED]} supported, {counts[REFUSED]} refused, "
        f"{counts[NOOP]} accepted then ignored, {counts[SKIPPED]} skipped, "
        f"{counts[BROKEN]} could not be asked"
    )
    write_step_summary(probe)
    probe.report.save(probe.CLIENT, probe.version)
    return 0


def run(cls) -> None:
    code = main(cls)
    sys.stdout.flush()
    # A JVM, gRPC or container thread left behind would hold a plain exit until the job timeout.
    os._exit(code)
