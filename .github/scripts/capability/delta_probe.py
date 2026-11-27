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
import threading
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
    Broken,
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
# A table partitioned by day: two rows a day over five days, one data file per day.
DAYS = [f"2026-01-0{i}" for i in range(1, 6)]
DATED = [(i * 10 + j, (i * 10 + j) * 10, day) for i, day in enumerate(DAYS, 1) for j in (1, 2)]
# The incremental batch: every row in the last two days, two updates and one insert.
MERGE_DATED = [(41, 999, DAYS[3]), (51, 999, DAYS[4]), (53, 530, DAYS[4])]
MERGED_DATED = sorted(
    [(i, 999 if i in (41, 51) else v, d) for i, v, d in DATED] + [(53, 530, DAYS[4])]
)


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
        self.schemas: list[str] = []

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
        """`VALUES (...), (...)` for an INSERT; a string is quoted."""
        lit = lambda x: f"'{x}'" if isinstance(x, str) else str(x)  # noqa: E731
        return "VALUES " + ", ".join("(" + ", ".join(map(lit, r)) + ")" for r in rows)

    def insert_sql(self, t: str, rows, cols=("id", "v")) -> str:
        """INSERT of literal rows; `cols` names their columns, for a client that needs them."""
        return f"INSERT INTO {t} {self.values(rows)}"

    def insert_select_sql(self, t: str, select) -> str:
        """INSERT of a `select` (the hook's own, such as self.select(SEED))."""
        return f"INSERT INTO {t} {select}"

    def insert_self_sql(self, t: str) -> str:
        """The table's own rows appended again, ids + 10."""
        return f"INSERT INTO {t} SELECT id + 10, v FROM {t}"

    def read_sql(self, t: str, cols: str = "id, v", where: str | None = None, order=True) -> str:
        where_ = f" WHERE {where}" if where else ""
        return f"SELECT {cols} FROM {t}{where_}" + (" ORDER BY id" if order else "")

    def sum_sql(self, t: str, where: str | None = None) -> str:
        """sum(v), not count(*): count(*) is answered from the log's row counts."""
        return f"SELECT sum(v) FROM {t}" + (f" WHERE {where}" if where else "")

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

    def replace_sql(self, name: str, select: str) -> list[str]:
        """CREATE OR REPLACE TABLE ... AS SELECT on an existing table, at the same location."""
        return [
            f"CREATE OR REPLACE TABLE {self.ref(name)} USING delta "
            f"LOCATION '{self.oracle.uri(name)}' AS {select}"
        ]

    def register_sql(self, name: str, uri: str | None = None) -> list[str]:
        """Make an existing Delta table readable by name: by default the probe table `name`
        delta-rs wrote, or the table at `uri`."""
        where = uri or self.oracle.uri(name)
        return [f"CREATE TABLE {self.ref(name)} USING delta LOCATION '{where}'"]

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

    def merge_sql(self, t: str, source: str, col: str = "v") -> str:
        """Upsert of `source`'s (id, v) rows; `col` is the target's value column (a renamed one)."""
        return (
            f"MERGE INTO {t} AS tg USING ({source}) AS s ON tg.id = s.id "
            f"WHEN MATCHED THEN UPDATE SET {col} = s.v "
            f"WHEN NOT MATCHED THEN INSERT (id, {col}) VALUES (s.id, s.v)"
        )

    def select_dated(self, rows) -> str:
        """A SELECT yielding (id BIGINT, v BIGINT, d DATE) rows."""
        values = ", ".join(f"({i}, {v}, '{d}')" for i, v, d in rows)
        return (
            "SELECT CAST(id AS BIGINT) AS id, CAST(v AS BIGINT) AS v, CAST(d AS DATE) AS d "
            f"FROM VALUES {values} AS s(id, v, d)"
        )

    def merge_dated_sql(self, t: str, source: str, since: str) -> str:
        """The incremental MERGE: matched on the key and the partition, with the window the new
        rows fall in stated in the ON clause, the way an incremental model writes it."""
        return (
            f"MERGE INTO {t} AS tg USING ({source}) AS s "
            f"ON tg.id = s.id AND tg.d = s.d AND tg.d >= DATE '{since}' "
            "WHEN MATCHED THEN UPDATE SET v = s.v "
            "WHEN NOT MATCHED THEN INSERT (id, v, d) VALUES (s.id, s.v, s.d)"
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

    def full_sql(self, name: str) -> tuple[list[str], str]:
        """(statements that make the table reachable by a full name, that name): the table sits
        at Tables/<schema>/<name>, and the name is <catalog>.<schema>.<table>."""
        catalog = self.execute("SELECT current_catalog()")[0][0]
        ident = f"`{catalog}`.`{NAMESPACE}`.`{name}`"
        return [
            f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{NAMESPACE}` "
            f"LOCATION '{self.oracle.folder_uri}'",
            f"CREATE TABLE {ident} USING delta LOCATION '{self.oracle.uri(name)}'",
        ], ident

    # -- schemas: a folder under Tables/ -------------------------------------------------------

    def catalog(self) -> str:
        if not hasattr(self, "_catalog_name"):
            self._catalog_name = self.execute("SELECT current_catalog()")[0][0]
        return self._catalog_name

    def schema_create_sql(self, schema: str, if_not_exists: bool = False) -> list[str]:
        ine = "IF NOT EXISTS " if if_not_exists else ""
        return [
            f"CREATE SCHEMA {ine}`{self.catalog()}`.`{schema}` "
            f"LOCATION '{self.oracle.schema_uri(schema)}'"
        ]

    def schema_drop_sql(self, schema: str) -> str:
        return f"DROP SCHEMA `{self.catalog()}`.`{schema}`"

    def in_schema(self, schema: str, table: str) -> str:
        return f"`{self.catalog()}`.`{schema}`.`{table}`"

    def create_in_schema_sql(self, schema: str, table: str) -> list[str]:
        """CREATE TABLE by name in the schema, no LOCATION: the engine places it."""
        return [f"CREATE TABLE {self.in_schema(schema, table)} ({self._cols()}) USING delta"]

    def variant_sql(self, t: str) -> str:
        """One row (1, {"a": 1}) into (id BIGINT, x VARIANT)."""
        return f"""INSERT INTO {t} SELECT CAST(1 AS BIGINT) AS id, parse_json('{{"a": 1}}') AS x"""

    def stats_sql(self, t: str) -> str:
        return f"SELECT max(id), count(*) FROM {t}"

    def convert_sql(self, name: str) -> str:
        return f"CONVERT TO DELTA parquet.`{self.oracle.uri(name)}`"

    def detail_sql(self, t: str) -> str:
        return f"DESCRIBE DETAIL {t}"

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
            self.sql(self.insert_select_sql(self.ref(name), self.select(SEED)))
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
            return [tuple(r) for r in self.execute(self.read_sql(self.ref(name), cols))]
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
        found = self.sql(self.read_sql(self.ref(name)))
        if found != SEED:
            raise NoOp(f"reads {found}, delta-rs wrote {SEED}")
        return f"reads the {len(found)} rows delta-rs wrote"

    def ctas(self) -> str:
        name = self._claim("ctas")
        self.run_all(self.ctas_sql(name, self.select(SEED)))
        return self._expect(name, SEED, "CTAS")

    def create_or_replace(self) -> str:
        """Replace an existing table with a query's rows. On Delta that is one new commit in the
        same _delta_log: the table keeps its path and history, and is never missing."""
        name = self._fresh("replace")
        before = self.oracle.latest(name)
        self.run_all(self.replace_sql(name, self.select(NEW_ROWS)))
        after = self.oracle.versions(name)
        found = self.oracle.rows(name)
        if found != NEW_ROWS:
            raise NoOp(f"returned success; expected {NEW_ROWS}, DuckDB reads {found}")
        if not after or after[-1] <= before or before not in after:
            raise NoOp(
                f"the rows were replaced by starting a new log (versions {after}), "
                f"not by a commit after version {before}: the table's history is gone"
            )
        old = self.oracle.rows_at(name, before)
        if old != SEED:
            raise NoOp(f"replaced in version {after[-1]}; version {before} reads {old}")
        return (
            f"one commit, version {before} -> {after[-1]}; DuckDB reads {found} now and "
            f"{old} at version {before}; {self._ops(name)}"
        )

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
        self.sql(self.insert_sql(self.ref(name), SEED))
        return self._expect(name, SEED, "INSERT INTO ... VALUES")

    def insert_select(self) -> str:
        name = self._fresh("insertsel")
        t = self.ref(name)
        self.sql(self.insert_self_sql(t))
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

    # -- catalog: schemas and tables by name ----------------------------------------------------

    def _schema(self, what: str) -> str:
        """A schema of this run's own, created at Tables/<schema>; purged at teardown."""
        schema = f"_bench_delta_{what}_{self.run}".lower()
        self.schemas.append(schema)
        try:
            self.run_all(self.schema_create_sql(schema))
        except Refused as exc:
            raise Refused(f"CREATE SCHEMA at Tables/{schema}: {exc}") from None
        return schema

    def schema_create(self) -> str:
        """CREATE SCHEMA at a folder, then a table by name in it: the table must land at
        Tables/<schema>/<table>, where Fabric and every path-based reader looks for it."""
        schema = self._schema("mk")
        table = "t1"
        self.run_all(self.create_in_schema_sql(schema, table))
        ident = self.in_schema(schema, table)
        self.sql(self.insert_sql(ident, SEED))
        where = f"{self.oracle.schema_relative(schema)}/{table}"
        if not self.oracle.has_log(where):
            raise NoOp(f"the schema was created, and its table is not at Tables/{schema}/{table}")
        found = self.sql(self.read_sql(ident))
        if not _same(found, SEED):
            raise NoOp(f"the table is at Tables/{schema}/{table}, and {ident} reads {found}")
        return (
            f"schema at Tables/{schema}; a table created by name lands at Tables/{schema}/{table}"
        )

    def schema_existing(self) -> str:
        schema = self._schema("ine")
        self.run_all(self.schema_create_sql(schema, if_not_exists=True))
        return f"CREATE SCHEMA IF NOT EXISTS on the existing {schema}: accepted"

    def schema_drop(self) -> str:
        """DROP SCHEMA on an empty schema: the folder is the schema, so it has to go."""
        schema = self._schema("rm")
        made = (
            ""
            if self.oracle.folder_exists(self.oracle.schema_relative(schema))
            else " (CREATE SCHEMA made no folder, so there was none to remove)"
        )
        self.sql(self.schema_drop_sql(schema))
        if self.oracle.folder_exists(self.oracle.schema_relative(schema)):
            raise NoOp(f"DROP SCHEMA returned success; the folder Tables/{schema} is still there")
        return f"no folder Tables/{schema} left{made}"

    def drop_table(self) -> str:
        """A table created by name in a schema, then DROP TABLE: its folder has to go."""
        schema = self._schema("drop")
        table = "t1"
        ident = self.in_schema(schema, table)
        try:
            self.run_all(self.create_in_schema_sql(schema, table))
            self.sql(self.insert_sql(ident, SEED))
        except Refused as exc:
            raise Refused(f"a table could not be created by name in the schema: {exc}") from None
        where = f"{self.oracle.schema_relative(schema)}/{table}"
        if not self.oracle.has_log(where):
            raise Skip(f"the table was not placed at Tables/{schema}/{table}, so nothing to drop")
        self.sql(f"DROP TABLE {ident}")
        if self.oracle.has_log(where):
            raise NoOp(
                f"DROP TABLE returned success; the table is still at Tables/{schema}/{table}"
            )
        return f"DROP TABLE removed Tables/{schema}/{table}"

    def full_identifier(self) -> str:
        """A table at Tables/<schema>/<table>, read and written as catalog.schema.table."""
        name = self._claim("fullid")
        self.oracle.write(name, SEED)
        setup, ident = self.full_sql(name)
        self.run_all(setup)
        self.sql(self.insert_sql(ident, [(4, 40)]))
        found = self.sql(self.read_sql(ident))
        expected = SEED + [(4, 40)]
        if not _same(found, expected):
            raise NoOp(f"{ident} reads {found}, expected {expected}")
        found = self.oracle.rows(name)
        if found != expected:
            raise NoOp(f"{ident} reads {expected}, DuckDB reads {found}")
        return f"INSERT and SELECT as {ident}: DuckDB reads {found}; {self.TITLE} agrees"

    RACE = 10

    def _race(self, name: str, statements: list[str]) -> tuple[dict[int, str], list[int], int]:
        """Run `statements` through the engine while delta-rs appends RACE rows (ids 2001..)
        to the same table from another thread. ({index of each engine statement that failed:
        its error}, the delta-rs ids that did not survive, how many times the commits swapped).

        delta-rs is the probe, not the engine under test: when its own append loses a conflict
        it retries until the row lands, so every one of its rows has to be in the table."""
        theirs = [2000 + i for i in range(1, self.RACE + 1)]
        second: list[str] = []

        def delta_rs() -> None:
            for i in theirs:
                for attempt in range(8):
                    try:
                        self.oracle.write(name, [(i, i)])
                        break
                    except Exception as exc:  # noqa: BLE001 - retried, then reported
                        if attempt == 7:
                            second.append(f"{i}: {one_line(exc, 200)}")
                        time.sleep(0.5)

        writer = threading.Thread(target=delta_rs)
        writer.start()
        failed: dict[int, str] = {}
        for index, statement in enumerate(statements):
            try:
                self.sql(statement)
            except Refused as exc:
                failed[index] = str(exc)
        writer.join()
        if second:
            raise Broken(f"the second writer (delta-rs) could not land its rows: {second[0]}")
        versions = self.oracle.versions(name)
        holes = sorted(set(range(versions[-1] + 1)) - set(versions)) if versions else []
        if holes:
            raise Refused(f"the log has no version {holes}: a commit was overwritten")
        authors = self.oracle.commit_authors(name)
        swaps = sum(a != b for a, b in zip(authors, authors[1:], strict=False))
        found = self.oracle.rows(name, ("id", "v"))
        if isinstance(found, str):
            raise NoOp(f"after the race the table cannot be read: {found}")
        ids = {r[0] for r in found}
        return failed, [i for i in theirs if i not in ids], swaps

    def race_append(self) -> str:
        """The engine's INSERTs while delta-rs appends to the same table. Every INSERT has to
        land: an append that loses a conflict is retried, since nothing it read changed. A
        writer that overwrites a commit file loses the other writer's rows."""
        name = self._fresh("race")
        mine = [1000 + i for i in range(1, self.RACE + 1)]
        t = self.ref(name)
        failed, lost, swaps = self._race(name, [self.insert_sql(t, [(i, i)]) for i in mine])
        ids = {r[0] for r in self.oracle.rows(name, ("id", "v"))}
        missing = [i for i in mine if i not in ids]
        problems = []
        if lost:
            problems.append(f"lost {len(lost)} delta-rs append(s): {lost}")
        if failed:
            first = next(iter(failed.values()))
            problems.append(f"{len(failed)} {self.TITLE} INSERT(s) failed: {first}")
        elif missing:
            problems.append(f"lost {len(missing)} {self.TITLE} append(s): {missing}")
        if problems:
            raise Refused("; ".join(problems))
        if not swaps:
            raise Skip("the two writers' commits never interleaved, so no race happened")
        return (
            f"{self.RACE} {self.TITLE} and {self.RACE} delta-rs commits, "
            f"interleaved {swaps} times, all {len(ids)} rows kept"
        )

    def _race_seed(self, what: str) -> tuple[str, list[int]]:
        """A table holding RACE rows (ids 101..), one INSERT, for the DELETE and UPDATE races."""
        if not self.can_create:
            raise Skip("no table could be created")
        mine = [100 + i for i in range(1, self.RACE + 1)]
        try:
            name = self._create(what)
            self.sql(self.insert_select_sql(self.ref(name), self.select([(i, i) for i in mine])))
        except Refused as exc:
            raise Skip(f"could not seed the table: {exc}") from None
        return name, mine

    def _change_verdict(self, what, failed, lost, wrong, swaps, total) -> str:
        """The Iceberg grid's rule for a DELETE or UPDATE racing an append: a statement that
        fails on the conflict and changes nothing is safe; one that succeeds must apply in
        full, and the appended rows must all survive. One that never runs at all is a no."""
        if len(failed) == total:
            raise Refused(f"every {what} failed: {next(iter(failed.values()))}")
        problems = []
        if lost:
            problems.append(f"lost {len(lost)} delta-rs append(s): {lost}")
        if wrong:
            problems.append(wrong)
        if problems:
            raise Refused("; ".join(problems))
        if not swaps:
            raise Skip("the two writers' commits never interleaved, so no race happened")
        refused = (
            f"; {len(failed)} refused on the conflict, nothing changed: "
            f"{one_line(next(iter(failed.values())), 160)}"
            if failed
            else ""
        )
        return (
            f"{total - len(failed)} {self.TITLE} {what}s applied, every delta-rs append kept, "
            f"interleaved {swaps} times{refused}"
        )

    def race_delete(self) -> str:
        """The engine DELETEs its rows one by one while delta-rs appends."""
        name, mine = self._race_seed("racedel")
        t = self.ref(name)
        failed, lost, swaps = self._race(name, [self.delete_sql(t, f"id = {i}") for i in mine])
        ids = {r[0] for r in self.oracle.rows(name, ("id", "v"))}
        wrong = []
        for index, i in enumerate(mine):
            if index in failed and i not in ids:
                wrong.append(f"{i} gone though its DELETE failed")
            if index not in failed and i in ids:
                wrong.append(f"{i} still there though its DELETE succeeded")
        return self._change_verdict("DELETE", failed, lost, "; ".join(wrong), swaps, len(mine))

    def race_update(self) -> str:
        """The engine UPDATEs its rows one by one (v = v + 1000) while delta-rs appends."""
        name, mine = self._race_seed("raceupd")
        t = self.ref(name)
        failed, lost, swaps = self._race(
            name, [self.update_sql(t, "v = v + 1000", f"id = {i}") for i in mine]
        )
        values = dict(self.oracle.rows(name, ("id", "v")))
        wrong = []
        for index, i in enumerate(mine):
            expected = i if index in failed else i + 1000
            if values.get(i) != expected:
                wrong.append(f"{i} reads {values.get(i)}, expected {expected}")
        return self._change_verdict("UPDATE", failed, lost, "; ".join(wrong), swaps, len(mine))

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
            self.sql(self.insert_select_sql(self.ref(name), self.select(many)))
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
            self.insert_sql(
                self.ref(name), [(1, 10, "a"), (2, 20, "a"), (3, 30, "b")], ("id", "v", "p")
            )
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
            self.sql(self.insert_sql(self.ref(name), [(1, 7)], ("id", "c")))
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
        self.sql(self.insert_sql(t, [(4, 40, 400)], ("id", "v", "w")))
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
            lambda: self.sql(self.read_sql(t, "v", f"id = {PRUNE_ID}", order=False)),
            PRUNE_EXPECTED,
            lambda: self.sql(self.sum_sql(t)),
            f"{len(gone)} of {len(files)} data files deleted",
        )

    def _dated_by_delta_rs(self, what: str) -> str:
        """DATED written by delta-rs, one data file per day, made readable to the engine."""
        name = self._claim(what)
        self.oracle.write_dated(name, DATED)
        try:
            self.run_all(self.register_sql(name))
        except Refused as exc:
            raise Refused(
                f"a partitioned table written by delta-rs could not be opened: {exc}"
            ) from None
        return name

    def _delete_days(self, name: str, keep: set[str]) -> str:
        """Delete the data files of every day not in `keep` from storage; the log stays."""
        files = self.oracle.partition_of(name)
        gone = [path for path, day in files.items() if day not in keep]
        if len(set(files.values())) != len(DAYS) or not gone:
            raise Skip(f"expected one partition per day, found {sorted(set(files.values()))}")
        for path in gone:
            self.oracle.delete_file(name, path)
        if any(self.oracle.file_exists(name, p) for p in gone):
            raise Skip("data files still in storage after the delete")
        return f"the files of {len(DAYS) - len(keep)} of {len(DAYS)} days deleted"

    # SPARK-WRITTEN FIXTURES, ONE READER FEATURE EACH, so a "no" names the feature an engine cannot
    # read. Spark writes each once, at Tables/_bench_delta_fixtures/<name>, and it stays for every
    # engine and every later run (Spark is first in the matrix). key: (folder, the one reader
    # feature, columns read, rows expected, statements after CREATE TABLE ... LOCATION).
    FIXTURES = {
        "dv": (
            "spark_deletion_vectors_v1",
            "deletionVectors",
            "id, v",
            [(i, i * 10) for i in range(2, 21)],
            "(id BIGINT, v BIGINT)",
            "'delta.enableDeletionVectors' = 'true'",
            [
                # 20 rows in one INSERT, so the deleted row shares its file: a deletion vector.
                "INSERT INTO {t} SELECT id, id * 10 FROM range(1, 21)",
                "DELETE FROM {t} WHERE id = 1",
            ],
        ),
        "cm": (
            "spark_column_mapping_v1",
            "columnMapping",
            "id, v2",
            [(i, i * 10) for i in range(1, 21)],
            "(id BIGINT, v BIGINT)",
            # Reader 3 / writer 7, or Spark gives a column-mapping-only table the legacy (2, 5)
            # protocol, which lists no features: the fixture would read as Broken.
            "'delta.columnMapping.mode' = 'id', "
            "'delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7'",
            [
                "INSERT INTO {t} SELECT id, id * 10 FROM range(1, 21)",
                "ALTER TABLE {t} RENAME COLUMN v TO v2",
            ],
        ),
        "cdf": (
            "spark_change_data_feed_v1",
            "changeDataFeed",
            "id, v",
            [(i, i * 10) for i in range(1, 20)] + [(20, 201)],
            "(id BIGINT, v BIGINT)",
            # Writer 7, so the feature is named in the log (the legacy writer 4 lists none).
            "'delta.enableChangeDataFeed' = 'true', "
            "'delta.minReaderVersion' = '1', 'delta.minWriterVersion' = '7'",
            [
                "INSERT INTO {t} SELECT id, id * 10 FROM range(1, 21)",
                # A row-level change, so the fixture carries Spark's own change files.
                "UPDATE {t} SET v = 201 WHERE id = 20",
            ],
        ),
        "tw": (
            "spark_type_widening_v1",
            "typeWidening",
            "id, c",
            [(i, i) for i in range(1, 21)] + [(21, 3_000_000_000)],
            "(id BIGINT, c INT)",
            "'delta.enableTypeWidening' = 'true'",
            [
                "INSERT INTO {t} SELECT id, CAST(id AS INT) FROM range(1, 21)",
                "ALTER TABLE {t} ALTER COLUMN c TYPE BIGINT",
                # Past the INT range: the reader must widen the old files and read this one.
                "INSERT INTO {t} VALUES (21, 3000000000)",
            ],
        ),
    }

    def _spark_fixture(self, key: str) -> None:
        folder, _feature, _cols, _rows, columns, props, statements = self.FIXTURES[key]
        uri = self.oracle.fixture_uri(folder)
        t = f"`fx{key}_{self.run}`"
        try:
            self.run_all(
                [f"CREATE TABLE {t} {columns} USING delta LOCATION '{uri}' TBLPROPERTIES ({props})"]
                + [q.format(t=t) for q in statements]
            )
        except Refused:
            with contextlib.suppress(Exception):
                self.oracle.purge_path(self.oracle.fixture_relative(folder))
            raise

    def _read_spark_fixture(self, key: str) -> str:
        folder, feature, cols, rows, *_ = self.FIXTURES[key]
        where = self.oracle.fixture_relative(folder)
        if not self.oracle.has_log(where):
            if self.CLIENT != "spark":
                raise Skip("Spark has not written the fixture table yet")
            self._spark_fixture(key)
        features = set(self.oracle.features_at(where))
        if feature not in features:
            raise Broken(f"the fixture has features {sorted(features)}")
        name = f"fxr{key}_{self.run}".lower()
        try:
            self.run_all(self.register_sql(name, self.oracle.fixture_uri(folder)))
        except Refused as exc:
            raise Refused(f"the table could not be opened: {exc}") from None
        found = self.sql(self.read_sql(self.ref(name), cols))
        if not _same(found, rows):
            raise NoOp(f"reads {found[:4]}... ({len(found)} rows), expected {rows[:4]}...")
        return f"reads all {len(found)} rows right; table features {sorted(features)}"

    def read_deletion_vectors(self) -> str:
        """A table Spark wrote with a deletion vector (a DELETE of one row of 20), read back."""
        return self._read_spark_fixture("dv")

    def read_column_mapping(self) -> str:
        """A table Spark wrote with column mapping (id) and a renamed column, read back."""
        return self._read_spark_fixture("cm")

    def read_type_widening(self) -> str:
        """A table Spark wrote with INT widened to BIGINT and a value past INT, read back."""
        return self._read_spark_fixture("tw")

    def _write_spark_fixture(self, key: str) -> str:
        """INSERT, UPDATE, DELETE and MERGE, each into its own byte-for-byte copy of a table Spark
        wrote (so the shared fixture stays as Spark left it). Each gets a fresh copy because the
        first statement that rewrites Spark's data file also drops its deletion vector: in one
        shared copy, only the first of UPDATE, DELETE and MERGE would meet the vector. Every one
        hits a row in Spark's own data file -- for the deletion-vector fixture, the file that
        carries the vector. DuckDB decides, reading each copy's log afresh. The copies that took
        their statement are kept in self.fixture_copies, for the change-data-feed check."""
        folder, feature, cols, rows, *_ = self.FIXTURES[key]
        where = self.oracle.fixture_relative(folder)
        if not self.oracle.has_log(where):
            if self.CLIENT != "spark":
                raise Skip("Spark has not written the fixture table yet")
            self._spark_fixture(key)
        if feature not in set(self.oracle.features_at(where)):
            raise Broken(f"the fixture has features {self.oracle.features_at(where)}")
        idc, col = (c.strip() for c in cols.split(","))
        steps = [
            ("INSERT", lambda t: self.insert_sql(t, [(100, 1000)], cols=(idc, col)), {100: 1000}),
            ("UPDATE", lambda t: self.update_sql(t, f"{col} = -1", f"{idc} = 5"), {5: -1}),
            ("DELETE", lambda t: self.delete_sql(t, f"{idc} = 6"), {6: None}),
            (
                "MERGE",
                lambda t: self.merge_sql(t, self.select([(7, -7), (101, 1010)]), col),
                {7: -7, 101: 1010},
            ),
        ]
        self.fixture_copies: list[str] = []
        done, refused, wrong = [], [], []
        features = None
        for what, statement, changes in steps:
            name = self._claim(f"fxw{key}_{what.lower()}")
            self.oracle.copy_table(where, name)
            try:
                self.run_all(self.register_sql(name))
            except Refused as exc:
                raise Refused(f"the copy of Spark's table could not be opened: {exc}") from None
            try:
                self.sql(statement(self.ref(name)))
            except Refused as exc:
                refused.append(f"{what}: {exc}")
                continue
            expected = dict(rows)
            for k, v in changes.items():
                if v is None:
                    expected.pop(k, None)
                else:
                    expected[k] = v
            want = sorted(expected.items())
            found = self.oracle.rows(name, (idc, col))
            if found != want:
                shown = found if isinstance(found, str) else found[:6]
                wrong.append(
                    f"{what} returned success; DuckDB reads {shown}..., expected {want[:6]}..."
                )
                continue
            done.append(what)
            self.fixture_copies.append(name)
            features = self.oracle.features(name)
        worked = f" ({', '.join(done)} worked)" if done else ""
        if refused:
            raise Refused("; ".join(refused) + worked)
        if wrong:
            raise NoOp("; ".join(wrong) + worked)
        return (
            f"INSERT, UPDATE, DELETE and MERGE, each into its own copy of Spark's table: DuckDB "
            f"reads every row right; table features {features}"
        )

    def write_spark_deletion_vectors(self) -> str:
        """Writes into a table Spark wrote with a deletion vector, rows in the vector's file."""
        return self._write_spark_fixture("dv")

    def write_spark_column_mapping(self) -> str:
        """Writes into a table Spark wrote with column mapping (id) and a renamed column."""
        return self._write_spark_fixture("cm")

    def write_spark_change_data_feed(self) -> str:
        """Writes into a table Spark wrote with change data feed on: a commit that rewrites files
        must carry change files, or the feed reads every carried-over row as deleted + re-added."""
        found = self._write_spark_fixture("cdf")
        # Spark's own commits are copied too; only the engine's, after the copy, are asked.
        since = self._fixture_versions("cdf")
        missing = []
        for name in self.fixture_copies:
            authors = self.oracle.commit_authors(name)
            for v, who in enumerate(authors):
                if (
                    who == "engine"
                    and v > since
                    and self.oracle.actions(name, "remove", v)
                    and not self.oracle.actions(name, "cdc", v)
                ):
                    missing.append(f"{name.rsplit('_', 1)[-1].upper()} (version {v})")
        if missing:
            raise NoOp(
                f"{found}; but {', '.join(missing)} rewrote files with no change files, so the "
                f"change data feed is wrong"
            )
        return found

    def _fixture_versions(self, key: str) -> int:
        """The last version Spark committed to the fixture (the copy starts there)."""
        folder = self.FIXTURES[key][0]
        paths = self.oracle.fs().get_paths(
            f"{self.oracle.fixture_relative(folder)}/_delta_log", recursive=False
        )
        import re

        return max(
            int(leaf.split(".")[0])
            for leaf in (p.name.rsplit("/", 1)[-1] for p in paths)
            if re.fullmatch(r"\d{20}\.json", leaf)
        )

    def variant(self) -> str:
        """A VARIANT column written by the engine: the log must carry the variantType feature,
        and DuckDB must read the value back."""
        if not self.can_create:
            raise Skip("no table could be created")
        name = self._create("variant", f"id {self.BIGINT}, x VARIANT")
        self.sql(self.variant_sql(self.ref(name)))
        features = self.oracle.features(name)
        if not any(f.startswith("variantType") for f in features):
            raise NoOp(f"written; the table has no variantType feature: {features}")
        value = self.oracle.column(name)
        if isinstance(value, list) and len(value) == 1:
            value = value[0]
        if isinstance(value, str):
            with contextlib.suppress(ValueError):
                value = json.loads(value)
        if value != {"a": 1}:
            raise NoOp(f"variantType on the table, and DuckDB reads {value!r}")
        return f"features {features}; DuckDB reads {value}"

    def check_constraint(self) -> str:
        """delta-rs adds CHECK (v > 0); the engine is asked to INSERT v = -1. Refused is right
        whichever way: by enforcing the constraint, or by refusing a writer feature it does not
        support. Writing the row breaks the table for every engine that enforces it."""
        name = self._seeded_by_delta_rs("check")
        self.oracle.add_constraint(name, "v_positive", "v > 0")
        try:
            self.sql(self.insert_sql(self.ref(name), [(5, -1)]))
        except Refused as exc:
            found = self.oracle.rows(name)
            if found != SEED:
                raise NoOp(f"the INSERT failed, and the table now reads {found}") from None
            return f"refused: {exc}"
        found = self.oracle.rows(name)
        raise NoOp(f"wrote (5, -1) past CHECK (v > 0): DuckDB reads {found}")

    def stats_from_log(self) -> str:
        """max(id) and count(*) with every data file deleted from storage: the right answer can
        only come from the file statistics in the log."""
        name = self._seeded_by_delta_rs("stats", STATS_FILES)
        files = [a["path"] for a in self.oracle.active_files(name)]
        for path in files:
            self.oracle.delete_file(name, path)
        if any(self.oracle.file_exists(name, p) for p in files):
            raise Skip("data files still in storage after the delete")
        t = self.ref(name)
        rows = [r for batch in STATS_FILES for r in batch]
        return read_without_files(
            "max(id), count(*)",
            lambda: self.sql(self.stats_sql(t)),
            [(max(i for i, _ in rows), len(rows))],
            lambda: self.sql(self.sum_sql(t)),
            f"all {len(files)} data files deleted",
        )

    def describe_detail(self) -> str:
        name = self._fresh("detail")
        found = self.sql(self.detail_sql(self.ref(name)))
        files = len(self.oracle.active_files(name))
        flat = [v for row in found for v in row]
        has_count = any(isinstance(v, int) and not isinstance(v, bool) and v == files for v in flat)
        has_location = any(isinstance(v, str) and v.rstrip("/").endswith(name) for v in flat)
        if not (has_count and has_location):
            raise NoOp(
                f"returns {found}; the log has {files} data file(s) at Tables/{NAMESPACE}/{name}"
            )
        return f"location and numFiles ({files}) match the log"

    def partition_pruning(self) -> str:
        """WHERE on the partition column, with every other partition's files deleted from
        storage: the right answer means the engine never opened them."""
        name = self._dated_by_delta_rs("ppart")
        day = DAYS[2]
        gone = self._delete_days(name, {day})
        t = self.ref(name)
        expected = [(sum(v for _, v, d in DATED if d == day),)]
        return read_without_files(
            f"WHERE d = DATE '{day}'",
            lambda: self.sql(self.sum_sql(t, f"d = DATE '{day}'")),
            expected,
            lambda: self.sql(self.sum_sql(t)),
            gone,
        )

    def merge_partitions(self) -> str:
        """An incremental MERGE whose new rows all fall in the last two days, on a table
        partitioned by day. Two questions: does it rewrite only those two partitions (the
        commit's add and remove actions), and does it read only them (the same MERGE with the
        older days' files deleted from storage must still succeed)?"""
        since = DAYS[-2]
        recent = set(DAYS[-2:])
        source = self.select_dated(MERGE_DATED)
        results = []

        name = self._dated_by_delta_rs("mergepart")
        try:
            self.sql(self.merge_dated_sql(self.ref(name), source, since))
        except Refused as exc:
            raise Refused(f"MERGE on the partitioned table: {exc}") from None
        found = self.oracle.rows(name, ("id", "v", "d"))
        found = found if isinstance(found, str) else [(i, v, str(d)) for i, v, d in found]
        if found != MERGED_DATED:
            raise NoOp(f"MERGE returned success; expected {MERGED_DATED}, DuckDB reads {found}")
        where = self.oracle.partition_of(name)
        removed = {where.get(a["path"], "?") for a in self.oracle.actions(name, "remove")}
        added = {
            str((a.get("partitionValues") or {}).get("d")) for a in self.oracle.actions(name, "add")
        }
        touched = sorted(removed | added)
        if set(touched) <= recent:
            results.append(("rewrite", SUPPORTED, f"rewrote only {touched}"))
        else:
            results.append(
                ("rewrite", NOOP, f"rewrote {touched}, the source only holds {sorted(recent)}")
            )

        cold = self._dated_by_delta_rs("mergecold")
        gone = self._delete_days(cold, recent)
        try:
            self.sql(self.merge_dated_sql(self.ref(cold), source, since))
            try:
                self.sql(self.sum_sql(self.ref(cold)))
                results.append(
                    ("read", BROKEN, f"with {gone}, a full scan still works: proves nothing")
                )
            except Refused:
                results.append(("read", SUPPORTED, f"succeeds with {gone}"))
        except Refused as exc:
            results.append(("read", REFUSED, f"fails with {gone}, so it opens them: {exc}"))
        if any(o == BROKEN for _, o, _ in results):
            raise Broken("; ".join(d for _, o, d in results if o == BROKEN))
        return verdict(results)

    def _history_table(self, what: str, gap: float = 0) -> tuple[str, int, str]:
        """SEED, then two more rows: (name, the seeded version, a UTC time between the two)."""
        name = self._fresh(what)
        seeded = self.oracle.latest(name)
        at = ""
        if gap:
            time.sleep(gap)
            at = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
            time.sleep(gap)
        self.sql(self.insert_select_sql(self.ref(name), self.select(NEW_ROWS)))
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

    def convert_to_delta(self) -> str:
        """Two plain Parquet files in a folder, converted in place: a _delta_log over the same
        files, and every row readable."""
        name = self._claim("convert")
        self.oracle.upload_parquet(name, "part-0.parquet", SEED[:2])
        self.oracle.upload_parquet(name, "part-1.parquet", SEED[2:])
        self.sql(self.convert_sql(name))
        if not self.oracle.versions(name):
            raise NoOp("returned success; the folder has no _delta_log")
        paths = sorted(a["path"] for a in self.oracle.active_files(name))
        found = self.oracle.rows(name)
        if found != SEED:
            raise NoOp(f"converted, and DuckDB reads {found}")
        how = "in place" if paths == ["part-0.parquet", "part-1.parquet"] else f"as {paths}"
        return f"a _delta_log over the files {how}; DuckDB reads {found}"

    def checkpoint(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        name = self._create("checkpoint")
        t = self.ref(name)
        try:
            for i in range(CHECKPOINT_COMMITS):
                self.sql(self.insert_sql(t, [(i, i)]))
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
        for schema in self.schemas:
            try:
                if self.oracle.folder_exists(self.oracle.schema_relative(schema)):
                    self.oracle.purge_path(self.oracle.schema_relative(schema))
                    dropped += 1
            except Exception as exc:  # noqa: BLE001 - teardown is best effort
                left.append(f"schema {schema} ({one_line(exc, 80)})")
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
    ("create", "CREATE OR REPLACE TABLE ... AS SELECT on an existing table", "create_or_replace"),
    ("create", "CREATE TABLE IF NOT EXISTS on an existing table", "create_if_not_exists"),
    ("write", "INSERT INTO ... VALUES", "insert_into"),
    ("catalog", "INSERT and SELECT as catalog.schema.table", "full_identifier"),
    ("concurrency", "INSERTs while delta-rs appends to the same table", "race_append"),
    ("concurrency", "DELETEs while delta-rs appends to the same table", "race_delete"),
    ("concurrency", "UPDATEs while delta-rs appends to the same table", "race_update"),
    ("catalog", "CREATE SCHEMA at a folder, a table by name lands in it", "schema_create"),
    ("catalog", "CREATE SCHEMA IF NOT EXISTS, existing schema", "schema_existing"),
    ("catalog", "DROP SCHEMA removes the folder", "schema_drop"),
    ("catalog", "DROP TABLE removes the table folder", "drop_table"),
    ("write", "INSERT INTO ... SELECT", "insert_select"),
    ("write", "DELETE", "delete_from"),
    ("write", "UPDATE", "update"),
    ("write", "INSERT OVERWRITE, whole table", "insert_overwrite"),
    ("write", "MERGE, UPDATE + INSERT", "merge_upsert"),
    ("write", "MERGE of the last two days into a table partitioned by day", "merge_partitions"),
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
    ("read", "WHERE d = <day> with the other days' files deleted", "partition_pruning"),
    ("read", "time travel by version", "time_travel_version"),
    ("read", "time travel by timestamp", "time_travel_timestamp"),
    ("read", "table history", "history"),
    ("read", "max(id), count(*) with every data file deleted", "stats_from_log"),
    ("read", "DESCRIBE DETAIL", "describe_detail"),
    ("read", "a Spark table with deletion vectors", "read_deletion_vectors"),
    ("read", "a Spark table with column mapping (a renamed column)", "read_column_mapping"),
    ("read", "a Spark table with type widening (INT widened to BIGINT)", "read_type_widening"),
    (
        "write",
        "INSERT/UPDATE/DELETE/MERGE into a Spark table with deletion vectors",
        "write_spark_deletion_vectors",
    ),
    (
        "write",
        "INSERT/UPDATE/DELETE/MERGE into a Spark table with column mapping",
        "write_spark_column_mapping",
    ),
    (
        "write",
        "INSERT/UPDATE/DELETE/MERGE into a Spark table with change data feed",
        "write_spark_change_data_feed",
    ),
    ("create", "VARIANT column: write, DuckDB reads it", "variant"),
    ("write", "INSERT past a CHECK constraint", "check_constraint"),
    ("read", "change data feed", "change_data_feed"),
    ("maintenance", "OPTIMIZE (compaction)", "optimize"),
    ("maintenance", "OPTIMIZE ... ZORDER BY", "zorder"),
    ("maintenance", "CLUSTER BY: clustering columns in the log", "liquid_clustering"),
    ("maintenance", "VACUUM", "vacuum"),
    ("maintenance", f"checkpoint after {CHECKPOINT_COMMITS} commits", "checkpoint"),
    ("maintenance", "CONVERT TO DELTA, Parquet files in place", "convert_to_delta"),
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
