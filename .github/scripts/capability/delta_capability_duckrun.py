"""What duckrun can do writing Delta tables into OneLake, in SQL.

duckrun (`duckrun.connect`, its notebook API) is DuckDB SQL over Delta tables: DuckDB reads them
through `delta_scan` views and plans every statement, and the writes -- CREATE TABLE, CTAS,
INSERT, UPDATE, DELETE, MERGE, TRUNCATE, ALTER TABLE ... ADD/DROP/RENAME COLUMN and SET
TBLPROPERTIES, DROP TABLE, OPTIMIZE, VACUUM -- are committed by delta-rs. So this column is
DuckDB's spelling throughout (`AT (VERSION => n)` time travel, `WITH (...)` table properties), plus
duckrun's own Delta extensions where it has one (`INSERT INTO t REPLACE WHERE` for the overwrites,
`DESCRIBE DETAIL / HISTORY`).

The session is opened at Tables/ with discovery limited to the probe schema, as catalog `lake`;
a table another writer creates is seen after `conn.refresh()`, which is register_sql here. The
Spark-written fixtures sit in a sibling schema folder, which a session over the same root cannot
attach a second time, so they are read through a `delta_scan` view, duckrun's own read path.
"""

from __future__ import annotations

from delta_capability_delta_rs import Op
from delta_probe import DeltaProbe, run

from bench.capability.delta import NAMESPACE

CATALOG = "lake"


class DuckrunDelta(DeltaProbe):
    CLIENT, TITLE, PREFIX = "duckrun", "duckrun", "dr"
    INT, STRING, TIMESTAMP = "INTEGER", "VARCHAR", "TIMESTAMP"
    TYPES = {
        "decimal": ("DECIMAL(10, 2)", "12.34::DECIMAL(10, 2)"),
        "date": ("DATE", "DATE '2026-01-02'"),
        "timestamp": ("TIMESTAMP", "TIMESTAMP '2026-01-02 03:04:05.123456'"),
        "timestamptz": ("TIMESTAMPTZ", "TIMESTAMPTZ '2026-01-02 03:04:05.123456+00'"),
        "binary": ("BLOB", "'\\x01\\x02'::BLOB"),
        "struct": ("STRUCT(a BIGINT, b VARCHAR)", "{'a': 1::BIGINT, 'b': 'x'}"),
        "list": ("BIGINT[]", "[1, 2, 3]::BIGINT[]"),
        "map": ("MAP(VARCHAR, BIGINT)", "MAP {'k': 1::BIGINT}"),
    }

    def __init__(self, cfg):
        super().__init__(cfg)
        # Tables read from outside the session's schema: the probe name -> its view.
        self.where: dict[str, str] = {}

    # == the client ==========================================================================

    def start(self) -> str:
        from importlib.metadata import version

        import duckrun

        self.conn = duckrun.connect(
            f"{self.cfg.base_path}/Tables",
            schema=NAMESPACE,
            read_only=False,
            name=CATALOG,
            storage_options=self.oracle.storage_options(),
        )
        self.conn.sql("SET TimeZone = 'UTC'")
        return (
            f"duckrun {version('duckrun')} (duckdb {version('duckdb')}, "
            f"deltalake {version('deltalake')})"
        )

    def execute(self, statement) -> list[tuple]:
        if isinstance(statement, Op):
            found = statement.fn()
            return found if isinstance(found, list) else []
        rel = self.conn.sql(statement)
        return rel.fetchall() if rel is not None else []

    def close(self) -> None:
        if getattr(self, "conn", None) is not None:
            self.conn.close()

    def _refresh(self) -> Op:
        return Op("conn.refresh()", lambda: self.conn.refresh(quiet=True))

    def ref(self, name: str) -> str:
        return self.where.get(name, f"{CATALOG}.{NAMESPACE}.{name}")

    def select(self, pairs) -> str:
        values = ", ".join(f"({i}, {v})" for i, v in pairs)
        return (
            "SELECT CAST(id AS BIGINT) AS id, CAST(v AS BIGINT) AS v "
            f"FROM (VALUES {values}) AS s(id, v)"
        )

    def select_dated(self, rows) -> str:
        values = ", ".join(f"({i}, {v}, DATE '{d}')" for i, v, d in rows)
        return (
            "SELECT CAST(id AS BIGINT) AS id, CAST(v AS BIGINT) AS v, d "
            f"FROM (VALUES {values}) AS s(id, v, d)"
        )

    # == the statements ======================================================================

    def create_sql(
        self, name, columns, *, partition=None, props=None, cluster=None, if_not_exists=False
    ):
        ine = "IF NOT EXISTS " if if_not_exists else ""
        part = f" PARTITIONED BY ({', '.join(partition)})" if partition else ""
        clus = f" CLUSTER BY ({', '.join(cluster)})" if cluster else ""
        with_ = ""
        if props:
            with_ = " WITH (" + ", ".join(f"'{k}' = '{v}'" for k, v in props.items()) + ")"
        return [f"CREATE TABLE {ine}{self.ref(name)} ({columns}){part}{clus}{with_}"]

    def ctas_sql(self, name, select):
        return [f"CREATE TABLE {self.ref(name)} AS {select}"]

    def replace_sql(self, name, select):
        return [f"CREATE OR REPLACE TABLE {self.ref(name)} AS {select}"]

    def register_sql(self, name, uri=None):
        if uri is None:
            # A table delta-rs wrote under the session's schema: re-discover it.
            return [self._refresh()]
        view = f"fx_{name}"
        self.where[name] = view
        return [f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM delta_scan('{uri}')"]

    def generated_sql(self, name):
        return [
            f"CREATE TABLE {self.ref(name)} (id BIGINT, ts TIMESTAMP, "
            "d DATE GENERATED ALWAYS AS (CAST(ts AS DATE))) PARTITIONED BY (d)"
        ]

    def generated_insert(self, t):
        return f"INSERT INTO {t} (id, ts) VALUES (1, TIMESTAMP '2026-01-02 03:04:05')"

    def truncate_sql(self, t):
        return f"TRUNCATE {t}"

    def overwrite_sql(self, t, select):
        # duckrun has no INSERT OVERWRITE; its overwrite is delta-rs replaceWhere, here over
        # every row.
        return f"INSERT INTO {t} REPLACE WHERE true {select}"

    def overwrite_partition_sql(self, t):
        return (
            f"INSERT INTO {t} REPLACE WHERE p = 'a' "
            "SELECT CAST(7 AS BIGINT) AS id, CAST(70 AS BIGINT) AS v, 'a' AS p"
        )

    def widen_sql(self, t):
        return f"ALTER TABLE {t} ALTER COLUMN c TYPE BIGINT"

    def version_sql(self, t, version):
        return f"SELECT id, v FROM {t} AT (VERSION => {version}) ORDER BY id"

    def timestamp_sql(self, t, at):
        return f"SELECT id, v FROM {t} AT (TIMESTAMP => TIMESTAMPTZ '{at}+00') ORDER BY id"

    def full_sql(self, name):
        return [self._refresh()], f"{CATALOG}.{NAMESPACE}.{name}"

    def schema_create_sql(self, schema, if_not_exists=False):
        ine = "IF NOT EXISTS " if if_not_exists else ""
        return [f"CREATE SCHEMA {ine}{CATALOG}.{schema}"]

    def schema_drop_sql(self, schema):
        return f"DROP SCHEMA {CATALOG}.{schema}"

    def in_schema(self, schema, table):
        return f"{CATALOG}.{schema}.{table}"

    def create_in_schema_sql(self, schema, table):
        return [f"CREATE TABLE {self.in_schema(schema, table)} ({self._cols()})"]

    def variant_sql(self, t):
        return f"""INSERT INTO {t} SELECT 1::BIGINT AS id, '{{"a": 1}}'::JSON::VARIANT AS x"""

    def convert_sql(self, name):
        def run_() -> None:
            self.conn.convert_to_delta(f"parquet.`{self.oracle.uri(name)}`")
            self.conn.refresh(quiet=True)

        return Op(f"conn.convert_to_delta(parquet.`{name}`)", run_)

    def drop_sql(self, name):
        if name in self.where:
            return f"DROP VIEW IF EXISTS {self.where.pop(name)}"
        return f"DROP TABLE IF EXISTS {self.ref(name)}"


if __name__ == "__main__":
    run(DuckrunDelta)
