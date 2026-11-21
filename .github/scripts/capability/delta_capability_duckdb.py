"""What DuckDB can do writing Delta tables into OneLake, in SQL.

The delta extension (Delta Kernel underneath) on the same pinned CLI as the Iceberg grid. A Delta
table is ATTACHed by path as a database holding one table of the same name; CREATE TABLE names
it in full (`t.t`), every other statement by the database alone. Storage is the azure extension
under an access-token secret, with the curl transport (bench.config.azure_transport).

DuckDB 2.0 writes CREATE TABLE, CTAS and INSERT; the rest of the vocabulary is still asked, each
in DuckDB's own spelling where it has one, so every `no` is in DuckDB's words.
"""

from __future__ import annotations

from delta_probe import DeltaProbe, run

from bench import auth
from bench.capability.engines.duckdb_iceberg import connect, version
from bench.config import azure_transport


class DuckdbDelta(DeltaProbe):
    CLIENT, TITLE, PREFIX = "duckdb", "DuckDB", "dk"
    INT, STRING = "INTEGER", "VARCHAR"
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

    def start(self) -> str:
        self.conn = connect()
        self.conn.sql(f"""
            SET GLOBAL azure_transport_option_type = '{azure_transport()}';
            SET TimeZone = 'UTC';
            CREATE OR REPLACE SECRET onelake_storage (
                TYPE azure, PROVIDER access_token, ACCESS_TOKEN '{auth.onelake_token()}');
        """)
        self.conn.sql("LOAD delta")
        ext = self.conn.sql(
            "SELECT extension_name || ' ' || extension_version FROM duckdb_extensions() "
            "WHERE extension_name IN ('delta', 'azure') AND loaded"
        ).fetchall()
        return f"{version()} ({', '.join(str(r[0]) for r in ext)})"

    def execute(self, statement: str) -> list[tuple]:
        return self.conn.sql(statement).fetchall()

    def close(self) -> None:
        if getattr(self, "conn", None) is not None:
            self.conn.close()

    def ref(self, name: str) -> str:
        return name

    def _attach(self, name: str) -> str:
        return f"ATTACH OR REPLACE '{self.oracle.uri(name)}' AS {name} (TYPE delta)"

    def select(self, pairs) -> str:
        values = ", ".join(f"({i}, {v})" for i, v in pairs)
        return (
            "SELECT CAST(id AS BIGINT) AS id, CAST(v AS BIGINT) AS v "
            f"FROM (VALUES {values}) AS s(id, v)"
        )

    def create_sql(
        self, name, columns, *, partition=None, props=None, cluster=None, if_not_exists=False
    ):
        ine = "IF NOT EXISTS " if if_not_exists else ""
        part = f" PARTITIONED BY ({', '.join(partition)})" if partition else ""
        clus = f" CLUSTER BY ({', '.join(cluster)})" if cluster else ""
        with_ = ""
        if props:
            with_ = " WITH (" + ", ".join(f"'{k}' = '{v}'" for k, v in props.items()) + ")"
        return [
            self._attach(name),
            f"CREATE TABLE {ine}{name}.{name} ({columns}){part}{clus}{with_}",
        ]

    def full_sql(self, name):
        # The closest core DuckDB has to a catalog over Tables/: attach the folder itself.
        lake = f"{self.cfg.base_path}/Tables"
        return [f"ATTACH OR REPLACE '{lake}' AS lake (TYPE delta)"], (
            f'lake."{self.oracle.folder_uri.rsplit("/", 1)[-1]}"."{name}"'
        )

    def ctas_sql(self, name, select):
        return [self._attach(name), f"CREATE TABLE {name}.{name} AS {select}"]

    def replace_sql(self, name, select):
        return [self._attach(name), f"CREATE OR REPLACE TABLE {name}.{name} AS {select}"]

    def register_sql(self, name):
        return [self._attach(name)]

    def generated_sql(self, name):
        return [
            self._attach(name),
            f"CREATE TABLE {name}.{name} (id BIGINT, ts TIMESTAMP, "
            "d DATE GENERATED ALWAYS AS (CAST(ts AS DATE))) PARTITIONED BY (d)",
        ]

    def generated_insert(self, t):
        return f"INSERT INTO {t} (id, ts) VALUES (1, TIMESTAMP '2026-01-02 03:04:05')"

    def truncate_sql(self, t):
        return f"TRUNCATE {t}"

    def version_sql(self, t, version):
        return f"SELECT id, v FROM {t} AT (VERSION => {version}) ORDER BY id"

    def timestamp_sql(self, t, at):
        return f"SELECT id, v FROM {t} AT (TIMESTAMP => TIMESTAMPTZ '{at}+00') ORDER BY id"

    def vacuum_sql(self, t):
        return f"VACUUM {t}"

    def checkpoint_sql(self, t):
        return f"CHECKPOINT {t}"

    def drop_sql(self, name):
        return f"DETACH DATABASE IF EXISTS {name}"


if __name__ == "__main__":
    run(DuckdbDelta)
