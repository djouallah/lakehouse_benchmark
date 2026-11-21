"""What Trino can do writing Delta tables into OneLake, in SQL.

The delta_lake connector, in a catalog created in SQL on the benchmark's container
(bench/trino.py): a Hive file metastore on local disk holds the table names, and every table is
created WITH (location = ...) under Tables/_bench_delta, on Trino's native Azure filesystem with
the workload-identity login the Iceberg catalog uses. `delta.enable-non-concurrent-writes` because
Trino has no conditional put it trusts on Azure; `register_table` so a table delta-rs wrote can be
opened by name.

Trino spells table properties its own way (`change_data_feed_enabled`, `column_mapping_mode`,
`deletion_vectors_enabled`). A Delta property it has no name for is sent as written, so the `no`
is Trino's.
"""

from __future__ import annotations

from delta_probe import DeltaProbe, run

from bench import trino

CATALOG, SCHEMA = "delta", "bench"
PROPERTY = {
    "delta.enableChangeDataFeed": ("change_data_feed_enabled", "true"),
    "delta.columnMapping.mode": ("column_mapping_mode", "'NAME'"),
    "delta.enableDeletionVectors": ("deletion_vectors_enabled", "true"),
}


class TrinoDelta(DeltaProbe):
    CLIENT, TITLE, PREFIX = "trino", "Trino", "tr"
    INT, STRING, TIMESTAMP = "INTEGER", "VARCHAR", "TIMESTAMP(6)"
    TYPES = {
        "decimal": ("DECIMAL(10, 2)", "DECIMAL '12.34'"),
        "date": ("DATE", "DATE '2026-01-02'"),
        "timestamp": ("TIMESTAMP(6)", "TIMESTAMP '2026-01-02 03:04:05.123456'"),
        "timestamptz": (
            "TIMESTAMP(6) WITH TIME ZONE",
            "TIMESTAMP '2026-01-02 03:04:05.123456 UTC'",
        ),
        "binary": ("VARBINARY", "X'0102'"),
        "struct": ("ROW(a BIGINT, b VARCHAR)", "CAST(ROW(1, 'x') AS ROW(a BIGINT, b VARCHAR))"),
        "list": ("ARRAY(BIGINT)", "CAST(ARRAY[1, 2, 3] AS ARRAY(BIGINT))"),
        "map": ("MAP(VARCHAR, BIGINT)", "MAP(ARRAY['k'], ARRAY[BIGINT '1'])"),
    }

    def start(self) -> str:
        trino.start()
        self.conn = trino.connect()
        trino.create_catalog(
            self.conn,
            CATALOG,
            "delta_lake",
            {
                "hive.metastore": "file",
                "hive.metastore.catalog.dir": "local:///trino-metastore",
                "fs.native-local.enabled": "true",
                "local.location": "/tmp",
                "delta.enable-non-concurrent-writes": "true",
                "delta.register-table-procedure.enabled": "true",
                "delta.vacuum.min-retention": "0s",
                # A table created by name goes to <schema location>/<name>, the layout Fabric
                # finds, not <name>-<uuid>. Every other probe gives its location explicitly.
                "delta.unique-table-location": "false",
                **trino.storage_properties(),
            },
        )
        trino.sql(
            self.conn,
            f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} "
            f"WITH (location = '{self.oracle.folder_uri}')",
        )
        return f"{trino.version(self.conn)} ({trino.IMAGE})"

    def execute(self, statement: str) -> list[tuple]:
        return trino.sql(self.conn, statement)

    def close(self) -> None:
        if getattr(self, "conn", None) is not None:
            self.conn.close()

    def ref(self, name: str) -> str:
        return f'{CATALOG}.{SCHEMA}."{name}"'

    def select(self, pairs) -> str:
        values = ", ".join(f"({i}, {v})" for i, v in pairs)
        return (
            "SELECT CAST(id AS BIGINT) AS id, CAST(v AS BIGINT) AS v "
            f"FROM (VALUES {values}) AS s(id, v)"
        )

    def schema_create_sql(self, schema, if_not_exists=False):
        ine = "IF NOT EXISTS " if if_not_exists else ""
        uri = self.oracle.schema_uri(schema)
        return [f"CREATE SCHEMA {ine}{CATALOG}.\"{schema}\" WITH (location = '{uri}')"]

    def schema_drop_sql(self, schema):
        return f'DROP SCHEMA {CATALOG}."{schema}"'

    def in_schema(self, schema, table):
        return f'{CATALOG}."{schema}"."{table}"'

    def create_in_schema_sql(self, schema, table):
        return [f"CREATE TABLE {self.in_schema(schema, table)} ({self._cols()})"]

    def select_dated(self, rows) -> str:
        values = ", ".join(f"({i}, {v}, DATE '{d}')" for i, v, d in rows)
        return (
            "SELECT CAST(id AS BIGINT) AS id, CAST(v AS BIGINT) AS v, d "
            f"FROM (VALUES {values}) AS s(id, v, d)"
        )

    def _with(self, name, partition=None, props=None, cluster=None) -> str:
        parts = [f"location = '{self.oracle.uri(name)}'"]
        if partition:
            parts.append("partitioned_by = ARRAY[" + ", ".join(f"'{p}'" for p in partition) + "]")
        if cluster:
            parts.append("clustered_by = ARRAY[" + ", ".join(f"'{c}'" for c in cluster) + "]")
        for key, value in (props or {}).items():
            prop, literal = PROPERTY.get(key, (f'"{key}"', f"'{value}'"))
            parts.append(f"{prop} = {literal}")
        return f" WITH ({', '.join(parts)})"

    def create_sql(
        self, name, columns, *, partition=None, props=None, cluster=None, if_not_exists=False
    ):
        ine = "IF NOT EXISTS " if if_not_exists else ""
        tail = self._with(name, partition, props, cluster)
        return [f"CREATE TABLE {ine}{self.ref(name)} ({columns}){tail}"]

    def full_sql(self, name):
        # The probe tables already live in schema delta.bench, located at Tables/_bench_delta.
        return self.register_sql(name), self.ref(name)

    def ctas_sql(self, name, select):
        return [f"CREATE TABLE {self.ref(name)}{self._with(name)} AS {select}"]

    def replace_sql(self, name, select):
        return [f"CREATE OR REPLACE TABLE {self.ref(name)}{self._with(name)} AS {select}"]

    def register_sql(self, name):
        return [
            f"CALL {CATALOG}.system.register_table(schema_name => '{SCHEMA}', "
            f"table_name => '{name}', table_location => '{self.oracle.uri(name)}')"
        ]

    def generated_sql(self, name):
        return [
            f"CREATE TABLE {self.ref(name)} (id BIGINT, ts TIMESTAMP(6), "
            "d DATE GENERATED ALWAYS AS (CAST(ts AS DATE)))" + self._with(name, partition=["d"])
        ]

    def generated_insert(self, t):
        return f"INSERT INTO {t} (id, ts) VALUES (1, TIMESTAMP '2026-01-02 03:04:05')"

    def widen_sql(self, t):
        return f"ALTER TABLE {t} ALTER COLUMN c SET DATA TYPE BIGINT"

    def set_property_sql(self, t):
        return f"ALTER TABLE {t} SET PROPERTIES change_data_feed_enabled = true"

    def version_sql(self, t, version):
        return f"SELECT id, v FROM {t} FOR VERSION AS OF {version} ORDER BY id"

    def timestamp_sql(self, t, at):
        return f"SELECT id, v FROM {t} FOR TIMESTAMP AS OF TIMESTAMP '{at} UTC' ORDER BY id"

    def history_sql(self, name):
        return f'SELECT version, operation FROM {CATALOG}.{SCHEMA}."{name}$history"'

    def changes_sql(self, name, since):
        # since_version is exclusive in Trino: the changes AFTER it.
        return (
            f"SELECT id, v, _change_type FROM TABLE({CATALOG}.system.table_changes("
            f"schema_name => '{SCHEMA}', table_name => '{name}', since_version => {since - 1})) "
            "ORDER BY id, _change_type"
        )

    def optimize_sql(self, t):
        return f"ALTER TABLE {t} EXECUTE optimize"

    def vacuum_sql(self, t):
        name = t.rsplit(".", 1)[-1].strip('"')
        return f"CALL {CATALOG}.system.vacuum('{SCHEMA}', '{name}', '0s')"


if __name__ == "__main__":
    run(TrinoDelta)
