"""What chDB (ClickHouse in-process) can do writing Delta tables into OneLake, in SQL.

ClickHouse's DeltaLakeAzure table engine, which writes through Delta Kernel on Azure from 26.9
(chdb-core 26.11.1 here). `allow_delta_lake_writes` turns writes on and
`allow_delta_lake_create_table` lets a CREATE TABLE with columns start a new table.

STORAGE IS A SAS, NOT THE BEARER. The engine takes a connection string or an account key, and its
docs say Delta Kernel does not take workload identity, so the connection string carries a user
delegation SAS on the lakehouse directory, signed on the job's OIDC login (the one Gluten uses,
bench/tpch/engines/pyspark_gluten_iceberg.onelake_sas). It lives about 55 minutes; the probes
take a few.

Where ClickHouse has a statement of its own for an operation (ALTER ... UPDATE, MODIFY COLUMN,
OPTIMIZE TABLE, a time-travel setting) it is sent that one; otherwise Spark's, so the `no` is
chDB's own words.
"""

from __future__ import annotations

import contextlib
import tempfile

from delta_probe import DeltaProbe, run

from bench import scrub
from bench.capability.engines.chdb_iceberg import _last_document
from bench.config import ONELAKE_BLOB
from bench.tpch.engines.pyspark_gluten_iceberg import onelake_sas

SESSION_SETTINGS = (
    "SET allow_delta_lake_writes = 1",
    "SET allow_delta_lake_create_table = 1",
    "SET output_format_json_quote_64bit_integers = 0",
    "SET session_timezone = 'UTC'",
)


class ChdbDelta(DeltaProbe):
    CLIENT, TITLE, PREFIX = "chdb", "chDB", "ch"
    BIGINT, INT, STRING = "Nullable(Int64)", "Nullable(Int32)", "Nullable(String)"
    DATE, TIMESTAMP = "Nullable(Date32)", "Nullable(DateTime64(6))"
    TYPES = {
        "decimal": ("Nullable(Decimal(10, 2))", "toDecimal64('12.34', 2)"),
        "date": ("Nullable(Date32)", "toDate32('2026-01-02')"),
        "timestamp": (
            "Nullable(DateTime64(6))",
            "toDateTime64('2026-01-02 03:04:05.123456', 6)",
        ),
        "timestamptz": (
            "Nullable(DateTime64(6, 'UTC'))",
            "toDateTime64('2026-01-02 03:04:05.123456', 6, 'UTC')",
        ),
        "binary": ("Nullable(String)", "unhex('0102')"),
        "struct": (
            "Tuple(a Nullable(Int64), b Nullable(String))",
            "CAST(tuple(1, 'x'), 'Tuple(a Nullable(Int64), b Nullable(String))')",
        ),
        "list": ("Array(Nullable(Int64))", "CAST([1, 2, 3], 'Array(Nullable(Int64))')"),
        "map": (
            "Map(String, Nullable(Int64))",
            "CAST(map('k', 1), 'Map(String, Nullable(Int64))')",
        ),
    }

    def start(self) -> str:
        import chdb
        from chdb import session

        self._session = session.Session(tempfile.mkdtemp(prefix="chdb_delta_"))
        # A gate this build does not know is reported, not fatal: the statements it gates then
        # answer for themselves.
        unknown = []
        for statement in SESSION_SETTINGS:
            try:
                self._session.query(statement)
            except Exception as exc:  # noqa: BLE001
                unknown.append(f"{statement}: {str(exc)[:120]}")
        if unknown:
            scrub.safe_print("  settings refused: " + "; ".join(unknown))
        sas, _ = onelake_sas(self.cfg.workspace_id, self.cfg.lakehouse_id, write=True)
        self._conn = f"BlobEndpoint=https://{ONELAKE_BLOB};SharedAccessSignature={sas}"
        scrub.register(self._conn)
        # The time-travel settings this build has, so the probes send one that exists.
        self._settings = [
            r[0]
            for r in self.execute(
                "SELECT name FROM system.settings WHERE name LIKE 'delta_lake%' ORDER BY name"
            )
        ]
        return f"{chdb.__version__} (delta_lake settings: {', '.join(self._settings) or 'none'})"

    def execute(self, statement: str) -> list[tuple]:
        try:
            result = self._session.query(statement, "JSONCompact")
        except Exception:
            # A failed statement leaves output behind, which the next would read as its own.
            with contextlib.suppress(Exception):
                self._session.query("SELECT 1", "JSONCompact")
            raise
        return [tuple(r) for r in _last_document(str(result)).get("data", [])]

    def close(self) -> None:
        if getattr(self, "_session", None) is not None:
            with contextlib.suppress(Exception):
                self._session.close()

    def ref(self, name: str) -> str:
        return f"`{name}`"

    def _engine(self, name: str) -> str:
        blob = f"{self.cfg.lakehouse_id}/Tables/_bench_delta/{name}/"
        return f"ENGINE = DeltaLakeAzure('{self._conn}', '{self.cfg.workspace_id}', '{blob}')"

    def select(self, pairs) -> str:
        values = ", ".join(f"({i}, {v})" for i, v in pairs)
        return f"SELECT * FROM values('id Nullable(Int64), v Nullable(Int64)', {values})"

    def create_sql(
        self, name, columns, *, partition=None, props=None, cluster=None, if_not_exists=False
    ):
        ine = "IF NOT EXISTS " if if_not_exists else ""
        tail = f" PARTITION BY ({', '.join(partition)})" if partition else ""
        if cluster:
            tail += f" CLUSTER BY ({', '.join(cluster)})"
        if props:
            tail += " SETTINGS " + ", ".join(f"`{k}` = '{v}'" for k, v in props.items())
        return [f"CREATE TABLE {ine}{self.ref(name)} ({columns}) {self._engine(name)}{tail}"]

    def ctas_sql(self, name, select):
        return [f"CREATE TABLE {self.ref(name)} {self._engine(name)} AS {select}"]

    def register_sql(self, name):
        return [f"CREATE TABLE {self.ref(name)} {self._engine(name)}"]

    def generated_sql(self, name):
        return [
            f"CREATE TABLE {self.ref(name)} (id Nullable(Int64), ts Nullable(DateTime64(6)), "
            f"d Date32 MATERIALIZED toDate32(ts)) {self._engine(name)} PARTITION BY (d)"
        ]

    def generated_insert(self, t):
        return f"INSERT INTO {t} (id, ts) VALUES (1, '2026-01-02 03:04:05')"

    def update_sql(self, t, assign, where):
        return f"ALTER TABLE {t} UPDATE {assign} WHERE {where}"

    def widen_sql(self, t):
        return f"ALTER TABLE {t} MODIFY COLUMN c Nullable(Int64)"

    def _travel(self, t: str, kind: str, value: str) -> str:
        setting = next((s for s in self._settings if kind in s), f"delta_lake_snapshot_{kind}")
        return f"SELECT id, v FROM {t} ORDER BY id SETTINGS {setting} = {value}"

    def version_sql(self, t, version):
        return self._travel(t, "version", str(version))

    def timestamp_sql(self, t, at):
        return self._travel(t, "timestamp", f"'{at}'")

    def optimize_sql(self, t):
        return f"OPTIMIZE TABLE {t} FINAL"

    def drop_sql(self, name):
        return f"DROP TABLE IF EXISTS {self.ref(name)}"


if __name__ == "__main__":
    run(ChdbDelta)
