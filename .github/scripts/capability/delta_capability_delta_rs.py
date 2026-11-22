"""What delta-rs can do writing Delta tables into OneLake, through its Python API.

THE ONE DELTA COLUMN NOT ASKED IN SQL. delta-rs (the `deltalake` package) is a library with no SQL
write path, so each row is asked with the deltalake call that does what the SQL engines' statement
does: `write_deltalake` for INSERT, `DeltaTable.delete / update / merge` for the row-level writes,
`alter`, `optimize`, `vacuum`, `load_cdf`. Where the API has no such call, the probe makes the
call that would be it, so the `no` is Python's own words (an AttributeError or a TypeError).

EVERY CHECK IS THE SQL ENGINES': this class only swaps DeltaProbe's statements for calls (each an
`Op`, shown in the log as the call it makes), so DuckDB reads the rows back and the _delta_log is
read for the rest, exactly as for Spark. delta-rs is also the second writer in the race rows, as
for every engine: this column's own commits carry `bench.writer = engine` in commitInfo, which
is how DeltaOracle.commit_authors tells the two delta-rs writers apart.
"""

from __future__ import annotations

import datetime as dt
import decimal
import re

from delta_probe import DeltaProbe, one_line, run
from harness import NOOP, REFUSED, STATS_FILES, SUPPORTED, TYPE_EXPECTED, NoOp, normalize

from bench.capability.delta import NAMESPACE

# Every write this column makes is tagged, so a race can tell it from the second writer.
ENGINE_TAG = {"bench.writer": "engine"}
# The Delta type of each column name the probes use, and its Arrow type for the rows written.
DELTA_TYPE = {"BIGINT": "long", "INT": "integer", "STRING": "string", "DATE": "date"}


def _arrow_type(col: str):
    import pyarrow as pa

    return {"c": pa.int32(), "p": pa.string(), "d": pa.date32()}.get(col, pa.int64())


def _table(rows, cols=("id", "v")):
    """Literal rows as an Arrow table, by column name: d is a date, p a string, c an int."""
    import pyarrow as pa

    data = {}
    for k, col in enumerate(cols):
        values = [r[k] for r in rows]
        if col == "d":
            values = [dt.date.fromisoformat(str(x)) for x in values]
        data[col] = pa.array(values, _arrow_type(col))
    return pa.table(data)


def _arrow(found):
    """A deltalake result (an Arrow table, or a stream of batches) as a pyarrow Table."""
    import pyarrow as pa

    if isinstance(found, pa.Table):
        return found
    if isinstance(found, pa.RecordBatchReader):
        return found.read_all()
    return pa.RecordBatchReader.from_stream(found).read_all()


def _rows(table, cols) -> list[tuple]:
    return [tuple(r[c] for c in cols) for r in table.select(list(cols)).to_pylist()]


def _filter(where: str | None):
    """The probes' WHERE clauses (`id = 22`, `d = DATE '2026-01-03'`) as an Arrow filter."""
    import pyarrow.compute as pc

    if not where:
        return None
    m = re.fullmatch(r"(\w+) = (?:DATE '([^']+)'|(-?\d+))", where.strip())
    if not m:
        raise ValueError(f"no Arrow filter for {where!r}")
    value = dt.date.fromisoformat(m.group(2)) if m.group(2) else int(m.group(3))
    return pc.field(m.group(1)) == value


def _create_at(uri: str, schema, storage_options: dict) -> None:
    from deltalake import DeltaTable

    DeltaTable.create(uri, schema=schema, storage_options=storage_options)


def _short(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]


class Op(str):
    """One deltalake call. It is a string, the call as the log shows it, so DeltaProbe's
    `sql` prints and runs it like a statement; `fn` makes the call and returns rows or None."""

    def __new__(cls, text: str, fn):
        op = super().__new__(cls, text)
        op.fn = fn
        return op


class DeltaRs(DeltaProbe):
    CLIENT, TITLE, PREFIX = "delta_rs", "delta-rs", "rs"
    BIGINT, INT, STRING, DATE, TIMESTAMP = "BIGINT", "INT", "STRING", "DATE", "TIMESTAMP"

    def __init__(self, cfg):
        super().__init__(cfg)
        self.where: dict[str, str] = {}

    # == the client ==========================================================================

    def start(self) -> str:
        import deltalake

        return f"deltalake {deltalake.__version__} (delta-rs {deltalake.rust_core_version()})"

    def execute(self, statement) -> list[tuple]:
        if not isinstance(statement, Op):
            raise TypeError(f"delta-rs takes no SQL: {statement}")
        found = statement.fn()
        return found if isinstance(found, list) else []

    def so(self) -> dict[str, str]:
        return self.oracle.storage_options()

    def dt(self, uri: str, version: int | None = None):
        from deltalake import DeltaTable

        return DeltaTable(uri, version=version, storage_options=self.so())

    def commit(self):
        from deltalake import CommitProperties

        return CommitProperties(custom_metadata=ENGINE_TAG)

    def ref(self, name: str) -> str:
        """A table is its uri: the probe's own, or the one register_sql opened it at."""
        return self.where.get(name, self.oracle.uri(name))

    def _delta_schema(self, columns: str):
        """`id BIGINT, v BIGINT` as a deltalake Schema; a type it has no name for is its no."""
        from deltalake import Field, Schema

        fields = []
        for part in columns.split(", "):
            col, kind = part.split(" ", 1)
            fields.append(Field(col, DELTA_TYPE.get(kind, kind.lower()), nullable=True))
        return Schema(fields)

    # == the calls in place of DeltaProbe's statements ========================================

    def select(self, pairs):
        return _table(pairs)

    def select_dated(self, rows):
        return _table(rows, ("id", "v", "d"))

    def _append(self, t: str, data, **kw) -> Op:
        from deltalake import write_deltalake

        extra = "".join(f", {k}={v!r}" for k, v in kw.items())
        return Op(
            f"write_deltalake({_short(t)}, {data.num_rows} rows, mode='append'{extra})",
            lambda: write_deltalake(
                t,
                data,
                mode="append",
                storage_options=self.so(),
                commit_properties=self.commit(),
                **kw,
            ),
        )

    def insert_sql(self, t, rows, cols=("id", "v")):
        return self._append(t, _table(rows, cols))

    def insert_select_sql(self, t, select):
        return self._append(t, select)

    def insert_self_sql(self, t):
        from deltalake import write_deltalake

        def run_() -> None:
            current = self.dt(t).to_pyarrow_table(columns=["id", "v"])
            again = _table([(i + 10, v) for i, v in _rows(current, ("id", "v"))])
            write_deltalake(t, again, mode="append", storage_options=self.so())

        return Op(f"write_deltalake({_short(t)}, its own rows with id + 10, mode='append')", run_)

    def read_sql(self, t, cols="id, v", where=None, order=True):
        names = [c.strip() for c in cols.split(",")]

        def run_() -> list[tuple]:
            table = self.dt(t).to_pyarrow_dataset().to_table(columns=names, filter=_filter(where))
            found = _rows(table, names)
            return sorted(found) if order else found

        return Op(f"DeltaTable({_short(t)}).to_pyarrow_dataset() {names} where {where}", run_)

    def sum_sql(self, t, where=None):
        import pyarrow.compute as pc

        def run_() -> list[tuple]:
            table = self.dt(t).to_pyarrow_dataset().to_table(columns=["v"], filter=_filter(where))
            return [(pc.sum(table["v"]).as_py(),)]

        return Op(f"sum(v) of DeltaTable({_short(t)}).to_pyarrow_dataset() where {where}", run_)

    def create_sql(
        self, name, columns, *, partition=None, props=None, cluster=None, if_not_exists=False
    ):
        from deltalake import DeltaTable

        kw = {"mode": "ignore" if if_not_exists else "error"}
        if partition:
            kw["partition_by"] = partition
        if props:
            kw["configuration"] = props
        if cluster:
            # DeltaTable.create has no clustering argument: its TypeError is the answer.
            kw["cluster_by"] = cluster
        uri = self.oracle.uri(name)
        return [
            Op(
                f"DeltaTable.create({name}, {columns!r}, {kw})",
                lambda: DeltaTable.create(
                    uri, schema=self._delta_schema(columns), storage_options=self.so(), **kw
                ),
            )
        ]

    def ctas_sql(self, name, select):
        from deltalake import write_deltalake

        uri = self.oracle.uri(name)
        return [
            Op(
                f"write_deltalake({name}, {select.num_rows} rows, mode='error')",
                lambda: write_deltalake(uri, select, mode="error", storage_options=self.so()),
            )
        ]

    def replace_sql(self, name, select):
        from deltalake import write_deltalake

        uri = self.oracle.uri(name)
        return [
            Op(
                f"write_deltalake({name}, {select.num_rows} rows, mode='overwrite', "
                "schema_mode='overwrite')",
                lambda: write_deltalake(
                    uri,
                    select,
                    mode="overwrite",
                    schema_mode="overwrite",
                    storage_options=self.so(),
                ),
            )
        ]

    def register_sql(self, name, uri=None):
        where = uri or self.oracle.uri(name)
        self.where[name] = where
        return [Op(f"DeltaTable({_short(where)})", lambda: self.dt(where) and None)]

    def generated_sql(self, name):
        from deltalake import DeltaTable, Field, Schema

        uri = self.oracle.uri(name)
        schema = Schema(
            [
                Field("id", "long"),
                Field("ts", "timestamp_ntz"),
                Field("d", "date", metadata={"delta.generationExpression": "CAST(ts AS DATE)"}),
            ]
        )
        return [
            Op(
                f"DeltaTable.create({name}, d date generated as CAST(ts AS DATE), "
                "partition_by=['d'])",
                lambda: DeltaTable.create(
                    uri, schema=schema, partition_by=["d"], storage_options=self.so()
                ),
            )
        ]

    def generated_insert(self, t):
        import pyarrow as pa

        data = pa.table(
            {
                "id": pa.array([1], pa.int64()),
                "ts": pa.array([dt.datetime(2026, 1, 2, 3, 4, 5)], pa.timestamp("us")),
            }
        )
        return self._append(t, data)

    def delete_sql(self, t, where):
        return Op(
            f"DeltaTable({_short(t)}).delete({where!r})",
            lambda: self.dt(t).delete(where, commit_properties=self.commit()) and None,
        )

    def update_sql(self, t, assign, where):
        col, expr = (x.strip() for x in assign.split("=", 1))
        return Op(
            f"DeltaTable({_short(t)}).update({{{col!r}: {expr!r}}}, predicate={where!r})",
            lambda: (
                self.dt(t).update(
                    updates={col: expr}, predicate=where, commit_properties=self.commit()
                )
                and None
            ),
        )

    def _merge(self, t, source, on: str, insert: dict) -> Op:
        def run_() -> None:
            (
                self.dt(t)
                .merge(
                    source, on, source_alias="s", target_alias="tg", commit_properties=self.commit()
                )
                .when_matched_update({"v": "s.v"})
                .when_not_matched_insert(insert)
                .execute()
            )

        return Op(
            f"DeltaTable({_short(t)}).merge({source.num_rows} rows, {on!r})"
            ".when_matched_update.when_not_matched_insert",
            run_,
        )

    def merge_sql(self, t, source):
        return self._merge(t, source, "tg.id = s.id", {"id": "s.id", "v": "s.v"})

    def merge_dated_sql(self, t, source, since):
        on = f"tg.id = s.id AND tg.d = s.d AND tg.d >= CAST('{since}' AS DATE)"
        return self._merge(t, source, on, {"id": "s.id", "v": "s.v", "d": "s.d"})

    def truncate_sql(self, t):
        return Op(f"DeltaTable({_short(t)}).delete()", lambda: self.dt(t).delete() and None)

    def overwrite_sql(self, t, select):
        from deltalake import write_deltalake

        return Op(
            f"write_deltalake({_short(t)}, {select.num_rows} rows, mode='overwrite')",
            lambda: write_deltalake(t, select, mode="overwrite", storage_options=self.so()),
        )

    def overwrite_partition_sql(self, t):
        from deltalake import write_deltalake

        data = _table([(7, 70, "a")], ("id", "v", "p"))
        return Op(
            f"write_deltalake({_short(t)}, [(7, 70, 'a')], mode='overwrite', "
            "predicate=\"p = 'a'\")",
            lambda: write_deltalake(
                t, data, mode="overwrite", predicate="p = 'a'", storage_options=self.so()
            ),
        )

    def add_column_sql(self, t):
        from deltalake import Field

        return Op(
            f"DeltaTable({_short(t)}).alter.add_columns(Field('w', 'long'))",
            lambda: self.dt(t).alter.add_columns(Field("w", "long", nullable=True)),
        )

    def drop_column_sql(self, t):
        # TableAlterer has add_columns and no way to drop one: its AttributeError is the answer.
        return Op(
            f"DeltaTable({_short(t)}).alter.drop_columns(['v'])",
            lambda: self.dt(t).alter.drop_columns(["v"]),
        )

    def rename_column_sql(self, t):
        return Op(
            f"DeltaTable({_short(t)}).alter.rename_column('v', 'v2')",
            lambda: self.dt(t).alter.rename_column("v", "v2"),
        )

    def widen_sql(self, t):
        """delta-rs's schema evolution is on write: an append with c as a long, merged."""
        import pyarrow as pa

        data = pa.table({"id": pa.array([], pa.int64()), "c": pa.array([], pa.int64())})
        return self._append(t, data, schema_mode="merge")

    def set_property_sql(self, t):
        props = {"delta.enableChangeDataFeed": "true"}
        return Op(
            f"DeltaTable({_short(t)}).alter.set_table_properties({props})",
            lambda: self.dt(t).alter.set_table_properties(props),
        )

    def version_sql(self, t, version):
        return Op(
            f"DeltaTable({_short(t)}, version={version}).to_pyarrow_table()",
            lambda: sorted(
                _rows(self.dt(t, version).to_pyarrow_table(columns=["id", "v"]), ("id", "v"))
            ),
        )

    def timestamp_sql(self, t, at):
        when = dt.datetime.fromisoformat(at).replace(tzinfo=dt.UTC)

        def run_() -> list[tuple]:
            table = self.dt(t)
            table.load_as_version(when)
            return sorted(_rows(table.to_pyarrow_table(columns=["id", "v"]), ("id", "v")))

        return Op(f"DeltaTable({_short(t)}).load_as_version({at!r} UTC)", run_)

    def history_sql(self, name):
        t = self.ref(name)
        return Op(
            f"DeltaTable({name}).history()",
            lambda: [(h.get("version"), h.get("operation")) for h in self.dt(t).history()],
        )

    def changes_sql(self, name, since):
        t = self.ref(name)
        cols = ("id", "v", "_change_type")

        def run_() -> list[tuple]:
            found = _arrow(self.dt(t).load_cdf(starting_version=since))
            return sorted(_rows(found, cols), key=lambda r: (r[0], str(r[2])))

        return Op(f"DeltaTable({name}).load_cdf(starting_version={since})", run_)

    def optimize_sql(self, t):
        return Op(
            f"DeltaTable({_short(t)}).optimize.compact()",
            lambda: self.dt(t).optimize.compact() and None,
        )

    def zorder_sql(self, t):
        return Op(
            f"DeltaTable({_short(t)}).optimize.z_order(['id'])",
            lambda: self.dt(t).optimize.z_order(["id"]) and None,
        )

    def vacuum_sql(self, t):
        return Op(
            f"DeltaTable({_short(t)}).vacuum(retention_hours=0, dry_run=False, "
            "enforce_retention_duration=False)",
            lambda: (
                self.dt(t).vacuum(
                    retention_hours=0, dry_run=False, enforce_retention_duration=False
                )
                and None
            ),
        )

    def checkpoint_sql(self, t):
        return Op(
            f"DeltaTable({_short(t)}).create_checkpoint()", lambda: self.dt(t).create_checkpoint()
        )

    def full_sql(self, name):
        # delta-rs opens a table by its path only; a name is not one.
        ident = f"lake.{NAMESPACE}.{name}"
        return [Op(f"DeltaTable({ident!r})", lambda: self.dt(ident) and None)], ident

    def schema_create_sql(self, schema, if_not_exists=False):
        import deltalake

        # deltalake has no schemas, only table paths: its AttributeError is the answer.
        return [Op(f"deltalake.create_schema({schema!r})", lambda: deltalake.create_schema(schema))]

    def schema_drop_sql(self, schema):
        import deltalake

        return Op(f"deltalake.drop_schema({schema!r})", lambda: deltalake.drop_schema(schema))

    def in_schema(self, schema, table):
        return f"{self.oracle.schema_uri(schema)}/{table}"

    def create_in_schema_sql(self, schema, table):
        """No names in delta-rs: a table in a schema is a table at the schema folder's path."""
        uri = self.in_schema(schema, table)
        return [
            Op(
                f"DeltaTable.create(Tables/{schema}/{table})",
                lambda: _create_at(uri, self._delta_schema(self._cols()), self.so()),
            )
        ]

    def variant_sql(self, t):
        """Arrow has no VARIANT: the value is sent as its JSON text, for delta-rs to refuse."""
        import pyarrow as pa

        data = pa.table({"id": pa.array([1], pa.int64()), "x": pa.array(['{"a": 1}'])})
        return self._append(t, data)

    def stats_sql(self, t):
        import pyarrow.compute as pc

        def run_() -> list[tuple]:
            table = self.dt(t)
            ids = table.to_pyarrow_dataset().to_table(columns=["id"])["id"]
            return [(pc.max(ids).as_py(), table.count())]

        return Op(f"max(id) of DeltaTable({_short(t)}).to_pyarrow_dataset(), and .count()", run_)

    def convert_sql(self, name):
        from deltalake import convert_to_deltalake

        uri = self.oracle.uri(name)
        return Op(
            f"convert_to_deltalake({name})",
            lambda: convert_to_deltalake(uri, storage_options=self.so()),
        )

    def detail_sql(self, t):
        def run_() -> list[tuple]:
            table = self.dt(t)
            return [(table.table_uri, len(table.file_uris()), table.metadata().id)]

        return Op(f"DeltaTable({_short(t)}): table_uri, file_uris(), metadata()", run_)

    def drop_sql(self, name):
        return None

    # == the probes whose check is delta-rs's own ============================================

    def zorder(self) -> str:
        """delta-rs records no zOrderBy in its OPTIMIZE commit (only targetSize and predicate),
        so the call is the evidence: z_order(['id']), one OPTIMIZE commit that rewrote the files,
        and every row still there."""

        name = self._seeded_by_delta_rs("zorder", STATS_FILES)
        before = len(self.oracle.active_files(name))
        self.sql(self.zorder_sql(self.ref(name)))
        info = self.oracle.actions(name, "commitInfo")
        removed = len(self.oracle.actions(name, "remove"))
        if not info or info[0].get("operation") != "OPTIMIZE" or not removed:
            raise NoOp(f"returned success; the last commit is {info[0] if info else 'none'}")
        expected = sorted(p for batch in STATS_FILES for p in batch)
        after = len(self.oracle.active_files(name))
        return (
            f"z_order(['id']): one OPTIMIZE commit rewrote {before} data files into {after} "
            f"(delta-rs records no zOrderBy); " + self._expect(name, expected, "Z-order")
        )

    # == the probes whose statement is a column type ==========================================

    def _typed(self, kind: str) -> tuple[str, str, str]:
        """DeltaTable.create with the type, then one row written: the Arrow value of each case."""
        import pyarrow as pa
        from deltalake import DeltaTable, write_deltalake

        ts = dt.datetime(2026, 1, 2, 3, 4, 5, 123456)
        arrow, value = {
            "decimal": (pa.decimal128(10, 2), decimal.Decimal("12.34")),
            "date": (pa.date32(), dt.date(2026, 1, 2)),
            "timestamp": (pa.timestamp("us"), ts),
            "timestamptz": (pa.timestamp("us", tz="UTC"), ts.replace(tzinfo=dt.UTC)),
            "binary": (pa.binary(), b"\x01\x02"),
            "struct": (pa.struct([("a", pa.int64()), ("b", pa.string())]), {"a": 1, "b": "x"}),
            "list": (pa.list_(pa.int64()), [1, 2, 3]),
            "map": (pa.map_(pa.string(), pa.int64()), [("k", 1)]),
        }[kind]
        data = pa.table({"id": pa.array([1], pa.int64()), "x": pa.array([value], arrow)})
        name = self._claim(f"type_{kind}")
        uri = self.oracle.uri(name)
        try:
            self.sql(
                Op(
                    f"DeltaTable.create({name}, x {arrow})",
                    lambda: DeltaTable.create(uri, schema=data.schema, storage_options=self.so()),
                )
            )
            self.sql(
                Op(
                    f"write_deltalake({name}, (1, {value!r}), mode='append')",
                    lambda: write_deltalake(uri, data, mode="append", storage_options=self.so()),
                )
            )
        except Exception as exc:  # noqa: BLE001 - delta-rs's refusal is the answer
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


if __name__ == "__main__":
    run(DeltaRs)
