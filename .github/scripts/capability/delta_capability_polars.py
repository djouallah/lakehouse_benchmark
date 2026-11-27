"""What Polars can do writing Delta tables into OneLake, through its Python API.

Polars has no SQL over Delta tables, so like the delta-rs column each row is asked with the Polars
call that does what the SQL engines' statement does: polars-deltalake's `scan_delta` and `read_cdf`
for every read (a Polars I/O plugin on delta-kernel-rs, which reads deletion vectors,
column mapping and the change data feed; Polars' built-in `pl.scan_delta` refuses column mapping),
`DataFrame.write_delta` for every write (Polars' native Parquet sink for a plain append or
overwrite, deltalake under it for options the sink does not take), and `write_delta(mode="merge")`
for MERGE. Where Polars has no call -- DELETE, UPDATE, ALTER, OPTIMIZE, VACUUM, history -- the
probe makes the call that would be it (`pl.delete_delta`, as Polars has `pl.delete_iceberg`), so
the `no` is Python's own words.

EVERY CHECK IS THE SQL ENGINES': this class only swaps the delta-rs column's calls (each an `Op`)
for Polars', so DuckDB reads the rows back and the _delta_log is read for the rest.
"""

from __future__ import annotations

import datetime as dt

from delta_capability_delta_rs import DeltaRs, Op, _short
from delta_probe import run
from harness import NOOP, REFUSED, SUPPORTED, TYPE_EXPECTED, normalize

from bench.capability.delta import NAMESPACE


def _dtype(kind: str):
    """The Polars dtype of a column type the probes name; one Polars has no name for is its no."""
    import polars as pl

    known = {
        "BIGINT": pl.Int64,
        "INT": pl.Int32,
        "STRING": pl.String,
        "DATE": pl.Date,
        "TIMESTAMP": pl.Datetime("us"),
    }
    return known[kind] if kind in known else getattr(pl, kind.capitalize())


class PolarsDelta(DeltaRs):
    CLIENT, TITLE, PREFIX = "polars", "Polars", "pl"

    # == the client ==========================================================================

    def start(self) -> str:
        from importlib.metadata import version

        import deltalake
        import polars as pl

        return (
            f"polars {pl.__version__} + polars-deltalake {version('polars-deltalake')} "
            f"(deltalake {deltalake.__version__})"
        )

    def execute(self, statement) -> list[tuple]:
        if not isinstance(statement, Op):
            raise TypeError(f"Polars takes no SQL on Delta tables: {statement}")
        found = statement.fn()
        return found if isinstance(found, list) else []

    def _scan(self, t: str, version=None):
        """Every read: polars-deltalake's scan_delta. It time-travels by version number only, so
        a timestamp is its TypeError."""
        from polars_deltalake import scan_delta

        return scan_delta(t, version=version, storage_options=self.so())

    def _write(self, t: str, data, mode: str, options: dict | None = None):
        import polars as pl

        frame = data if isinstance(data, pl.DataFrame) else pl.from_arrow(data)
        return frame.write_delta(
            t,
            mode=mode,
            storage_options=self.so(),
            credential_provider=None,
            delta_write_options=options,
        )

    def _write_op(self, t: str, data, mode: str, options: dict | None = None) -> Op:
        extra = f", delta_write_options={options}" if options else ""
        return Op(
            f"write_delta({_short(t)}, {data.height if hasattr(data, 'height') else data.num_rows}"
            f" rows, mode={mode!r}{extra})",
            lambda: self._write(t, data, mode, options),
        )

    def _missing(self, fn: str, *args) -> Op:
        """A call Polars has no API for: its AttributeError is the answer."""
        import polars as pl

        shown = ", ".join(_short(a) if isinstance(a, str) and "://" in a else repr(a) for a in args)
        return Op(f"pl.{fn}({shown})", lambda: getattr(pl, fn)(*args))

    def _empty(self, columns: str):
        import polars as pl

        schema = {}
        for part in columns.split(", "):
            col, kind = part.split(" ", 1)
            schema[col] = _dtype(kind)
        return pl.DataFrame(schema=schema)

    # == the calls in place of the delta-rs column's ==========================================

    def _append(self, t: str, data, **kw) -> Op:
        return self._write_op(t, data, "append", kw or None)

    def insert_self_sql(self, t):
        import polars as pl

        def run_() -> None:
            again = self._scan(t).select(pl.col("id") + 10, "v").collect()
            self._write(t, again, "append")

        return Op(f"write_delta({_short(t)}, its own rows with id + 10, mode='append')", run_)

    def read_sql(self, t, cols="id, v", where=None, order=True):
        import polars as pl

        names = [c.strip() for c in cols.split(",")]

        def run_() -> list[tuple]:
            lf = self._scan(t)
            if where:
                lf = lf.filter(pl.sql_expr(where))
            found = lf.select(names).collect().rows()
            return sorted(found) if order else found

        return Op(
            f"polars_deltalake.scan_delta({_short(t)}).filter({where!r}).select({names})", run_
        )

    def sum_sql(self, t, where=None):
        import polars as pl

        def run_() -> list[tuple]:
            lf = self._scan(t)
            if where:
                lf = lf.filter(pl.sql_expr(where))
            return lf.select(pl.col("v").sum()).collect().rows()

        return Op(
            f"polars_deltalake.scan_delta({_short(t)}).filter({where!r}).select(sum(v))", run_
        )

    def create_sql(
        self, name, columns, *, partition=None, props=None, cluster=None, if_not_exists=False
    ):
        options = {}
        if partition:
            options["partition_by"] = partition
        if props:
            options["configuration"] = props
        if cluster:
            # write_delta has no clustering option: deltalake's TypeError is the answer.
            options["cluster_by"] = cluster
        uri = self.oracle.uri(name)
        mode = "ignore" if if_not_exists else "error"
        return [
            Op(
                f"pl.DataFrame(schema={columns!r}).write_delta({name}, mode={mode!r}"
                + (f", delta_write_options={options}" if options else "")
                + ")",
                lambda: self._write(uri, self._empty(columns), mode, options or None),
            )
        ]

    def ctas_sql(self, name, select):
        return [self._write_op(self.oracle.uri(name), select, "error")]

    def replace_sql(self, name, select):
        return [
            self._write_op(self.oracle.uri(name), select, "overwrite", {"schema_mode": "overwrite"})
        ]

    def register_sql(self, name, uri=None):
        where = uri or self.oracle.uri(name)
        self.where[name] = where
        return [
            Op(
                f"polars_deltalake.scan_delta({_short(where)}).collect_schema()",
                lambda: self._scan(where).collect_schema() and None,
            )
        ]

    def generated_sql(self, name):
        """Polars has no generated columns: the table is d as a plain partition column, and the
        INSERT without d that follows is the answer."""
        uri = self.oracle.uri(name)
        cols = "id BIGINT, ts TIMESTAMP, d DATE"
        return [
            Op(
                f"pl.DataFrame(schema={cols!r}).write_delta({name}, partition_by=['d'])",
                lambda: self._write(uri, self._empty(cols), "error", {"partition_by": ["d"]}),
            )
        ]

    def delete_sql(self, t, where):
        return self._missing("delete_delta", t, where)

    def update_sql(self, t, assign, where):
        return self._missing("update_delta", t, assign, where)

    def _merge(self, t, source, on: str, insert: dict, col: str = "v") -> Op:
        def merge_() -> None:
            import polars as pl

            (
                pl.from_arrow(source)
                .write_delta(
                    t,
                    mode="merge",
                    storage_options=self.so(),
                    credential_provider=None,
                    delta_merge_options={
                        "predicate": on,
                        "source_alias": "s",
                        "target_alias": "tg",
                    },
                )
                .when_matched_update({col: "s.v"})
                .when_not_matched_insert(insert)
                .execute()
            )

        return Op(
            f"write_delta({_short(t)}, {source.num_rows} rows, mode='merge', predicate={on!r})"
            ".when_matched_update.when_not_matched_insert",
            merge_,
        )

    def truncate_sql(self, t):
        return self._missing("delete_delta", t)

    def overwrite_sql(self, t, select):
        return self._write_op(t, select, "overwrite")

    def overwrite_partition_sql(self, t):
        from delta_capability_delta_rs import _table

        data = _table([(7, 70, "a")], ("id", "v", "p"))
        return self._write_op(t, data, "overwrite", {"predicate": "p = 'a'"})

    def add_column_sql(self, t):
        """Polars adds a column on write: an empty append with w, schema_mode='merge'."""
        return self._write_op(
            t, self._empty("id BIGINT, v BIGINT, w BIGINT"), "append", {"schema_mode": "merge"}
        )

    def drop_column_sql(self, t):
        return self._missing("alter_delta", t, "DROP COLUMN v")

    def rename_column_sql(self, t):
        return self._missing("alter_delta", t, "RENAME COLUMN v TO v2")

    def widen_sql(self, t):
        """Schema evolution on write: an empty append with c as a long, schema_mode='merge'."""
        return self._write_op(
            t, self._empty("id BIGINT, c BIGINT"), "append", {"schema_mode": "merge"}
        )

    def set_property_sql(self, t):
        return self._missing("alter_delta", t, "SET TBLPROPERTIES delta.enableChangeDataFeed")

    def version_sql(self, t, version):
        return Op(
            f"polars_deltalake.scan_delta({_short(t)}, version={version})",
            lambda: sorted(self._scan(t, version).select("id", "v").collect().rows()),
        )

    def timestamp_sql(self, t, at):
        when = dt.datetime.fromisoformat(at).replace(tzinfo=dt.UTC)
        return Op(
            f"polars_deltalake.scan_delta({_short(t)}, version={at!r} UTC)",
            lambda: sorted(self._scan(t, when).select("id", "v").collect().rows()),
        )

    def history_sql(self, name):
        return self._missing("delta_history", self.ref(name))

    def changes_sql(self, name, since):
        from polars_deltalake import read_cdf

        t = self.ref(name)

        def run_() -> list[tuple]:
            found = read_cdf(t, start_version=since, storage_options=self.so())
            rows = found.select("id", "v", "_change_type").rows()
            return sorted(rows, key=lambda r: (r[0], str(r[2])))

        return Op(f"polars_deltalake.read_cdf({_short(t)}, start_version={since})", run_)

    def optimize_sql(self, t):
        return self._missing("optimize_delta", t)

    def zorder_sql(self, t):
        return self._missing("optimize_delta", t, ["id"])

    def vacuum_sql(self, t):
        return self._missing("vacuum_delta", t)

    def checkpoint_sql(self, t):
        # No call to ask for one: the probe checks whether the writes leave one.
        return None

    def full_sql(self, name):
        ident = f"lake.{NAMESPACE}.{name}"
        return [
            Op(
                f"polars_deltalake.scan_delta({ident!r}).collect()",
                lambda: self._scan(ident).collect() and None,
            )
        ], ident

    def schema_create_sql(self, schema, if_not_exists=False):
        return [self._missing("create_delta_schema", schema)]

    def schema_drop_sql(self, schema):
        return self._missing("drop_delta_schema", schema)

    def create_in_schema_sql(self, schema, table):
        """No names: a table in a schema is a table at the schema folder's path."""
        uri = self.in_schema(schema, table)
        cols = self._cols()
        return [
            Op(
                f"pl.DataFrame(schema={cols!r}).write_delta(Tables/{schema}/{table})",
                lambda: self._write(uri, self._empty(cols), "error"),
            )
        ]

    def stats_sql(self, t):
        import polars as pl

        return Op(
            f"polars_deltalake.scan_delta({_short(t)}).select(max(id), len())",
            lambda: self._scan(t).select(pl.col("id").max(), pl.len()).collect().rows(),
        )

    def convert_sql(self, name):
        return self._missing("convert_to_delta", self.oracle.uri(name))

    def detail_sql(self, t):
        return self._missing("describe_delta", t)

    # == the probes whose statement is a column type ==========================================

    def _typed(self, kind: str) -> tuple[str, str, str]:
        """One row written with write_delta(mode='error'): the Polars value of each case."""
        import decimal

        import polars as pl

        ts = dt.datetime(2026, 1, 2, 3, 4, 5, 123456)
        dtype, value = {
            "decimal": (pl.Decimal(10, 2), decimal.Decimal("12.34")),
            "date": (pl.Date, dt.date(2026, 1, 2)),
            "timestamp": (pl.Datetime("us"), ts),
            "timestamptz": (pl.Datetime("us", "UTC"), ts.replace(tzinfo=dt.UTC)),
            "binary": (pl.Binary, b"\x01\x02"),
            "struct": (pl.Struct({"a": pl.Int64, "b": pl.String}), {"a": 1, "b": "x"}),
            "list": (pl.List(pl.Int64), [1, 2, 3]),
            # Polars has no map type: a map is a list of key/value structs.
            "map": (
                pl.List(pl.Struct({"key": pl.String, "value": pl.Int64})),
                [{"key": "k", "value": 1}],
            ),
        }[kind]
        data = pl.DataFrame(
            {"id": pl.Series([1], dtype=pl.Int64), "x": pl.Series([value], dtype=dtype)}
        )
        name = self._claim(f"type_{kind}")
        try:
            self.sql(self._write_op(self.oracle.uri(name), data, "error"))
        except Exception as exc:  # noqa: BLE001 - Polars' refusal is the answer
            return kind, REFUSED, str(exc)
        try:
            found = self.oracle.column(name)
        except Exception as exc:  # noqa: BLE001 - the read-back failing is the answer
            return kind, NOOP, f"written; DuckDB cannot read it: {exc}"
        try:
            got = normalize(kind, found[0]) if len(found) == 1 else f"{len(found)} rows"
        except Exception:  # noqa: BLE001 - a value of another type reads back as itself
            got = repr(found[0])
        if got != TYPE_EXPECTED[kind]:
            return kind, NOOP, f"expected {TYPE_EXPECTED[kind]}, reads {got}"
        return kind, SUPPORTED, got


if __name__ == "__main__":
    run(PolarsDelta)
