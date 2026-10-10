"""What the OneLake Iceberg REST catalog lets Polars do.

Polars writes with `LazyFrame.sink_iceberg`: its own parquet writer puts the data files on
OneLake, and the commit goes through a pyiceberg Transaction on the table it is handed.
`DataFrame.write_iceberg` is not probed: it is pyiceberg's `append` / `overwrite` with a Polars
frame converted to Arrow, so it would only repeat pyiceberg's answers. Reads are `scan_iceberg`
with Polars' native reader.

POLARS HAS NO DDL. It cannot create a table, delete, update, merge, evolve a spec, or touch refs
and snapshots, so pyiceberg creates every table and those rows are `na` in the readme. What is
asked here is everything Polars itself can send: append, overwrite, the schema changes
`schema_mode` folds into a write, partitioned and typed writes, time travel.

EVERY PROBE CHECKS ITS EFFECT through pyiceberg, which reads what the CATALOG now says. Polars'
own read is reported beside it. Every error Polars raises is a `no` carrying its full message, so
the log says whose limit it is: a bucket partition, for one, Polars refuses before the catalog is
asked.

THE STORAGE CREDENTIAL is `{"bearer_token": ...}`, the same one lakehouse_benchmark's Polars
engines give `scan_iceberg`. Polars passes keys without a dot through to object_store, and has no
mapping for pyiceberg's `adls.token`.
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
    NoOp,
    Refused,
    Report,
    Skip,
    evolution_check,
    iceberg_schema,
    partition_result,
    promotion_schema,
    read_without_files,
    remove_data_files,
    rows,
    transform_arrow,
    transform_case,
    type_arrow,
    type_result,
    type_schema,
    verdict,
    write_stats_files,
)
from isolation_duckdb import race_probe

from bench import auth, scrub
from bench.capability.race import RaceProxy
from bench.config import Config

SEED = [(1, 10), (2, 20), (3, 30)]


def _one_line(text: object, limit: int = 700) -> str:
    flat = " ".join(scrub.scrub(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


class PolarsCapability:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.report = Report(reason=lambda exc: _one_line(scrub.scrub_exc(exc, 2000), 600))
        self.run = "{}_{}".format(
            os.environ.get("GITHUB_RUN_ID", "local"),
            os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
        )
        self.keep = os.environ.get("CAPABILITY_KEEP") == "1"
        self.version = "no session"
        self.catalog = None
        self.storage: dict[str, str] = {}
        self.created: list[str] = []
        self.proxy = None
        self.race_catalog = None

    # -- helpers -----------------------------------------------------------------------------

    def name(self, what: str) -> str:
        return f"pl_{self.run}_{what}"

    def identifier(self, table: str) -> str:
        return f"{NAMESPACE}.{table}"

    def location(self, table: str) -> str:
        return f"{self.cfg.base_path}/Tables/{NAMESPACE}/{table}"

    def create(self, what: str, **kwargs):
        """pyiceberg createTable: Polars has none. Remembered for the drop at the end."""
        if self.catalog is None:
            raise Skip("the catalog never loaded")
        table = self.name(what)
        made = self.catalog.create_table(
            self.identifier(table),
            schema=kwargs.pop("schema", iceberg_schema()),
            location=self.location(table),
            **kwargs,
        )
        self.created.append(table)
        return made

    def fresh(self, what: str, **kwargs):
        """A table holding SEED, appended by pyiceberg, so the probe asks only its own write."""
        table = self.create(what, **kwargs)
        try:
            table.append(rows(SEED))
        except Exception as exc:  # noqa: BLE001 - no seed, no question
            raise Skip(f"pyiceberg could not seed the table: {_one_line(exc, 300)}") from None
        return table

    def load(self, table) -> object:
        return self.catalog.load_table(table.name())

    def sink(self, table, data, mode: str = "append", **kwargs) -> None:
        """One Polars write. Polars' own `no` becomes `Refused`, carrying its message."""
        import polars as pl

        try:
            frame = data if isinstance(data, pl.DataFrame) else pl.from_arrow(data)
            frame.lazy().sink_iceberg(table, mode=mode, storage_options=self.storage, **kwargs)
        except Exception as exc:  # noqa: BLE001 - every Polars error is an answer
            message = f"{type(exc).__name__}: {scrub.scrub_exc(exc, 2000)}"
            raise Refused(_one_line(message)) from None

    def _py_rows(self, table, cols=("id", "v")):
        try:
            arrow = self.load(table).scan(selected_fields=cols).to_arrow()
            return sorted(tuple(row[c] for c in cols) for row in arrow.to_pylist())
        except Exception as exc:  # noqa: BLE001 - a cross-check must not decide the answer
            return f"pyiceberg could not read it: {_one_line(scrub.scrub_exc(exc, 300), 300)}"

    def scan(self, table, **kwargs):
        import polars as pl

        return pl.scan_iceberg(self.load(table), storage_options=self.storage, **kwargs)

    def _pl_rows(self, table, cols=("id", "v")):
        try:
            frame = self.scan(table).select(cols).collect()
            return sorted(frame.rows())
        except Exception as exc:  # noqa: BLE001 - a read-back must not decide the answer
            return f"Polars could not read it back: {_one_line(exc, 300)}"

    def _expect(self, table, expected: list[tuple], what: str, cols=("id", "v")) -> str:
        """pyiceberg decides, because it reads the catalog; Polars' own read is reported beside."""
        py = self._py_rows(table, cols)
        pl_rows = self._pl_rows(table, cols)
        if py != expected:
            raise NoOp(
                f"{what} returned success; expected {expected}, pyiceberg reads {py}, "
                f"Polars reads {pl_rows}"
            )
        agree = "Polars agrees" if pl_rows == py else f"Polars reads {pl_rows}"
        return f"{what}: pyiceberg reads {py}; {agree}"

    # -- session -----------------------------------------------------------------------------

    def session(self) -> str:
        # Polars sizes its thread pool at import time, so this goes first.
        os.environ.setdefault("POLARS_MAX_THREADS", "4")
        import polars as pl

        self.version = pl.__version__
        self.catalog = auth.catalog(self.cfg)
        self.storage = {"bearer_token": auth.onelake_token()}
        self.catalog.create_namespace_if_not_exists(NAMESPACE)
        return f"polars {self.version}; pyiceberg catalog attached, {NAMESPACE} exists"

    # -- write -------------------------------------------------------------------------------

    def sink_append(self) -> str:
        table = self.create("append")
        self.sink(table, rows(SEED))
        op = self.load(table).current_snapshot().summary.operation
        return self._expect(table, SEED, f"sink_iceberg(mode='append'), a {op.value} snapshot")

    # -- partitions and types ----------------------------------------------------------------

    def partitioned(self) -> str:
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import IdentityTransform

        spec = PartitionSpec(PartitionField(1, 1000, IdentityTransform(), "id"))
        table = self.create("part", partition_spec=spec)
        self.sink(table, rows([(1, 10), (2, 20)]))
        files = list(self.load(table).scan().plan_files())
        parts = {str(task.file.partition) for task in files}
        if len(parts) != 2:
            raise NoOp(f"written, and the data files sit in {len(parts)} partition(s), not 2")
        return self._expect(table, [(1, 10), (2, 20)], "identity(id), 2 partitions")

    def _transform(self, kind: str) -> tuple[str, str, str]:
        schema, spec, parts = transform_case(kind)
        try:
            table = self.create(f"tr_{kind}", schema=schema, partition_spec=spec)
            self.sink(table, transform_arrow(kind, schema))
        except Refused as exc:
            return kind, REFUSED, str(exc)
        return partition_result(self.load(table), kind, parts)

    def partition_bucket(self) -> str:
        return verdict([self._transform("bucket")])

    def partition_truncate(self) -> str:
        return verdict([self._transform("truncate")])

    def partition_temporal(self) -> str:
        return verdict([self._transform(kind) for kind in TEMPORAL])

    def _typed(self, name: str) -> tuple[str, str, str]:
        schema = type_schema(name)
        try:
            table = self.create(f"type_{name}", schema=schema)
            self.sink(table, type_arrow(name, schema))
        except Refused as exc:
            return name, REFUSED, str(exc)
        return type_result(self.load(table), name)

    def types_scalar(self) -> str:
        return verdict([self._typed(name) for name in SCALAR_TYPES])

    def types_nested(self) -> str:
        return verdict([self._typed(name) for name in NESTED_TYPES])

    # -- schema changes a write carries ------------------------------------------------------

    def schema_merge_add_column(self) -> str:
        """schema_mode='merge': the new column and the append in ONE commit."""
        import polars as pl

        table = self.fresh("addcol")
        self.sink(
            table,
            pl.DataFrame(
                {"id": [4], "v": [40], "note": ["x"]},
                schema={"id": pl.Int64, "v": pl.Int64, "note": pl.String},
            ),
            schema_mode="merge",
        )
        names = [f.name for f in self.load(table).schema().fields]
        if "note" not in names:
            raise NoOp(f"returned success and the columns are {names}")
        expected = [(i, v, None) for i, v in SEED] + [(4, 40, "x")]
        return self._expect(table, expected, "column `note` added", cols=("id", "v", "note"))

    def type_promotion(self) -> str:
        """An int column, then a write whose column is a long, with schema_mode='merge'."""
        import polars as pl
        import pyarrow as pa
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        schema = promotion_schema()
        table = self.create("promote", schema=schema)
        table.append(pa.Table.from_pylist([{"id": 1, "c": 7}], schema=schema_to_pyarrow(schema)))
        self.sink(
            table,
            pl.DataFrame({"id": [2], "c": [8]}, schema={"id": pl.Int64, "c": pl.Int64}),
            schema_mode="merge",
        )
        kind = str(self.load(table).schema().find_field("c").field_type)
        if kind != "long":
            raise NoOp(f"returned success and c is still {kind}")
        return self._expect(table, [(1, 7), (2, 8)], "c promoted to long", cols=("id", "c"))

    def write_after_evolution(self) -> str:
        """pyiceberg adds truncate(4, id) to the spec -- Polars cannot write bucket -- and Polars
        writes before and after."""
        from pyiceberg.transforms import TruncateTransform

        table = self.create("evowrite")
        self.sink(table, rows(EVOLVE_FIRST))
        with self.load(table).update_spec() as update:
            update.add_field("id", TruncateTransform(4), "id_trunc")
        self.sink(self.load(table), rows(EVOLVE_THEN))
        try:
            count = self.scan(table).collect().height
        except Exception:  # noqa: BLE001 - Polars' read is reported, pyiceberg decides
            count = None
        return evolution_check(self.load(table), count)

    # -- read --------------------------------------------------------------------------------

    def time_travel(self) -> str:
        table = self.fresh("travel")
        first = table.current_snapshot().snapshot_id
        table.append(rows([(4, 40)]))
        count = self.scan(table, snapshot_id=first).collect().height
        now = self.scan(table).collect().height
        if count != 3 or now != 4:
            raise NoOp(f"scan_iceberg(snapshot_id=<first>) reads {count} rows, the head {now}")
        return f"scan_iceberg(snapshot_id=<first snapshot>) reads {count} rows (the head {now})"

    def _stats_table(self, what: str):
        """harness.STATS_FILES, written by pyiceberg: four files with known min/max bounds."""
        table = self.create(what)
        write_stats_files(table)
        return self.load(table)

    def _sum(self, table):
        import polars as pl

        return self.scan(table).select(pl.col("v").sum()).collect().rows()

    def file_pruning(self) -> str:
        import polars as pl

        table = self._stats_table("prune")
        gone = remove_data_files(table, keep_id=PRUNE_ID)
        return read_without_files(
            f"filter(id == {PRUNE_ID}).select(v)",
            lambda: self.scan(table).filter(pl.col("id") == PRUNE_ID).select("v").collect().rows(),
            PRUNE_EXPECTED,
            lambda: self._sum(table),
            gone,
        )

    def max_from_metadata(self) -> str:
        import polars as pl

        table = self._stats_table("max")
        gone = remove_data_files(table)
        return read_without_files(
            "select(id.max())",
            lambda: self.scan(table).select(pl.col("id").max()).collect().rows(),
            MAX_EXPECTED,
            lambda: self._sum(table),
            gone,
        )

    # -- commit ------------------------------------------------------------------------------

    def stale_assertion(self) -> str:
        """A Polars commit built on a head pyiceberg has since moved, with retries off. A refusal
        is the answer we want: the catalog enforces assert-ref-snapshot-id."""
        table = self.fresh("stale", properties={"commit.retry.num-retries": "0"})
        a, b = self.load(table), self.load(table)
        b.append(rows([(8, 80)]))
        try:
            self.sink(a, rows([(9, 90)]))
        except Refused as exc:
            return f"enforced, declined with: {_one_line(exc, 220)}"
        raise Refused(
            "not enforced: a commit against a stale head was accepted with retries off, and the "
            f"table now holds {self._py_rows(table)}"
        )

    # -- concurrency -------------------------------------------------------------------------

    def _race(self, key: str, write) -> str:
        """Polars writes through a catalog on the race proxy, which lands pyiceberg's commit
        between Polars' read and Polars' commit."""
        if self.proxy is None:
            self.proxy = RaceProxy().start()
            self.race_catalog = auth.catalog(self.cfg, self.proxy.endpoint)
        table = self.fresh(key)
        raced = self.race_catalog.load_table(table.name())
        return race_probe(key, self.catalog, self.proxy, table.name()[-1], lambda: write(raced))

    def race_append(self) -> str:
        return self._race("race_append", lambda t: self.sink(t, rows([(5, 50)])))

    # -- teardown ----------------------------------------------------------------------------

    def drop_everything(self) -> None:
        if not self.created:
            return
        if self.keep:
            print(f"\nCAPABILITY_KEEP=1; left {len(self.created)} table(s) behind")
            return
        dropped, left = 0, []
        for table in self.created:
            try:
                if self.catalog.table_exists(self.identifier(table)):
                    self.catalog.purge_table(self.identifier(table))
                dropped += 1
            except Exception as exc:  # noqa: BLE001 - teardown is best effort, by design
                left.append(f"{table} ({_one_line(exc, 80)})")
        print(f"\ndropped {dropped} probe table(s)" + (f"; left behind {left}" if left else ""))


PROBES = [
    ("session", "Polars imports and the pyiceberg catalog attaches", "session"),
    ("write", "sink_iceberg(mode='append')", "sink_append"),
    ("create", "write an identity-partitioned table", "partitioned"),
    ("create", "write a table partitioned by bucket(4, id)", "partition_bucket"),
    ("create", "write a table partitioned by truncate(2, x)", "partition_truncate"),
    ("create", "write tables partitioned by year / month / day / hour", "partition_temporal"),
    ("create", "types: decimal, date, timestamp, timestamptz, uuid, binary", "types_scalar"),
    ("create", "nested types: struct, list, map", "types_nested"),
    ("schema", "schema_mode='merge' adds a column", "schema_merge_add_column"),
    ("schema", "schema_mode='merge' promotes int -> long", "type_promotion"),
    ("schema", "write before and after partition evolution", "write_after_evolution"),
    ("read", "time travel, scan_iceberg(snapshot_id=)", "time_travel"),
    ("read", "filter(id == 22) with the other data files deleted (file pruning)", "file_pruning"),
    ("read", "id.max() with every data file deleted (min/max from metadata)", "max_from_metadata"),
    ("commit", "is assert-ref-snapshot-id enforced on a Polars commit", "stale_assertion"),
    ("concurrency", "sink append; B appends between Polars' read and commit", "race_append"),
]


def write_step_summary(probe: PolarsCapability) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    counts = probe.report.tally()
    lines = [
        "## OneLake Iceberg REST catalog, asked by Polars",
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
    cfg = Config.from_env()
    probe = PolarsCapability(cfg)
    print(f"workspace {cfg.workspace_id}  lakehouse {cfg.lakehouse_id}  namespace {NAMESPACE}")

    try:
        for group, question, method in PROBES:
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
