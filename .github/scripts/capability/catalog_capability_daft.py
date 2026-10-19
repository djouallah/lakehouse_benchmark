"""What the OneLake Iceberg REST catalog lets Daft do.

Daft writes with `DataFrame.write_iceberg(table, mode=...)`: it executes the plan, writes the
parquet itself and commits the files through a pyiceberg Transaction on the table it is handed.
Reads are `daft.read_iceberg` with Daft's native reader. Its one create is the catalog API,
`Catalog.from_iceberg(...).create_table(name, frame)`: pyiceberg's createTable from the frame's
schema, then the frame appended -- its CREATE TABLE and its CREATE TABLE AS SELECT.

DAFT HAS NO DDL AND NO SCHEMA OPERATION. It cannot delete, update, merge, evolve a schema or a
spec, or touch refs and snapshots, so those rows are `na` in the readme. A write whose frame has
a column the table lacks is coerced to the table's schema (`coerce_pyarrow_table_to_schema`):
there is no `schema_mode` to ask for a new column or a wider type. What is asked here is
everything Daft itself can send: create, append, overwrite, partitioned and typed writes, a
write after pyiceberg evolves the spec, time travel, reads from the metadata.

EVERY PROBE CHECKS ITS EFFECT through pyiceberg, which reads what the CATALOG now says. Daft's
own read is reported beside it. Every error Daft raises is a `no` carrying its full message.

DAFT CANNOT PARSE ONELAKE'S abfss:// HOST in a release yet (Eventual-Inc/Daft#7533 is merged;
bench/etl/engines/daft_iceberg.py has the detail): it sends the host as the container and OneLake
answers `400 FriendlyNameSupportDisabled`. So the tables created here carry the ETL engine's
`write.data.path` in the `az://` form Daft does parse, and Daft's own writes land in the same
folder they would have. Daft's own create cannot set that property, and any file pyiceberg wrote
is recorded as abfss://, so those reads come back as that 400, quoted. The same storage
credential as the TPC-H engine: `AzureConfig(storage_account="onelake", bearer_token=...,
use_fabric_endpoint=True)`, and adlfs pointed at OneLake for the write
(`_register_onelake_filesystem`).
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
    any_wanted,
    asked,
    evolution_check,
    iceberg_schema,
    partition_result,
    read_without_files,
    remove_data_files,
    rows,
    transform_arrow,
    transform_case,
    type_arrow,
    type_result,
    type_schema,
    verdict,
    wanted,
    write_stats_files,
)
from isolation_duckdb import race_probe

from bench import auth, scrub
from bench.capability.race import RaceProxy
from bench.config import Config
from bench.etl.engines.daft_iceberg import DaftIceberg

SEED = [(1, 10), (2, 20), (3, 30)]


def _one_line(text: object, limit: int = 700) -> str:
    flat = " ".join(scrub.scrub(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _refusal(exc: Exception) -> Refused:
    return Refused(_one_line(f"{type(exc).__name__}: {scrub.scrub_exc(exc, 2000)}"))


class DaftCapability:
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
        self.io = None
        self.created: list[str] = []
        self.proxy = None
        self.race_catalog = None

    # -- helpers -----------------------------------------------------------------------------

    def name(self, what: str) -> str:
        return f"df_{self.run}_{what}"

    def identifier(self, table: str) -> str:
        return f"{NAMESPACE}.{table}"

    def location(self, table: str) -> str:
        return f"{self.cfg.base_path}/Tables/{NAMESPACE}/{table}"

    def data_path(self, table: str) -> str:
        """The table's data folder in the az:// form Daft parses (the module docstring says why)."""
        return (
            f"az://{self.cfg.workspace_id}/{self.cfg.lakehouse_id}/Tables/{NAMESPACE}/{table}/data"
        )

    def create(self, what: str, **kwargs):
        """pyiceberg createTable, with Daft's write path set. Remembered for the drop at the end."""
        if self.catalog is None:
            raise Skip("the catalog never loaded")
        table = self.name(what)
        properties = {"write.data.path": self.data_path(table), **kwargs.pop("properties", {})}
        made = self.catalog.create_table(
            self.identifier(table),
            schema=kwargs.pop("schema", iceberg_schema()),
            location=self.location(table),
            properties=properties,
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

    def frame(self, data):
        import daft

        return data if isinstance(data, daft.DataFrame) else daft.from_arrow(data)

    def sink(self, table, data, mode: str = "append") -> None:
        """One Daft write. Daft's own `no` becomes `Refused`, carrying its message."""
        try:
            self.frame(data).write_iceberg(table, mode=mode, io_config=self.io)
        except Exception as exc:  # noqa: BLE001 - every Daft error is an answer
            raise _refusal(exc) from None

    def _py_rows(self, table, cols=("id", "v")):
        try:
            arrow = self.load(table).scan(selected_fields=cols).to_arrow()
            return sorted(tuple(row[c] for c in cols) for row in arrow.to_pylist())
        except Exception as exc:  # noqa: BLE001 - a cross-check must not decide the answer
            return f"pyiceberg could not read it: {_one_line(scrub.scrub_exc(exc, 300), 300)}"

    def scan(self, table, **kwargs):
        import daft

        return daft.read_iceberg(self.load(table), io_config=self.io, **kwargs)

    @staticmethod
    def _collect(frame, cols) -> list[tuple]:
        return sorted(tuple(row[c] for c in cols) for row in frame.to_arrow().to_pylist())

    def _df_rows(self, table, cols=("id", "v")):
        try:
            return self._collect(self.scan(table).select(*cols), cols)
        except Exception as exc:  # noqa: BLE001 - a read-back must not decide the answer
            return f"Daft could not read it back: {_one_line(exc, 300)}"

    def _expect(self, table, expected: list[tuple], what: str, cols=("id", "v")) -> str:
        """pyiceberg decides, because it reads the catalog; Daft's own read is reported beside."""
        py = self._py_rows(table, cols)
        df_rows = self._df_rows(table, cols)
        if py != expected:
            raise NoOp(
                f"{what} returned success; expected {expected}, pyiceberg reads {py}, "
                f"Daft reads {df_rows}"
            )
        agree = "Daft agrees" if df_rows == py else f"Daft reads {df_rows}"
        return f"{what}: pyiceberg reads {py}; {agree}"

    def _count(self, table, **kwargs) -> int:
        """Daft's row count; its error is the answer."""
        try:
            return self.scan(table, **kwargs).count_rows()
        except Exception as exc:  # noqa: BLE001 - every Daft error is an answer
            raise _refusal(exc) from None

    # -- session -----------------------------------------------------------------------------

    def session(self) -> str:
        from importlib.metadata import version

        from daft.io import AzureConfig, IOConfig

        self.version = version("daft")
        self.catalog = auth.catalog(self.cfg)
        self.io = IOConfig(
            azure=AzureConfig(
                storage_account="onelake",
                bearer_token=auth.onelake_token(),
                use_fabric_endpoint=True,
            )
        )
        DaftIceberg._register_onelake_filesystem()
        self.catalog.create_namespace_if_not_exists(NAMESPACE)
        return f"daft {self.version}; pyiceberg catalog attached, {NAMESPACE} exists"

    # -- write -------------------------------------------------------------------------------

    def sink_append(self) -> str:
        table = self.create("append")
        self.sink(table, rows(SEED))
        op = self.load(table).current_snapshot().summary.operation
        return self._expect(table, SEED, f"write_iceberg(mode='append'), a {op.value} snapshot")

    def sink_create(self) -> str:
        """Catalog.create_table(name, frame): Daft creates the table from the frame's schema,
        unpartitioned, and appends the frame. Its CREATE TABLE, and its CREATE TABLE AS SELECT."""
        from daft.catalog import Catalog

        if self.catalog is None:
            raise Skip("the catalog never loaded")
        table = self.name("create")
        try:
            Catalog.from_iceberg(self.catalog).create_table(
                self.identifier(table), self.frame(rows(SEED))
            )
        except Exception as exc:  # noqa: BLE001 - every Daft error is an answer
            raise _refusal(exc) from None
        finally:
            if self.catalog.table_exists(self.identifier(table)):
                self.created.append(table)
        made = self.catalog.load_table(self.identifier(table))
        ids = [f.field_id for f in made.schema().fields]
        return self._expect(made, SEED, "Catalog.create_table(name, frame)") + (
            f"; field ids {ids}"
        )

    def sink_overwrite(self) -> str:
        table = self.fresh("overwrite")
        self.sink(table, rows([(7, 70), (8, 80)]), mode="overwrite")
        op = self.load(table).current_snapshot().summary.operation
        return self._expect(
            table, [(7, 70), (8, 80)], f"write_iceberg(mode='overwrite'), a {op.value} snapshot"
        )

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

    # -- a write after the spec changes ------------------------------------------------------

    def write_after_evolution(self) -> str:
        """pyiceberg adds truncate(4, id) to the spec, and Daft writes before and after."""
        from pyiceberg.transforms import TruncateTransform

        table = self.create("evowrite")
        self.sink(table, rows(EVOLVE_FIRST))
        with self.load(table).update_spec() as update:
            update.add_field("id", TruncateTransform(4), "id_trunc")
        self.sink(self.load(table), rows(EVOLVE_THEN))
        try:
            count = self.scan(table).count_rows()
        except Exception:  # noqa: BLE001 - Daft's read is reported, pyiceberg decides
            count = None
        return evolution_check(self.load(table), count)

    # -- read --------------------------------------------------------------------------------

    def time_travel(self) -> str:
        table = self.fresh("travel")
        first = table.current_snapshot().snapshot_id
        table.append(rows([(4, 40)]))
        count = self._count(table, snapshot_id=first)
        now = self._count(table)
        if count != 3 or now != 4:
            raise NoOp(f"read_iceberg(snapshot_id=<first>) reads {count} rows, the head {now}")
        return f"read_iceberg(snapshot_id=<first snapshot>) reads {count} rows (the head {now})"

    def _stats_table(self, what: str):
        """harness.STATS_FILES, written by pyiceberg: four files with known min/max bounds. Daft
        must read them whole first, or a failure after the delete would prove nothing."""
        table = self.create(what)
        write_stats_files(table)
        table = self.load(table)
        try:
            self._sum(table)
        except Exception as exc:  # noqa: BLE001 - every Daft error is an answer
            raise Refused(
                "Daft cannot read the data files before any is deleted: "
                f"{_one_line(scrub.scrub_exc(exc, 2000))}"
            ) from None
        return table

    def _sum(self, table):
        from daft import col

        return self._collect(self.scan(table).agg(col("v").sum()), ("v",))

    def file_pruning(self) -> str:
        from daft import col

        table = self._stats_table("prune")
        gone = remove_data_files(table, keep_id=PRUNE_ID)
        return read_without_files(
            f"where(id == {PRUNE_ID}).select(v)",
            lambda: self._collect(
                self.scan(table).where(col("id") == PRUNE_ID).select("v"), ("v",)
            ),
            PRUNE_EXPECTED,
            lambda: self._sum(table),
            gone,
        )

    def max_from_metadata(self) -> str:
        from daft import col

        table = self._stats_table("max")
        gone = remove_data_files(table)
        return read_without_files(
            "agg(id.max())",
            lambda: self._collect(self.scan(table).agg(col("id").max()), ("id",)),
            MAX_EXPECTED,
            lambda: self._sum(table),
            gone,
        )

    # -- commit ------------------------------------------------------------------------------

    def stale_assertion(self) -> str:
        """A Daft commit built on a head pyiceberg has since moved, with retries off. A refusal
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
        """Daft writes through a catalog on the race proxy, which lands pyiceberg's commit
        between Daft's read and Daft's commit."""
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
    ("session", "Daft imports and the pyiceberg catalog attaches", "session"),
    ("create", "Catalog.create_table(name, frame) to a new table", "sink_create"),
    ("write", "write_iceberg(mode='append')", "sink_append"),
    ("write", "write_iceberg(mode='overwrite')", "sink_overwrite"),
    ("create", "write an identity-partitioned table", "partitioned"),
    ("create", "write a table partitioned by bucket(4, id)", "partition_bucket"),
    ("create", "write a table partitioned by truncate(2, x)", "partition_truncate"),
    ("create", "write tables partitioned by year / month / day / hour", "partition_temporal"),
    ("create", "types: decimal, date, timestamp, timestamptz, uuid, binary", "types_scalar"),
    ("create", "nested types: struct, list, map", "types_nested"),
    ("schema", "write before and after partition evolution", "write_after_evolution"),
    ("read", "time travel, read_iceberg(snapshot_id=)", "time_travel"),
    ("read", "where(id == 22) with the other data files deleted (file pruning)", "file_pruning"),
    ("read", "id.max() with every data file deleted (min/max from metadata)", "max_from_metadata"),
    ("commit", "is assert-ref-snapshot-id enforced on a Daft commit", "stale_assertion"),
    ("concurrency", "append; B appends between Daft's read and commit", "race_append"),
]


def write_step_summary(probe: DaftCapability) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    counts = probe.report.tally()
    lines = [
        "## OneLake Iceberg REST catalog, asked by Daft",
        "",
        f"{counts[SUPPORTED]} supported · {counts[REFUSED]} refused · "
        f"{counts[NOOP]} accepted then ignored · "
        f"{counts[SKIPPED]} skipped · {counts[BROKEN]} could not be asked",
        "",
        *probe.report.markdown(),
        "",
        f"daft `{probe.version}` · namespace `{NAMESPACE}` · run `{probe.run}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    if not any_wanted(PROBES):
        print(f"this run asks none of {', '.join(sorted(asked()))} of this client")
        return 0
    cfg = Config.from_env()
    probe = DaftCapability(cfg)
    print(f"workspace {cfg.workspace_id}  lakehouse {cfg.lakehouse_id}  namespace {NAMESPACE}")

    try:
        for group, question, method in PROBES:
            if not wanted(method):
                continue
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
    probe.report.save("daft", probe.version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
