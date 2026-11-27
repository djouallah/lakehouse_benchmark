"""What the OneLake Iceberg REST catalog lets pyiceberg do, through its Python API.

THE ONE ICEBERG COLUMN NOT ASKED IN SQL. pyiceberg is a library with no SQL, so each row is asked
with the pyiceberg call that does what the SQL engines' statement does: `append`, `delete`,
`upsert`, `overwrite`, `update_schema`, `update_spec`, `manage_snapshots`, `add_files`. Where the
API has no such call, the probe makes the call that would be it, so the `no` is Python's own
words (an AttributeError). UPDATE is `upsert` on the key with inserts off: pyiceberg has no
UPDATE, and that is how its docs change rows in place.

EVERY CHECK IS THE ENGINES': the effect is read back by reloading the table from the CATALOG (a
fresh `load_table`, never the object that wrote), with harness.py's checks. The concurrency rows
use the same race as every engine (isolation_duckdb.race_probe): this column writes through a
second catalog on the race proxy, and B is pyiceberg on the real endpoint.
"""

from __future__ import annotations

import os
import sys
from datetime import UTC

from harness import (
    ADD_FILES_ROWS,
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
    Broken,
    NoOp,
    Refused,
    Report,
    Skip,
    any_wanted,
    asked,
    evolution_check,
    evolve_spec,
    iceberg_schema,
    orphan_check,
    partition_result,
    promotion_check,
    promotion_schema,
    read_without_files,
    remove_data_files,
    rows,
    stage_orphan,
    stage_parquet,
    transform_arrow,
    transform_case,
    truncate_kind,
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

SEED = [(1, 10), (2, 20), (3, 30)]


def _one_line(text: object, limit: int = 700) -> str:
    flat = " ".join(scrub.scrub(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _say(text: object) -> None:
    print(scrub.scrub(text), flush=True)


class PyicebergCapability:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.report = Report(reason=lambda exc: _one_line(scrub.scrub_exc(exc, 2000), 600))
        self.run = "{}_{}".format(
            os.environ.get("GITHUB_RUN_ID", "local"),
            os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
        )
        self.keep = os.environ.get("CAPABILITY_KEEP") == "1"
        self.ns = NAMESPACE
        self.version = "no session"
        self.catalog = None
        self.created: list[str] = []
        self.namespaces: list[str] = []
        self.proxy = None
        self.race_catalog = None

    # -- helpers -----------------------------------------------------------------------------

    def call(self, what: str, fn):
        """One pyiceberg call. Its exception is the answer: `Refused`, carrying its words."""
        _say(f"       > {_one_line(what, 300)}")
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - every pyiceberg error is an answer
            message = f"{type(exc).__name__}: {scrub.scrub_exc(exc, 2000)}"
            raise Refused(_one_line(message)) from None

    def name(self, what: str) -> str:
        return f"pi_{self.run}_{what}"

    def iceberg(self, table: str):
        """The table as the catalog has it now: every check reloads it."""
        return self.catalog.load_table((self.ns, table))

    def _snapshots(self, table: str) -> list:
        return sorted(
            self.iceberg(table).metadata.snapshots,
            key=lambda s: (s.sequence_number or 0, s.timestamp_ms),
        )

    def _rows(self, table: str, cols=("id", "v")):
        try:
            arrow = self.iceberg(table).scan(selected_fields=cols).to_arrow()
            return sorted(tuple(row[c] for c in cols) for row in arrow.to_pylist())
        except Exception as exc:  # noqa: BLE001 - the read-back failing is part of the answer
            return f"pyiceberg could not read it: {_one_line(scrub.scrub_exc(exc, 300), 300)}"

    def _expect(self, table: str, expected: list[tuple], what: str, cols=("id", "v")) -> str:
        found = self._rows(table, cols)
        if found != expected:
            raise NoOp(f"{what} returned; expected {expected}, the reloaded table reads {found}")
        return f"{what}: the reloaded table reads {found}"

    def _create(self, what: str, schema=None, **kwargs) -> str:
        """catalog.create_table; a success that leaves no table in the catalog is a no-op."""
        if self.catalog is None:
            raise Skip("the catalog never loaded")
        table = self.name(what)
        schema = schema or iceberg_schema()
        self.call(
            f"catalog.create_table({table}{''.join(f', {k}=...' for k in kwargs)})",
            lambda: self.catalog.create_table((self.ns, table), schema=schema, **kwargs),
        )
        if not self.catalog.table_exists((self.ns, table)):
            raise NoOp("create_table returned and the catalog has no such table")
        self.created.append(table)
        return table

    def _fresh(self, what: str, **kwargs) -> str:
        """A table holding SEED, created and appended through pyiceberg."""
        table = self._create(what, **kwargs)
        try:
            self.iceberg(table).append(rows(SEED))
        except Exception as exc:  # noqa: BLE001 - no seed, no question
            raise Skip(f"could not seed the table: {_one_line(exc, 300)}") from None
        return table

    # -- session -----------------------------------------------------------------------------

    def session(self) -> str:
        import pyiceberg

        self.version = pyiceberg.__version__
        self.catalog = auth.catalog(self.cfg)
        self.catalog.create_namespace_if_not_exists(self.ns)
        return f"pyiceberg {self.version}; the REST catalog loaded with load_catalog"

    # -- create ------------------------------------------------------------------------------

    def create_table(self) -> str:
        table = self._create("base")
        return f"field ids {[f.field_id for f in self.iceberg(table).schema().fields]}"

    def create_if_not_exists(self) -> str:
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField

        table = self._fresh("ine")
        wider = Schema(
            *iceberg_schema().fields, NestedField(3, "extra", LongType(), required=False)
        )
        self.call(
            "catalog.create_table_if_not_exists(the existing table, with a column more)",
            lambda: self.catalog.create_table_if_not_exists((self.ns, table), schema=wider),
        )
        names = [f.name for f in self.iceberg(table).schema().fields]
        found = self._rows(table)
        if "extra" in names or found != SEED:
            raise NoOp(f"replaced the existing table: columns {names}, rows {found}")
        return f"accepted, the existing table untouched: columns {names}, rows {found}"

    def ctas(self) -> str:
        """Create and fill in one commit: create_table_transaction, the staged create."""
        table = self.name("ctas")

        def staged() -> None:
            with self.catalog.create_table_transaction(
                (self.ns, table), schema=iceberg_schema()
            ) as txn:
                txn.append(rows(SEED))

        self.call(f"catalog.create_table_transaction({table}) + append, one commit", staged)
        if not self.catalog.table_exists((self.ns, table)):
            raise NoOp("the transaction committed and the catalog has no such table")
        self.created.append(table)
        return self._expect(table, SEED, "create_table_transaction + append")

    def partitioned(self) -> str:
        import pyarrow as pa
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.schema import Schema
        from pyiceberg.transforms import IdentityTransform
        from pyiceberg.types import LongType, NestedField, StringType

        schema = Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "v", LongType(), required=False),
            NestedField(3, "p", StringType(), required=False),
        )
        spec = PartitionSpec(PartitionField(3, 1000, IdentityTransform(), "p"))
        table = self._create("part", schema, partition_spec=spec)
        data = pa.table(
            {
                "id": pa.array([1, 2, 3], pa.int64()),
                "v": pa.array([10, 20, 30], pa.int64()),
                "p": pa.array(["a", "a", "b"]),
            }
        )
        self.call("append 3 rows over 2 partitions", lambda: self.iceberg(table).append(data))
        spec_ = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec_:
            raise NoOp("created with a spec, and the spec is empty")
        files = len(list(self.iceberg(table).scan().plan_files()))
        return f"spec {spec_}; 3 rows over 2 partitions wrote {files} data file(s)"

    def _transform(self, kind: str) -> tuple[str, str, str]:
        schema, spec, parts = transform_case(kind)
        try:
            table = self._create(f"tr_{kind}", schema, partition_spec=spec)
            data = transform_arrow(kind, schema)
            self.call(f"append, {kind} partitions", lambda: self.iceberg(table).append(data))
        except (Refused, NoOp) as exc:
            return kind, REFUSED, str(exc)
        return partition_result(self.iceberg(table), kind, parts)

    def partition_bucket(self) -> str:
        return verdict([self._transform("bucket")])

    def partition_truncate(self) -> str:
        return verdict([self._transform("truncate")])

    def partition_temporal(self) -> str:
        return verdict([self._transform(kind) for kind in TEMPORAL])

    def _typed(self, name: str) -> tuple[str, str, str]:
        schema = type_schema(name)
        try:
            table = self._create(f"type_{name}", schema)
            data = type_arrow(name, schema)
            self.call(f"append a {name}", lambda: self.iceberg(table).append(data))
        except (Refused, NoOp) as exc:
            return name, REFUSED, str(exc)
        return type_result(self.iceberg(table), name)

    def types_scalar(self) -> str:
        return verdict([self._typed(name) for name in SCALAR_TYPES])

    def types_nested(self) -> str:
        return verdict([self._typed(name) for name in NESTED_TYPES])

    def sort_order(self) -> str:
        from pyiceberg.transforms import IdentityTransform

        table = self._fresh("sorted")
        self.call(
            "update_sort_order().asc('id')",
            lambda: self.iceberg(table).update_sort_order().asc("id", IdentityTransform()).commit(),
        )
        if not self.iceberg(table).sort_order().fields:
            raise NoOp("committed and the table has no sort order")
        return "sort order set"

    # -- write -------------------------------------------------------------------------------

    def insert_into(self) -> str:
        table = self._create("insert")
        self.call("append(SEED)", lambda: self.iceberg(table).append(rows(SEED)))
        return self._expect(table, SEED, "append")

    def insert_select(self) -> str:
        table = self._fresh("insertsel")

        def again() -> None:
            tbl = self.iceberg(table)
            found = tbl.scan(selected_fields=("id", "v")).to_arrow().to_pylist()
            tbl.append(rows([(r["id"] + 10, r["v"]) for r in found]))

        self.call("append(scan() with id + 10)", again)
        return self._expect(table, SEED + [(11, 10), (12, 20), (13, 30)], "append of a scan")

    def delete_from(self) -> str:
        table = self._fresh("delete")
        self.call("delete('id = 1')", lambda: self.iceberg(table).delete("id = 1"))
        return self._expect(table, SEED[1:], "delete")

    def _update(self, tbl, pairs) -> None:
        tbl.upsert(rows(pairs), join_cols=["id"], when_not_matched_insert_all=False)

    def update(self) -> str:
        table = self._fresh("update")
        self.call(
            "upsert((1, 999), join_cols=['id'], when_not_matched_insert_all=False)",
            lambda: self._update(self.iceberg(table), [(1, 999)]),
        )
        return self._expect(table, [(1, 999), (2, 20), (3, 30)], "upsert, matched rows only")

    def merge_upsert(self) -> str:
        table = self._fresh("merge")
        self.call(
            "upsert([(1, 777), (9, 90)], join_cols=['id'])",
            lambda: self.iceberg(table).upsert(rows([(1, 777), (9, 90)]), join_cols=["id"]),
        )
        return self._expect(table, [(1, 777), (2, 20), (3, 30), (9, 90)], "upsert")

    def truncate(self) -> str:
        from pyiceberg.expressions import AlwaysTrue

        table = self._fresh("truncate")
        self.call("delete(AlwaysTrue())", lambda: self.iceberg(table).delete(AlwaysTrue()))
        result = self._expect(table, [], "delete(AlwaysTrue())")
        return f"{result}; {truncate_kind(self.iceberg(table))}"

    def insert_overwrite(self) -> str:
        table = self._fresh("overwrite")
        self.call(
            "overwrite([(7, 70), (8, 80)])",
            lambda: self.iceberg(table).overwrite(rows([(7, 70), (8, 80)])),
        )
        return self._expect(table, [(7, 70), (8, 80)], "overwrite")

    # -- schema ------------------------------------------------------------------------------

    def _schema_change(self, table: str, what: str, change) -> None:
        def commit() -> None:
            with self.iceberg(table).update_schema() as update:
                change(update)

        self.call(f"update_schema().{what}", commit)

    def add_column(self) -> str:
        from pyiceberg.types import LongType

        table = self._fresh("addcol")
        self._schema_change(table, "add_column('w', long)", lambda u: u.add_column("w", LongType()))
        names = [f.name for f in self.iceberg(table).schema().fields]
        if "w" not in names:
            raise NoOp(f"committed and the schema is {names}")
        return f"schema {names}"

    def drop_column(self) -> str:
        table = self._fresh("dropcol")
        self._schema_change(table, "delete_column('v')", lambda u: u.delete_column("v"))
        names = [f.name for f in self.iceberg(table).schema().fields]
        if "v" in names:
            raise NoOp(f"committed and the schema is still {names}")
        return f"schema now {names}"

    def rename_column(self) -> str:
        table = self._fresh("renamecol")
        self._schema_change(table, "rename_column('v', 'v2')", lambda u: u.rename_column("v", "v2"))
        names = [f.name for f in self.iceberg(table).schema().fields]
        if "v2" not in names:
            raise NoOp(f"committed and the schema is still {names}")
        return self._expect(table, SEED, "renamed, data kept", ("id", "v2"))

    def type_promotion(self) -> str:
        import pyarrow as pa
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        from pyiceberg.types import LongType

        schema = promotion_schema()
        table = self._create("promote", schema)
        self.iceberg(table).append(
            pa.Table.from_pylist([{"id": 1, "c": 7}], schema=schema_to_pyarrow(schema))
        )
        self._schema_change(
            table, "update_column('c', long)", lambda u: u.update_column("c", LongType())
        )
        return promotion_check(self.iceberg(table))

    def partition_evolution(self) -> str:
        table = self._fresh("specevo")
        self.call(
            "update_spec().add_field('id', bucket(4))", lambda: evolve_spec(self.iceberg(table))
        )
        spec = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("committed and the table is still unpartitioned")
        return f"spec now {spec}"

    def write_after_evolution(self) -> str:
        table = self._create("evowrite")
        self.call("append, first spec", lambda: self.iceberg(table).append(rows(EVOLVE_FIRST)))
        evolve_spec(self.iceberg(table))
        self.call("append, evolved spec", lambda: self.iceberg(table).append(rows(EVOLVE_THEN)))
        count = self.iceberg(table).scan().to_arrow().num_rows
        return evolution_check(self.iceberg(table), count)

    def set_property(self) -> str:
        table = self._fresh("props")

        def commit() -> None:
            with self.iceberg(table).transaction() as txn:
                txn.set_properties({"probed-at": self.run})

        self.call("transaction().set_properties(probed-at)", commit)
        if self.iceberg(table).properties.get("probed-at") != self.run:
            raise NoOp("committed and the property is not on the table")
        return "the property is on the table"

    # -- read --------------------------------------------------------------------------------

    def time_travel(self) -> str:
        table = self._fresh("travel")
        seeded = self.iceberg(table).current_snapshot().snapshot_id
        self.iceberg(table).append(rows([(4, 40)]))
        found = self.call(
            "scan(snapshot_id=the seed's)",
            lambda: self.iceberg(table).scan(snapshot_id=seeded).to_arrow().num_rows,
        )
        if found != 3:
            raise NoOp(f"scan(snapshot_id=the seed's snapshot) reads {found} rows, not 3")
        return "scan(snapshot_id=the seed's snapshot) reads 3 rows (the table has 4)"

    def metadata_tables(self) -> str:
        table = self._fresh("inspect")
        found = self.call("inspect.snapshots()", lambda: self.iceberg(table).inspect.snapshots())
        return f"inspect.snapshots() reads {found.num_rows} snapshot(s)"

    def select_table(self) -> str:
        table = self._fresh("select")
        found = self.call(
            "load_table(by name).scan().to_arrow()",
            lambda: sorted(
                (r["id"], r["v"])
                for r in self.catalog.load_table((self.ns, table)).scan().to_arrow().to_pylist()
            ),
        )
        if found != SEED:
            raise NoOp(f"reads {found}, wrote {SEED}")
        return f"load_table by its catalog name reads the {len(SEED)} rows"

    def _stats_table(self, what: str) -> str:
        table = self._create(what)
        write_stats_files(self.iceberg(table))
        return table

    def _read(self, table: str, query):
        return lambda: self.call(query[0], lambda: query[1](self.iceberg(table)))

    def file_pruning(self) -> str:
        import pyarrow.compute as pc
        from pyiceberg.expressions import EqualTo

        table = self._stats_table("prune")
        gone = remove_data_files(self.iceberg(table), keep_id=PRUNE_ID)
        return read_without_files(
            f"scan(row_filter=id == {PRUNE_ID})",
            self._read(
                table,
                (
                    f"scan(row_filter=EqualTo('id', {PRUNE_ID}), selected_fields=('v',))",
                    lambda t: [
                        (r["v"],)
                        for r in t.scan(row_filter=EqualTo("id", PRUNE_ID), selected_fields=("v",))
                        .to_arrow()
                        .to_pylist()
                    ],
                ),
            ),
            PRUNE_EXPECTED,
            self._read(table, ("sum(v) of scan()", lambda t: pc.sum(t.scan().to_arrow()["v"]))),
            gone,
        )

    def max_from_metadata(self) -> str:
        """pyiceberg has no aggregate: max(id) is a scan of the column."""
        import pyarrow.compute as pc

        table = self._stats_table("max")
        gone = remove_data_files(self.iceberg(table))
        return read_without_files(
            "max(id) of scan(selected_fields=('id',))",
            self._read(
                table,
                (
                    "max(id) of scan(selected_fields=('id',))",
                    lambda t: [(pc.max(t.scan(selected_fields=("id",)).to_arrow()["id"]).as_py(),)],
                ),
            ),
            MAX_EXPECTED,
            self._read(table, ("sum(v) of scan()", lambda t: pc.sum(t.scan().to_arrow()["v"]))),
            gone,
        )

    # -- refs --------------------------------------------------------------------------------

    def _ref(self, kind: str) -> str:
        table = self._fresh(kind)
        tbl = self.iceberg(table)
        snapshot = tbl.current_snapshot().snapshot_id
        make = {"branch": "create_branch", "tag": "create_tag"}[kind]
        self.call(
            f"manage_snapshots().{make}(current, 'probe_{kind}')",
            lambda: getattr(tbl.manage_snapshots(), make)(snapshot, f"probe_{kind}").commit(),
        )
        refs = sorted(self.iceberg(table).metadata.refs)
        if f"probe_{kind}" not in refs:
            raise NoOp(f"committed and the refs are {refs}")
        return f"refs now {refs}"

    def create_branch(self) -> str:
        return self._ref("branch")

    def create_tag(self) -> str:
        return self._ref("tag")

    # -- maintenance -------------------------------------------------------------------------

    def rewrite_data_files(self) -> str:
        """pyiceberg's maintenance has expire_snapshots and no compaction: the AttributeError of
        the call that would be it is the answer."""
        table = self._fresh("compact")
        self.iceberg(table).append(rows([(4, 40)]))
        before = len(list(self.iceberg(table).scan().plan_files()))
        self.call(
            "maintenance.rewrite_data_files()",
            lambda: self.iceberg(table).maintenance.rewrite_data_files(),
        )
        after = len(list(self.iceberg(table).scan().plan_files()))
        if after >= before:
            raise NoOp(f"returned and data files went {before} -> {after}")
        return self._expect(table, SEED + [(4, 40)], f"{before} -> {after} data file(s), data kept")

    def rewrite_manifests(self) -> str:
        table = self._fresh("manifests")
        self.iceberg(table).append(rows([(4, 40)]))
        before = len(self._snapshots(table))
        self.call(
            "maintenance.rewrite_manifests()",
            lambda: self.iceberg(table).maintenance.rewrite_manifests(),
        )
        if len(self._snapshots(table)) == before:
            raise NoOp("returned and no new snapshot was committed")
        return "a new snapshot rewrote the manifests"

    def add_files(self) -> str:
        table = self._fresh("addfiles")
        folder = stage_parquet(self.iceberg(table))
        self.call(
            "add_files([the staged Parquet file])",
            lambda: self.iceberg(table).add_files([f"{folder}/part-0.parquet"]),
        )
        return self._expect(table, sorted(SEED + ADD_FILES_ROWS), "add_files")

    def expire_snapshots(self) -> str:
        """maintenance.expire_snapshots().older_than(now): the current snapshot is protected."""
        from datetime import datetime

        table = self._fresh("expire")
        self.iceberg(table).append(rows([(4, 40)]))
        self.iceberg(table).append(rows([(5, 50)]))
        before = len(self._snapshots(table))
        self.call(
            "maintenance.expire_snapshots().older_than(now).commit()",
            lambda: (
                self.iceberg(table)
                .maintenance.expire_snapshots()
                .older_than(datetime.now(UTC))
                .commit()
            ),
        )
        after = len(self._snapshots(table))
        if after >= before:
            raise NoOp(f"returned and history went {before} -> {after}")
        return f"history {before} -> {after} snapshot(s)"

    def remove_orphan_files(self) -> str:
        """Delete a file no snapshot references (harness.stage_orphan); the rows stay."""
        table = self._fresh("orphans")
        path = stage_orphan(self.iceberg(table))
        self.call(
            "maintenance.remove_orphan_files()",
            lambda: self.iceberg(table).maintenance.remove_orphan_files(),
        )
        return self._expect(table, SEED, orphan_check(self.iceberg(table), path))

    # -- catalog -----------------------------------------------------------------------------

    def namespace_create(self) -> str:
        ns = self.name("ns")
        self.call(f"catalog.create_namespace({ns})", lambda: self.catalog.create_namespace(ns))
        if (ns,) not in self.catalog.list_namespaces():
            raise NoOp("create_namespace returned and list_namespaces does not have it")
        self.namespaces.append(ns)
        return f"`{ns}` created; list_namespaces has it"

    def namespace_drop(self) -> str:
        ns = self.name("nsdrop")
        self.catalog.create_namespace(ns)
        self.namespaces.append(ns)
        self.call(f"catalog.drop_namespace({ns})", lambda: self.catalog.drop_namespace(ns))
        if (ns,) in self.catalog.list_namespaces():
            raise NoOp("drop_namespace returned and list_namespaces still has it")
        self.namespaces.remove(ns)
        return "the empty namespace is gone"

    def namespace_existing(self) -> str:
        self.call(
            f"catalog.create_namespace_if_not_exists({self.ns})",
            lambda: self.catalog.create_namespace_if_not_exists(self.ns),
        )
        return f"accepted on the existing `{self.ns}`"

    def drop_purge(self) -> str:
        table = self._fresh("droppable")
        tbl = self.iceberg(table)
        data = [task.file.file_path for task in tbl.scan().plan_files()]
        self.call(
            f"catalog.purge_table({table})", lambda: self.catalog.purge_table((self.ns, table))
        )
        if self.catalog.table_exists((self.ns, table)):
            raise NoOp("returned and the catalog still has it")
        self.created.remove(table)
        try:
            left = sum(1 for path in data if tbl.io.new_input(path).exists())
        except Exception as exc:  # noqa: BLE001 - informational
            return f"gone from the catalog; could not check storage: {_one_line(exc, 200)}"
        if left:
            raise NoOp(f"gone from the catalog, and {left} of {len(data)} data file(s) remain")
        return f"gone from the catalog; 0 of {len(data)} data file(s) still in storage"

    # -- concurrency -------------------------------------------------------------------------

    def _race(self, key: str, write) -> str:
        """The table holds SEED; this column writes through a second catalog on the race proxy,
        whose commit bench.capability.race holds until B's (pyiceberg, real endpoint) has landed."""
        if self.catalog is None:
            raise Skip("the catalog never loaded")
        if self.proxy is None:
            self.proxy = RaceProxy().start()
            self.race_catalog = auth.catalog(self.cfg, self.proxy.endpoint)
        table = self._fresh(key)
        refused: list[Refused] = []

        def go() -> None:
            try:
                self.call(
                    write[0], lambda: write[1](self.race_catalog.load_table((self.ns, table)))
                )
            except Refused as exc:
                refused.append(exc)
                raise

        try:
            return race_probe(key, self.catalog, self.proxy, table, go)
        except Broken:
            if refused and not self.proxy.commits.get((self.ns, table)):
                raise refused[0] from None
            raise

    def race_append(self) -> str:
        return self._race("race_append", ("append((5, 50))", lambda t: t.append(rows([(5, 50)]))))

    def race_delete(self) -> str:
        return self._race("race_delete", ("delete('id = 1')", lambda t: t.delete("id = 1")))

    def race_update(self) -> str:
        def bump(t) -> None:
            v = t.scan(row_filter="id = 2", selected_fields=("v",)).to_arrow()["v"][0].as_py()
            self._update(t, [(2, v + 1)])

        return self._race("race_update", ("upsert (2, v + 1), matched rows only", bump))

    # -- teardown ----------------------------------------------------------------------------

    def drop_everything(self) -> None:
        if not (self.created or self.namespaces):
            return
        if self.keep:
            print(f"\nCAPABILITY_KEEP=1; left {len(self.created)} table(s) behind")
            return
        dropped, left = 0, []
        for table in self.created:
            try:
                if self.catalog.table_exists((self.ns, table)):
                    self.catalog.purge_table((self.ns, table))
                dropped += 1
            except Exception as exc:  # noqa: BLE001 - teardown is best effort, by design
                left.append(f"{table} ({_one_line(exc, 80)})")
        for ns in self.namespaces:
            try:
                self.catalog.drop_namespace(ns)
            except Exception as exc:  # noqa: BLE001
                left.append(f"namespace {ns} ({_one_line(exc, 80)})")
        print(f"\ndropped {dropped} probe table(s)" + (f"; left behind {left}" if left else ""))


PROBES = [
    ("session", "pyiceberg loads the REST catalog", "session"),
    ("create", "catalog.create_table", "create_table"),
    ("create", "create_table_if_not_exists on an existing table", "create_if_not_exists"),
    ("create", "create_table_transaction + append (CTAS)", "ctas"),
    ("create", "create_table with an identity partition, then append", "partitioned"),
    ("create", "update_sort_order (sort order)", "sort_order"),
    ("create", "bucket(4, id) partition spec, then append", "partition_bucket"),
    ("create", "truncate(2, x) partition spec, then append", "partition_truncate"),
    ("create", "year / month / day / hour partition specs, then append", "partition_temporal"),
    ("create", "types: decimal, date, timestamp, timestamptz, uuid, binary", "types_scalar"),
    ("create", "nested types: struct, list, map", "types_nested"),
    ("write", "append", "insert_into"),
    ("write", "append of a scan of the table", "insert_select"),
    ("write", "delete(filter)", "delete_from"),
    ("write", "upsert on the key, matched rows only (UPDATE)", "update"),
    ("write", "upsert: matched update + not matched insert", "merge_upsert"),
    ("write", "delete(AlwaysTrue()) (TRUNCATE)", "truncate"),
    ("write", "overwrite, whole table", "insert_overwrite"),
    ("schema", "update_schema().add_column", "add_column"),
    ("schema", "update_schema().delete_column", "drop_column"),
    ("schema", "update_schema().rename_column", "rename_column"),
    ("schema", "update_schema().update_column (int -> long)", "type_promotion"),
    ("schema", "update_spec().add_field (partition evolution)", "partition_evolution"),
    ("schema", "append, update_spec adds bucket(4, id), append", "write_after_evolution"),
    ("schema", "transaction().set_properties", "set_property"),
    ("read", "scan(snapshot_id=...) (time travel)", "time_travel"),
    ("read", "inspect.snapshots() (metadata tables)", "metadata_tables"),
    ("read", "load_table by its catalog name, scan", "select_table"),
    ("read", "scan(row_filter=id == 22) with the other data files deleted", "file_pruning"),
    ("read", "max(id) with every data file deleted", "max_from_metadata"),
    ("refs", "manage_snapshots().create_branch", "create_branch"),
    ("refs", "manage_snapshots().create_tag", "create_tag"),
    ("maintenance", "compaction", "rewrite_data_files"),
    ("maintenance", "rewrite manifests", "rewrite_manifests"),
    ("maintenance", "add_files", "add_files"),
    ("maintenance", "maintenance.expire_snapshots().older_than(now)", "expire_snapshots"),
    ("maintenance", "maintenance.remove_orphan_files", "remove_orphan_files"),
    ("catalog", "catalog.create_namespace", "namespace_create"),
    ("catalog", "catalog.drop_namespace", "namespace_drop"),
    ("catalog", "create_namespace_if_not_exists, existing namespace", "namespace_existing"),
    ("catalog", "catalog.purge_table", "drop_purge"),
    ("concurrency", "append; B appends between the read and the commit", "race_append"),
    ("concurrency", "delete id 1; B appends", "race_delete"),
    ("concurrency", "upsert v + 1 on id 2; B appends", "race_update"),
]


def write_step_summary(probe: PyicebergCapability) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    counts = probe.report.tally()
    lines = [
        "## OneLake Iceberg REST catalog, asked by pyiceberg's Python API",
        "",
        f"{counts[SUPPORTED]} supported · {counts[REFUSED]} refused · "
        f"{counts[NOOP]} accepted then ignored · "
        f"{counts[SKIPPED]} skipped · {counts[BROKEN]} could not be asked",
        "",
        *probe.report.markdown(),
        "",
        f"pyiceberg `{probe.version}` · namespace `{NAMESPACE}` · run `{probe.run}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    if not any_wanted(PROBES):
        print(f"this run asks none of {', '.join(sorted(asked()))} of this client")
        return 0
    cfg = Config.from_env()
    probe = PyicebergCapability(cfg)
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
    probe.report.save("pyiceberg", probe.version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
