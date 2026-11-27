"""What the OneLake Iceberg REST catalog lets apache/iceberg-go do, through its Go API.

The library table maintenance runs on (.github/scripts/maintain, the same pinned version). Like
pyiceberg it has no SQL, so each row is asked with the iceberg-go call that does what the SQL
engines' statement does. The call itself runs in iceberg_go/ (a small Go program, one op per run;
its error is the answer, in iceberg-go's own words). A row with no such call in iceberg-go (CTAS,
UPDATE, MERGE) is a no that names the missing call: Go has no AttributeError to quote.

EVERY SETUP AND CHECK IS PYICEBERG'S, as for every column: the tables are created and seeded through
pyiceberg (catalog_capability_pyiceberg.PyicebergCapability, whose helpers this reuses), and the
effect is read back by reloading the table from the catalog. Rows go to the Go side as an Arrow IPC
file, schemas and partition specs as Iceberg JSON.

    IGO_PROBE=<the built binary> python .github/scripts/capability/catalog_capability_iceberg_go.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

from catalog_capability_pyiceberg import SEED, PyicebergCapability, _one_line, _say
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
    NoOp,
    Refused,
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
)
from isolation_duckdb import race_probe

from bench import auth, scrub
from bench.capability.race import RaceProxy
from bench.config import ICEBERG_ENDPOINT, Config

BINARY = os.environ.get("IGO_PROBE", "iceberg-go-probe")


def no_call(what: str) -> Refused:
    """A row iceberg-go's API has no call for. Go has no AttributeError to quote: the answer is
    that the call does not exist, checked against the pinned version's table package."""
    return Refused(f"iceberg-go has no {what} call (its table API: Append, Overwrite, Delete)")


class IcebergGoCapability(PyicebergCapability):
    def __init__(self, cfg: Config):
        super().__init__(cfg)
        self.tmp = tempfile.mkdtemp(prefix="igo_")

    def name(self, what: str) -> str:
        return f"ig_{self.run}_{what}"

    # -- the Go side ---------------------------------------------------------------------------

    def go(self, what: str, op: str, table: str = "", uri: str = ICEBERG_ENDPOINT, **args):
        """One iceberg-go call. Its error is the answer: `Refused`, carrying its words."""
        _say(f"       > {_one_line(what, 300)}")
        env = {
            **os.environ,
            "ICEBERG_URI": uri,
            "ICEBERG_TOKEN": auth.onelake_token(),
            "ICEBERG_WAREHOUSE": self.cfg.warehouse,
        }
        request = json.dumps({"op": op, "ns": args.pop("ns", self.ns), "table": table, **args})
        done = subprocess.run(
            [BINARY], input=request, env=env, capture_output=True, text=True, timeout=900
        )
        lines = [line for line in done.stdout.splitlines() if line.startswith("{")]
        if not lines:
            raise Refused(_one_line(f"no answer (exit {done.returncode}): {done.stderr}"))
        answer = json.loads(lines[-1])
        if not answer.get("ok"):
            raise Refused(_one_line(scrub.scrub(answer.get("error", ""))))
        return answer.get("result")

    def ipc(self, data) -> str:
        """Rows (pairs) or an Arrow table, as an Arrow IPC file the Go side reads."""
        import pyarrow as pa

        table = data if isinstance(data, pa.Table) else rows(data)
        path = os.path.join(self.tmp, f"rows_{len(os.listdir(self.tmp))}.arrow")
        with pa.OSFile(path, "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
        return path

    def go_append(self, table: str, data, what: str = "") -> None:
        self.go(what or f"AppendTable({len(data)} row(s))", "append", table, ipc=self.ipc(data))

    def go_scan(self, table: str, what: str, **args) -> list[tuple]:
        return sorted(tuple(r) for r in self.go(what, "scan", table, **args) or [])

    def go_create(self, what: str, schema=None, spec=None) -> str:
        """catalog.CreateTable; a success that leaves no table in the catalog is a no-op."""
        if self.catalog is None:
            raise Skip("the catalog never loaded")
        table = self.name(what)
        args = {"schema": json.loads((schema or iceberg_schema()).model_dump_json())}
        if spec is not None:
            args["spec"] = json.loads(spec.model_dump_json())
        self.go(
            f"catalog.CreateTable({table}{', partition spec' if spec else ''})",
            "create",
            table,
            **args,
        )
        if not self.catalog.table_exists((self.ns, table)):
            raise NoOp("CreateTable returned and the catalog has no such table")
        self.created.append(table)
        return table

    # -- session -----------------------------------------------------------------------------

    def session(self) -> str:
        super().session()  # pyiceberg, for the setups and the checks
        self.version = self.go("iceberg-go version", "version")
        return self.go("rest.NewCatalog, ListNamespaces", "session")

    # -- create ------------------------------------------------------------------------------

    def create_table(self) -> str:
        table = self.go_create("base")
        return f"field ids {[f.field_id for f in self.iceberg(table).schema().fields]}"

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
        table = self.go_create("part", schema, spec)
        data = pa.table(
            {
                "id": pa.array([1, 2, 3], pa.int64()),
                "v": pa.array([10, 20, 30], pa.int64()),
                "p": pa.array(["a", "a", "b"]),
            }
        )
        self.go("AppendTable, 3 rows over 2 partitions", "append", table, ipc=self.ipc(data))
        spec_ = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec_:
            raise NoOp("created with a spec, and the spec is empty")
        files = len(list(self.iceberg(table).scan().plan_files()))
        return f"spec {spec_}; 3 rows over 2 partitions wrote {files} data file(s)"

    def _transform(self, kind: str) -> tuple[str, str, str]:
        schema, spec, parts = transform_case(kind)
        try:
            table = self.go_create(f"tr_{kind}", schema, spec)
            data = transform_arrow(kind, schema)
            self.go(f"AppendTable, {kind} partitions", "append", table, ipc=self.ipc(data))
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
            table = self.go_create(f"type_{name}", schema)
            data = type_arrow(name, schema)
            self.go(f"AppendTable a {name}", "append", table, ipc=self.ipc(data))
        except (Refused, NoOp) as exc:
            return name, REFUSED, str(exc)
        return type_result(self.iceberg(table), name)

    def create_if_not_exists(self) -> str:
        """iceberg-go has no if-not-exists form: CreateTable on the existing table must not touch
        it, and the error it returns is the answer."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField

        table = self._fresh("ine")
        wider = Schema(
            *iceberg_schema().fields, NestedField(3, "extra", LongType(), required=False)
        )
        self.go(
            "catalog.CreateTable(the existing table, with a column more)",
            "create",
            table,
            schema=json.loads(wider.model_dump_json()),
        )
        names = [f.name for f in self.iceberg(table).schema().fields]
        raise NoOp(f"CreateTable on an existing table returned success; columns now {names}")

    def ctas(self) -> str:
        raise no_call("create-and-fill-in-one-commit (CTAS)")

    def types_scalar(self) -> str:
        return verdict([self._typed(name) for name in SCALAR_TYPES])

    def types_nested(self) -> str:
        return verdict([self._typed(name) for name in NESTED_TYPES])

    def sort_order(self) -> str:
        table = self._fresh("sorted")
        self.go("NewReplaceSortOrder().Asc('id').Commit()", "sort_order", table)
        if not self.iceberg(table).sort_order().fields:
            raise NoOp("committed and the table has no sort order")
        return "sort order set"

    # -- write -------------------------------------------------------------------------------

    def insert_into(self) -> str:
        table = self._create("insert")
        self.go_append(table, SEED, "AppendTable(SEED)")
        return self._expect(table, SEED, "AppendTable")

    def insert_select(self) -> str:
        table = self._fresh("insertsel")
        found = self.go_scan(table, "Scan(id, v)", columns=["id", "v"])
        self.go_append(table, [(i + 10, v) for i, v in found], "AppendTable(the scan, id + 10)")
        return self._expect(table, SEED + [(11, 10), (12, 20), (13, 30)], "append of a scan")

    def delete_from(self) -> str:
        table = self._fresh("delete")
        self.go("Delete(EqualTo(id, 1))", "delete", table, filter="id = 1")
        return self._expect(table, SEED[1:], "Delete")

    def truncate(self) -> str:
        table = self._fresh("truncate")
        self.go("Delete(AlwaysTrue)", "delete", table, filter="all")
        result = self._expect(table, [], "Delete(AlwaysTrue)")
        return f"{result}; {truncate_kind(self.iceberg(table))}"

    def insert_overwrite(self) -> str:
        table = self._fresh("overwrite")
        self.go(
            "OverwriteTable([(7, 70), (8, 80)])",
            "overwrite",
            table,
            ipc=self.ipc([(7, 70), (8, 80)]),
        )
        return self._expect(table, [(7, 70), (8, 80)], "OverwriteTable")

    def update(self) -> str:
        raise no_call("row update or upsert")

    def merge_upsert(self) -> str:
        raise no_call("upsert or merge")

    # -- schema ------------------------------------------------------------------------------

    def add_column(self) -> str:
        table = self._fresh("addcol")
        self.go("UpdateSchema().AddColumn('w', long)", "add_column", table)
        names = [f.name for f in self.iceberg(table).schema().fields]
        if "w" not in names:
            raise NoOp(f"committed and the schema is {names}")
        return f"schema {names}"

    def drop_column(self) -> str:
        table = self._fresh("dropcol")
        self.go("UpdateSchema().DeleteColumn('v')", "drop_column", table)
        names = [f.name for f in self.iceberg(table).schema().fields]
        if "v" in names:
            raise NoOp(f"committed and the schema is still {names}")
        return f"schema now {names}"

    def rename_column(self) -> str:
        table = self._fresh("renamecol")
        self.go("UpdateSchema().RenameColumn('v', 'v2')", "rename_column", table)
        names = [f.name for f in self.iceberg(table).schema().fields]
        if "v2" not in names:
            raise NoOp(f"committed and the schema is still {names}")
        return self._expect(table, SEED, "renamed, data kept", ("id", "v2"))

    def type_promotion(self) -> str:
        import pyarrow as pa
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        schema = promotion_schema()
        table = self._create("promote", schema)
        self.iceberg(table).append(
            pa.Table.from_pylist([{"id": 1, "c": 7}], schema=schema_to_pyarrow(schema))
        )
        self.go("UpdateSchema().UpdateColumn('c', long)", "type_promotion", table)
        return promotion_check(self.iceberg(table))

    def partition_evolution(self) -> str:
        table = self._fresh("specevo")
        self.go("UpdateSpec().AddField('id', bucket(4))", "partition_evolution", table)
        spec = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("committed and the table is still unpartitioned")
        return f"spec now {spec}"

    def write_after_evolution(self) -> str:
        table = self._create("evowrite")
        self.go_append(table, EVOLVE_FIRST, "AppendTable, first spec")
        evolve_spec(self.iceberg(table))
        self.go_append(table, EVOLVE_THEN, "AppendTable, evolved spec")
        count = self.iceberg(table).scan().to_arrow().num_rows
        return evolution_check(self.iceberg(table), count)

    def set_property(self) -> str:
        table = self._fresh("props")
        self.go("SetProperties(probed-at)", "set_property", table, value=self.run)
        if self.iceberg(table).properties.get("probed-at") != self.run:
            raise NoOp("committed and the property is not on the table")
        return "the property is on the table"

    # -- read --------------------------------------------------------------------------------

    def time_travel(self) -> str:
        table = self._fresh("travel")
        seeded = self.iceberg(table).current_snapshot().snapshot_id
        self.iceberg(table).append(rows([(4, 40)]))
        found = self.go_scan(
            table, "Scan(WithSnapshotID(the seed's))", columns=["id", "v"], snapshot=seeded
        )
        if len(found) != 3:
            raise NoOp(f"Scan(WithSnapshotID(the seed's snapshot)) reads {len(found)} rows, not 3")
        return "Scan(WithSnapshotID(the seed's snapshot)) reads 3 rows (the table has 4)"

    def metadata_tables(self) -> str:
        table = self._fresh("inspect")
        found = self.go("inspect.Snapshots()", "snapshots", table)
        expected = len(self._snapshots(table))
        if found != expected:
            raise NoOp(f"inspect.Snapshots() reads {found} snapshot(s), the table has {expected}")
        return f"inspect.Snapshots() reads {found} snapshot(s)"

    def select_table(self) -> str:
        table = self._fresh("select")
        found = self.go_scan(table, "LoadTable(by name).Scan()", columns=["id", "v"])
        if found != SEED:
            raise NoOp(f"reads {found}, wrote {SEED}")
        return f"LoadTable by its catalog name reads the {len(SEED)} rows"

    def file_pruning(self) -> str:
        table = self._stats_table("prune")
        gone = remove_data_files(self.iceberg(table), keep_id=PRUNE_ID)
        return read_without_files(
            f"Scan(WithRowFilter(id == {PRUNE_ID}))",
            lambda: self.go_scan(
                table,
                f"Scan(WithRowFilter(EqualTo(id, {PRUNE_ID})), v)",
                columns=["v"],
                equals_id=PRUNE_ID,
            ),
            PRUNE_EXPECTED,
            lambda: self.go_scan(table, "Scan() of every row", columns=["v"]),
            gone,
        )

    def max_from_metadata(self) -> str:
        """iceberg-go has no aggregate: max(id) is a scan of the column."""
        table = self._stats_table("max")
        gone = remove_data_files(self.iceberg(table))

        def biggest():
            ids = self.go_scan(table, "max(id) of Scan(id)", columns=["id"])
            return [(max(i for (i,) in ids),)] if ids else []

        return read_without_files(
            "max(id) of Scan(id)",
            biggest,
            MAX_EXPECTED,
            lambda: self.go_scan(table, "Scan() of every row", columns=["v"]),
            gone,
        )

    # -- refs: a set-snapshot-ref commit, as iceberg-go's own CLI (cmd/iceberg) makes them ------

    def _ref(self, kind: str) -> str:
        table = self._fresh(kind)
        self.go(
            f"CommitTable(NewSetSnapshotRefUpdate(probe_{kind}, current, {kind}))",
            kind,
            table,
            name=f"probe_{kind}",
        )
        refs = sorted(self.iceberg(table).metadata.refs)
        if f"probe_{kind}" not in refs:
            raise NoOp(f"committed and the refs are {refs}")
        return f"refs now {refs}"

    def create_branch(self) -> str:
        return self._ref("branch")

    def create_tag(self) -> str:
        return self._ref("tag")

    # -- maintenance: the calls .github/scripts/maintain makes ----------------------------------

    def rewrite_data_files(self) -> str:
        table = self._fresh("compact")
        self.iceberg(table).append(rows([(4, 40)]))
        before = len(list(self.iceberg(table).scan().plan_files()))
        self.go("compaction.Analyze + RewriteDataFiles", "compaction", table)
        after = len(list(self.iceberg(table).scan().plan_files()))
        if after >= before:
            raise NoOp(f"returned and data files went {before} -> {after}")
        return self._expect(table, SEED + [(4, 40)], f"{before} -> {after} data file(s), data kept")

    def rewrite_manifests(self) -> str:
        table = self._fresh("manifests")
        self.iceberg(table).append(rows([(4, 40)]))
        before = len(self._snapshots(table))
        self.go("RewriteManifests()", "rewrite_manifests", table)
        if len(self._snapshots(table)) == before:
            raise NoOp("returned and no new snapshot was committed")
        return "a new snapshot rewrote the manifests"

    def add_files(self) -> str:
        table = self._fresh("addfiles")
        folder = stage_parquet(self.iceberg(table))
        self.go(
            "AddDataFilesByPath([the staged Parquet file])",
            "add_files",
            table,
            path=f"{folder}/part-0.parquet",
        )
        return self._expect(table, sorted(SEED + ADD_FILES_ROWS), "AddDataFilesByPath")

    def expire_snapshots(self) -> str:
        table = self._fresh("expire")
        self.iceberg(table).append(rows([(4, 40)]))
        self.iceberg(table).append(rows([(5, 50)]))
        before = len(self._snapshots(table))
        self.go("ExpireSnapshots(WithOlderThan(0), WithRetainLast(1))", "expire_snapshots", table)
        after = len(self._snapshots(table))
        if after >= before:
            raise NoOp(f"returned and history went {before} -> {after}")
        return f"history {before} -> {after} snapshot(s)"

    def remove_orphan_files(self) -> str:
        """Delete a file no snapshot references (harness.stage_orphan); the rows stay."""
        table = self._fresh("orphans")
        path = stage_orphan(self.iceberg(table))
        self.go(
            "maintenance.DeleteOrphanFiles(WithCleanupFilesOlderThan(0))",
            "remove_orphan_files",
            table,
        )
        return self._expect(table, SEED, orphan_check(self.iceberg(table), path))

    # -- catalog -----------------------------------------------------------------------------

    def namespace_create(self) -> str:
        ns = self.name("ns")
        self.go(f"catalog.CreateNamespace({ns})", "namespace_create", ns=ns)
        if (ns,) not in self.catalog.list_namespaces():
            raise NoOp("CreateNamespace returned and list_namespaces does not have it")
        self.namespaces.append(ns)
        return f"`{ns}` created; list_namespaces has it"

    def namespace_drop(self) -> str:
        ns = self.name("nsdrop")
        self.catalog.create_namespace(ns)
        self.namespaces.append(ns)
        self.go(f"catalog.DropNamespace({ns})", "namespace_drop", ns=ns)
        if (ns,) in self.catalog.list_namespaces():
            raise NoOp("DropNamespace returned and list_namespaces still has it")
        self.namespaces.remove(ns)
        return "the empty namespace is gone"

    def namespace_existing(self) -> str:
        """No if-not-exists form either: CreateNamespace on the existing one; its error is the
        answer."""
        self.go(f"catalog.CreateNamespace({self.ns}), existing", "namespace_create")
        return f"accepted on the existing `{self.ns}`"

    def drop_purge(self) -> str:
        table = self._fresh("droppable")
        tbl = self.iceberg(table)
        data = [task.file.file_path for task in tbl.scan().plan_files()]
        self.go(f"catalog.PurgeTable({table})", "purge", table)
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

    # -- concurrency: iceberg-go commits through the race proxy, B is pyiceberg -----------------

    def _go_race(self, key: str, what: str, op: str, **args) -> str:
        if self.catalog is None:
            raise Skip("the catalog never loaded")
        if self.proxy is None:
            self.proxy = RaceProxy().start()
        table = self._fresh(key)
        return race_probe(
            key,
            self.catalog,
            self.proxy,
            table,
            lambda: self.go(what, op, table, uri=self.proxy.endpoint, **args),
        )

    def race_append(self) -> str:
        return self._go_race(
            "race_append", "AppendTable((5, 50))", "append", ipc=self.ipc([(5, 50)])
        )

    def race_delete(self) -> str:
        return self._go_race("race_delete", "Delete(EqualTo(id, 1))", "delete", filter="id = 1")

    def race_update(self) -> str:
        raise no_call("row update or upsert")


PROBES = [
    ("session", "iceberg-go loads the REST catalog", "session"),
    ("create", "catalog.CreateTable", "create_table"),
    ("create", "CreateTable on an existing table (IF NOT EXISTS)", "create_if_not_exists"),
    ("create", "create and fill in one commit (CTAS)", "ctas"),
    ("create", "CreateTable with an identity partition, then AppendTable", "partitioned"),
    ("create", "NewReplaceSortOrder (sort order)", "sort_order"),
    ("create", "bucket(4, id) partition spec, then AppendTable", "partition_bucket"),
    ("create", "truncate(2, x) partition spec, then AppendTable", "partition_truncate"),
    ("create", "year / month / day / hour partition specs, then AppendTable", "partition_temporal"),
    ("create", "types: decimal, date, timestamp, timestamptz, uuid, binary", "types_scalar"),
    ("create", "nested types: struct, list, map", "types_nested"),
    ("write", "AppendTable", "insert_into"),
    ("write", "AppendTable of a scan of the table", "insert_select"),
    ("write", "Delete(filter)", "delete_from"),
    ("write", "update rows (UPDATE)", "update"),
    ("write", "upsert: matched update + not matched insert", "merge_upsert"),
    ("write", "Delete(AlwaysTrue) (TRUNCATE)", "truncate"),
    ("write", "OverwriteTable, whole table", "insert_overwrite"),
    ("schema", "UpdateSchema().AddColumn", "add_column"),
    ("schema", "UpdateSchema().DeleteColumn", "drop_column"),
    ("schema", "UpdateSchema().RenameColumn", "rename_column"),
    ("schema", "UpdateSchema().UpdateColumn (int -> long)", "type_promotion"),
    ("schema", "UpdateSpec().AddField (partition evolution)", "partition_evolution"),
    ("schema", "AppendTable, pyiceberg adds bucket(4, id), AppendTable", "write_after_evolution"),
    ("schema", "SetProperties", "set_property"),
    ("read", "Scan(WithSnapshotID) (time travel)", "time_travel"),
    ("read", "inspect.Snapshots (metadata tables)", "metadata_tables"),
    ("read", "LoadTable by its catalog name, Scan", "select_table"),
    ("read", "Scan(WithRowFilter(id == 22)) with the other data files deleted", "file_pruning"),
    ("read", "max(id) with every data file deleted", "max_from_metadata"),
    ("refs", "set-snapshot-ref commit, branch", "create_branch"),
    ("refs", "set-snapshot-ref commit, tag", "create_tag"),
    ("maintenance", "compaction.Analyze + RewriteDataFiles", "rewrite_data_files"),
    ("maintenance", "RewriteManifests", "rewrite_manifests"),
    ("maintenance", "AddDataFilesByPath", "add_files"),
    ("maintenance", "ExpireSnapshots", "expire_snapshots"),
    ("maintenance", "maintenance.DeleteOrphanFiles", "remove_orphan_files"),
    ("catalog", "catalog.CreateNamespace", "namespace_create"),
    ("catalog", "catalog.DropNamespace", "namespace_drop"),
    ("catalog", "CreateNamespace on an existing namespace (IF NOT EXISTS)", "namespace_existing"),
    ("catalog", "catalog.PurgeTable", "drop_purge"),
    ("concurrency", "AppendTable; B appends between the read and the commit", "race_append"),
    ("concurrency", "Delete id 1; B appends", "race_delete"),
    ("concurrency", "update v + 1 on id 2; B appends", "race_update"),
]


def write_step_summary(probe: IcebergGoCapability) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    counts = probe.report.tally()
    lines = [
        "## OneLake Iceberg REST catalog, asked by iceberg-go's Go API",
        "",
        f"{counts[SUPPORTED]} supported · {counts[REFUSED]} refused · "
        f"{counts[NOOP]} accepted then ignored · "
        f"{counts[SKIPPED]} skipped · {counts[BROKEN]} could not be asked",
        "",
        *probe.report.markdown(),
        "",
        f"iceberg-go `{probe.version}` · namespace `{NAMESPACE}` · run `{probe.run}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    if not any_wanted(PROBES):
        print(f"this run asks none of {', '.join(sorted(asked()))} of this client")
        return 0
    cfg = Config.from_env()
    probe = IcebergGoCapability(cfg)
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
    probe.report.save("iceberg_go", probe.version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
