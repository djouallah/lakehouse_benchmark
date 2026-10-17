"""What the OneLake Iceberg REST catalog lets Trino do.

Trino's Iceberg connector is built on the Iceberg Java library, like Spark, but every statement is
Trino's own SQL and planning: the column is Trino's answer, not a copy of Spark's.

THE SERVER IS bench.trino's: the official image, one JVM, storage through Trino's native Azure
filesystem on the runner's workload identity. The catalog is created here in SQL, with two
settings the probes need and the benchmark's catalog does not carry:

  * `iceberg.rest-catalog.server-assigned-table-location-enabled` (Trino 484, trinodb/trino#30438):
    Trino sends no location and the catalog assigns it, so a CREATE TABLE or CTAS is not staged.
    Without it every create is staged, and the catalog refuses the commit that finishes it.
  * `iceberg.expire-snapshots.min-retention = 0s`, so expire_snapshots may expire anything.

No file cache, so a probe that deletes data files from storage is never answered from local disk.
Every session writes with extended statistics off: Trino's default puts Puffin statistics into
the commit, and the catalog refuses that commit with a bare 400.

EVERY PROBE CHECKS ITS EFFECT through pyiceberg, which reads what the CATALOG now says rather
than what Trino cached. Accepted with no effect is a no-op, not a yes. Every error Trino raises
is a `no` carrying its full message, so the log says whose limit it is.

PYICEBERG READS TRINO'S DELETE FILES PLAINLY here. pyiceberg asks pyarrow for a position-delete
file's `file_path` column as a dictionary, Trino writes that column DELTA_LENGTH_BYTE_ARRAY, and
pyarrow cannot dictionary-decode that encoding: "Not yet implemented: DecodeArrow of
DictAccumulator for DeltaLengthByteArrayDecoder". The file is valid Parquet, so the check reads
it without the dictionary (`_read_deletes_plain`), and Trino is judged on what it wrote.
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
    TRANSFORM_ROWS,
    NoOp,
    Refused,
    Report,
    Skip,
    any_wanted,
    asked,
    evolution_check,
    evolve_spec,
    iceberg_schema,
    partition_result,
    promotion_check,
    read_without_files,
    remove_data_files,
    rows,
    transform_case,
    truncate_kind,
    type_result,
    verdict,
    wanted,
    write_stats_files,
)
from isolation_duckdb import race_probe

from bench import auth, scrub, trino
from bench.capability.race import RaceProxy
from bench.config import ICEBERG_ENDPOINT, Config

CATALOG = "onelake"
SEED = [(1, 10), (2, 20), (3, 30)]


def _read_deletes_plain(io, data_file):
    """pyiceberg's _read_deletes for a Parquet position-delete file, without asking for
    `file_path` as a dictionary (module docstring)."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    from pyiceberg.manifest import FileFormat

    if data_file.file_format != FileFormat.PARQUET:
        return _READ_DELETES(io, data_file)
    with io.new_input(data_file.file_path).open() as handle:
        table = pq.read_table(handle)
    return {
        path: table.filter(pc.equal(table["file_path"], path)).column("pos")
        for path in pc.unique(table["file_path"]).to_pylist()
    }


def _patch_pyiceberg() -> None:
    global _READ_DELETES
    from pyiceberg.io import pyarrow as pyiceberg_arrow

    _READ_DELETES = pyiceberg_arrow._read_deletes
    pyiceberg_arrow._read_deletes = _read_deletes_plain


_READ_DELETES = None


def _one_line(text: object, limit: int = 700) -> str:
    flat = " ".join(scrub.scrub(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _say(text: object) -> None:
    print(scrub.scrub(text), flush=True)


# Column type and literal for each TYPE_EXPECTED case.
TYPES = {
    "decimal": ("DECIMAL(10, 2)", "DECIMAL '12.34'"),
    "date": ("DATE", "DATE '2026-01-02'"),
    "timestamp": ("TIMESTAMP(6)", "TIMESTAMP '2026-01-02 03:04:05.123456'"),
    "timestamptz": ("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2026-01-02 03:04:05.123456 UTC'"),
    "uuid": ("UUID", "UUID '6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab'"),
    "binary": ("VARBINARY", "X'0102'"),
    "struct": ("ROW(a BIGINT, b VARCHAR)", "CAST(ROW(1, 'x') AS ROW(a BIGINT, b VARCHAR))"),
    "list": ("ARRAY(BIGINT)", "CAST(ARRAY[1, 2, 3] AS ARRAY(BIGINT))"),
    "map": ("MAP(VARCHAR, BIGINT)", "MAP(ARRAY['k'], ARRAY[BIGINT '1'])"),
}


def _transform_sql(kind: str) -> tuple[str, str, str]:
    """(columns, partition transform, INSERT's SELECT) for one transform_case."""
    if kind == "bucket":
        columns, part = "id BIGINT, x BIGINT", "bucket(id, 4)"
        values = [f"({i}, {v})" for i, v in TRANSFORM_ROWS["bucket"]]
        select = f"SELECT CAST(id AS BIGINT), CAST(x AS BIGINT) FROM (VALUES {', '.join(values)})"
    elif kind == "truncate":
        columns, part = "id BIGINT, x VARCHAR", "truncate(x, 2)"
        values = [f"({i}, '{v}')" for i, v in TRANSFORM_ROWS["truncate"]]
        select = f"SELECT CAST(id AS BIGINT), x FROM (VALUES {', '.join(values)})"
    else:
        columns, part = "id BIGINT, x TIMESTAMP(6)", f"{kind}(x)"
        values = [f"({i}, TIMESTAMP '{v}')" for i, v in TRANSFORM_ROWS["temporal"]]
        select = f"SELECT CAST(id AS BIGINT), x FROM (VALUES {', '.join(values)})"
    return columns, part, select + " AS s(id, x)"


def _select(pairs) -> str:
    """A SELECT yielding BIGINT (id, v) rows, so no insert depends on implicit casts."""
    values = ", ".join(f"({i}, {v})" for i, v in pairs)
    cols = "CAST(id AS BIGINT) AS id, CAST(v AS BIGINT) AS v"
    return f"SELECT {cols} FROM (VALUES {values}) AS s(id, v)"


class TrinoCapability:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.conn = None
        self.report = Report(reason=lambda exc: _one_line(scrub.scrub_exc(exc, 2000), 600))
        self.run = "{}_{}".format(
            os.environ.get("GITHUB_RUN_ID", "local"),
            os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
        )
        self.keep = os.environ.get("CAPABILITY_KEEP") == "1"
        self.ns = NAMESPACE
        self.version = "no session"
        self.can_create = False
        self.created: list[str] = []
        self.namespaces: list[str] = []
        self._catalog = None
        self.proxy = None

    # -- helpers -----------------------------------------------------------------------------

    def sql(self, statement: str) -> list[tuple]:
        """One statement. Trino's own `no` becomes `Refused`, carrying its cause chain: the
        message alone is often only the outermost wrapper."""
        from trino.exceptions import TrinoQueryError

        _say(f"       > {_one_line(statement, 300)}")
        cur = self.conn.cursor()
        try:
            cur.execute(statement)
            return [tuple(row) for row in cur.fetchall()]
        except TrinoQueryError as exc:
            chain, info = [], exc.failure_info or {}
            while info:
                chain.append(f"{info.get('type')}: {info.get('message')}")
                info = info.get("cause") or {}
            raise Refused(_one_line(" <- ".join(chain) or str(exc))) from None
        except Exception as exc:  # noqa: BLE001 - every Trino error is an answer
            raise Refused(_one_line(f"{type(exc).__name__}: {exc}")) from None

    def name(self, what: str) -> str:
        return f"tr_{self.run}_{what}"

    def t(self, table: str, ns: str | None = None, catalog: str = CATALOG) -> str:
        return f'{catalog}."{ns or self.ns}"."{table}"'

    def pyiceberg(self):
        if self._catalog is None:
            self._catalog = auth.catalog(self.cfg)
        return self._catalog

    def iceberg(self, table: str):
        return self.pyiceberg().load_table((self.ns, table))

    def _snapshots(self, table: str) -> list:
        return sorted(
            self.iceberg(table).metadata.snapshots,
            key=lambda s: (s.sequence_number or 0, s.timestamp_ms),
        )

    def _py_rows(self, table: str, cols=("id", "v")):
        try:
            arrow = self.iceberg(table).scan(selected_fields=cols).to_arrow()
            return sorted(tuple(row[c] for c in cols) for row in arrow.to_pylist())
        except Exception as exc:  # noqa: BLE001 - a cross-check must not decide the answer
            return f"pyiceberg could not read it: {_one_line(scrub.scrub_exc(exc, 300), 300)}"

    def _trino_rows(self, table: str, cols: str = "id, v"):
        try:
            cur = self.conn.cursor()
            cur.execute(f"SELECT {cols} FROM {self.t(table)} ORDER BY id")
            return [tuple(r) for r in cur.fetchall()]
        except Exception as exc:  # noqa: BLE001 - a read-back must not decide the answer
            return f"Trino could not read it back: {_one_line(exc, 300)}"

    def _expect(self, table: str, expected: list[tuple], what: str, cols=("id", "v")) -> str:
        """pyiceberg decides, because it reads the catalog; Trino's own read is reported beside."""
        py = self._py_rows(table, cols)
        own = self._trino_rows(table, ", ".join(cols))
        if py != expected:
            raise NoOp(
                f"{what} returned success; expected {expected}, pyiceberg reads {py}, "
                f"Trino reads {own}"
            )
        agree = "Trino agrees" if own == py else f"Trino reads {own}"
        return f"{what}: pyiceberg reads {py}; {agree}"

    def _delete_files(self, table: str) -> str:
        """What kind of delete files the current snapshot carries, read from its manifests."""
        try:
            tbl = self.iceberg(table)
            kinds: dict[str, int] = {}
            for manifest in tbl.current_snapshot().manifests(tbl.io):
                for entry in manifest.fetch_manifest_entry(tbl.io, discard_deleted=True):
                    kind = entry.data_file.content.name.lower()
                    if kind != "data":
                        kinds[kind] = kinds.get(kind, 0) + 1
            return ", ".join(f"{n} {k}" for k, n in sorted(kinds.items())) or "no delete files"
        except Exception as exc:  # noqa: BLE001 - informational only
            return f"delete files unreadable: {_one_line(exc, 200)}"

    def _create(self, what: str, columns: str = "id BIGINT, v BIGINT", tail: str = "") -> str:
        table = self.name(what)
        self.sql(f"CREATE TABLE {self.t(table)} ({columns}) {tail}")
        self.created.append(table)
        return table

    def _fresh(self, what: str, tail: str = "") -> str:
        """A table holding SEED, made the way the create probe proved works."""
        if not self.can_create:
            raise Skip("no table could be created, so there is nothing to write to")
        try:
            table = self._create(what, tail=tail)
            self.sql(f"INSERT INTO {self.t(table)} {_select(SEED)}")
        except Refused as exc:
            raise Skip(f"could not seed the table: {exc}") from None
        return table

    def _partitioned(self, what: str) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create(
            what, "id BIGINT, v BIGINT, p VARCHAR", "WITH (partitioning = ARRAY['p'])"
        )
        self.sql(
            f"INSERT INTO {self.t(table)} SELECT CAST(id AS BIGINT), CAST(v AS BIGINT), p "
            f"FROM (VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b')) AS s(id, v, p)"
        )
        return table

    # -- session -----------------------------------------------------------------------------

    def _catalog_ddl(self, name: str, uri: str, token: str) -> None:
        trino.create_catalog(
            self.conn,
            name,
            "iceberg",
            {
                "iceberg.catalog.type": "rest",
                "iceberg.rest-catalog.uri": uri,
                "iceberg.rest-catalog.warehouse": self.cfg.warehouse,
                "iceberg.rest-catalog.security": "OAUTH2",
                "iceberg.rest-catalog.oauth2.token": token,
                "iceberg.rest-catalog.vended-credentials-enabled": "false",
                "iceberg.rest-catalog.case-insensitive-name-matching": "true",
                "iceberg.rest-catalog.server-assigned-table-location-enabled": "true",
                "iceberg.expire-snapshots.min-retention": "0s",
                # extra_properties takes only the keys listed here.
                "iceberg.allowed-extra-properties": "*",
                **trino.storage_properties(),
            },
        )
        self.sql(f"SET SESSION {name}.collect_extended_statistics_on_write = false")

    def session(self) -> str:
        # A second catalog, `race`, on the race proxy, for the concurrency probes. The container
        # reaches the runner's proxy through host.docker.internal (bench/trino.py).
        self.proxy = RaceProxy(bind="0.0.0.0").start()
        trino.start()
        self.conn = trino.connect()
        token = auth.onelake_token()
        self._catalog_ddl(CATALOG, ICEBERG_ENDPOINT, token)
        self._catalog_ddl(
            "race", self.proxy.endpoint.replace("127.0.0.1", "host.docker.internal"), token
        )
        self.version = f"{trino.version(self.conn)} ({trino.IMAGE})"
        listed = (self.ns,) in self.pyiceberg().list_namespaces()
        seen = self.sql(f"SHOW SCHEMAS FROM {CATALOG} LIKE '{self.ns}'")
        return (
            f"Trino {self.version}; {NAMESPACE} {'listed' if listed else 'NOT listed'}, "
            f"{'seen' if seen else 'NOT seen'} by Trino"
        )

    # -- create ------------------------------------------------------------------------------

    def create_table(self) -> str:
        table = self._create("base")
        self.can_create = True
        tbl = self.iceberg(table)
        ids = [f.field_id for f in tbl.schema().fields]
        location = tbl.metadata.location
        return (
            f"field ids {ids}, format-version {tbl.metadata.format_version}, "
            f"at the catalog's location `…{location[location.find('/Tables/') :]}`"
        )

    def create_if_not_exists(self) -> str:
        table = self._fresh("ine")
        self.sql(f"CREATE TABLE IF NOT EXISTS {self.t(table)} (id BIGINT, v BIGINT, extra BIGINT)")
        names = [f.name for f in self.iceberg(table).schema().fields]
        rows_ = self._py_rows(table)
        if "extra" in names or rows_ != SEED:
            raise NoOp(f"IF NOT EXISTS replaced the existing table: columns {names}, rows {rows_}")
        return f"accepted, the existing table untouched: columns {names}, rows {rows_}"

    def ctas(self) -> str:
        table = self.name("ctas")
        self.created.append(table)
        self.sql(f"CREATE TABLE {self.t(table)} AS {_select(SEED)}")
        return self._expect(table, SEED, "CTAS")

    def partitioned(self) -> str:
        table = self._partitioned("part")
        spec = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("partitioning returned success and the spec is empty")
        files = len(list(self.iceberg(table).scan().plan_files()))
        return f"spec {spec}; 3 rows over 2 partitions wrote {files} data file(s)"

    def _transform(self, kind: str) -> tuple[str, str, str]:
        columns, part, select = _transform_sql(kind)
        _, _, parts = transform_case(kind)
        try:
            table = self._create(f"tr_{kind}", columns, f"WITH (partitioning = ARRAY['{part}'])")
            self.sql(f"INSERT INTO {self.t(table)} {select}")
        except Refused as exc:
            return kind, REFUSED, str(exc)
        return partition_result(self.iceberg(table), kind, parts)

    def _transforms(self, kinds) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        return verdict([self._transform(kind) for kind in kinds])

    def partition_bucket(self) -> str:
        return self._transforms(["bucket"])

    def partition_truncate(self) -> str:
        return self._transforms(["truncate"])

    def partition_temporal(self) -> str:
        return self._transforms(TEMPORAL)

    def _typed(self, name: str) -> tuple[str, str, str]:
        column, literal = TYPES[name]
        try:
            table = self._create(f"type_{name}", f"id BIGINT, x {column}")
            self.sql(f"INSERT INTO {self.t(table)} SELECT BIGINT '1', {literal}")
        except Refused as exc:
            return name, REFUSED, str(exc)
        return type_result(self.iceberg(table), name)

    def _types(self, names) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        return verdict([self._typed(name) for name in names])

    def types_scalar(self) -> str:
        return self._types(SCALAR_TYPES)

    def types_nested(self) -> str:
        return self._types(NESTED_TYPES)

    def sort_order(self) -> str:
        table = self._fresh("sorted")
        self.sql(f"ALTER TABLE {self.t(table)} SET PROPERTIES sorted_by = ARRAY['id']")
        if not self.iceberg(table).sort_order().fields:
            raise NoOp("SET PROPERTIES sorted_by returned success and the table has no sort order")
        return "sort order set"

    # -- write -------------------------------------------------------------------------------

    def insert_into(self) -> str:
        table = self._fresh("insert")
        return self._expect(table, SEED, "INSERT INTO") + (
            f"; {len(self._snapshots(table))} commit(s)"
        )

    def insert_select(self) -> str:
        table = self._fresh("insertsel")
        self.sql(f"INSERT INTO {self.t(table)} SELECT id + 10, v FROM {self.t(table)}")
        return self._expect(table, SEED + [(11, 10), (12, 20), (13, 30)], "INSERT INTO ... SELECT")

    def delete_from(self) -> str:
        table = self._fresh("delete")
        self.sql(f"DELETE FROM {self.t(table)} WHERE id = 1")
        return f"{self._expect(table, SEED[1:], 'DELETE')}; {self._delete_files(table)}"

    def update(self) -> str:
        table = self._fresh("update")
        self.sql(f"UPDATE {self.t(table)} SET v = 999 WHERE id = 1")
        return self._expect(table, [(1, 999), (2, 20), (3, 30)], "UPDATE")

    def merge_upsert(self) -> str:
        table = self._fresh("merge")
        self.sql(
            f"MERGE INTO {self.t(table)} t USING ({_select([(1, 777), (9, 90)])}) s "
            f"ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v "
            f"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
        return (
            self._expect(table, [(1, 777), (2, 20), (3, 30), (9, 90)], "MERGE, UPDATE + INSERT")
            + f"; {len(self._snapshots(table)) - 1} commit(s) after the seed; "
            + self._delete_files(table)
        )

    def truncate(self) -> str:
        table = self._fresh("truncate")
        self.sql(f"TRUNCATE TABLE {self.t(table)}")
        result = self._expect(table, [], "TRUNCATE")
        return f"{result}; {truncate_kind(self.iceberg(table))}"

    def insert_overwrite(self) -> str:
        """Trino has no INSERT OVERWRITE: its whole-table overwrite is CREATE OR REPLACE TABLE ...
        AS, one commit on the same table, history kept (as Polars is asked its sink mode).
        Sent through the race catalog, unarmed, so the proxy records the commit's requirements
        and updates: a refusal then says what was asserted against what the table holds."""
        table = self._fresh("overwrite")
        tbl = self.iceberg(table)
        meta = tbl.metadata
        held = (
            f"table holds uuid {meta.table_uuid}, current-schema-id {meta.current_schema_id}, "
            f"last-column-id {meta.last_column_id}, default-spec-id {meta.default_spec_id}, "
            f"last-partition-id {meta.last_partition_id}, "
            f"default-sort-order-id {meta.default_sort_order_id}, "
            f"main {meta.current_snapshot_id}"
        )
        before = len(self._snapshots(table))
        key = (self.ns, table)
        try:
            self.sql(
                f"CREATE OR REPLACE TABLE {self.t(table, catalog='race')} AS "
                f"{_select([(7, 70), (8, 80)])}"
            )
        except Refused as exc:
            sent = self.proxy.requirements.get(key, [])
            updates = self.proxy.updates.get(key, [])
            statuses = self.proxy.statuses(*key)
            raise Refused(
                f"{exc}; commits {statuses}; requirements sent {sent}; updates {updates}; {held}"
            ) from None
        tbl = self.iceberg(table)
        snapshots = self._snapshots(table)
        summary = snapshots[-1].summary
        operation = summary.operation.value if summary is not None else "?"
        same = "same table" if tbl.metadata.table_uuid == meta.table_uuid else "a NEW table"
        result = self._expect(table, [(7, 70), (8, 80)], "CREATE OR REPLACE TABLE ... AS")
        return (
            f"{result}; {same}, {len(snapshots) - before} snapshot(s) added, "
            f"{len(snapshots)} kept, operation {operation}"
        )

    # -- schema ------------------------------------------------------------------------------

    def add_column(self) -> str:
        table = self._fresh("addcol")
        self.sql(f"ALTER TABLE {self.t(table)} ADD COLUMN w BIGINT")
        names = [f.name for f in self.iceberg(table).schema().fields]
        if "w" not in names:
            raise NoOp(f"returned success and the schema is {names}")
        return f"schema {names}"

    def drop_column(self) -> str:
        table = self._fresh("dropcol")
        self.sql(f"ALTER TABLE {self.t(table)} DROP COLUMN v")
        names = [f.name for f in self.iceberg(table).schema().fields]
        if "v" in names:
            raise NoOp(f"returned success and the schema is still {names}")
        return f"schema now {names}"

    def rename_column(self) -> str:
        table = self._fresh("renamecol")
        self.sql(f"ALTER TABLE {self.t(table)} RENAME COLUMN v TO v2")
        names = [f.name for f in self.iceberg(table).schema().fields]
        if "v2" not in names:
            raise NoOp(f"returned success and the schema is still {names}")
        return self._expect(table, SEED, "renamed, data kept", ("id", "v2"))

    def type_promotion(self) -> str:
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create("promote", "id BIGINT, c INTEGER")
        self.sql(f"INSERT INTO {self.t(table)} VALUES (BIGINT '1', 7)")
        self.sql(f"ALTER TABLE {self.t(table)} ALTER COLUMN c SET DATA TYPE BIGINT")
        return promotion_check(self.iceberg(table))

    def partition_evolution(self) -> str:
        table = self._fresh("specevo")
        self.sql(
            f"ALTER TABLE {self.t(table)} SET PROPERTIES partitioning = ARRAY['bucket(id, 4)']"
        )
        spec = [str(f.transform) for f in self.iceberg(table).spec().fields]
        if not spec:
            raise NoOp("returned success and the table is still unpartitioned")
        return f"spec now {spec}"

    def write_after_evolution(self) -> str:
        """pyiceberg adds bucket(4, id), as for every engine; Trino writes before and after."""
        if not self.can_create:
            raise Skip("no table could be created")
        table = self._create("evowrite")
        first = ", ".join(f"(BIGINT '{i}', BIGINT '{v}')" for i, v in EVOLVE_FIRST)
        then = ", ".join(f"(BIGINT '{i}', BIGINT '{v}')" for i, v in EVOLVE_THEN)
        self.sql(f"INSERT INTO {self.t(table)} VALUES {first}")
        evolve_spec(self.iceberg(table))
        self.sql(f"INSERT INTO {self.t(table)} VALUES {then}")
        count = self.sql(f"SELECT count(*) FROM {self.t(table)}")[0][0]
        return evolution_check(self.iceberg(table), count)

    def set_property(self) -> str:
        table = self._fresh("props")
        self.sql(
            f"ALTER TABLE {self.t(table)} SET PROPERTIES "
            f"extra_properties = MAP(ARRAY['probed-at'], ARRAY['{self.run}'])"
        )
        if self.iceberg(table).properties.get("probed-at") != self.run:
            raise NoOp("returned success and the property is not on the table")
        return "the property is on the table"

    # -- read --------------------------------------------------------------------------------

    def time_travel(self) -> str:
        table = self._fresh("travel")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        # The seed's snapshot, the one before the last INSERT: Trino's CREATE TABLE commits an
        # empty snapshot of its own first.
        seeded = self._snapshots(table)[-2].snapshot_id
        found = self.sql(f"SELECT count(*) FROM {self.t(table)} FOR VERSION AS OF {seeded}")
        if found[0][0] != 3:
            raise NoOp(f"FOR VERSION AS OF the seed's snapshot reads {found[0][0]} rows, not 3")
        return "FOR VERSION AS OF the seed's snapshot reads 3 rows (the table has 4)"

    def metadata_tables(self) -> str:
        table = self._fresh("inspect")
        found = self.sql(f"SELECT count(*) FROM {self.t(table + '$snapshots')}")
        return f'"t$snapshots" reads {found[0][0]} snapshot(s)'

    def _stats_table(self, what: str) -> str:
        """harness.STATS_FILES, written by pyiceberg: four files with known min/max bounds."""
        table = self.name(what)
        self.pyiceberg().create_table((self.ns, table), iceberg_schema())
        self.created.append(table)
        write_stats_files(self.iceberg(table))
        return table

    def file_pruning(self) -> str:
        table = self._stats_table("prune")
        gone = remove_data_files(self.iceberg(table), keep_id=PRUNE_ID)
        return read_without_files(
            f"SELECT v WHERE id = {PRUNE_ID}",
            lambda: self.sql(f"SELECT v FROM {self.t(table)} WHERE id = {PRUNE_ID}"),
            PRUNE_EXPECTED,
            lambda: self.sql(f"SELECT sum(v) FROM {self.t(table)}"),
            gone,
        )

    def max_from_metadata(self) -> str:
        table = self._stats_table("max")
        gone = remove_data_files(self.iceberg(table))
        return read_without_files(
            "SELECT max(id)",
            lambda: self.sql(f"SELECT max(id) FROM {self.t(table)}"),
            MAX_EXPECTED,
            lambda: self.sql(f"SELECT sum(v) FROM {self.t(table)}"),
            gone,
        )

    # -- refs --------------------------------------------------------------------------------

    def create_branch(self) -> str:
        table = self._fresh("branch")
        self.sql(f"CREATE BRANCH probe_branch IN TABLE {self.t(table)}")
        refs = sorted(self.iceberg(table).metadata.refs)
        if "probe_branch" not in refs:
            raise NoOp(f"returned success and the refs are {refs}")
        return f"refs now {refs}"

    def create_tag(self) -> str:
        table = self._fresh("tag")
        # Trino has CREATE BRANCH and no tag statement: this is its branch syntax for a tag.
        self.sql(f"CREATE TAG probe_tag IN TABLE {self.t(table)}")
        refs = sorted(self.iceberg(table).metadata.refs)
        if "probe_tag" not in refs:
            raise NoOp(f"returned success and the refs are {refs}")
        return f"refs now {refs}"

    # -- maintenance -------------------------------------------------------------------------

    def expire_snapshots(self) -> str:
        table = self._fresh("expire")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(5, 50)])}")
        before = len(self._snapshots(table))
        self.sql(
            f"ALTER TABLE {self.t(table)} EXECUTE expire_snapshots(retention_threshold => '0s')"
        )
        after = len(self._snapshots(table))
        if after >= before:
            raise NoOp(f"returned success and history went {before} -> {after}")
        return f"history {before} -> {after} snapshot(s)"

    def rewrite_data_files(self) -> str:
        table = self._fresh("compact")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        before = len(list(self.iceberg(table).scan().plan_files()))
        self.sql(f"ALTER TABLE {self.t(table)} EXECUTE optimize")
        after = len(list(self.iceberg(table).scan().plan_files()))
        if after >= before:
            raise NoOp(f"returned success and data files went {before} -> {after}")
        return self._expect(table, SEED + [(4, 40)], f"{before} -> {after} data file(s), data kept")

    def rewrite_manifests(self) -> str:
        table = self._fresh("manifests")
        self.sql(f"INSERT INTO {self.t(table)} {_select([(4, 40)])}")
        before = len(self._snapshots(table))
        self.sql(f"ALTER TABLE {self.t(table)} EXECUTE optimize_manifests")
        if len(self._snapshots(table)) == before:
            raise NoOp("returned success and no new snapshot was committed")
        return "a new snapshot rewrote the manifests"

    # -- catalog -----------------------------------------------------------------------------

    def namespace(self) -> str:
        ns = self.name("ns")
        self.sql(f'CREATE SCHEMA {CATALOG}."{ns}"')
        self.namespaces.append(ns)
        if (ns,) not in self.pyiceberg().list_namespaces():
            raise NoOp("CREATE SCHEMA returned success and pyiceberg does not list it")
        self.sql(f'DROP SCHEMA {CATALOG}."{ns}"')
        if (ns,) in self.pyiceberg().list_namespaces():
            raise NoOp("DROP SCHEMA returned success and pyiceberg still lists it")
        self.namespaces.remove(ns)
        return f"`{ns}` created, listed, dropped"

    def namespace_existing(self) -> str:
        self.sql(f'CREATE SCHEMA IF NOT EXISTS {CATALOG}."{self.ns}"')
        return f"accepted on the existing `{self.ns}`"

    def drop_purge(self) -> str:
        """Trino's DROP TABLE has no PURGE clause: it removes the table's files itself."""
        table = self._fresh("droppable")
        tbl = self.iceberg(table)
        data = [task.file.file_path for task in tbl.scan().plan_files()]
        self.sql(f"DROP TABLE {self.t(table)}")
        if self.pyiceberg().table_exists((self.ns, table)):
            raise NoOp("returned success and the catalog still has it")
        self.created.remove(table)
        try:
            left = sum(1 for path in data if tbl.io.new_input(path).exists())
        except Exception as exc:  # noqa: BLE001 - informational
            return f"gone from the catalog; could not check storage: {_one_line(exc, 200)}"
        if left:
            raise NoOp(f"gone from the catalog, and {left} of {len(data)} data file(s) remain")
        return f"gone from the catalog; 0 of {len(data)} data file(s) still in storage"

    # -- concurrency -------------------------------------------------------------------------

    def _race(self, key: str, statement: str) -> str:
        """pyiceberg makes and seeds the table; Trino writes through the `race` catalog, whose
        commit bench.capability.race holds until pyiceberg's has landed."""
        if self.conn is None:
            raise Skip("no Trino session")
        table = self.name(key)
        self.pyiceberg().create_table((self.ns, table), schema=iceberg_schema())
        self.created.append(table)
        self.iceberg(table).append(rows(SEED))
        raced = self.t(table, catalog="race")
        return race_probe(
            key, self.pyiceberg(), self.proxy, table, lambda: self.sql(statement.format(t=raced))
        )

    def race_append(self) -> str:
        return self._race("race_append", "INSERT INTO {t} " + _select([(5, 50)]))

    def race_delete(self) -> str:
        return self._race("race_delete", "DELETE FROM {t} WHERE id = 1")

    def race_update(self) -> str:
        return self._race("race_update", "UPDATE {t} SET v = v + 1 WHERE id = 2")

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
                if self.pyiceberg().table_exists((self.ns, table)):
                    self.pyiceberg().purge_table((self.ns, table))
                dropped += 1
            except Exception as exc:  # noqa: BLE001 - teardown is best effort, by design
                left.append(f"{table} ({_one_line(exc, 80)})")
        for ns in self.namespaces:
            try:
                self.pyiceberg().drop_namespace(ns)
            except Exception as exc:  # noqa: BLE001
                left.append(f"namespace {ns} ({_one_line(exc, 80)})")
        print(f"\ndropped {dropped} probe table(s)" + (f"; left behind {left}" if left else ""))


PROBES = [
    ("session", "Trino starts and attaches the REST catalog", "session"),
    ("create", "CREATE TABLE (the catalog assigns the location)", "create_table"),
    ("create", "CREATE TABLE IF NOT EXISTS on an existing table", "create_if_not_exists"),
    ("create", "CREATE TABLE ... AS SELECT", "ctas"),
    ("create", "WITH (partitioning = ARRAY['p']), then INSERT", "partitioned"),
    ("create", "SET PROPERTIES sorted_by (sort order)", "sort_order"),
    ("create", "partitioning bucket(id, 4), then INSERT", "partition_bucket"),
    ("create", "partitioning truncate(x, 2), then INSERT", "partition_truncate"),
    ("create", "partitioning year / month / day / hour, then INSERT", "partition_temporal"),
    ("create", "types: decimal, date, timestamp, timestamptz, uuid, binary", "types_scalar"),
    ("create", "nested types: struct, list, map", "types_nested"),
    ("write", "INSERT INTO", "insert_into"),
    ("write", "INSERT INTO ... SELECT", "insert_select"),
    ("write", "DELETE FROM", "delete_from"),
    ("write", "UPDATE", "update"),
    ("write", "MERGE, two actions: MATCHED UPDATE + NOT MATCHED INSERT", "merge_upsert"),
    ("write", "TRUNCATE TABLE", "truncate"),
    ("write", "CREATE OR REPLACE TABLE ... AS (whole-table overwrite)", "insert_overwrite"),
    ("schema", "ALTER TABLE ADD COLUMN", "add_column"),
    ("schema", "ALTER TABLE DROP COLUMN", "drop_column"),
    ("schema", "ALTER TABLE RENAME COLUMN", "rename_column"),
    ("schema", "ALTER COLUMN c SET DATA TYPE BIGINT (int -> long)", "type_promotion"),
    ("schema", "SET PROPERTIES partitioning (partition evolution)", "partition_evolution"),
    ("schema", "INSERT, pyiceberg adds bucket(4, id), INSERT", "write_after_evolution"),
    ("schema", "SET PROPERTIES extra_properties", "set_property"),
    ("read", "time travel, FOR VERSION AS OF", "time_travel"),
    ("read", 'metadata tables ("t$snapshots")', "metadata_tables"),
    ("read", "WHERE id = 22 with the other data files deleted (file pruning)", "file_pruning"),
    ("read", "max(id) with every data file deleted (min/max from metadata)", "max_from_metadata"),
    ("refs", "CREATE BRANCH ... IN TABLE", "create_branch"),
    ("refs", "CREATE TAG ... IN TABLE", "create_tag"),
    ("maintenance", "EXECUTE expire_snapshots", "expire_snapshots"),
    ("maintenance", "EXECUTE optimize (compaction)", "rewrite_data_files"),
    ("maintenance", "EXECUTE optimize_manifests", "rewrite_manifests"),
    ("catalog", "CREATE SCHEMA, then DROP SCHEMA", "namespace"),
    ("catalog", "CREATE SCHEMA IF NOT EXISTS, existing namespace", "namespace_existing"),
    ("catalog", "DROP TABLE, its files removed", "drop_purge"),
    ("concurrency", "INSERT INTO; B appends between Trino's read and commit", "race_append"),
    ("concurrency", "DELETE id 1; B appends", "race_delete"),
    ("concurrency", "UPDATE v = v + 1 on id 2; B appends", "race_update"),
]


def write_step_summary(probe: TrinoCapability) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    counts = probe.report.tally()
    lines = [
        "## OneLake Iceberg REST catalog, asked by Trino",
        "",
        f"{counts[SUPPORTED]} supported · {counts[REFUSED]} refused · "
        f"{counts[NOOP]} accepted then ignored · "
        f"{counts[SKIPPED]} skipped · {counts[BROKEN]} could not be asked",
        "",
        *probe.report.markdown(),
        "",
        f"Trino `{probe.version}` · namespace `{probe.ns}` · run `{probe.run}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    if not any_wanted(PROBES):
        print(f"this run asks none of {', '.join(sorted(asked()))} of this client")
        return 0
    _patch_pyiceberg()
    cfg = Config.from_env()
    probe = TrinoCapability(cfg)
    print(f"workspace {cfg.workspace_id}  lakehouse {cfg.lakehouse_id}  namespace {NAMESPACE}")

    try:
        for group, question, method in PROBES:
            if not wanted(method):
                continue
            ok = probe.report.run(group, question, getattr(probe, method))
            if not ok and method == "session":
                print("\nstopped: no Trino session, so nothing below could be asked")
                write_step_summary(probe)
                return 1
    finally:
        try:
            probe.drop_everything()
        except Exception as exc:  # noqa: BLE001 - teardown must not mask the findings
            print(f"  warning: teardown failed: {_one_line(exc, 200)}")
        if probe.conn is not None:
            probe.conn.close()
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
    probe.report.save("trino", probe.version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
