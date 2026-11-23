"""Where the Delta support probes write, and the oracle that checks what each statement did.

Delta has no REST catalog here: a Delta table on OneLake is a directory with a `_delta_log`. Every
probe table is one under the lakehouse's Tables/_bench_delta/, which Fabric shows as schema
`_bench_delta`, apart from the Iceberg grid's _bench_capability.

TWO READERS, NEITHER OF THEM THE ENGINE'S SESSION:

* DuckDB (the `duckdb` wheel, the same 2.0.1 as the pinned CLI, and its delta extension on Delta
  Kernel) reads the rows, the way pyiceberg does for the Iceberg grid: a fresh `delta_scan` of what
  the LOG now says, not what the engine cached. Not delta-rs: delta-rs does not read column
  mapping, type widening or deletion vectors, so it called a working rename a no-op.
* The log itself, read as JSON straight off storage (`commit`), for the protocol's table
  features, deletion vectors, domain metadata, the operation each commit recorded, and whether a
  checkpoint was written.

delta-rs only WRITES: the seed tables the engines are asked to read (and, in the delta-rs
column, everything that column is asked). Storage goes through
azure-storage-file-datalake on the job's OIDC credential (bench.auth); DuckDB and delta-rs take
the same bearer.
"""

from __future__ import annotations

import json
import os
import re

from bench import auth, scrub
from bench.config import ONELAKE_DFS, Config

NAMESPACE = "_bench_delta"
# Every commit the oracle's own delta-rs writes (the seeds, the race's second writer) carries this
# in commitInfo, so DeltaOracle.commit_authors tells it from the engine under test, whatever that
# engine writes with -- delta-rs included (the delta-rs column, duckrun).
ORACLE_TAG = {"bench.writer": "oracle"}


def _oracle_commit():
    from deltalake import CommitProperties

    return CommitProperties(custom_metadata=ORACLE_TAG)


class DeltaOracle:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._fs = None
        self._duck = None

    # -- where -------------------------------------------------------------------------------

    def relative(self, name: str) -> str:
        """The table's directory inside the workspace's file system."""
        return f"{self.cfg.lakehouse_id}/Tables/{NAMESPACE}/{name}"

    def uri(self, name: str) -> str:
        """abfss:// location of one probe table: what every engine is pointed at."""
        return f"{self.cfg.base_path}/Tables/{NAMESPACE}/{name}"

    @property
    def folder_uri(self) -> str:
        return f"{self.cfg.base_path}/Tables/{NAMESPACE}"

    def fs(self):
        if self._fs is None:
            from azure.storage.filedatalake import DataLakeServiceClient

            service = DataLakeServiceClient(f"https://{ONELAKE_DFS}", credential=auth.credential())
            self._fs = service.get_file_system_client(self.cfg.workspace_id)
        return self._fs

    def storage_options(self) -> dict[str, str]:
        return {"bearer_token": auth.onelake_token(), "use_fabric_endpoint": "true"}

    # -- DuckDB reads ------------------------------------------------------------------------

    def _scan(
        self, name: str, cols: str, version: int | None = None, uri: str | None = None
    ) -> list[tuple]:
        """`SELECT cols FROM delta_scan(...)`, on a secret holding the current bearer."""
        import duckdb

        from bench.config import azure_transport

        if self._duck is None:
            self._duck = duckdb.connect()
            transport = azure_transport()
            if transport:
                self._duck.execute(f"SET azure_transport_option_type = '{transport}'")
            self._duck.execute("SET TimeZone = 'UTC'")
        self._duck.execute(
            "CREATE OR REPLACE SECRET onelake (TYPE azure, PROVIDER access_token, "
            f"ACCESS_TOKEN '{auth.onelake_token()}')"
        )
        at = f", version => {version}" if version is not None else ""
        return self._duck.execute(
            f"SELECT {cols} FROM delta_scan('{uri or self.uri(name)}'{at})"
        ).fetchall()

    def rows(self, name: str, cols=("id", "v")):
        """The table's rows as sorted tuples, through DuckDB; a string when it cannot read."""
        try:
            return sorted(tuple(r) for r in self._scan(name, ", ".join(cols)))
        except Exception as exc:  # noqa: BLE001 - the read-back failing is part of the answer
            return f"DuckDB could not read it: {scrub.scrub_exc(exc, 300)}"

    def rows_at(self, name: str, version: int, cols=("id", "v")):
        """The rows of one earlier version, through DuckDB; a string when it cannot read."""
        try:
            return sorted(tuple(r) for r in self._scan(name, ", ".join(cols), version))
        except Exception as exc:  # noqa: BLE001 - the read-back failing is part of the answer
            return f"DuckDB could not read version {version}: {scrub.scrub_exc(exc, 300)}"

    def rows_uri(self, uri: str, cols: tuple[str, ...]):
        """The rows of the Delta table at `uri`, through DuckDB; a string when it cannot read."""
        try:
            return sorted(tuple(r) for r in self._scan("", ", ".join(cols), uri=uri))
        except Exception as exc:  # noqa: BLE001 - the read-back failing is part of the answer
            return f"DuckDB could not read it: {scrub.scrub_exc(exc, 300)}"

    def add_constraint(self, name: str, constraint: str, expression: str) -> None:
        """A CHECK constraint, added by delta-rs: the checkConstraints writer feature."""
        self.table_rs(name).alter.add_constraint({constraint: expression})

    def table_rs(self, name: str):
        from deltalake import DeltaTable

        return DeltaTable(self.uri(name), storage_options=self.storage_options())

    def column(self, name: str, col: str = "x") -> list:
        return [r[0] for r in self._scan(name, col)]

    def schema(self, name: str) -> dict[str, str]:
        """{column: Delta type} of the current version, from the log's metaData."""
        meta = self.metadata(name)
        fields = json.loads(meta["schemaString"])["fields"]
        return {f["name"]: _type(f["type"]) for f in fields}

    def write(self, name: str, pairs, mode: str = "append") -> None:
        """Rows (id, v) written by delta-rs: the seed and the stats files every engine reads."""
        import pyarrow as pa
        from deltalake import write_deltalake

        data = pa.table(
            {
                "id": pa.array([i for i, _ in pairs], pa.int64()),
                "v": pa.array([v for _, v in pairs], pa.int64()),
            }
        )
        write_deltalake(
            self.uri(name),
            data,
            mode=mode,
            storage_options=self.storage_options(),
            commit_properties=_oracle_commit(),
        )

    def write_dated(self, name: str, rows) -> None:
        """Rows (id, v, d) written by delta-rs, partitioned by the DATE column d: one data file
        per partition, the table the partition probes read and merge into."""
        import datetime as dt

        import pyarrow as pa
        from deltalake import write_deltalake

        data = pa.table(
            {
                "id": pa.array([r[0] for r in rows], pa.int64()),
                "v": pa.array([r[1] for r in rows], pa.int64()),
                "d": pa.array([dt.date.fromisoformat(r[2]) for r in rows], pa.date32()),
            }
        )
        write_deltalake(
            self.uri(name),
            data,
            partition_by=["d"],
            storage_options=self.storage_options(),
            commit_properties=_oracle_commit(),
        )

    def partition_of(self, name: str) -> dict[str, str]:
        """{data file path: its partition value of d}, for every file any add action named."""
        found = {}
        for add in self.all_actions(name, "add"):
            found[add["path"]] = str((add.get("partitionValues") or {}).get("d"))
        return found

    # -- the log -----------------------------------------------------------------------------

    def _read(self, path: str) -> bytes:
        return self.fs().get_file_client(path).download_file().readall()

    def exists(self, path: str) -> bool:
        return self.fs().get_file_client(path).exists()

    def log_files(self, name: str) -> list[str]:
        """The names in _delta_log, sorted."""
        folder = f"{self.relative(name)}/_delta_log"
        try:
            # Not recursive: writers keep their own files in subdirectories of the log (Trino's
            # _trino_meta/extended_stats.json), which are not commits.
            paths = self.fs().get_paths(folder, recursive=False)
            return sorted(p.name.rsplit("/", 1)[-1] for p in paths if not p.is_directory)
        except Exception:  # noqa: BLE001 - no log yet
            return []

    def versions(self, name: str) -> list[int]:
        return sorted(
            int(f.split(".")[0]) for f in self.log_files(name) if re.fullmatch(r"\d{20}\.json", f)
        )

    def latest(self, name: str) -> int:
        found = self.versions(name)
        if not found:
            raise FileNotFoundError(f"no _delta_log commit under Tables/{NAMESPACE}/{name}")
        return found[-1]

    def commit(self, name: str, version: int | None = None) -> list[dict]:
        """One commit's actions, the latest by default."""
        version = self.latest(name) if version is None else version
        raw = self._read(f"{self.relative(name)}/_delta_log/{version:020d}.json").decode()
        return [json.loads(line) for line in raw.splitlines() if line.strip()]

    def actions(self, name: str, kind: str, version: int | None = None) -> list[dict]:
        return [a[kind] for a in self.commit(name, version) if kind in a]

    def all_actions(self, name: str, kind: str) -> list[dict]:
        """Every `kind` action in the log, oldest first (probes are short: no checkpoint)."""
        found = []
        for version in self.versions(name):
            found += self.actions(name, kind, version)
        return found

    def metadata(self, name: str) -> dict:
        found = self.all_actions(name, "metaData")
        if not found:
            raise FileNotFoundError(f"no metaData action in Tables/{NAMESPACE}/{name}")
        return found[-1]

    def protocol(self, name: str) -> dict:
        found = self.all_actions(name, "protocol")
        return found[-1] if found else {}

    def features(self, name: str) -> list[str]:
        proto = self.protocol(name)
        return sorted(set(proto.get("readerFeatures", []) + proto.get("writerFeatures", [])))

    def operations(self, name: str) -> list[str]:
        """The operation each commit recorded, oldest first."""
        return [c.get("operation", "?") for c in self.all_actions(name, "commitInfo")]

    def commit_authors(self, name: str) -> list[str]:
        """Who wrote each version, oldest first: "delta-rs" (the oracle's own writes, tagged
        ORACLE_TAG) or "engine" (every other commit). Not clientVersion: the delta-rs column and
        duckrun commit through delta-rs too."""
        found = []
        for version in self.versions(name):
            info = self.actions(name, "commitInfo", version)
            info = info[0] if info else {}
            tagged = '"bench.writer": "oracle"' in json.dumps(info)
            found.append("delta-rs" if tagged else "engine")
        return found

    def active_files(self, name: str) -> list[dict]:
        """The add actions no later remove cancels: the files the current version reads."""
        live: dict[str, dict] = {}
        for version in self.versions(name):
            for action in self.commit(name, version):
                if "add" in action:
                    live[action["add"]["path"]] = action["add"]
                elif "remove" in action:
                    live.pop(action["remove"]["path"], None)
        return list(live.values())

    def file_path(self, name: str, relative_path: str) -> str:
        from urllib.parse import unquote

        return f"{self.relative(name)}/{unquote(relative_path)}"

    def delete_file(self, name: str, relative_path: str) -> None:
        self.fs().get_file_client(self.file_path(name, relative_path)).delete_file()

    def file_exists(self, name: str, relative_path: str) -> bool:
        return self.exists(self.file_path(name, relative_path))

    def checkpointed(self, name: str) -> bool:
        return "_last_checkpoint" in self.log_files(name) or any(
            ".checkpoint." in f for f in self.log_files(name)
        )

    # -- schemas, folders, plain Parquet ------------------------------------------------------

    def schema_relative(self, schema: str) -> str:
        return f"{self.cfg.lakehouse_id}/Tables/{schema}"

    def schema_uri(self, schema: str) -> str:
        return f"{self.cfg.base_path}/Tables/{schema}"

    def folder_exists(self, relative: str) -> bool:
        return self.fs().get_directory_client(relative).exists()

    def fixture_relative(self, fixture: str) -> str:
        return f"{self.cfg.lakehouse_id}/Tables/_bench_delta_fixtures/{fixture}"

    def fixture_uri(self, fixture: str) -> str:
        return f"{self.cfg.base_path}/Tables/_bench_delta_fixtures/{fixture}"

    def features_at(self, relative: str) -> list[str]:
        """Table features of the table at `relative`, from every protocol action in its log."""
        found: set[str] = set()
        folder = f"{relative}/_delta_log"
        for p in self.fs().get_paths(folder, recursive=False):
            leaf = p.name.rsplit("/", 1)[-1]
            if not re.fullmatch(r"\d{20}\.json", leaf):
                continue
            for line in self._read(p.name).decode().splitlines():
                if '"protocol"' in line:
                    proto = json.loads(line)["protocol"]
                    found |= set(proto.get("readerFeatures", []) + proto.get("writerFeatures", []))
        return sorted(found)

    def has_log(self, relative: str) -> bool:
        """A Delta table's directory: one holding a _delta_log with a commit in it."""
        try:
            paths = self.fs().get_paths(f"{relative}/_delta_log", recursive=False)
            return any(re.fullmatch(r"\d{20}\.json", p.name.rsplit("/", 1)[-1]) for p in paths)
        except Exception:  # noqa: BLE001 - no such folder
            return False

    def upload_parquet(self, name: str, filename: str, pairs) -> None:
        """Rows (id, v) as one plain Parquet file in the table's directory, with no log."""
        import io

        import pyarrow as pa
        import pyarrow.parquet as pq

        data = pa.table(
            {
                "id": pa.array([i for i, _ in pairs], pa.int64()),
                "v": pa.array([v for _, v in pairs], pa.int64()),
            }
        )
        buffer = io.BytesIO()
        pq.write_table(data, buffer)
        self.fs().get_file_client(f"{self.relative(name)}/{filename}").upload_data(
            buffer.getvalue(), overwrite=True
        )

    def purge_path(self, relative: str) -> None:
        self.fs().get_directory_client(relative).delete_directory()

    # -- teardown ----------------------------------------------------------------------------

    def purge(self, name: str) -> None:
        self.fs().get_directory_client(self.relative(name)).delete_directory()


def _type(kind) -> str:
    """A Delta schema type as one string: `long`, `struct`, `array`, `map`, `decimal(10,2)`."""
    return kind if isinstance(kind, str) else kind.get("type", "?")


def keep() -> bool:
    return os.environ.get("CAPABILITY_KEEP") == "1"
