"""Where the Delta support probes write, and the oracle that checks what each statement did.

Delta has no REST catalog here: a Delta table on OneLake is a directory with a `_delta_log`. Every
probe table is one under the lakehouse's Tables/_bench_delta/, which Fabric shows as schema
`_bench_delta`, apart from the Iceberg grid's _bench_capability.

TWO READERS, NEITHER OF THEM AN ENGINE UNDER TEST:

* delta-rs (`deltalake`) reads the rows, the schema and the active files, the way pyiceberg does
  for the Iceberg grid: it reads what the LOG now says, not what the engine cached.
* The log itself, read as JSON straight off storage (`commit`), for what delta-rs does not expose
  the same way across versions: the protocol's table features, deletion vectors, domain metadata,
  the operation each commit recorded, and whether a checkpoint was written.

Storage goes through azure-storage-file-datalake on the job's OIDC credential (bench.auth), and
delta-rs takes the same bearer.
"""

from __future__ import annotations

import json
import os

from bench import auth, scrub
from bench.config import ONELAKE_DFS, Config

NAMESPACE = "_bench_delta"


class DeltaOracle:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._fs = None

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

    # -- delta-rs ----------------------------------------------------------------------------

    def table(self, name: str, version: int | None = None):
        from deltalake import DeltaTable

        return DeltaTable(self.uri(name), version=version, storage_options=self.storage_options())

    def rows(self, name: str, cols=("id", "v")):
        """The table's rows as sorted tuples, through delta-rs; a string when it cannot read."""
        try:
            arrow = self.table(name).to_pyarrow_table(columns=list(cols))
            return sorted(tuple(row[c] for c in cols) for row in arrow.to_pylist())
        except Exception as exc:  # noqa: BLE001 - the read-back failing is part of the answer
            return f"delta-rs could not read it: {scrub.scrub_exc(exc, 300)}"

    def column(self, name: str, col: str = "x") -> list:
        arrow = self.table(name).to_pyarrow_table(columns=[col])
        return [row[col] for row in arrow.to_pylist()]

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
        write_deltalake(self.uri(name), data, mode=mode, storage_options=self.storage_options())

    # -- the log -----------------------------------------------------------------------------

    def _read(self, path: str) -> bytes:
        return self.fs().get_file_client(path).download_file().readall()

    def exists(self, path: str) -> bool:
        return self.fs().get_file_client(path).exists()

    def log_files(self, name: str) -> list[str]:
        """The names in _delta_log, sorted."""
        folder = f"{self.relative(name)}/_delta_log"
        try:
            return sorted(p.name.rsplit("/", 1)[-1] for p in self.fs().get_paths(folder))
        except Exception:  # noqa: BLE001 - no log yet
            return []

    def versions(self, name: str) -> list[int]:
        return sorted(
            int(f.split(".")[0])
            for f in self.log_files(name)
            if f.endswith(".json") and f[0] != "_"
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

    # -- teardown ----------------------------------------------------------------------------

    def purge(self, name: str) -> None:
        self.fs().get_directory_client(self.relative(name)).delete_directory()


def _type(kind) -> str:
    """A Delta schema type as one string: `long`, `struct`, `array`, `map`, `decimal(10,2)`."""
    return kind if isinstance(kind, str) else kind.get("type", "?")


def keep() -> bool:
    return os.environ.get("CAPABILITY_KEEP") == "1"
