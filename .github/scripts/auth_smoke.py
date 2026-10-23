"""Prove the credential chain in about a minute, before anything expensive runs.

THIS SCRIPT EXISTS TO MAKE FAILURES CHEAP AND LEGIBLE. Five separate things have to be right
before a benchmark job can do anything, they are configured in five different places, and they
all fail with a 401 or a 403 that looks the same from inside a 25-minute run:

  1. the federated credential's SUBJECT matches this repo and ref
  2. its AUDIENCE is api://AzureADTokenExchange
  3. the Fabric tenant setting "Service principals can use Fabric APIs" is on
  4. the tenant setting "Users can access data stored in OneLake with apps external to Fabric" is on
  5. the service principal has a CONTRIBUTOR role on the workspace (Viewer cannot commit)

Each probe below isolates one of them, so the log says which. It exits non-zero on the first
failure, because there is no point testing a write when the token did not mint.

THE WRITE PROBE IS THE POINT. Probes 1-3 only prove the token is good for READING. `prepare` has
to CREATE a namespace, CREATE tables and COMMIT snapshots, and a Viewer role gets all the way to
probe 3 and then fails there. Probe 4 finds that out in two minutes instead of twenty-five.

It creates `_bench_probe` and leaves it behind. Deleting requires more permission than creating,
and a probe that needs elevated rights to clean up after itself is worse than a stray empty
namespace.
"""

from __future__ import annotations

import os
import sys
import urllib.request

from bench import auth, scrub
from bench.config import ICEBERG_ENDPOINT, Config, azure_transport

FABRIC_API = "https://api.fabric.microsoft.com"


def _probe(number: int, what: str, fn):
    print(f"\n[{number}] {what}", flush=True)
    try:
        detail = fn()
    except Exception as exc:  # noqa: BLE001 - this script's whole job is reporting failures
        print(f"    FAIL  {scrub.scrub_exc(exc, 600)}", flush=True)
        return False
    print(f"    PASS  {scrub.scrub(detail)}", flush=True)
    return True


def main() -> int:
    cfg = Config.from_env()
    print(f"workspace {cfg.workspace_id}  lakehouse {cfg.lakehouse_id}  sf {cfg.sf}")

    def mint():
        token = auth.onelake_token()
        # Never the token itself; its length is enough to prove one came back.
        return f"minted a {len(token)}-char bearer for storage.azure.com"

    def fabric_api():
        """Separate audience, separate failure. A 401 HERE and a pass on probe 1 means the
        service-principal tenant setting is off, not that the credential is wrong."""
        api_token = auth.credential().get_token(f"{FABRIC_API}/.default")
        scrub.register(api_token.token)
        request = urllib.request.Request(
            f"{FABRIC_API}/v1/workspaces/{cfg.workspace_id}/lakehouses",
            headers={"Authorization": f"Bearer {api_token.token}"},
        )
        import json

        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
        names = [item["displayName"] for item in payload.get("value", [])]
        ids = {item["id"] for item in payload.get("value", [])}
        if cfg.lakehouse_id not in ids:
            raise RuntimeError(
                f"FABRIC_LAKEHOUSE_ID {cfg.lakehouse_id} is not a lakehouse in this workspace; "
                f"found {sorted(names)}"
            )
        return f"{len(names)} lakehouses, and FABRIC_LAKEHOUSE_ID is one of them"

    def catalog_read():
        """The Iceberg REST endpoint itself, which is a different host from the Fabric API."""
        namespaces = auth.catalog(cfg).list_namespaces()
        return f"{ICEBERG_ENDPOINT} lists {len(namespaces)} namespaces: " + ", ".join(
            ".".join(ns) for ns in namespaces[:8]
        )

    def catalog_write():
        import pyarrow as pa
        from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids

        catalog = auth.catalog(cfg)
        namespace = "_bench_probe"
        catalog.create_namespace_if_not_exists(namespace)
        name = f"{namespace}.probe_{os.environ.get('GITHUB_RUN_ID', 'local')}"
        location = f"{cfg.base_path}/Tables/{namespace}/{name.split('.')[-1]}"
        table = catalog.create_table_if_not_exists(
            identifier=name,
            schema=_pyarrow_to_schema_without_ids(pa.schema([pa.field("n", pa.int64())])),
            location=location,
        )
        # AND THE FILE PATH, which is a SEPARATE failure. Creating a table is pure REST; reading
        # or writing its data goes through a FileIO that pyiceberg picks per scheme, and for
        # abfss:// that is FsspecFileIO, which imports `adlfs` lazily. A missing adlfs does not
        # surface here -- it surfaces inside add_files, after the whole dataset has been
        # generated and uploaded. Touching it now costs one HEAD request.
        table.io.new_input(f"{location}/_probe_does_not_exist.parquet").exists()
        return f"created {name}, and its FileIO resolves -- REST commit and blob access both work"

    def duckdb_reads_a_data_file():
        """Read one real parquet THROUGH DUCKDB.

        PROBE 4 CANNOT SEE THIS FAILURE. It goes through pyiceberg's FileIO, which is adlfs --
        a completely different HTTP stack from DuckDB's azure extension. adlfs was perfectly
        happy while DuckDB could not read a single byte, so every probe passed and the benchmark
        then failed 44 times out of 44 on:

            IOException: AzureStorageFileSystem could not open file: 'abfss://.../Tables/...'

        The cause is the azure extension's HTTP transport, not the credential -- see
        config.azure_transport(). This probe exercises exactly that path, so the failure costs 90
        seconds instead of a full generate-and-benchmark cycle.

        It also reports WHICH transports work, because that is the fact worth knowing: if DuckDB
        ever fixes its default, this log says so.

        Through the nightly CLI the bench runs (bench/duckdb_cli.py), not a wheel.
        """
        from bench.duckdb_cli import DuckDBCli

        catalog = auth.catalog(cfg)
        data_file = namespace_name = None
        for ns in catalog.list_namespaces():
            for identifier in catalog.list_tables(ns):
                files = list(catalog.load_table(identifier).scan().plan_files())
                if files:
                    data_file = files[0].file.file_path
                    namespace_name = ".".join(identifier)
                    break
            if data_file:
                break
        if not data_file:
            return "SKIPPED -- no table in this lakehouse has data files yet"

        token = auth.onelake_token()
        # The configured transport first; the others only to report what would have worked.
        candidates = [azure_transport() or "default"]
        candidates += [t for t in ("curl", "default") if t not in candidates]

        outcomes, winner = [], None
        for transport in candidates:
            con = DuckDBCli()
            try:
                con.sql("INSTALL azure; LOAD azure;")
                con.sql(f"SET GLOBAL azure_transport_option_type = '{transport}'")
                con.sql(
                    f"CREATE OR REPLACE SECRET s (TYPE azure, PROVIDER access_token, "
                    f"ACCESS_TOKEN '{token}')"
                )
                rows = int(
                    con.sql(f"SELECT count(*) FROM read_parquet('{data_file}')").fetchone()[0]
                )
                outcomes.append(f"{transport}=OK({rows:,} rows)")
                winner = winner or transport
            except Exception as exc:  # noqa: BLE001 - the outcome per transport IS the result
                outcomes.append(f"{transport}=FAILED({type(exc).__name__})")
            finally:
                con.close()

        detail = f"{namespace_name} via " + ", ".join(outcomes)
        if winner is None:
            raise RuntimeError(f"DuckDB could not read {namespace_name} on any transport: {detail}")
        if winner != candidates[0]:
            raise RuntimeError(
                f"the configured transport ({candidates[0]}) failed but {winner} works -- "
                f"set AZURE_TRANSPORT_OPTION_TYPE={winner}: {detail}"
            )
        return detail

    probes = [
        ("mint a OneLake token (federated credential, audience, tenant)", mint),
        ("Fabric API: list lakehouses (SP-can-use-Fabric-APIs, workspace role)", fabric_api),
        ("Iceberg REST: list namespaces (external-apps-to-OneLake setting)", catalog_read),
        ("Iceberg REST: create a table + resolve its FileIO (Contributor, adlfs)", catalog_write),
        (
            "DuckDB: read a real parquet over abfss (azure extension transport)",
            duckdb_reads_a_data_file,
        ),
    ]
    for number, (what, fn) in enumerate(probes, start=1):
        if not _probe(number, what, fn):
            print(f"\nstopped at probe {number}; see Agent.md section 2")
            return 1

    print("\nall probes passed -- this repo can generate data and run the benchmark")
    return 0


if __name__ == "__main__":
    sys.exit(main())
