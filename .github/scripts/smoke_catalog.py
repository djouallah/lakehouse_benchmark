"""PHASE 2: can this engine actually reach OneLake?

Runs only after smoke_sql.py has passed, and that ORDER IS THE WHOLE DESIGN. Phase 1 proved the
engine speaks the dialect against local parquet. So anything that fails here is the catalog, the
credential, or the storage path -- never the SQL. The two questions never have to be untangled
from one log again.

IT CALLS THE ENGINE'S REAL setup(), not a copy of it. That is the code bench.yml runs, with the
same token, the same ATTACH, the same transport setting. A smoke test that reimplements the thing
it is testing proves nothing, and this repo has already paid for that lesson twice:

  * auth_smoke.py probe 4 checks pyiceberg's FileIO, which is adlfs -- a completely different
    HTTP stack from DuckDB's azure extension. It passed happily while DuckDB could not read one
    byte, and the benchmark then failed 44 times out of 44.
  * LakeSail's own logs said "vended storage credentials... not implemented yet" and carried on,
    so the attach succeeded and every query died on a credential built from the wrong env vars.

Then TWO queries, not the whole suite: the suite config's PROBE_QUERIES (TPC-H: Q1 scans lineitem
whole, Q6 scans it filtered; TPC-DS: Q3 and Q42 both scan store_sales). Either pair proves data
files are readable rather than merely listed -- which is the exact failure both bugs above
produced. Timing is printed but is NOT a result; publish never sees it.

NOT reusing auth_smoke.py's probes, deliberately: its probe 5 imports duckdb, and this job
installs only the engine under test.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request

from bench import auth, scrub
from bench.config import ICEBERG_ENDPOINT, Config
from bench.suite import suite_class
from bench.tpch import queries
from bench.tpch.engines import get_engine


def _get(url: str, token: str) -> dict:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def list_namespaces(token: str, cfg: Config) -> list[str]:
    """The IRC namespace listing, over plain HTTP.

    Two calls, exactly as the Iceberg REST spec defines: /v1/config returns a `prefix` override
    that every later path is built from -- for OneLake it happens to equal the warehouse, but it
    is documented as server-chosen and is not safe to assume.
    """
    config = _get(f"{ICEBERG_ENDPOINT}/v1/config?warehouse={cfg.warehouse}", token)
    prefix = config.get("overrides", {}).get("prefix", cfg.warehouse)
    payload = _get(f"{ICEBERG_ENDPOINT}/v1/{prefix}/namespaces", token)
    return [".".join(ns) for ns in payload.get("namespaces", [])]


def main() -> int:
    cfg = suite_class().from_env()
    if not cfg.engine:
        raise SystemExit("BENCH_ENGINE is not set")

    print(
        f"{cfg.engine} | {cfg.TITLE} SF={cfg.sf} | namespace {cfg.schema} | "
        f"warehouse {cfg.warehouse}"
    )

    # The shared credential half first, so an expired federated credential does not get reported
    # as an engine bug. Cheap: one token, two REST calls.
    #
    # PLAIN HTTP, NOT pyiceberg. Only the Polars job installs pyiceberg -- the other
    # engines attach the catalog natively, and putting pyiceberg in their environment would be
    # dependency skew in the thing being measured. An earlier version called
    # auth.catalog().list_namespaces() here and died with ModuleNotFoundError before it had
    # tested anything at all.
    try:
        token = auth.onelake_token()
        namespaces = list_namespaces(token, cfg)
    except Exception as exc:  # noqa: BLE001 - reporting the failure is the job
        print(
            f"::error::credential chain failed before the engine was touched: "
            f"{scrub.scrub_exc(exc, 600)}"
        )
        print("see Agent.md section 2, or run .github/scripts/auth_smoke.py for the full chain")
        return 1
    print(f"  token ok ({len(token)} chars); catalog lists {len(namespaces)} namespaces")
    if cfg.schema not in namespaces:
        workflow = "tpcds.yml" if cfg.TEST == "tpcds" else "bench.yml"
        print(f"::error::namespace {cfg.schema} is not in the catalog: {sorted(namespaces)[:12]}")
        print(f"run {workflow} at sf={cfg.sf} first, or its prepare job, to generate it")
        return 1

    engine = get_engine(cfg.engine, cfg)
    statements = queries.load(cfg.engine, cfg.schema, cfg.sf, cfg.SQL_PATH, cfg.N_QUERIES)

    started = time.perf_counter()
    try:
        engine.setup()
    except Exception as exc:  # noqa: BLE001
        print(f"::error::{cfg.engine} setup() failed: {scrub.scrub_exc(exc, 800)}")
        return 1
    print(f"  setup {time.perf_counter() - started:.2f}s")

    # SMOKE_QUERIES, when a dispatch sets it, replaces the suite's two probes.
    raw = os.environ.get("SMOKE_QUERIES", "").strip()
    probes = [int(q) for q in raw.split(",") if q.strip()] if raw else list(cfg.PROBE_QUERIES)

    failed = 0
    try:
        for number in probes:
            query_started = time.perf_counter()
            try:
                count = engine.execute(statements[number - 1])
                print(
                    f"  Q{number:<2} ok    {count:>8,} rows  "
                    f"{time.perf_counter() - query_started:6.2f}s",
                    flush=True,
                )
            except Exception as exc:  # noqa: BLE001
                failed += 1
                print(f"  Q{number:<2} FAIL  {scrub.scrub_exc(exc, 500)}", flush=True)
    finally:
        try:
            engine.close()
        except Exception as exc:  # noqa: BLE001 - teardown must not mask the result
            print(f"  warning: close() failed: {exc}")

    if failed:
        print(
            f"\n::error::{cfg.engine} attached but could not read data -- "
            f"{failed} of {len(probes)} probe queries failed. "
            f"Phase 1 passed, so this is storage or credentials, not SQL."
        )
        return 1
    print(f"\n{cfg.engine} {engine.version} reads {cfg.schema} from OneLake")
    return 0


if __name__ == "__main__":
    sys.exit(main())
