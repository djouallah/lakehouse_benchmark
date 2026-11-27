"""TEMPORARY: what iceberg-go's and pyiceberg's expire_snapshots send the OneLake REST catalog,
and what comes back.

iceberg-go's expire passed in capability run 45918631827 (3 -> 1 snapshots) and got 400
"Malformed request" in 45920209093, same code and table shape; pyiceberg passed. Each trial
makes a fresh table with 3 snapshots (pyiceberg: create, then 3 appends) and expires it to the
current one, through a proxy that keeps every commit in full:

    a. iceberg-go   ExpireSnapshots(WithOlderThan(0), WithRetainLast(1))   (IGO_PROBE)
    b. pyiceberg    maintenance.expire_snapshots().older_than(now)
    c. plain HTTP   assert-table-uuid + one remove-snapshots of the two old snapshots

    IGO_PROBE=<binary> python .github/scripts/tmp/onelake_expire_check.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.parse
from datetime import UTC, datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "capability"))

from harness import NAMESPACE, iceberg_schema, rows  # noqa: E402

from bench import auth  # noqa: E402
from bench.capability.race import RaceProxy  # noqa: E402
from bench.config import Config  # noqa: E402

RUN = os.environ.get("GITHUB_RUN_ID", "local")
TRIALS = int(os.environ.get("TRIALS", "3"))
SEEDS = [[(1, 10), (2, 20), (3, 30)], [(4, 40)], [(5, 50)]]


class Recording(RaceProxy):
    """The race proxy, unarmed, keeping each POST to a table in full."""

    def __init__(self):
        super().__init__()
        self.seen: list[dict] = []
        forward = self._forward

        def recorded(method, path, headers, body):
            status, out_headers, out = forward(method, path, headers, body)
            if method == "POST" and "/tables/" in path:
                self.seen.append(
                    {
                        "path": urllib.parse.unquote(path.split("?")[0]).split("/")[-1],
                        "request": json.loads(body or b"{}"),
                        "status": status,
                        "response": out[:1500].decode("utf-8", "replace"),
                    }
                )
            return status, out_headers, out

        self._forward = recorded


def fresh(catalog, name: str):
    tbl = catalog.create_table((NAMESPACE, name), schema=iceberg_schema())
    for batch in SEEDS:
        tbl.append(rows(batch))
    return catalog.load_table((NAMESPACE, name))


def go_expire(cfg, uri: str, name: str) -> str:
    env = {
        **os.environ,
        "ICEBERG_URI": uri,
        "ICEBERG_TOKEN": auth.onelake_token(),
        "ICEBERG_WAREHOUSE": cfg.warehouse,
    }
    request = json.dumps({"op": "expire_snapshots", "ns": NAMESPACE, "table": name})
    done = subprocess.run(
        [os.environ["IGO_PROBE"]], input=request, env=env, capture_output=True, text=True
    )
    return done.stdout.strip().splitlines()[-1] if done.stdout.strip() else done.stderr[-500:]


def main() -> int:
    cfg = Config.from_env()
    catalog = auth.catalog(cfg)
    catalog.create_namespace_if_not_exists(NAMESPACE)
    proxy = Recording().start()
    proxied = auth.catalog(cfg, proxy.endpoint)
    made, out = [], []

    def report(label: str, name: str, before: int, answer: str) -> None:
        after = len(catalog.load_table((NAMESPACE, name)).metadata.snapshots)
        line = f"{label:12} {name}: snapshots {before} -> {after}; {answer}"
        print(line, flush=True)
        out.append(line)
        for x in [s for s in proxy.seen if s["path"] == name]:
            print(f"  COMMIT {x['status']} {json.dumps(x['request'])}", flush=True)
            if x["status"] >= 300:
                print(f"  ANSWER {x['response']}", flush=True)
            out.append(f"  {x['status']} {json.dumps(x['request'])[:900]}")

    try:
        for trial in range(TRIALS):
            name = f"ex_{RUN}_{trial}_go"
            made.append(name)
            before = len(fresh(catalog, name).metadata.snapshots)
            report("iceberg-go", name, before, go_expire(cfg, proxy.endpoint, name))

            name = f"ex_{RUN}_{trial}_py"
            made.append(name)
            before = len(fresh(catalog, name).metadata.snapshots)
            try:
                proxied.load_table((NAMESPACE, name)).maintenance.expire_snapshots().older_than(
                    datetime.now(UTC)
                ).commit()
                answer = "returned"
            except Exception as exc:  # noqa: BLE001 - the answer
                answer = f"{type(exc).__name__}: {exc}"[:400]
            report("pyiceberg", name, before, answer)

            name = f"ex_{RUN}_{trial}_raw"
            made.append(name)
            tbl = fresh(catalog, name)
            old = [
                s.snapshot_id
                for s in tbl.metadata.snapshots
                if s.snapshot_id != tbl.metadata.current_snapshot_id
            ]
            body = {
                "requirements": [
                    {"type": "assert-table-uuid", "uuid": str(tbl.metadata.table_uuid)}
                ],
                "updates": [{"action": "remove-snapshots", "snapshot-ids": old}],
            }
            try:
                proxied._session.post(  # noqa: SLF001 - the catalog's own authenticated session
                    proxied.url(f"namespaces/{NAMESPACE}/tables/{name}", prefixed=True), json=body
                )
                answer = "sent"
            except Exception as exc:  # noqa: BLE001
                answer = f"{type(exc).__name__}: {exc}"[:400]
            report("plain HTTP", name, len(tbl.metadata.snapshots), answer)
    finally:
        proxy.close()
        for name in made:
            try:
                catalog.purge_table((NAMESPACE, name))
            except Exception as exc:  # noqa: BLE001
                print(f"  could not drop {name}: {exc}")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write("## OneLake: expire_snapshots from iceberg-go, pyiceberg, plain HTTP\n\n```\n")
            fh.write("\n".join(out) + "\n```\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
