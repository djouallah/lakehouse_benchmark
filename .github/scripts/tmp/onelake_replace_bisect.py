"""TEMPORARY: which part of Iceberg Java's replace commit the OneLake REST catalog refuses.

Spark's CREATE OR REPLACE TABLE ... AS (capability run 45923438779) sends one commit to the
existing table, asserting only its uuid, and OneLake answers 409 "One or more requirements
failed":

    set-current-schema 0, set-default-spec 0, set-default-sort-order 0,
    remove-snapshot-ref main, set-properties, add-snapshot (parent none), set-snapshot-ref main

Each variant below is sent as plain HTTP to a fresh table holding SEED (made by pyiceberg, as the
probe's was). The snapshot is a real one: pyiceberg stages an append on the fresh table without
committing it, and the variant takes its add-snapshot. A variant that changes one thing from the
full commit says, by its status, whether that thing is what OneLake refuses.

    python .github/scripts/tmp/onelake_replace_bisect.py
"""

from __future__ import annotations

import copy
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "capability"))

from harness import NAMESPACE, iceberg_schema, rows  # noqa: E402

from bench import auth  # noqa: E402
from bench.config import ICEBERG_ENDPOINT, Config  # noqa: E402

SEED = [(1, 10), (2, 20), (3, 30)]
NEW_ROWS = [(7, 70), (8, 80)]
RUN = os.environ.get("GITHUB_RUN_ID", "local")


class Rest:
    def __init__(self, cfg: Config):
        self.token = auth.onelake_token()
        config = self.send("GET", f"{ICEBERG_ENDPOINT}/v1/config?warehouse={cfg.warehouse}")[1]
        server = {**config.get("defaults", {}), **config.get("overrides", {})}
        prefix = server.get("prefix")
        self.base = f"{ICEBERG_ENDPOINT}/v1/{prefix}" if prefix else f"{ICEBERG_ENDPOINT}/v1"

    def send(self, method: str, url: str, body: dict | None = None) -> tuple[int, dict | str]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {self.token}")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                out = resp.read()
                return resp.status, json.loads(out) if out.strip() else {}
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode("utf-8", "replace")[:600]

    def commit(self, table: str, body: dict) -> tuple[int, dict | str]:
        ns = urllib.parse.quote(NAMESPACE, safe="")
        return self.send("POST", f"{self.base}/namespaces/{ns}/tables/{table}", body)


def staged_snapshot(tbl) -> dict:
    """The add-snapshot of an append of NEW_ROWS, staged by pyiceberg and never committed."""
    txn = tbl.transaction()
    txn.append(rows(NEW_ROWS))
    for update in txn._updates:  # noqa: SLF001 - the staged updates are the point
        if update.action == "add-snapshot":
            return json.loads(update.snapshot.model_dump_json(by_alias=True, exclude_none=True))
    raise RuntimeError(f"no add-snapshot in {[u.action for u in txn._updates]}")  # noqa: SLF001


def java_replace(uuid: str, snapshot: dict) -> dict:
    """Spark's replace commit, as captured, on this table's uuid and with this snapshot."""
    replacing = dict(snapshot)
    replacing.pop("parent-snapshot-id", None)  # a replace starts a new history
    replacing["summary"] = {**replacing.get("summary", {}), "operation": "overwrite"}
    return {
        "requirements": [{"type": "assert-table-uuid", "uuid": uuid}],
        "updates": [
            {"action": "set-current-schema", "schema-id": 0},
            {"action": "set-default-spec", "spec-id": 0},
            {"action": "set-default-sort-order", "sort-order-id": 0},
            {"action": "remove-snapshot-ref", "ref-name": "main"},
            {"action": "set-properties", "updates": {"probe": "replace"}},
            {"action": "add-snapshot", "snapshot": replacing},
            {
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "snapshot-id": replacing["snapshot-id"],
                "type": "branch",
            },
        ],
    }


def without(*actions):
    def change(body, ctx):
        body["updates"] = [u for u in body["updates"] if u["action"] not in actions]

    return change


def with_parent(body, ctx):
    for u in body["updates"]:
        if u["action"] == "add-snapshot":
            u["snapshot"]["parent-snapshot-id"] = ctx["current"]


def assert_main(body, ctx):
    body["requirements"].append(
        {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": ctx["current"]}
    )


def only(*actions):
    def change(body, ctx):
        body["updates"] = [u for u in body["updates"] if u["action"] in actions]

    return change


def both(*changes):
    def change(body, ctx):
        for c in changes:
            c(body, ctx)

    return change


VARIANTS = [
    ("the full replace, as Spark sends it", lambda b, c: None),
    (
        "without set-current-schema / set-default-spec / set-default-sort-order",
        without("set-current-schema", "set-default-spec", "set-default-sort-order"),
    ),
    ("without remove-snapshot-ref main", without("remove-snapshot-ref")),
    ("without set-properties", without("set-properties")),
    ("add-snapshot with parent = the current snapshot", with_parent),
    (
        "parent = current, and without remove-snapshot-ref",
        both(with_parent, without("remove-snapshot-ref")),
    ),
    ("plus assert-ref-snapshot-id main = current", assert_main),
    ("only add-snapshot (no parent) + set-snapshot-ref", only("add-snapshot", "set-snapshot-ref")),
    (
        "only remove-snapshot-ref + add-snapshot (no parent) + set-snapshot-ref",
        only("remove-snapshot-ref", "add-snapshot", "set-snapshot-ref"),
    ),
    ("only remove-snapshot-ref main", only("remove-snapshot-ref")),
    (
        "only set-current-schema / set-default-spec / set-default-sort-order",
        only("set-current-schema", "set-default-spec", "set-default-sort-order"),
    ),
    (
        "an ordinary append: parent = current, only add-snapshot + set-snapshot-ref, assert main",
        both(with_parent, only("add-snapshot", "set-snapshot-ref"), assert_main),
    ),
]


def main() -> int:
    cfg = Config.from_env()
    catalog = auth.catalog(cfg)
    catalog.create_namespace_if_not_exists(NAMESPACE)
    rest = Rest(cfg)
    made, lines = [], []
    try:
        for i, (what, change) in enumerate(VARIANTS):
            name = f"rb_{RUN}_{i}"
            tbl = catalog.create_table((NAMESPACE, name), schema=iceberg_schema())
            made.append(name)
            tbl.append(rows(SEED))
            tbl = catalog.load_table((NAMESPACE, name))
            ctx = {"current": tbl.metadata.current_snapshot_id}
            body = java_replace(str(tbl.metadata.table_uuid), staged_snapshot(tbl))
            change(body, ctx)
            status, answer = rest.commit(name, copy.deepcopy(body))
            after = catalog.load_table((NAMESPACE, name))
            refs = {k: v.snapshot_id for k, v in after.metadata.refs.items()}
            sent = [u["action"] for u in body["updates"]]
            line = (
                f"{status}  {what}\n      updates {sent}\n      requirements "
                f"{[r['type'] for r in body['requirements']]}\n      "
                + (
                    f"answer {answer}"
                    if status >= 300
                    else f"main now {refs.get('main')} "
                    f"(was {ctx['current']}), {len(after.metadata.snapshots)} snapshot(s)"
                )
            )
            print(line, flush=True)
            lines.append(line)
    finally:
        for name in made:
            try:
                catalog.purge_table((NAMESPACE, name))
            except Exception as exc:  # noqa: BLE001 - teardown is best effort
                print(f"  could not drop {name}: {exc}")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write("## OneLake: Iceberg Java's replace commit, one change at a time\n\n```\n")
            fh.write("\n".join(lines) + "\n```\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
