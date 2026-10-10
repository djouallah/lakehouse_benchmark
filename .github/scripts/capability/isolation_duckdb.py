"""DuckDB against a concurrent writer: what each isolation level does, and how transactions behave.

DuckDB is the one engine here with transactions: `BEGIN ... COMMIT` spans statements. So it gets
a section of its own.

PART 1, ISOLATION LEVELS. One statement, autocommit, and writer B commits between DuckDB's read
and DuckDB's commit. The race is injected at the commit through the race proxy: DuckDB talks to the
catalog through a local proxy, which holds DuckDB's commit, lands B's change through pyiceberg on
the real endpoint, then forwards DuckDB's commit, now built on a snapshot the table has moved
past. Each race runs on three tables that differ only in their properties:

    serializable   nothing set; Iceberg's default level
    snapshot       write.delete / update / merge.isolation-level = snapshot
    no retries     commit.retry.num-retries = 0

PART 2, TRANSACTIONS. BEGIN ... COMMIT on one connection, with B committing in the middle, natively:
what a transaction reads, when its snapshot is taken, what it commits and when the commit fails,
ROLLBACK, a failing statement, visibility of its own uncommitted rows, two DuckDB connections
racing.

PART 3, COMBINATIONS. No concurrent writer: which DDL and DML sequences one transaction accepts
(DROP then CREATE, CREATE then INSERT, ALTER then write, ...), and whether
what the catalog holds afterwards is what the sequence says, read back through pyiceberg.

EVERY CONNECTION RUNS `SET iceberg_use_metadata_log = false`. With it on (the default), DuckDB picks
the table metadata as of the transaction start by comparing its own clock with the catalog's
commit timestamps, and a catalog clock ahead of the client hides the newest commit, its own
included (duckdb-iceberg#1475). Off, a transaction pins each table at its first read.

    python .github/scripts/capability/isolation_duckdb.py [--no-race] [--only key,key]

"""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
from dataclasses import dataclass, field

from harness import (
    BROKEN,
    NAMESPACE,
    NOOP,
    REFUSED,
    SUPPORTED,
    Broken,
    NoOp,
    Refused,
    iceberg_schema,
    rows,
    save_results,
)

from bench import auth, scrub
from bench.capability.race import RaceProxy
from bench.config import Config

SEED = [(1, 10), (2, 20), (3, 30)]
APPENDED = (4, 40)

CONFIGS = {
    "serializable": {},
    "snapshot": {
        "write.delete.isolation-level": "snapshot",
        "write.update.isolation-level": "snapshot",
        "write.merge.isolation-level": "snapshot",
    },
    "no retries": {"commit.retry.num-retries": "0"},
}


def _one_line(text: object, limit: int = 400) -> str:
    flat = " ".join(scrub.scrub(text).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _say(text: object) -> None:
    print(scrub.scrub(text), flush=True)


def _s(*pairs) -> list:
    return sorted(pairs)


def _err(exc: Exception, limit: int = 300) -> str:
    return _one_line(f"{type(exc).__name__}: {scrub.scrub_exc(exc, 1500)}", limit)


# --------------------------------------------------------------------------------------------
# part 1: one statement against one concurrent commit
# --------------------------------------------------------------------------------------------

WITH_B = _s(*SEED, APPENDED)
DOUBLED = [(1, 20), (2, 40), (3, 60)]


@dataclass(frozen=True)
class Race:
    key: str
    title: str
    statements: tuple  # DuckDB's, run in order on one connection; {t} is the table
    inject: str  # B's change: "append" (4, 40), "delete3" id 3
    solo: list  # DuckDB's statements alone
    after_b: list  # B's change alone: what a refused statement leaves
    states: dict = field(default_factory=dict)  # outcome -> final states that name it


UPDATE_OTHER = {
    "retried": [_s((1, 10), (2, 21), (3, 30), APPENDED)],
    "lost": [_s((1, 10), (2, 21), (3, 30))],
}
MERGE_ID = (
    "MERGE INTO {t} AS tg USING (SELECT {id}::BIGINT AS id, 1::BIGINT AS d) AS s "
    "ON tg.id = s.id WHEN MATCHED THEN UPDATE SET v = tg.v + s.d"
)

RACES = [
    Race(
        "insert",
        "INSERT a row; B appends a row",
        ("INSERT INTO {t} VALUES (5, 50)",),
        "append",
        solo=_s(*SEED, (5, 50)),
        after_b=WITH_B,
        states={"retried": [_s(*SEED, APPENDED, (5, 50))], "lost": [_s(*SEED, (5, 50))]},
    ),
    Race(
        "delete",
        "DELETE id 1; B appends a row",
        ("DELETE FROM {t} WHERE id = 1",),
        "append",
        solo=_s((2, 20), (3, 30)),
        after_b=WITH_B,
        states={"retried": [_s((2, 20), (3, 30), APPENDED)], "lost": [_s((2, 20), (3, 30))]},
    ),
    Race(
        "update_other",
        "UPDATE v = v + 1 on id 2; B appends a row",
        ("UPDATE {t} SET v = v + 1 WHERE id = 2",),
        "append",
        solo=_s((1, 10), (2, 21), (3, 30)),
        after_b=WITH_B,
        states=UPDATE_OTHER,
    ),
    Race(
        "merge_other",
        "MERGE ... UPDATE v = v + 1 on id 2; B appends a row",
        (MERGE_ID.replace("{id}", "2"),),
        "append",
        solo=_s((1, 10), (2, 21), (3, 30)),
        after_b=WITH_B,
        states=UPDATE_OTHER,
    ),
    Race(
        "delete_vs_delete",
        "DELETE id 1; B deletes id 3",
        ("DELETE FROM {t} WHERE id = 1",),
        "delete3",
        solo=_s((2, 20), (3, 30)),
        after_b=_s((1, 10), (2, 20)),
        states={"retried": [_s((2, 20))], "lost": [_s((2, 20), (3, 30))]},
    ),
]


def classify(race: Race, final, raised: bool, statuses: list[int]) -> str:
    """One race's outcome, from the final rows, whether DuckDB raised, and the commit statuses
    the proxy saw.

    refused  the commit failed and B's change stands: safe and loud
    retried  DuckDB refreshed and committed on top of B; both changes are in
    lost     DuckDB's statement succeeded and B's change is gone
    corrupt  the table matches no order of the two writes
    error    DuckDB failed before it ever committed, so the race was never run
    no-op    DuckDB returned success and never committed
    """
    if not statuses:
        return "error" if raised else "no-op"
    if raised and final == race.after_b:
        return "refused"
    for outcome, states in race.states.items():
        if final in states:
            return outcome
    return "corrupt"


def inject_b(catalog, table: str, kind: str):
    """Writer B's change, through pyiceberg on the real endpoint."""

    def run():
        tbl = catalog.load_table((NAMESPACE, table))
        if kind == "append":
            tbl.append(rows([APPENDED]))
        elif kind == "delete3":
            tbl.delete("id == 3")
        else:
            raise ValueError(kind)

    return run


def final_rows(catalog, table: str) -> list:
    scan = catalog.load_table((NAMESPACE, table)).scan(selected_fields=("id", "v"))
    return sorted((r["id"], r["v"]) for r in scan.to_arrow().to_pylist())


# --------------------------------------------------------------------------------------------
# the capability matrix's concurrency rows: the same races, asked of every engine
# --------------------------------------------------------------------------------------------

# probe key -> (race, the outcomes that are safe). A refusal is safe wherever the engine's write
# depends on what it read; for a blind append it is not, because nothing stops it re-applying.
RACE_ROWS = {
    "race_append": ("insert", {"retried"}),
    "race_delete": ("delete", {"refused", "retried"}),
    "race_update": ("update_other", {"refused", "retried"}),
}
RACE = {race.key: race for race in RACES}


def matrix_outcome(key: str, outcome: str) -> str:
    """One race outcome as a capability-matrix outcome."""
    if outcome in RACE_ROWS[key][1]:
        return SUPPORTED
    if outcome in ("error", "broken"):
        return BROKEN
    return NOOP if outcome == "no-op" else REFUSED


def matrix_rows(levels: dict, level: str = "serializable") -> list[dict]:
    """DuckDB's races at Iceberg's default level, as rows the readme's DuckDB column reads."""
    found = []
    for key, (race, _) in RACE_ROWS.items():
        if (race, level) in levels:
            outcome, detail = levels[(race, level)]
            found.append(
                {
                    "key": key,
                    "group": "concurrency",
                    "question": RACE[race].title,
                    "outcome": matrix_outcome(key, outcome),
                    "detail": f"{outcome}: {detail}",
                }
            )
    return found


def race_probe(key: str, catalog, proxy, table: str, write) -> str:
    """One concurrency probe for any engine: `table` holds SEED; B is armed to commit between
    the engine's read and its commit, `write()` is the engine's statement through `proxy`, and
    the final rows read through pyiceberg decide."""
    race = RACE[RACE_ROWS[key][0]]
    proxy.arm(NAMESPACE, table, inject_b(catalog, table, race.inject))
    raised = ""
    try:
        write()
    except Exception as exc:  # noqa: BLE001 - the engine's error is the reading
        raised = _err(exc)
    final = final_rows(catalog, table)
    statuses = proxy.statuses(NAMESPACE, table)
    seen = proxy.commits.get((NAMESPACE, table), [])
    detail = f"final {final}; commits [{', '.join(str(s) for s, _ in seen) or 'none'}]"
    if raised:
        detail += f"; raised {raised}"
    if statuses and proxy.injected.get((NAMESPACE, table)) != "landed":
        outcome = "broken"
        detail += f"; B {proxy.injected.get((NAMESPACE, table))}"
    else:
        outcome = classify(race, final, bool(raised), statuses)
    verdict_ = matrix_outcome(key, outcome)
    detail = f"{outcome}: {detail}"
    if verdict_ == SUPPORTED:
        return detail
    if verdict_ == BROKEN:
        raise Broken(detail)
    raise NoOp(detail) if verdict_ == NOOP else Refused(detail)


# --------------------------------------------------------------------------------------------
# the probe
# --------------------------------------------------------------------------------------------


class DuckDBIsolation:
    def __init__(self, cfg: Config):
        from bench.capability.engines.duckdb_iceberg import version

        self.cfg = cfg
        self.version = version()
        self.proxy = RaceProxy().start()
        self.catalog = auth.catalog(cfg)  # B, and every read of the final state
        self.token = auth.onelake_token()
        self.run = "{}_{}".format(
            os.environ.get("GITHUB_RUN_ID", "local"), os.environ.get("GITHUB_RUN_ATTEMPT", "1")
        )
        self.keep = os.environ.get("CAPABILITY_KEEP") == "1"
        self.created: list[str] = []
        self.levels: dict[tuple[str, str], tuple[str, str]] = {}
        self.transactions: list[tuple[str, str, str]] = []
        self.combos: list[tuple[str, str, str, str]] = []

    # -- plumbing --------------------------------------------------------------------------------

    def refresh(self) -> None:
        """A run outlives one token: pyiceberg's catalog and DuckDB's ATTACH each hold the string
        they were given, so both are re-made when auth mints a new one."""
        token = auth.onelake_token()
        if token != self.token:
            self.token = token
            self.catalog = auth.catalog(self.cfg)

    def conn(self):
        from bench.capability.engines.duckdb_iceberg import attach, connect

        conn = connect()
        attach(conn, self.cfg, self.token, self.proxy.endpoint)
        conn.execute("SET iceberg_use_metadata_log = false")
        return conn

    @staticmethod
    def t(table: str) -> str:
        return f'onelake."{NAMESPACE}"."{table}"'

    def table(self, key: str, properties: dict | None = None) -> str:
        name = f"iso_dk_{self.run}_{key}".replace(" ", "_")
        if self.catalog.table_exists((NAMESPACE, name)):
            self.catalog.purge_table((NAMESPACE, name))
        tbl = self.catalog.create_table(
            (NAMESPACE, name), schema=iceberg_schema(), properties=properties or {}
        )
        self.created.append(name)
        tbl.append(rows(SEED))
        return name

    def final(self, table: str):
        return final_rows(self.catalog, table)

    def snapshots(self, table: str) -> int:
        return len(self.catalog.load_table((NAMESPACE, table)).snapshots())

    def b(self, kind: str, table: str):
        return inject_b(self.catalog, table, kind)

    def commits(self, table: str) -> str:
        seen = self.proxy.commits.get((NAMESPACE, table), [])
        return "[" + ", ".join(str(s) for s, _ in seen) + "]" if seen else "[none]"

    # -- part 1 ----------------------------------------------------------------------------------

    def race(self, race: Race, level: str, inject: bool) -> None:
        _say(f"\n[{race.key} / {level}] {race.title}")
        self.refresh()
        table = self.table(f"{race.key}_{level}", CONFIGS[level])
        if inject:
            self.proxy.arm(NAMESPACE, table, self.b(race.inject, table))
        raised = ""
        conn = self.conn()
        try:
            for statement in race.statements:
                conn.execute(statement.format(t=self.t(table)))
        except Exception as exc:  # noqa: BLE001 - DuckDB's error is the reading
            raised = _err(exc)
        finally:
            conn.close()
        final = self.final(table)
        statuses = self.proxy.statuses(NAMESPACE, table)
        detail = f"final {final}; commits {self.commits(table)}"
        if raised:
            detail += f"; raised {raised}"
        if not inject:
            outcome = "ok" if (not raised and final == race.solo) else "WRONG"
        elif statuses and self.proxy.injected.get((NAMESPACE, table)) != "landed":
            outcome = "broken"
            detail += f"; B {self.proxy.injected.get((NAMESPACE, table))}"
        else:
            outcome = classify(race, final, bool(raised), statuses)
        self.levels[(race.key, level)] = (outcome, detail)
        _say(f"     {outcome:<9} {detail}")

    # -- part 2 ----------------------------------------------------------------------------------

    def tx(self, key: str, title: str, fn) -> None:
        _say(f"\n[tx {key}] {title}")
        self.refresh()
        try:
            outcome, detail = fn()
        except Exception as exc:  # noqa: BLE001 - a probe that fell over says so
            outcome, detail = "broken", _err(exc, 500)
        self.transactions.append((key, outcome, detail))
        _say(f"     {outcome:<12} {detail}")

    @staticmethod
    def commit(conn) -> str:
        try:
            conn.execute("COMMIT")
            return ""
        except Exception as exc:  # noqa: BLE001
            return _err(exc, 260)

    def count(self, conn, table: str) -> int:
        return conn.execute(f"SELECT count(*) FROM {self.t(table)}").fetchall()[0][0]

    def tx_repeatable_read(self):
        def run():
            table = self.table("tx_rr")
            conn = self.conn()
            conn.execute("BEGIN")
            first = self.count(conn, table)
            self.b("append", table)()
            second = self.count(conn, table)
            error = self.commit(conn)
            conn.close()
            outcome = "repeatable" if first == second else "reads latest"
            return outcome, f"reads {first}, B appends, reads {second}; COMMIT {error or 'ok'}"

        return run

    def tx_snapshot_point(self):
        def run():
            table = self.table("tx_point")
            conn = self.conn()
            conn.execute("BEGIN")
            conn.execute("SELECT 42").fetchall()
            self.b("append", table)()
            first = self.count(conn, table)
            self.b("append", table)()
            second = self.count(conn, table)
            self.commit(conn)
            conn.close()
            outcome = "at BEGIN" if first == 3 else "at first read"
            return outcome, (
                f"BEGIN, B appends, the first read sees {first} rows; B appends again, "
                f"the next read sees {second}"
            )

        return run

    def tx_rollback(self):
        table = self.table("tx_rollback")
        before = self.snapshots(table)
        conn = self.conn()
        conn.execute("BEGIN")
        conn.execute(f"INSERT INTO {self.t(table)} VALUES (5, 50)")
        conn.execute(f"DELETE FROM {self.t(table)} WHERE id = 1")
        conn.execute("ROLLBACK")
        conn.close()
        added, final = self.snapshots(table) - before, self.final(table)
        outcome = "nothing sent" if (added == 0 and final == SEED) else "LEAKED"
        return outcome, f"INSERT + DELETE, ROLLBACK; commits {self.commits(table)}; {final}"

    def tx_failing_statement(self):
        table = self.table("tx_fail")
        conn = self.conn()
        conn.execute("BEGIN")
        conn.execute(f"INSERT INTO {self.t(table)} VALUES (5, 50)")
        try:
            conn.execute(f"INSERT INTO {self.t(table)} VALUES ('not a number', 1)")
            second = "accepted"
        except Exception as exc:  # noqa: BLE001
            second = f"failed ({_err(exc, 90)})"
        error = self.commit(conn)
        conn.close()
        final = self.final(table)
        outcome = "all rolled back" if final == SEED else "partial commit"
        return outcome, f"INSERT ok, then INSERT {second}; COMMIT {error or 'ok'}; {final}"

    def tx_visibility(self):
        table = self.table("tx_visible")
        conn = self.conn()
        conn.execute("BEGIN")
        conn.execute(f"INSERT INTO {self.t(table)} VALUES (5, 50)")
        inside = self.count(conn, table)
        other = self.conn()
        outside = self.count(other, table)
        other.close()
        error = self.commit(conn)
        conn.close()
        outcome = "yes" if (inside, outside) == (4, 3) else "no"
        return outcome, (
            f"uncommitted INSERT: this transaction reads {inside}, another connection "
            f"{outside}; COMMIT {error or 'ok'}; catalog {len(self.final(table))} rows"
        )

    def tx_two_connections(self):
        table = self.table("tx_two_conn")
        first, second = self.conn(), self.conn()
        for c in (first, second):
            c.execute("BEGIN")
            c.execute(f"SELECT v FROM {self.t(table)} WHERE id = 1").fetchall()
        first.execute(f"UPDATE {self.t(table)} SET v = v + 1 WHERE id = 1")
        second.execute(f"UPDATE {self.t(table)} SET v = v + 100 WHERE id = 1")
        e1, e2 = self.commit(first), self.commit(second)
        first.close()
        second.close()
        final = self.final(table)
        v = dict(final).get(1)
        outcome = {11: "second refused", 111: "second retried", 110: "first lost"}.get(v, "corrupt")
        return outcome, (
            f"both read v = 10, +1 and +100; first COMMIT {e1 or 'ok'}; second COMMIT "
            f"{e2 or 'ok'}; {final}"
        )

    # -- part 3 ----------------------------------------------------------------------------------

    def state(self, table: str):
        """(columns, sorted rows) as the catalog holds the table, or None if it is gone."""
        if not self.catalog.table_exists((NAMESPACE, table)):
            return None
        tbl = self.catalog.load_table((NAMESPACE, table))
        cols = tuple(f.name for f in tbl.schema().fields)
        found = tbl.scan().to_arrow().to_pylist()
        return cols, sorted((tuple(r[c] for c in cols) for r in found), key=repr)

    def combo(self, combo) -> None:
        _say(f"\n[combo {combo.key}] {combo.title}")
        self.refresh()
        t = self.table(f"cb_{combo.key}")
        n = f"{t}_new"
        if self.catalog.table_exists((NAMESPACE, n)):
            self.catalog.purge_table((NAMESPACE, n))
        self.created.append(n)
        before = self.state(t)
        mark = len(self.proxy.log)
        conn = self.conn()
        failed = ""
        conn.execute("BEGIN")
        for statement in combo.statements:
            try:
                conn.execute(statement.format(t=self.t(t), n=self.t(n)))
            except Exception as exc:  # noqa: BLE001 - the refusal is the reading
                failed = f"{statement.split()[0]} refused: {_err(exc, 300)}"
                break
        if failed:
            with contextlib.suppress(Exception):
                conn.execute("ROLLBACK")
        conn.close()
        sent = [
            f"{m} {p.rsplit('/', 1)[-1].replace(n, 'n').replace(t, 't')} {s}"
            for m, p, s, _ in self.proxy.log[mark:]
            if m in ("POST", "DELETE")
        ]
        got_t, got_n = self.state(t), self.state(n)

        def want(spec):
            return (tuple(spec[0]), sorted(map(tuple, spec[1]), key=repr)) if spec else None

        want_t, want_n = want(combo.t), want(combo.n)
        spec = ""
        if combo.spec and got_t:
            fields = self.catalog.load_table((NAMESPACE, t)).spec().fields
            spec = str([str(f.transform) for f in fields])
        if failed:
            outcome = "refused" if (got_t == before and got_n is None) else "PARTIAL"
        elif got_t == want_t and got_n == want_n and (not combo.spec or combo.spec in spec):
            outcome = "works"
        else:
            outcome = "WRONG"
        detail = f"t {got_t}; n {got_n}; sent {sent}"
        if spec:
            detail += f"; spec {spec}"
        if failed:
            detail = f"{failed}; {detail}"
        elif outcome == "WRONG":
            detail += f"; expected t {want_t}, n {want_n}"
        self.combos.append((combo.key, combo.title, outcome, detail))
        _say(f"     {outcome:<9} {detail}")

    # -- teardown --------------------------------------------------------------------------------

    def teardown(self) -> None:
        self.refresh()
        if self.keep:
            _say(f"\nCAPABILITY_KEEP=1; left {len(self.created)} table(s) behind")
            return
        left = []
        for name in self.created:
            try:
                if self.catalog.table_exists((NAMESPACE, name)):
                    self.catalog.purge_table((NAMESPACE, name))
            except Exception as exc:  # noqa: BLE001 - teardown is best effort, by design
                left.append(f"{name} ({_one_line(exc, 80)})")
        _say(
            f"\ndropped {len(self.created) - len(left)} table(s)"
            + (f"; left {left}" if left else "")
        )


TRANSACTIONS = [
    ("repeatable_read", "BEGIN; read; B appends; read again", lambda p: p.tx_repeatable_read()),
    (
        "snapshot_point",
        "B commits after BEGIN, before the first read",
        lambda p: p.tx_snapshot_point(),
    ),
    ("rollback", "INSERT + DELETE, then ROLLBACK", lambda p: p.tx_rollback),
    (
        "failing_statement",
        "a failing statement inside the transaction",
        lambda p: p.tx_failing_statement,
    ),
    (
        "visibility",
        "own uncommitted rows, inside and from another connection",
        lambda p: p.tx_visibility,
    ),
    (
        "two_connections",
        "two DuckDB transactions update the same row",
        lambda p: p.tx_two_connections,
    ),
]


# --------------------------------------------------------------------------------------------
# part 3: DDL and DML combined in one transaction, no concurrent writer
# --------------------------------------------------------------------------------------------

SEED_COLS = ("id", "v")


@dataclass(frozen=True)
class Combo:
    key: str
    title: str
    statements: tuple  # after BEGIN; ends with COMMIT or ROLLBACK. {t} seeded, {n} a new name
    t: tuple | None  # (columns, rows) the seeded table must hold afterwards; None = gone
    n: tuple | None = None  # the same for {n}; None = must not exist
    spec: str = ""  # a partition transform {t}'s spec must carry afterwards


UNCHANGED = (SEED_COLS, SEED)
DOUBLED_T = (SEED_COLS, DOUBLED)

COMBOS = [
    Combo(
        "drop_create",
        "DROP TABLE, CREATE TABLE the same name with a new column, INSERT",
        (
            "DROP TABLE {t}",
            "CREATE TABLE {t} (id BIGINT, v BIGINT, w BIGINT)",
            "INSERT INTO {t} VALUES (9, 90, 900)",
            "COMMIT",
        ),
        (("id", "v", "w"), [(9, 90, 900)]),
    ),
    Combo(
        "create_or_replace_self",
        "CREATE OR REPLACE TABLE t AS SELECT ... FROM t",
        ("CREATE OR REPLACE TABLE {t} AS SELECT id, v * 2 AS v FROM {t}", "COMMIT"),
        DOUBLED_T,
    ),
    Combo(
        "create_insert",
        "CREATE TABLE, INSERT",
        ("CREATE TABLE {n} (id BIGINT, v BIGINT)", "INSERT INTO {n} VALUES (1, 1)", "COMMIT"),
        UNCHANGED,
        (SEED_COLS, [(1, 1)]),
    ),
    Combo(
        "ctas",
        "CREATE TABLE AS SELECT from the seeded table",
        ("CREATE TABLE {n} AS SELECT id, v * 2 AS v FROM {t}", "COMMIT"),
        UNCHANGED,
        DOUBLED_T,
    ),
    Combo(
        "add_column_insert",
        "ADD COLUMN, INSERT a row that fills it",
        ("ALTER TABLE {t} ADD COLUMN w BIGINT", "INSERT INTO {t} VALUES (4, 40, 400)", "COMMIT"),
        (("id", "v", "w"), [(1, 10, None), (2, 20, None), (3, 30, None), (4, 40, 400)]),
    ),
    Combo(
        "add_column_update",
        "ADD COLUMN, UPDATE it",
        ("ALTER TABLE {t} ADD COLUMN w BIGINT", "UPDATE {t} SET w = v * 10", "COMMIT"),
        (("id", "v", "w"), [(1, 10, 100), (2, 20, 200), (3, 30, 300)]),
    ),
    Combo(
        "rename_column_insert",
        "RENAME COLUMN, INSERT",
        ("ALTER TABLE {t} RENAME COLUMN v TO v2", "INSERT INTO {t} VALUES (4, 40)", "COMMIT"),
        (("id", "v2"), [*SEED, (4, 40)]),
    ),
    Combo(
        "drop_column_insert",
        "DROP COLUMN, INSERT",
        ("ALTER TABLE {t} DROP COLUMN v", "INSERT INTO {t} VALUES (4)", "COMMIT"),
        (("id",), [(1,), (2,), (3,), (4,)]),
    ),
    Combo(
        "partition_insert",
        "SET PARTITIONED BY (bucket(4, id)), INSERT",
        (
            "ALTER TABLE {t} SET PARTITIONED BY (bucket(4, id))",
            "INSERT INTO {t} VALUES (4, 40)",
            "COMMIT",
        ),
        (SEED_COLS, [*SEED, (4, 40)]),
        spec="bucket",
    ),
    Combo(
        "truncate_insert_rollback",
        "TRUNCATE, INSERT, ROLLBACK",
        ("TRUNCATE {t}", "INSERT INTO {t} VALUES (9, 90)", "ROLLBACK"),
        UNCHANGED,
    ),
    Combo(
        "drop_rollback",
        "DROP TABLE, ROLLBACK",
        ("DROP TABLE {t}", "ROLLBACK"),
        UNCHANGED,
    ),
    Combo(
        "drop_create_rollback",
        "DROP TABLE, CREATE TABLE the same name, ROLLBACK",
        ("DROP TABLE {t}", "CREATE TABLE {t} (id BIGINT, v BIGINT, w BIGINT)", "ROLLBACK"),
        UNCHANGED,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-race", action="store_true", help="run each race's statements alone")
    parser.add_argument(
        "--only",
        help="comma-separated race / transaction / combination keys, or levels / tx / combos",
    )
    args = parser.parse_args()
    only = set(args.only.split(",")) if args.only else None

    def wanted(key: str, part: str) -> bool:
        return only is None or key in only or part in only

    probe = DuckDBIsolation(Config.from_env())
    _say(f"duckdb {probe.version} through {probe.proxy.endpoint}")
    try:
        levels = ["serializable"] if args.no_race else list(CONFIGS)
        for race in RACES:
            if wanted(race.key, "levels"):
                for level in levels:
                    probe.race(race, level, inject=not args.no_race)
        if not args.no_race:
            for key, title, fn in TRANSACTIONS:
                if wanted(key, "tx"):
                    probe.tx(key, title, fn(probe))
            for combo in COMBOS:
                if wanted(combo.key, "combos"):
                    probe.combo(combo)
    finally:
        try:
            probe.teardown()
        finally:
            probe.proxy.close()

    titles = {key: title for key, title, _ in TRANSACTIONS}
    save_results(
        {
            "engine": "duckdb_isolation",
            "version": probe.version,
            "configs": CONFIGS,
            "levels": {
                f"{key}/{level}": {"outcome": outcome, "detail": detail}
                for (key, level), (outcome, detail) in probe.levels.items()
            },
            # The readme's concurrency rows, in DuckDB's column.
            "rows": matrix_rows(probe.levels),
            "transactions": [
                {"key": key, "title": titles[key], "outcome": outcome, "detail": detail}
                for key, outcome, detail in probe.transactions
            ],
            "combos": [
                {"key": key, "title": title, "outcome": outcome, "detail": detail}
                for key, title, outcome, detail in probe.combos
            ],
        }
    )
    if probe.levels:
        _say("\n| DuckDB writes, B commits in between | " + " | ".join(levels) + " |")
        _say("|---|" + "---|" * len(levels))
        for race in RACES:
            cells = [probe.levels.get((race.key, lv), ("—", ""))[0] for lv in levels]
            if any(c != "—" for c in cells):
                _say(f"| {race.title} | " + " | ".join(cells) + " |")
    if probe.transactions:
        _say("\n| Transaction | Outcome | What came back |\n|---|---|---|")
        for key, outcome, detail in probe.transactions:
            _say(f"| {key} | {outcome} | {detail.replace('|', '/')[:300]} |")
    if probe.combos:
        _say("\n| In one transaction | Outcome | What came back |\n|---|---|---|")
        for _key, title, outcome, detail in probe.combos:
            _say(f"| {title} | {outcome} | {detail.replace('|', '/')[:400]} |")
    return 0


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    os._exit(code)
