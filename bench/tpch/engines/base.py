"""The contract every engine implements.

REPLACES the execution half of cell 15, which was:

    if engine_lower == 'chdb_iceberg':
        print(conn.query(value, 'Pretty'))
    else:
        conn.sql(value).show()

That is not a fair measurement, for three separate reasons, and `execute` exists to fix all three:

1. `pl.sql(...)` returns a LAZYFRAME. Without `.collect()` the timer measures PLAN CONSTRUCTION --
   microseconds -- and not the query. Every Polars number the notebook ever produced for a lazy
   path is meaningless.
2. Spark Connect's `.show()` implies `limit(20)`. Q2, Q3, Q10, Q18 and Q21 end in `LIMIT 100`, so
   LakeSail was asked for 20 rows where the others computed 100. Under-execution, silently.
3. chDB's `'Pretty'` format renders an ASCII table INSIDE the timed window, and echoes the
   statement on error -- which, for the attach, is the bearer token (see bench/scrub.py).

So `execute` returns a ROW COUNT and every implementation must fully materialize to produce it.
All 22 results are <=100 rows, so collecting them costs nothing and buys a correctness signal a
timing chart can never give you: if two engines disagree about cardinality, one of them is wrong.
"""

from __future__ import annotations

import time
from typing import Protocol

from bench import auth, scrub
from bench.config import TOKEN_MIN_LIFETIME_SECONDS


class Engine(Protocol):
    """One embedded query engine, attached to the OneLake Iceberg REST catalog."""

    name: str

    @property
    def version(self) -> str:
        """The resolved package version, recorded with every result.

        Not cosmetic: duckdb tracks the nightly CLI and polars the pre-release line, so what
        resolved differs run to run. A benchmark chart without the version that produced it is
        an assertion rather than a result.
        """
        ...

    def setup(self) -> None:
        """Attach the catalog. Timed as its own row (`phase='setup'`, `query=0`).

        Raising from here is fatal to the job -- an engine that cannot attach has no numbers to
        report. Contrast a failing QUERY, which is recorded as `status='error'` and does not stop
        the other 21.
        """
        ...

    def execute(self, sql: str) -> int:
        """Run one statement to completion and return its row count."""
        ...

    def refresh(self) -> None:
        """Renew the OneLake token. EVERY engine has one (tests/test_refresh.py checks).

        Called by the runner before every statement, OUTSIDE the timer. An engine holds the token
        as a string from setup, so once less than config.TOKEN_MIN_LIFETIME_SECONDS of it
        remains it must put a fresh one in place, or a pass longer than the token's hour dies
        `Unauthorized` partway through. Above that threshold it must cost nothing -- a clock
        comparison. An engine that can swap the token in place does (DuckDB's secret, StarRocks'
        and Trino's catalog); one that cannot restarts with `restart_on_fresh_token`.
        """
        ...

    def close(self) -> None:
        """Release whatever the engine holds. Must be safe to call twice."""
        ...


def restart_on_fresh_token(engine, expires: float) -> None:
    """`refresh` for an engine that can only take a token at setup: close, re-mint, set up again.

    `expires` is the token's expiry as setup saw it (`auth.token_expires_on()` right after
    minting). The restart is untimed, like every refresh.
    """
    if expires - time.time() > TOKEN_MIN_LIFETIME_SECONDS:
        return
    start = time.perf_counter()
    engine.close()
    auth.onelake_token(fresh=True)
    engine.setup()
    scrub.safe_print(
        f"  credentials within {TOKEN_MIN_LIFETIME_SECONDS // 60} min of expiry: "
        f"{engine.name} restarted in {time.perf_counter() - start:.1f}s"
    )
