"""Why do the legs' fct_summary decimals differ in the last place? Row-level evidence.

    python .github/scripts/workload_diff.py

The parity grade compares SUMS, which says THAT Polars lands lower than Spark (fabric-dbt-examples
run 39257525479: mw_sum -7.1, price_sum -6.3 over 3.08M rows, 10x the DuckDB legs' gap) but not
WHY. This reads every leg's fct_summary back through DuckDB and, for each pair of legs:

  1. joins them on the grain (date, time, DUID) and counts the rows whose mw / price differ
  2. tabulates the differences -- a cast that ROUNDS differently on exact ties gives +-0.0001
     on a handful of rows in both directions; one that TRUNCATES gives -0.0001 on many rows,
     one direction only
  3. for a sample of differing rows, recovers the DOUBLE mw was cast from (MAX(INITIALMW) over
     the duckdb leg's fct_scada, the same aggregate fct_summary takes) and casts that exact
     double to DECIMAL(18,4) in DuckDB, in Polars (Series.cast and SQL CAST) and by hand
     (exact binary value, half-up, half-even, truncation), side by side

Read-only. Prints to stdout and the job summary; writes nothing to OneLake.
"""

from __future__ import annotations

import itertools
import os
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, ROUND_HALF_UP, Decimal

from bench import auth, scrub
from bench.config import ICEBERG_ENDPOINT
from bench.workload.config import ENGINES, WorkloadConfig

SAMPLE = 15
Q = Decimal("0.0001")

out: list[str] = []


def say(line: str = "") -> None:
    scrub.safe_print(line)
    out.append(scrub.scrub(line))


def connect(cfg: WorkloadConfig):
    import duckdb

    token = auth.onelake_token(fresh=True)
    con = duckdb.connect()
    con.sql("INSTALL iceberg; LOAD iceberg; INSTALL azure; LOAD azure;")
    transport = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE", "curl")
    con.sql(f"SET azure_transport_option_type = '{transport}'")
    con.sql(
        f"CREATE SECRET onelake_storage (TYPE azure, PROVIDER access_token, ACCESS_TOKEN '{token}')"
    )
    con.sql(
        f"ATTACH '{cfg.warehouse}' AS onelake (TYPE ICEBERG, ENDPOINT '{ICEBERG_ENDPOINT}', "
        f"TOKEN '{token}', ACCESS_DELEGATION_MODE 'none')"
    )
    return con


def summary_of(engine: str) -> str:
    return f"onelake.dbt_{engine}_mart.fct_summary"


def exists(con, engine: str) -> bool:
    try:
        con.sql(f"SELECT 1 FROM {summary_of(engine)} LIMIT 1").fetchall()
        return True
    except Exception as exc:  # noqa: BLE001 - a leg that never built has no table
        say(f"- {engine}: no fct_summary ({scrub.scrub_exc(exc, 200)})")
        return False


def compare(con, a: str, b: str) -> list[tuple]:
    """Counts and the difference histogram for one pair; returns differing mw rows."""
    ta, tb = summary_of(a), summary_of(b)
    joined = f"{ta} x JOIN {tb} y USING (date, time, DUID)"
    n, only_a, only_b = con.sql(
        f"""SELECT (SELECT count(*) FROM {joined}),
                   (SELECT count(*) FROM {ta}) - (SELECT count(*) FROM {joined}),
                   (SELECT count(*) FROM {tb}) - (SELECT count(*) FROM {joined})"""
    ).fetchone()
    say(f"### {a} vs {b}")
    say(f"{n:,} rows on the grain; {only_a:,} only in {a}, {only_b:,} only in {b}")
    for col in ("mw", "price"):
        rows = con.sql(
            f"""SELECT CAST(x.{col} - y.{col} AS VARCHAR) AS d, count(*) AS n
                FROM {joined} WHERE x.{col} <> y.{col} GROUP BY 1 ORDER BY n DESC LIMIT 8"""
        ).fetchall()
        total = sum(r[1] for r in rows)
        hist = ", ".join(f"{d}: {c:,}" for d, c in rows) or "none"
        say(f"- {col} ({a} - {b}): {total:,} rows differ (top values) -> {hist}")
    return con.sql(
        f"""SELECT date, time, DUID, CAST(x.mw AS VARCHAR), CAST(y.mw AS VARCHAR)
            FROM {joined} WHERE x.mw <> y.mw ORDER BY date DESC, time, DUID LIMIT {SAMPLE}"""
    ).fetchall()


def source_doubles(con, keys: list[tuple]) -> dict[tuple, float]:
    """MAX(INITIALMW) per (date, time, DUID) from the duckdb leg's fct_scada: the DOUBLE that
    fct_summary's daily branch casts to DECIMAL(18,4)."""
    if not keys:
        return {}
    values = ", ".join(f"(DATE '{d}', {t}, '{u}')" for d, t, u in keys)
    rows = con.sql(
        f"""WITH k(date, time, DUID) AS (VALUES {values})
            SELECT k.date, k.time, k.DUID, max(s.INITIALMW)
            FROM k JOIN onelake.dbt_duckdb_landing.fct_scada s
              ON s.DATE = k.date AND s.DUID = k.DUID
             AND CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) = k.time
            WHERE s.INTERVENTION = 0
            GROUP BY ALL"""
    ).fetchall()
    return {(str(d), t, u): v for d, t, u, v in rows}


def cast_probe(con, doubles: list[float]) -> None:
    """The same doubles through each engine's DOUBLE -> DECIMAL(18,4), and by hand."""
    import polars as pl

    say("")
    say(
        "| double (repr) | exact binary value | half-up | half-even | truncate "
        "| DuckDB | Polars cast | Polars SQL |"
    )
    say("|---|---|---|---|---|---|---|---|")
    series = pl.Series("x", doubles, dtype=pl.Float64)
    pl_cast = series.cast(pl.Decimal(18, 4)).cast(pl.String).to_list()
    pl_sql = (
        pl.SQLContext(t=pl.DataFrame({"x": series}))
        .execute("SELECT CAST(x AS DECIMAL(18, 4)) AS d FROM t", eager=True)["d"]
        .cast(pl.String)
        .to_list()
    )
    for i, x in enumerate(doubles):
        exact = Decimal(x)
        duck = con.execute(
            "SELECT CAST(CAST(? AS DOUBLE) AS DECIMAL(18, 4))::VARCHAR", [x]
        ).fetchone()[0]
        say(
            f"| {x!r} | {str(exact)[:32]} | {exact.quantize(Q, ROUND_HALF_UP)} "
            f"| {exact.quantize(Q, ROUND_HALF_EVEN)} | {exact.quantize(Q, ROUND_DOWN)} "
            f"| {duck} | {pl_cast[i]} | {pl_sql[i]} |"
        )


def main() -> None:
    import polars as pl

    cfg = WorkloadConfig.from_env()
    con = connect(cfg)
    say("## Decimal differences, row by row")
    say(f"polars {pl.__version__}")
    legs = [e for e in ENGINES if exists(con, e)]
    sample: dict[tuple, tuple] = {}
    for a, b in itertools.combinations(legs, 2):
        for d, t, u, xa, xb in compare(con, a, b):
            sample.setdefault((str(d), t, u), (a, xa, b, xb))
        say("")
    if not sample:
        say("No differing mw rows in any pair.")
    else:
        doubles = source_doubles(con, list(sample))
        say("### Sample differing rows, with the double they were cast from")
        say("| date | time | DUID | leg a | leg b | source double |")
        say("|---|---|---|---|---|---|")
        for key, (a, xa, b, xb) in sample.items():
            say(f"| {key[0]} | {key[1]} | {key[2]} | {a} {xa} | {b} {xb} | {doubles.get(key)!r} |")
        cast_probe(con, [v for v in doubles.values() if v is not None])
    # Doubles that are NOT near a tie, to show truncation on its own, independent of the data.
    say("")
    say("### Control: doubles one ulp below a 4-decimal value")
    cast_probe(con, [12.3456, 0.1 + 0.2, 1.0005, 2.675, 100.00015, -3.14159])
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write("\n".join(out) + "\n")


if __name__ == "__main__":
    main()
