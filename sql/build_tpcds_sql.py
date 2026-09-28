"""Write sql/tpcds.sql from DuckDB's own copy of the 99 TPC-DS queries. Run once, commit the output.

    python sql/build_tpcds_sql.py      # needs the duckdb wheel; installs the tpcds extension

WHAT IT DOES. `tpcds_queries()` returns the 99 statements in DuckDB's dialect with BARE table
names. bench/tpcds/rewrite.py qualifies every table reference as `` `{schema}.<table>` `` -- the
sql/tpch.sql convention bench/tpch/queries.py rewrites per engine -- and this script writes the
result, one `-- Query NN` header per statement, `;`-separated.

THE CHECK, which is what makes a tokenizer acceptable where a parser would be safer. SF=1 is
generated in a scratch DuckDB and every rewritten statement is run TWICE more than the original:

* in the dotted style against a real `DS0001` schema (what DuckDB, Spark and Sail see), and
* in the double-quoted style against views literally named `"DS0001.store_sales"` in the default
  schema -- the flat namespace chDB, Polars and Daft see, where a column qualified by a bare
  table name only resolves if the reference carries an alias.

All three row counts must agree for all 99. A rewrite that touched anything but a table
reference, or missed an alias, changes a count or fails to bind. Four rules in rewrite.py were
found by this check, not assumed.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bench.tpcds.config import TABLES, TpcdsConfig  # noqa: E402
from bench.tpcds.generate import load_tpcds_extension  # noqa: E402
from bench.tpcds.rewrite import portable, qualify  # noqa: E402
from bench.tpch import queries  # noqa: E402

OUT = Path(__file__).resolve().with_name("tpcds.sql")
CHECK_SCHEMA = "DS0001"
SRC_SCHEMA = "src"  # where the bare tables go, out of the default search path


def fetch(con) -> list[tuple[int, str]]:
    rows = con.execute("SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr").fetchall()
    assert len(rows) == TpcdsConfig.N_QUERIES, f"expected 99 queries, got {len(rows)}"
    return rows


def build(rows: list[tuple[int, str]]) -> str:
    chunks = []
    for number, sql in rows:
        body = sql.strip().rstrip(";").strip()
        assert ";" not in body, f"q{number} has a semicolon inside it; the loader splits on `;`"
        assert "{" not in body and "}" not in body, f"q{number} has a brace; render() uses format"
        chunks.append(f"-- Query {number:02d}\n{portable(qualify(body))};\n")
    return "\n".join(chunks)


def check(con, rows: list[tuple[int, str]], text: str) -> None:
    """Original, dotted and flat-namespace spellings must return the same row count, at SF=1.

    The generated tables are moved OUT of the default schema first, so a reference the rewrite
    missed cannot quietly bind to the bare table and pass: it fails with "table not found".
    """
    con.sql("CALL dsdgen(sf = 1)")
    con.sql(f"CREATE SCHEMA {SRC_SCHEMA}")
    con.sql(f"CREATE SCHEMA {CHECK_SCHEMA}")
    for table in TABLES:
        con.sql(f"CREATE TABLE {SRC_SCHEMA}.{table} AS SELECT * FROM main.{table}")
        con.sql(f"DROP TABLE main.{table}")
        con.sql(f"CREATE VIEW {CHECK_SCHEMA}.{table} AS SELECT * FROM {SRC_SCHEMA}.{table}")
        # One identifier with a dot in it: what the backticked/quoted engines register.
        con.sql(f'CREATE VIEW "{CHECK_SCHEMA}.{table}" AS SELECT * FROM {SRC_SCHEMA}.{table}')
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "tpcds.sql"
        path.write_text(text, encoding="utf-8")
        dotted = queries.load("duckdb_iceberg", CHECK_SCHEMA, 1, path, TpcdsConfig.N_QUERIES)
        flat = queries.load("daft_iceberg", CHECK_SCHEMA, 1, path, TpcdsConfig.N_QUERIES)
    for (number, original), a, b in zip(rows, dotted, flat, strict=True):
        con.sql(f"SET search_path = '{SRC_SCHEMA}'")
        want = len(con.execute(original).fetchall())
        con.sql("SET search_path = 'main'")
        got_dotted = len(con.execute(a).fetchall())
        got_flat = len(con.execute(b).fetchall())
        assert want == got_dotted == got_flat, (
            f"q{number}: {want} rows originally, {got_dotted} dotted, {got_flat} flat"
        )
        print(f"  q{number:02d} {want:>6} rows", flush=True)


def main() -> int:
    import duckdb

    con = duckdb.connect()
    load_tpcds_extension(con)
    rows = fetch(con)
    text = build(rows)
    assert qualify(text) == text, "the rewrite is not idempotent on its own output"
    assert portable(text) == text, "portable() is not idempotent on its own output"
    print(f"checking {len(rows)} rewritten statements against the originals at SF=1")
    check(con, rows, text)
    OUT.write_text(text, encoding="utf-8", newline="\n")
    print(f"wrote {OUT} ({len(text):,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
