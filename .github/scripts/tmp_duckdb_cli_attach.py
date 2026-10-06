"""TEMPORARY: where does the nightly DuckDB CLI hang on OneLake? Delete with its workflow.

Smoke 37425114956 and ETL 37425193969 both hung inside DuckDB setup() (after the token, before
"attached"). Stages, each in a fresh CLI fed a file on stdin with a hard timeout, so the first
stage that times out names the statement. Then the same through bench.duckdb_cli (the stdin
protocol), then through the pip wheel if it is installed.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import threading
import time

from bench import auth, scrub
from bench.config import ICEBERG_ENDPOINT, Config, azure_transport

TABLE = "DS0001.store_sales"


def statements(cfg: Config, token: str) -> list[tuple[str, str]]:
    return [
        (
            "transport",
            f"SET GLOBAL azure_transport_option_type = '{azure_transport() or 'default'}';",
        ),
        (
            "secret",
            "CREATE OR REPLACE SECRET onelake_storage (TYPE azure, PROVIDER access_token, "
            f"ACCESS_TOKEN '{token}');",
        ),
        (
            "attach",
            f"ATTACH OR REPLACE '{cfg.warehouse}' AS onelake (TYPE ICEBERG, "
            f"ENDPOINT '{ICEBERG_ENDPOINT}', TOKEN '{token}', ACCESS_DELEGATION_MODE 'none');",
        ),
        ("read", f"SELECT count(*) FROM onelake.{TABLE};"),
    ]


def cli_stages(stmts) -> None:
    for upto in range(1, len(stmts) + 1):
        name = stmts[upto - 1][0]
        body = "\n".join(s for _, s in stmts[:upto])
        sql = f".timer on\n{body}\nSELECT 'stage {name} done';\n"
        with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as handle:
            handle.write(sql)
        started = time.perf_counter()
        try:
            with open(handle.name) as stdin:
                out = subprocess.run(
                    ["duckdb", "-batch"], stdin=stdin, capture_output=True, text=True, timeout=180
                )
            print(f"[cli] {name}: exit {out.returncode} in {time.perf_counter() - started:.1f}s")
            print(scrub.scrub(out.stdout)[-1500:])
            print(scrub.scrub(out.stderr)[-1500:])
        except subprocess.TimeoutExpired as exc:
            print(f"[cli] {name}: TIMEOUT after 180s")
            print(scrub.scrub(exc.stdout or "")[-1500:])
            print(scrub.scrub(exc.stderr or "")[-1500:])
        os.unlink(handle.name)


def with_timeout(label: str, fn, seconds: int = 180) -> None:
    box = {}

    def run():
        try:
            box["value"] = fn()
        except Exception as exc:  # noqa: BLE001
            box["error"] = scrub.scrub_exc(exc, 1500)

    started = time.perf_counter()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(seconds)
    took = time.perf_counter() - started
    if thread.is_alive():
        print(f"[{label}] TIMEOUT after {seconds}s")
    else:
        print(f"[{label}] {took:.1f}s -> {box}")


def protocol(stmts) -> None:
    from bench.duckdb_cli import DuckDBCli

    conn = DuckDBCli()
    for name, sql in stmts:
        with_timeout(f"protocol {name}", lambda sql=sql: conn.sql(sql).fetchall())


def wheel(stmts) -> None:
    try:
        import duckdb
    except ImportError:
        print("[wheel] not installed")
        return
    print(f"[wheel] duckdb {duckdb.__version__}")
    conn = duckdb.connect()
    for name, sql in stmts:
        with_timeout(f"wheel {name}", lambda sql=sql: conn.execute(sql).fetchall())


def main() -> int:
    cfg = Config.from_env()
    token = auth.onelake_token()
    stmts = statements(cfg, token)
    print(subprocess.run(["duckdb", "--version"], capture_output=True, text=True).stdout)
    mode = sys.argv[1] if len(sys.argv) > 1 else "cli"
    {"cli": cli_stages, "protocol": protocol, "wheel": wheel}[mode](stmts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
