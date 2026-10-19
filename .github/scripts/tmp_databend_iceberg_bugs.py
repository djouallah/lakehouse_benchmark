"""TEMPORARY. Upstream repros for the Databend Iceberg bugs the capability probe found on OneLake
(run 39032752740), asked again on a plain Iceberg REST catalog (apache/iceberg-rest-fixture, file://
warehouse at /tmp/wh seen by the catalog, pyiceberg and Databend alike) -- no OneLake, no probe
harness -- so a failure here is Databend's. pyiceberg reads back what the catalog holds.
"""

from __future__ import annotations

import subprocess
import sys
import time

import pymysql
from pyiceberg.catalog import load_catalog

IMAGE = sys.argv[1]
WH = "/tmp/wh"
CAT = load_catalog("rest", uri="http://localhost:8181")
ATTACH = (
    "CREATE CATALOG ice TYPE=ICEBERG CONNECTION=(TYPE='rest' "
    f"ADDRESS='http://localhost:8181' WAREHOUSE='file://{WH}' \"root\"='/')"
)


def start():
    subprocess.run(
        ["docker", "run", "-d", "--name", "db", "--network", "host", "-v", f"{WH}:{WH}", IMAGE],
        check=True,
    )
    deadline = time.time() + 300
    while True:
        try:
            return pymysql.connect(host="127.0.0.1", port=3307, user="root", autocommit=True)
        except Exception:  # noqa: BLE001 - not up yet
            if time.time() > deadline:
                raise
            time.sleep(3)


def q(conn, sql: str):
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        print(f"  OK    {sql}\n          -> {rows}")
        return rows
    except Exception as exc:  # noqa: BLE001 - reporting is the point
        print(f"  ERROR {sql}\n          -> {exc}")
        return None


def py(table: str) -> str:
    try:
        t = CAT.load_table(f"demo.{table}")
        rows = sorted(tuple(r.values()) for r in t.scan().to_arrow().to_pylist())
        return (
            f"pyiceberg: schema {[f.name for f in t.schema().fields]}, "
            f"spec {[str(f) for f in t.spec().fields]}, "
            f"{len(t.metadata.snapshots)} snapshot(s), rows {rows}"
        )
    except Exception as exc:  # noqa: BLE001
        return f"pyiceberg: {exc}"


def case(title: str) -> None:
    print(f"\n=== {title}")


def main() -> int:
    CAT.create_namespace_if_not_exists("demo")
    conn = start()
    q(conn, "SELECT version()")
    q(conn, ATTACH)
    q(conn, "USE CATALOG ice")

    case("control: CREATE TABLE + INSERT")
    q(conn, "CREATE TABLE demo.ctl (id BIGINT, v BIGINT) ENGINE = ICEBERG")
    q(conn, "INSERT INTO demo.ctl VALUES (1, 10), (2, 20), (3, 30)")
    print("  " + py("ctl"))

    case("1. CREATE TABLE ... AS SELECT")
    q(conn, "CREATE TABLE demo.ctas ENGINE = ICEBERG AS SELECT 1::BIGINT AS id, 10::BIGINT AS v")
    q(conn, "CREATE TABLE demo.ctas2 AS SELECT 1::BIGINT AS id, 10::BIGINT AS v")

    case("2. PARTITION BY (identity), then INSERT")
    q(
        conn,
        "CREATE TABLE demo.part (id BIGINT, v BIGINT, p VARCHAR) ENGINE = ICEBERG PARTITION BY (p)",
    )
    print("  " + py("part"))
    q(conn, "INSERT INTO demo.part VALUES (1, 10, 'a'), (2, 20, 'a'), (3, 30, 'b')")
    print("  " + py("part"))

    case("3. DECIMAL column")
    q(conn, "CREATE TABLE demo.dec (id BIGINT, x DECIMAL(10, 2)) ENGINE = ICEBERG")

    case("4. nested columns")
    q(conn, "CREATE TABLE demo.st (id BIGINT, x TUPLE(a BIGINT, b VARCHAR)) ENGINE = ICEBERG")
    q(conn, "CREATE TABLE demo.li (id BIGINT, x ARRAY(BIGINT)) ENGINE = ICEBERG")
    q(conn, "CREATE TABLE demo.mp (id BIGINT, x MAP(VARCHAR, BIGINT)) ENGINE = ICEBERG")

    case("5. TRUNCATE TABLE")
    q(conn, "CREATE TABLE demo.tr (id BIGINT, v BIGINT) ENGINE = ICEBERG")
    q(conn, "INSERT INTO demo.tr VALUES (1, 10), (2, 20), (3, 30)")
    q(conn, "TRUNCATE TABLE demo.tr")
    q(conn, "SELECT count(*) FROM demo.tr")
    print("  " + py("tr"))

    case("6. INSERT OVERWRITE")
    q(conn, "CREATE TABLE demo.ow (id BIGINT, v BIGINT) ENGINE = ICEBERG")
    q(conn, "INSERT INTO demo.ow VALUES (1, 10), (2, 20), (3, 30)")
    q(conn, "INSERT OVERWRITE demo.ow VALUES (7, 70), (8, 80)")
    q(conn, "SELECT * FROM demo.ow ORDER BY id")
    print("  " + py("ow"))

    case("7. ALTER TABLE ADD COLUMN / DROP COLUMN")
    q(conn, "CREATE TABLE demo.al (id BIGINT, v BIGINT) ENGINE = ICEBERG")
    q(conn, "INSERT INTO demo.al VALUES (1, 10)")
    q(conn, "ALTER TABLE demo.al ADD COLUMN w BIGINT")
    q(conn, "DESC demo.al")
    print("  " + py("al"))
    q(conn, "ALTER TABLE demo.al DROP COLUMN v")
    q(conn, "DESC demo.al")
    print("  " + py("al"))

    case("8. time travel AT (SNAPSHOT => ...)")
    q(conn, "CREATE TABLE demo.tt (id BIGINT, v BIGINT) ENGINE = ICEBERG")
    q(conn, "INSERT INTO demo.tt VALUES (1, 10), (2, 20), (3, 30)")
    first = CAT.load_table("demo.tt").current_snapshot().snapshot_id
    q(conn, "INSERT INTO demo.tt VALUES (4, 40)")
    print(f"  first snapshot {first}; " + py("tt"))
    q(conn, f"SELECT count(*) FROM demo.tt AT (SNAPSHOT => '{first}')")
    q(conn, "SELECT snapshot_id FROM iceberg_snapshot('demo', 'tt')")

    case("9. INSERT after the partition spec changed outside Databend")
    q(conn, "CREATE TABLE demo.ev (id BIGINT, v BIGINT) ENGINE = ICEBERG")
    q(conn, "INSERT INTO demo.ev VALUES (1, 10), (2, 20)")
    from pyiceberg.transforms import BucketTransform

    with CAT.load_table("demo.ev").update_spec() as spec:
        spec.add_field("id", BucketTransform(4), "id_bucket")
    print("  pyiceberg added bucket(4, id); " + py("ev"))
    q(conn, "INSERT INTO demo.ev VALUES (3, 30), (4, 40)")
    print("  re-attaching the catalog, so no table is cached:")
    q(conn, "USE CATALOG default")
    q(conn, "DROP CATALOG ice")
    q(conn, ATTACH)
    q(conn, "INSERT INTO ice.demo.ev VALUES (3, 30), (4, 40)")
    print("  " + py("ev"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
