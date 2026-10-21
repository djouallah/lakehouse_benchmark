"""Land the AEMO source files the dbt workload reads, into bench's lakehouse under Files/dbt.

A PORT OF fabric-dbt-examples' download_aemo.py (the commit in workload/SOURCE), so the landing
zone has exactly the layout its models read:

    Files/dbt/csv_raw/daily/PUBLIC_DAILY_*.CSV         price + scada records, one file a day
    Files/dbt/csv_raw/scada_today/PUBLIC_DISPATCHSCADA_*.CSV
    Files/dbt/csv_raw/price_today/PUBLIC_DISPATCHIS_*.CSV
    Files/dbt/csv_raw/duid/{duid_data,facilities,WA_ENERGY,geo_data}.csv
    Files/dbt/csv_raw_archive_log.parquet               one row per landed file: the watermark

The listing SQL, the newest-first order, the limits and the archive-log schema are the
source's, unchanged. Only the transport differs: uploads go through this repo's ADLS client on
the refreshing OIDC credential (bench/onelake.py), and the log is read and written as a local
file around it, so DuckDB here needs no azure extension and no storage secret.

Idempotent: a file already in the log is never fetched again.
"""

from __future__ import annotations

import io
import os
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

from bench import onelake, scrub
from bench.workload.config import WorkloadConfig

LOG = "csv_raw_archive_log.parquet"

DUID_SOURCES = [
    (
        "duid_data",
        "duid_data",
        "https://raw.githubusercontent.com/djouallah/aemo_data/refs/heads/main/duid_data.csv",
        "duid_data.csv",
    ),
    (
        "duid_facilities",
        "facilities",
        "https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv",
        "facilities.csv",
    ),
    (
        "duid_wa_energy",
        "WA_ENERGY",
        "https://raw.githubusercontent.com/djouallah/aemo_data/refs/heads/main/WA_ENERGY.csv",
        "WA_ENERGY.csv",
    ),
    (
        "duid_geo_data",
        "geo_data",
        "https://raw.githubusercontent.com/djouallah/aemo_data/refs/heads/main/geo_data.csv",
        "geo_data.csv",
    ),
]


class Lake:
    """The landing root in OneLake, as an upload/download pair over the ADLS client."""

    def __init__(self, cfg: WorkloadConfig):
        self.fs = onelake.file_system(cfg)
        self.root = cfg.files_relative

    def exists(self, rel: str) -> bool:
        return self.fs.get_file_client(f"{self.root}/{rel}").exists()

    def put(self, local: str, rel: str, overwrite: bool) -> bool:
        """Upload one file. With overwrite=False a file already there is left alone."""
        client = self.fs.get_file_client(f"{self.root}/{rel}")
        if not overwrite and client.exists():
            return False
        with open(local, "rb") as f:
            client.upload_data(f, overwrite=True)
        return True

    def get(self, rel: str, local: str) -> bool:
        client = self.fs.get_file_client(f"{self.root}/{rel}")
        if not client.exists():
            return False
        with open(local, "wb") as f:
            f.write(client.download_file().readall())
        return True


def download_aemo(
    cfg: WorkloadConfig, download_limit: int, daily_download_limit: int
) -> dict[str, int]:
    import duckdb

    lake = Lake(cfg)
    session = duckdb.connect()
    session.sql("INSTALL httpfs; LOAD httpfs; INSTALL json; LOAD json;")
    batch_size, max_workers = 7, 8
    work = tempfile.mkdtemp()
    log_local = os.path.join(work, LOG).replace("\\", "/")

    # --- Load the existing log, or start an empty one --------------------------------
    if lake.get(LOG, log_local):
        session.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE _csv_archive_log AS
            SELECT source_type, source_filename, archive_path, archived_at,
                   row_count, source_url, etag, csv_filename
            FROM read_parquet('{log_local}') WHERE csv_filename IS NOT NULL
            """
        )
    else:
        session.sql(
            """
            CREATE OR REPLACE TEMP TABLE _csv_archive_log (
                source_type VARCHAR, source_filename VARCHAR, archive_path VARCHAR,
                archived_at TIMESTAMPTZ, row_count BIGINT, source_url VARCHAR,
                etag VARCHAR, csv_filename VARCHAR)
            """
        )

    def download_and_extract(url, temp_dir):
        """Download a ZIP and extract its CSVs UNCOMPRESSED. Thread-safe."""
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (dbt-aemo)"})
        for attempt in range(3):
            try:
                zip_bytes = urllib.request.urlopen(req, timeout=60).read()
                break
            except urllib.error.HTTPError:
                if attempt < 2:
                    time.sleep(2**attempt)
                    continue
                raise
        z = zipfile.ZipFile(io.BytesIO(zip_bytes))
        out = []
        for name in z.namelist():
            if name.upper().endswith(".CSV"):
                safe = name.replace("/", "_")
                p = os.path.join(temp_dir, safe)
                with open(p, "wb") as f:
                    f.write(z.read(name))
                out.append((name, safe, p))
        return out

    def save_log():
        session.sql(f"COPY _csv_archive_log TO '{log_local}' (FORMAT PARQUET)")
        lake.put(log_local, LOG, overwrite=True)

    def log_insert(source_type, source_filename, archive_path, now, url, csv_base):
        session.execute(
            "INSERT INTO _csv_archive_log VALUES "
            "(?, ?, ?, CAST(? AS TIMESTAMPTZ), NULL, ?, NULL, ?)",
            [source_type, source_filename, archive_path, now, url, csv_base],
        )

    def process(rows, source_type, subfolder):
        files = [(r[0], r[1]) for r in rows]
        for i in range(0, len(files), batch_size):
            batch = files[i : i + batch_size]
            with tempfile.TemporaryDirectory() as tmp:
                extracted = []
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futs = {ex.submit(download_and_extract, u, tmp): (u, fn) for u, fn in batch}
                    for fut in as_completed(futs):
                        u, fn = futs[fut]
                        try:
                            for _csv_name, safe, path in fut.result():
                                extracted.append((fn, safe, path, u))
                        except Exception as e:  # noqa: BLE001 - the source skips a bad zip too
                            scrub.safe_print(f"  WARN skip {fn}: {e}")
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    list(
                        ex.map(
                            lambda e: lake.put(e[2], f"csv_raw/{subfolder}/{e[1]}", False),
                            extracted,
                        )
                    )
                now = datetime.now(UTC).isoformat()
                for fn, csv_name, _path, u in extracted:
                    base = csv_name.removesuffix(".CSV").removesuffix(".csv")
                    log_insert(source_type, fn, f"/{subfolder}/{csv_name}", now, u, base)
            save_log()

    def sql_retry(q, attempts=4, base=5):
        """nemweb throws a transient 403/5xx when it rate-limits a burst; back off."""
        for a in range(attempts):
            try:
                return session.sql(q)
            except Exception as e:
                if a < attempts - 1:
                    w = base * (2**a)
                    scrub.safe_print(f"  WARN net {type(e).__name__}: {e}; retry in {w}s")
                    time.sleep(w)
                else:
                    raise

    def new_files(table, source_type):
        # NEWEST FIRST: a limit smaller than the backlog lands the recent end of the archive.
        limit = daily_download_limit if source_type == "daily" else download_limit
        return session.sql(
            f"""SELECT full_url, filename FROM {table}
                WHERE '{source_type}::' || filename NOT IN (
                    SELECT source_type || '::' || source_filename FROM _csv_archive_log)
                ORDER BY filename DESC
                LIMIT {limit}"""
        ).fetchall()

    # --- DAILY (the price and scada records share one file) --------------------------
    sql_retry(
        """CREATE OR REPLACE TEMP TABLE daily_files_web AS
        WITH h AS (SELECT content AS html FROM read_text('https://nemweb.com.au/Reports/Current/Daily_Reports/')),
             l AS (SELECT unnest(string_split(html, '<br>')) AS line FROM h)
        SELECT 'https://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
               split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\\.zip)"', 1), '.', 1) AS filename
        FROM l WHERE line LIKE '%PUBLIC_DAILY%.zip%'"""
    )
    aemo_new = session.sql(
        """SELECT count(*) FROM daily_files_web
           WHERE 'daily::' || filename NOT IN (
               SELECT source_type || '::' || source_filename FROM _csv_archive_log)"""
    ).fetchall()[0][0]

    if aemo_new < daily_download_limit:
        # nemweb keeps the current window only; history comes from the GitHub mirror.
        github_token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if github_token:
            scrub.register(github_token)
            # Inlined: DuckDB's CREATE SECRET takes no bind parameters.
            session.execute(
                "CREATE OR REPLACE SECRET github_api (TYPE HTTP, BEARER_TOKEN '"
                + github_token.replace("'", "''")
                + "', SCOPE 'https://api.github.com')"
            )
        years = range(2018, datetime.now(UTC).year + 1)
        reads = " UNION ALL ".join(
            "SELECT content AS j FROM read_text("
            f"'https://api.github.com/repos/djouallah/aemo_data/contents/data/archive/{y}')"
            for y in years
        )
        try:
            sql_retry(
                f"""INSERT INTO daily_files_web
                WITH api AS ({reads}),
                     p AS (SELECT unnest(from_json(j, '["json"]')) AS fi FROM api)
                SELECT json_extract_string(fi, '$.download_url') AS full_url,
                       split_part(json_extract_string(fi, '$.name'), '.', 1) AS filename
                FROM p WHERE json_extract_string(fi, '$.name') LIKE 'PUBLIC_DAILY%.zip'
                  AND split_part(json_extract_string(fi, '$.name'), '.', 1)
                      NOT IN (SELECT filename FROM daily_files_web)"""
            )
        except Exception as e:  # noqa: BLE001 - carry on with nemweb only, as the source does
            scrub.safe_print(f"  WARN: GitHub backfill listing unavailable: {e}")

    daily = new_files("daily_files_web", "daily")
    if daily:
        process(daily, "daily", "daily")

    # --- INTRADAY SCADA --------------------------------------------------------------
    sql_retry(
        """CREATE OR REPLACE TEMP TABLE intraday_scada_web AS
        WITH h AS (SELECT content AS html FROM read_text('http://nemweb.com.au/Reports/Current/Dispatch_SCADA/')),
             l AS (SELECT unnest(string_split(html, '<br>')) AS line FROM h)
        SELECT 'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
               split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\\.zip)"', 1), '.', 1) AS filename
        FROM l WHERE line LIKE '%PUBLIC_DISPATCHSCADA%' ORDER BY full_url DESC LIMIT 500"""
    )
    scada = new_files("intraday_scada_web", "scada_today")
    if scada:
        process(scada, "scada_today", "scada_today")

    # --- INTRADAY PRICE --------------------------------------------------------------
    sql_retry(
        """CREATE OR REPLACE TEMP TABLE intraday_price_web AS
        WITH h AS (SELECT content AS html FROM read_text('http://nemweb.com.au/Reports/Current/DispatchIS_Reports/')),
             l AS (SELECT unnest(string_split(html, '<br>')) AS line FROM h)
        SELECT 'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
               split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\\.zip)"', 1), '.', 1) AS filename
        FROM l WHERE line LIKE '%PUBLIC_DISPATCHIS_%.zip%' ORDER BY full_url DESC LIMIT 500"""
    )
    price = new_files("intraday_price_web", "price_today")
    if price:
        process(price, "price_today", "price_today")

    # --- DUID REFERENCE DATA (refreshed at most once a day) --------------------------
    last = session.sql(
        "SELECT max(archived_at) FROM _csv_archive_log WHERE source_type LIKE 'duid_%'"
    ).fetchall()[0][0]
    fresh = last is not None and (datetime.now(last.tzinfo) - last).total_seconds() < 86400
    if fresh:
        scrub.safe_print(f"  DUID data fresh ({last}), skipping")
    else:
        with tempfile.TemporaryDirectory() as dtmp:
            for _st, sf, url, fn in DUID_SOURCES:
                hdr = ", header=true" if sf == "WA_ENERGY" else ""
                lp = os.path.join(dtmp, fn).replace("\\", "/")
                session.sql(
                    f"""COPY (SELECT * FROM read_csv_auto('{url}',
                              null_padding=true, ignore_errors=true{hdr}))
                        TO '{lp}' (FORMAT CSV, HEADER)"""
                )
                lake.put(lp, f"csv_raw/duid/{fn}", overwrite=True)
        session.sql("DELETE FROM _csv_archive_log WHERE source_type LIKE 'duid_%'")
        now = datetime.now(UTC).isoformat()
        for st, sf, url, fn in DUID_SOURCES:
            log_insert(st, sf, f"/duid/{fn}", now, url, fn.rsplit(".", 1)[0])

    save_log()
    for source_type, files in session.sql(
        "SELECT source_type, count(*) FROM _csv_archive_log GROUP BY 1 ORDER BY 1"
    ).fetchall():
        scrub.safe_print(f"  {source_type}: {files} file(s) in the log")
    return {"daily": len(daily), "scada_today": len(scada), "price_today": len(price)}
