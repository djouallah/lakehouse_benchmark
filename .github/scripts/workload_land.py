"""Land the AEMO files the dbt workload reads. Idempotent. The `land` job of workload.yml.

THE SINGLE WRITER of Files/dbt: the engine legs only read it, so they cannot race on the
archive-log parquet (bench/workload/landing.py rewrites it), and every leg folds the same files,
which is what makes the parity grade mean anything.
"""

from __future__ import annotations

import os

from bench import scrub
from bench.workload.config import WorkloadConfig
from bench.workload.landing import download_aemo

if __name__ == "__main__":
    cfg = WorkloadConfig.from_env()
    download_limit = int(os.environ.get("DOWNLOAD_LIMIT") or 6)
    daily_download_limit = int(os.environ.get("DAILY_DOWNLOAD_LIMIT") or 60)
    scrub.safe_print(
        f"landing into {cfg.files_abfss} (intraday {download_limit}/feed, daily "
        f"{daily_download_limit})"
    )
    landed = download_aemo(cfg, download_limit, daily_download_limit)
    print("::notice::landed this run: " + ", ".join(f"{k}={v}" for k, v in landed.items()))
