"""Land the AEMO files the dbt workload reads into a LOCAL directory, the layout of Files/dbt.

For duckrun's release gate (djouallah/duckrun, .github/workflows/workload_gate.yml): it runs this
repo's dbt project on runner disk against a duckrun release candidate, touching no lakehouse.

    python .github/scripts/workload_land_local.py <dir> --daily 395 --intraday 6
"""

from __future__ import annotations

import argparse

from bench.workload.landing import LocalLake, download_aemo

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("root", help="the landing root (dbt's FILES_PATH)")
    ap.add_argument("--daily", type=int, default=395, help="daily archive files to land")
    ap.add_argument("--intraday", type=int, default=6, help="intraday files per feed")
    args = ap.parse_args()
    landed = download_aemo(None, args.intraday, args.daily, lake=LocalLake(args.root))
    print("landed: " + ", ".join(f"{k}={v}" for k, v in landed.items()))
