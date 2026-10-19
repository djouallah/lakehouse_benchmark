"""Generate the suite's dataset into OneLake, once, before the matrix fans out.

THE SINGLE WRITER. Four matrix jobs calling generate() would race: `create_table_if_not_exists`
is check-then-act with a multi-minute window between the check and the `add_files` commit, so two
jobs can both decide the table is missing, both create it, and both register the same files. That
is a corrupt table, not a slow one. The fix is structural -- generation happens here, in a job
with no matrix, and the bench jobs are read-only.

WHICH SUITE is BENCH_SUITE's call (bench/suite.py). Both generate with tpcgen-cli (`tpch` or
`tpcds`) and land the parquet the same way, through pyiceberg. Same marker, same idempotency. TPCDS_REGENERATE=true (tpcds.yml's `regenerate` input) makes the
TPC-DS one purge the namespace and write it again.
"""

from __future__ import annotations

import os
import sys

from bench.suite import suite_class

if __name__ == "__main__":
    cfg = suite_class().from_env()
    if cfg.TEST == "tpcds":
        from bench.tpcds.generate import generate

        stats = generate(cfg, force=os.environ.get("TPCDS_REGENERATE", "").lower() == "true")
    else:
        from bench.tpch.generate import generate

        stats = generate(cfg)
    print(f"::notice::{cfg.schema} ready (skipped={stats['skipped']})")
    sys.exit(0)
