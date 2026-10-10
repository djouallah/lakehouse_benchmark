"""Check the storage token result before it is published.

testing-iceberg-rest-catalog's storage-token workflow writes results/storage_token.json there;
.github/workflows/storage_token.yml copies it to docs/data/storage_token.json. This checks it is
the shape the Storage token tab of docs/index.html draws, and that it carries no token.

    python .github/scripts/storage_token_publish.py [path]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from bench.report import leak_check

TARGET = Path("docs/data/storage_token.json")
CATALOGS = {"onelake", "r2", "s3_table", "glue", "unity", "unity_default", "horizon"}
FIELDS = {"catalog", "label", "ok", "step", "error", "storage"}


def check(result: dict) -> dict:
    """The result, if it is one the page can draw; SystemExit naming the first thing wrong."""
    if not {"run", "date", "catalogs"} <= set(result):
        raise SystemExit(f"::error::missing run, date or catalogs: {sorted(result)}")
    seen = [c.get("catalog") for c in result["catalogs"]]
    if set(seen) != CATALOGS or len(seen) != len(CATALOGS):
        raise SystemExit(f"::error::expected one row per catalog {sorted(CATALOGS)}, got {seen}")
    for c in result["catalogs"]:
        if set(c) != FIELDS:
            raise SystemExit(f"::error::{c['catalog']}: fields {sorted(c)}, not {sorted(FIELDS)}")
        name = c["catalog"]
        if c["ok"] is not (c["step"] is None and c["error"] is None):
            raise SystemExit(f"::error::{name}: a failure needs a step and an error")
        if c["step"] == "attach":
            # The catalog refused our own credentials, so the test never reached the storage: the
            # question was not asked. Fix the secret in testing-iceberg-rest-catalog and rerun.
            raise SystemExit(f"::error::{name}: failed at attach ({c['error']}): bad credentials")
    return result


def main() -> int:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else TARGET
    result = check(json.loads(path.read_text(encoding="utf-8")))
    leak_check([path])
    failed = [c["catalog"] for c in result["catalogs"] if not c["ok"]]
    rows = len(result["catalogs"])
    print(f"checked {path}: {rows} catalogs, failing: {', '.join(failed) or 'none'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
