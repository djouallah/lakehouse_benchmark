"""Write docs/data/reports.json, the data behind the Bug reports tab of docs/index.html.

Every GitHub issue and pull request linked in the issue list on the page's Bug reports tab,
with the engine it is listed under, read from the GitHub API: who opened it, when, and whether
it was fixed. The list stays the one list of upstream bugs; this file only adds what GitHub
knows about each one.

    python .github/scripts/reports_publish.py [page] [target]

In CI it reads the API with GITHUB_TOKEN; by hand, with no GITHUB_TOKEN, through the gh CLI.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

from bench.report import leak_check

PAGE = Path("docs/index.html")
TARGET = Path("docs/data/reports.json")
REPORTER = "djouallah"  # the bench's own reports; issues by others that it links to are context
# Closed as completed on GitHub, but nothing was fixed: the issue list says why.
NOT_FIXED = {
    "duckdb/duckdb-iceberg#1006": "closed: DuckDB always writes positional deletes",
    "ClickHouse/ClickHouse#126628": (
        "closed: expected behavior, cast_keep_nullable = 1 is the opt-in"
    ),
    "ClickHouse/ClickHouse#126633": "closed: kept in ClickHouse Cloud only",
}
LINK = re.compile(r'href="https://github\.com/([\w.-]+/[\w.-]+)/(issues|pull)/(\d+)"')


def parse(page: str) -> list[dict]:
    """One entry per GitHub link in the issue list, under the engine its list item names."""
    start = page.index('<section class="details" id="issues"')
    section = page[start : page.index("</section>", start)]
    out, seen = [], set()
    for li in re.findall(r"<li>(.*?)</li>", section, re.S):
        engine = re.search(r"<b>([^<]+)</b>", li).group(1)
        for repo, kind, number in LINK.findall(li):
            key = f"{repo}#{number}"
            if key in seen:
                continue
            seen.add(key)
            out.append({"engine": engine, "id": key, "kind": "pr" if kind == "pull" else "issue"})
    return out


def fetch(item: dict, token: str | None) -> dict:
    """The entry with what GitHub knows about it: title, author, state, dates."""
    repo, number = item["id"].split("#")
    path = f"repos/{repo}/issues/{number}"
    if token:
        req = urllib.request.Request(f"https://api.github.com/{path}")
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(req, timeout=30) as r:
            gh = json.load(r)
    else:  # by hand: the gh CLI's own login
        gh = json.loads(subprocess.run(["gh", "api", path], capture_output=True, check=True).stdout)
    merged = (gh.get("pull_request") or {}).get("merged_at")
    closed = gh.get("closed_at")
    if item["kind"] == "pr":
        fixed = merged is not None
    else:
        fixed = gh["state_reason"] == "completed" and item["id"] not in NOT_FIXED
    return {
        **item,
        "url": gh["html_url"],
        "title": gh["title"],
        "author": gh["user"]["login"],
        "opened": gh["created_at"][:10],
        "closed": (merged or closed or "")[:10] or None,
        "state": "fixed" if fixed else "open" if gh["state"] == "open" else "closed",
        "note": NOT_FIXED.get(item["id"]),
    }


def main() -> None:
    page = Path(sys.argv[1]) if len(sys.argv) > 1 else PAGE
    target = Path(sys.argv[2]) if len(sys.argv) > 2 else TARGET
    token = os.environ.get("GITHUB_TOKEN")
    items = [fetch(i, token) for i in parse(page.read_text(encoding="utf-8"))]
    out = {
        "reporter": REPORTER,
        "updated": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "items": items,
    }
    text = json.dumps(out, indent=1, ensure_ascii=False) + "\n"
    target.write_text(text, encoding="utf-8")
    leak_check([target])
    mine = [i for i in items if i["author"] == REPORTER]
    print(
        f"{len(items)} links, {len(mine)} by {REPORTER}, "
        f"{sum(i['state'] == 'fixed' for i in mine)} of them fixed or merged -> {target}"
    )


if __name__ == "__main__":
    main()
