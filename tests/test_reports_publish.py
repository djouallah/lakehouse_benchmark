""".github/scripts/reports_publish.py: every GitHub link on the Issues tab, under its engine."""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

_ROOT = Path(__file__).parent.parent
_SCRIPT = _ROOT / ".github" / "scripts" / "reports_publish.py"
_spec = importlib.util.spec_from_file_location("reports_publish", _SCRIPT)
reports_publish = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reports_publish)

PAGE = (_ROOT / "docs" / "index.html").read_text(encoding="utf-8")


def _issues_tab() -> str:
    start = PAGE.index('<section class="details" id="issues"')
    return PAGE[start : PAGE.index("</section>", start)]


def test_every_github_issue_and_pull_request_on_the_issues_tab_is_parsed_once():
    links = {
        f"{repo}#{n}"
        for repo, n in re.findall(
            r'href="https://github\.com/([\w.-]+/[\w.-]+)/(?:issues|pull)/(\d+)"', _issues_tab()
        )
    }
    parsed = reports_publish.parse(PAGE)
    assert {p["id"] for p in parsed} == links
    assert len(parsed) == len(links)


def test_each_link_is_under_the_engine_its_list_item_names():
    parsed = {p["id"]: p for p in reports_publish.parse(PAGE)}
    assert parsed["pola-rs/polars#29449"]["engine"] == "Polars"
    assert parsed["datamindedbe/incubator-dbt-polars#45"]["engine"] == "dbt-polars"
    assert parsed["facebookincubator/velox#19508"]["engine"] == "Gluten/Velox"
    assert parsed["apache/doris#68877"]["engine"] == "Apache Doris"
    assert parsed["databendlabs/databend#20594"]["kind"] == "pr"


def test_every_not_fixed_override_is_linked_on_the_page():
    ids = {p["id"] for p in reports_publish.parse(PAGE)}
    assert set(reports_publish.NOT_FIXED) <= ids


def test_the_published_file_is_the_shape_the_bug_reports_tab_draws():
    data = json.loads((_ROOT / "docs" / "data" / "reports.json").read_text(encoding="utf-8"))
    assert data["reporter"] == reports_publish.REPORTER
    for item in data["items"]:
        assert item["kind"] in {"issue", "pr"}, item["id"]
        assert item["state"] in {"fixed", "open", "closed"}, item["id"]
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", item["opened"]), item["id"]
        assert (item["closed"] is None) == (item["state"] == "open"), item["id"]
