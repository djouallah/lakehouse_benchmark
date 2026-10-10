""".github/scripts/storage_token_publish.py: the Catalogs tab shows only what worked."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parent.parent / ".github" / "scripts" / "storage_token_publish.py"
SPEC = importlib.util.spec_from_file_location("storage_token_publish", SCRIPT)
publish = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publish)


def row(catalog: str, ok: bool) -> dict:
    return {
        "catalog": catalog,
        "label": catalog,
        "ok": ok,
        "step": None if ok else "attach",
        "error": None if ok else "Error",
        "storage": "token from the catalog",
    }


def result(rows: list[dict]) -> dict:
    return {"run": "https://example.invalid/run", "date": "2026-10-10", "catalogs": rows}


def test_only_the_catalogs_that_worked_are_kept():
    kept = publish.keep(result([row("onelake", True), row("unity", False)]))["catalogs"]
    assert kept == [{"catalog": "onelake", "label": "onelake", "storage": "token from the catalog"}]


def test_nothing_worked_is_refused():
    with pytest.raises(SystemExit):
        publish.keep(result([row("unity", False)]))
