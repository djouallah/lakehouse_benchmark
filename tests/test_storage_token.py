""".github/scripts/storage_token_publish.py: the Storage token tab only gets a result it can draw."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "storage_token_publish", Path(__file__).parent.parent / ".github" / "scripts" / "storage_token_publish.py"
)
publish = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publish)


def row(catalog: str, ok: bool = True) -> dict:
    return {
        "catalog": catalog,
        "label": catalog,
        "ok": ok,
        "step": None if ok else "create",
        "error": None if ok else "HTTPException",
        "storage": "token from the catalog",
    }


def result(rows: list[dict]) -> dict:
    return {"run": "https://example.invalid/run", "date": "2026-10-10", "catalogs": rows}


def test_a_full_result_passes():
    rows = [row(c, ok=c != "unity_default") for c in sorted(publish.CATALOGS)]
    assert publish.check(result(rows))["catalogs"] == rows


def test_a_missing_catalog_is_refused():
    rows = [row(c) for c in sorted(publish.CATALOGS) if c != "unity_default"]
    with pytest.raises(SystemExit):
        publish.check(result(rows))


def test_a_failure_without_its_error_is_refused():
    rows = [row(c) for c in sorted(publish.CATALOGS)]
    rows[0]["ok"] = False
    with pytest.raises(SystemExit):
        publish.check(result(rows))


def test_a_failure_at_attach_is_refused():
    rows = [row(c) for c in sorted(publish.CATALOGS)]
    rows[0].update(ok=False, step="attach", error="Error")
    with pytest.raises(SystemExit):
        publish.check(result(rows))
