""".github/scripts/capability_publish.py: the grid it accepts is one docs/index.html can draw."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parent.parent / ".github" / "scripts" / "capability_publish.py"
_spec = importlib.util.spec_from_file_location("capability_publish", _SCRIPT)
capability_publish = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(capability_publish)

PAGE = (Path(__file__).parent.parent / "docs" / "index.html").read_text(encoding="utf-8")


def _grid(**overrides):
    grid = {
        "engines": {"duckdb_iceberg": {"version": "v2", "run": "1", "url": "u", "date": "d"}},
        "groups": ["Write"],
        "rows": [
            {"group": "Write", "label": "DELETE", "cells": {"duckdb_iceberg": {"o": "supported"}}}
        ],
        "blocked": ["format-version 3"],
    }
    grid.update(overrides)
    return grid


def test_a_well_formed_grid_passes():
    assert capability_publish.check(_grid())["rows"][0]["label"] == "DELETE"


def test_an_engine_without_a_label_is_refused():
    engines = {"mystery_iceberg": {"version": "1", "run": "1", "url": "u", "date": "d"}}
    with pytest.raises(SystemExit, match="mystery_iceberg"):
        capability_publish.check(_grid(engines=engines))


def test_an_unknown_outcome_is_refused():
    rows = [{"group": "Write", "label": "DELETE", "cells": {"duckdb_iceberg": {"o": "maybe"}}}]
    with pytest.raises(SystemExit, match="maybe"):
        capability_publish.check(_grid(rows=rows))


def test_the_page_draws_every_outcome_the_publisher_accepts():
    for outcome in capability_publish.OUTCOMES:
        assert f'"{outcome}"' in PAGE or f"{outcome}:" in PAGE, outcome
