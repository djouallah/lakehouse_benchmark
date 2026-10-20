"""The dbt workload test (workload/, bench/workload/, workload.yml): offline checks.

What would otherwise fail only minutes into a real run, or -- worse -- pass green having built
nothing: a target with no model tree, a leg missing from the workflow, a grade that misses a
row-count difference. And the page reads exactly the file the publish step writes.
"""

from __future__ import annotations

import re
from pathlib import Path

from bench.workload import parity
from bench.workload.config import BASELINE, BENCH_ENGINE, ENGINES

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "workload"


def _fp(engine: str, **over) -> dict:
    fp = {
        "engine": engine,
        "rows_total": 3_078_798,
        "duids": 500,
        "days": 50,
        "date_min": "2026-08-01",
        "date_max": "2026-09-19",
        "mw_sum": 278533061.3401898,
        "price_sum": 184858937.75101513,
    }
    return fp | over


def test_every_engine_has_a_target_a_model_tree_and_a_gate():
    profiles = (PROJECT / "profiles.yml").read_text(encoding="utf-8")
    project = (PROJECT / "dbt_project.yml").read_text(encoding="utf-8")
    for engine in ENGINES:
        assert re.search(rf"^    {engine}:\n", profiles, re.M), engine
        assert f"target.name == '{engine}'" in project, engine
        models = sorted(p.name for p in (PROJECT / "models" / "aemo" / engine).rglob("*.sql"))
        assert len(models) == 8, (engine, models)
        assert (PROJECT / "tests" / "aemo" / engine).is_dir(), engine
        assert (ROOT / "requirements" / f"workload_{engine}.txt").exists(), engine
        assert engine in BENCH_ENGINE


def test_the_same_models_in_every_tree():
    trees = {
        e: sorted(p.name for p in (PROJECT / "models" / "aemo" / e).rglob("*.sql")) for e in ENGINES
    }
    assert len({tuple(v) for v in trees.values()}) == 1, trees


def test_gluten_tree_writes_iceberg_not_delta():
    for path in (PROJECT / "models" / "aemo" / "gluten").rglob("*.sql"):
        text = path.read_text(encoding="utf-8")
        assert "file_format='delta'" not in text, path
        assert "USING DELTA" not in text.upper(), path


def test_workflow_default_engines_are_the_engines():
    text = (ROOT / ".github" / "workflows" / "workload.yml").read_text(encoding="utf-8")
    default = re.search(r"engines:\n(?:.*\n){2}\s+default: (\S+)", text).group(1)
    assert tuple(default.split(",")) == ENGINES


def test_the_page_reads_what_publish_writes():
    page = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
    assert 'json: "data/workload.json"' in page
    publish = (ROOT / ".github" / "scripts" / "workload_publish.py").read_text(encoding="utf-8")
    assert 'PAGE_DATA = REPO / "docs" / "data" / "workload.json"' in publish
    workflow = (ROOT / ".github" / "workflows" / "workload.yml").read_text(encoding="utf-8")
    assert "git add results/workload docs/data/workload.json" in workflow
    pages = (ROOT / ".github" / "workflows" / "pages.yml").read_text(encoding="utf-8")
    assert "dbt workload" in pages


def test_grade_passes_rounding_noise():
    g = parity.grade(
        {
            "gluten": _fp("gluten"),
            "duckdb": _fp("duckdb", mw_sum=278533060.8253, price_sum=184858936.879),
        }
    )
    assert g["baseline"] == BASELINE
    assert g["failures"] == []
    assert 1e-9 < g["diffs"]["duckdb"]["mw_sum"] < 1e-8


def test_grade_fails_a_row_count_difference_and_a_drift():
    g = parity.grade(
        {
            "gluten": _fp("gluten"),
            "polars": _fp("polars", rows_total=3_078_797, price_sum=184858937.75101513 * 1.001),
        }
    )
    assert any(f.startswith("rows_total") for f in g["failures"])
    assert any(f.startswith("price_sum") for f in g["failures"])


def test_grade_without_the_baseline_falls_back_to_the_first_by_name():
    g = parity.grade({"polars": _fp("polars"), "duckdb": _fp("duckdb")})
    assert g["baseline"] == "duckdb"


def test_grade_of_one_leg_grades_nothing():
    assert parity.grade({"duckdb": _fp("duckdb")})["failures"] == []


def test_extract_reads_the_fingerprint_log():
    log = 'noise\n{\n  "engine": "duckdb",\n  "rows_total": 3,\n  "mw_sum": 1.5\n}\nmore'
    assert parity.extract(log) == {"engine": "duckdb", "rows_total": 3, "mw_sum": 1.5}
    assert parity.extract("nothing here") is None


def test_runner_script_is_not_named_like_a_dbt_plugin():
    # dbt imports every top-level dbt_* module as a plugin; the script dir is sys.path[0].
    scripts = (ROOT / ".github" / "scripts").glob("*.py")
    assert not [p.name for p in scripts if p.name.startswith("dbt_")]
