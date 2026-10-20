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
        assert len(models) == 6, (engine, models)
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


def test_page_data_carries_the_dag_and_each_legs_models():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "workload_publish", ROOT / ".github" / "scripts" / "workload_publish.py"
    )
    publish = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(publish)

    def part(engine, adapter, dag):
        return {
            "engine": engine,
            "run_id": "1",
            "status": "success",
            "build_seconds": 10.0,
            "finished": "2026-10-20T00:00:00Z",
            "versions": {"dbt-core": "1.10.0", adapter: "1.10.1"},
            "nodes": [
                {"node": "model.aemo_electricity.stg", "status": "success", "seconds": 1.5},
                {"node": "model.aemo_electricity.fct", "status": "success", "seconds": 4.0},
                {"node": "test.aemo_electricity.t", "status": "pass", "seconds": 0.1},
            ],
            "fingerprint": _fp(engine),
            "dag": dag,
        }

    legs = {
        "gluten": part("gluten", "dbt-spark", {"stg": {"materialized": "table", "parents": []}}),
        "duckdb": part(
            "duckdb",
            "dbt-duckdb",
            {
                "stg": {"materialized": "view", "parents": []},
                "fct": {
                    "materialized": "incremental",
                    "parents": ["stg"],
                    "strategy": "merge",
                    "merge_clauses": True,
                    "path": "models/aemo/duckdb/marts/fct.sql",
                    "sql_lines": 40,
                    "description": "A fact.",
                    "differences": ["`GROUP BY ALL` here."],
                },
            },
        ),
    }
    out = publish.page_data(legs, parity.grade({e: leg["fingerprint"] for e, leg in legs.items()}))
    assert out["dag"]["fct"]["parents"] == ["stg"]
    assert set(out["dag"]) == {"stg", "fct"}
    assert out["legs"]["duckdb_iceberg"]["adapter"] == "dbt-duckdb 1.10.1"
    assert out["legs"]["duckdb_iceberg"]["model_seconds"] == {"stg": 1.5, "fct": 4.0}
    assert out["legs"]["duckdb_iceberg"]["models"] == 2  # the count, not the seconds
    assert out["parity_ok"]
    assert out["dag"]["fct"]["differences"] == ["`GROUP BY ALL` here."]
    assert out["legs"]["duckdb_iceberg"]["model_config"]["fct"]["strategy"] == "merge"
    assert out["legs"]["duckdb_iceberg"]["model_config"]["fct"]["sql_lines"] == 40


def test_every_model_says_where_the_engines_sql_differs():
    """The Real workload panel reads each model's meta.differences from the shared patch files."""
    import yaml

    names = set()
    for patch in ("_dimensions", "_marts"):
        doc = yaml.safe_load((PROJECT / "models" / "aemo" / f"{patch}.yml").read_text("utf-8"))
        for model in doc["models"]:
            names.add(model["name"])
            assert model["config"]["meta"]["differences"], model["name"]
    trees = {p.stem for p in (PROJECT / "models" / "aemo" / "duckdb").rglob("*.sql")}
    assert names == trees


def test_every_engine_builds_every_model_the_same_way():
    """One materialization per model on every engine: an incremental merge on the natural key.

    A merge commits against the snapshot it read, so two concurrent runs cannot insert a file
    twice; an append (or an insert-only merge, a plain append commit) would keep both. The landed
    files are dbt sources, not models, so nothing here copies them.
    """

    def config(path: Path) -> dict:
        text = re.sub(r"\{#.*?#\}", "", path.read_text(encoding="utf-8"), flags=re.S)
        block = text[text.index("config(") : text.index(")", text.index("config("))]
        return {
            "materialized": re.search(r"materialized='(\w+)'", block).group(1),
            "strategy": (re.search(r"incremental_strategy='([\w+]+)'", block) or [None, ""])[1],
            "merge_clauses": "merge_clauses" in block,
        }

    by_model: dict[str, dict] = {}
    for engine in ENGINES:
        for path in (PROJECT / "models" / "aemo" / engine).rglob("*.sql"):
            by_model.setdefault(path.stem, {})[engine] = config(path)
    for model, engines in by_model.items():
        for engine, c in engines.items():
            assert c == {
                "materialized": "incremental",
                "strategy": "merge",
                "merge_clauses": False,
            }, (
                model,
                engine,
                c,
            )


def test_the_archive_log_is_a_source_not_a_copy():
    """Nothing builds the landing log: it is a dbt source, and every model reads it through
    archive_log() -- the source, or on Polars (no external sources on OneLake) the parquet."""
    for engine in ENGINES:
        tree = PROJECT / "models" / "aemo" / engine
        assert not list(tree.rglob("stg_*.sql")), engine
        for path in tree.rglob("*.sql"):
            assert "ref('stg_csv_archive_log')" not in path.read_text(encoding="utf-8"), path
    sources = (PROJECT / "models" / "sources.yml").read_text(encoding="utf-8")
    assert "name: csv_raw_archive_log" in sources
    assert 'external_location: "read_parquet(' in sources
