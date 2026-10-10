"""Benchmark results: one immutable JSON file per run, committed to the repo.

REPLACES cell 18, which appended two pandas frames to a Delta table on OneLake and then ran
`optimize.compact()` and `vacuum(retention_hours=0)` against it.

WHY LOCAL JSON AND NOT A TABLE ANYWHERE. The point of this repo is that the numbers are
reproducible and checkable BY ANYONE. A results table in OneLake is private: a reader would need a
Fabric account and a role assignment to see the data behind the charts, which defeats the
exercise. Results in git are public, diffable, and survive the lakehouse being deleted.

It also deletes an entire class of failure. Four matrix jobs appending to one Iceberg table race
on the commit; one file per run cannot. Nothing is ever appended to, rewritten or compacted, so
there is no conflict to retry and no snapshot expiry to get wrong. `git log` is the audit trail.

SIZE. 46 rows per engine-run (setup + 22 queries, cold and warm), four engines, a few KB per file.
Nightly is well under 2MB/year. No rotation, no retention policy. Revisit past ~1000 runs.
"""

from __future__ import annotations

import json
import platform
from dataclasses import asdict, dataclass, field, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1

# `error` goes into git permanently, so it is capped. Long enough for a ClickHouse exception with
# its error code, short enough that a pathological stack does not bloat the file.
ERROR_LIMIT = 2000

# One row per timed statement, after flattening. These are the columns charts.py queries.
ROW_FIELDS = (
    "run_id",
    "run_started_at",
    "run_url",
    "git_sha",
    "sf",
    "test",
    "runner",
    "cpu",
    "mem_gb",
    "python",
    "engine",
    "engine_version",
    "engine_status",
    "run_type",
    "phase",
    "query",
    "dur",
    "rows",
    "status",
    "error",
)


@dataclass
class Row:
    """One timed statement.

    `phase='setup'` / `query=0` is the engine attach, recorded as ITS OWN ROW.

    Cell 18 folded it into query 1 instead -- `df.at[0,'dur'] = df.at[0,'dur'] + setup_time` --
    which silently made Q1 look seconds slower than it is, on every engine, in every chart the
    notebook ever produced. Totals sum queries 0..22; per-query charts filter to 1..22.
    """

    run_type: str  # cold | warm
    phase: str  # setup | query
    query: int  # 0 for setup, 1..22
    dur: float | None
    rows: int | None = None  # result cardinality: the silent-wrong-answer detector
    status: str = "ok"  # ok | error | skipped
    error: str | None = None  # scrubbed and truncated; this goes into git forever


@dataclass
class EngineResult:
    version: str
    status: str = "ok"  # ok | setup_failed | timed_out
    rows: list[Row] = field(default_factory=list)
    # The machine THIS engine ran on, captured in the bench job.
    #
    # It has to be recorded here and not in `publish`, because publish runs on a DIFFERENT
    # runner. Reading psutil there would stamp every result with the publisher's hardware --
    # identical today, since both are ubuntu-latest, and quietly wrong the moment a bench job
    # moves to a larger runner. A benchmark that misreports its own machine is worse than one
    # that does not report it at all.
    host: dict = field(default_factory=dict)


@dataclass
class Run:
    run_id: str
    run_started_at: str
    sf: int
    run_url: str = ""
    git_sha: str = ""
    test: str = "tpch"
    runner: str = ""
    cpu: int = 0
    mem_gb: float = 0.0
    python: str = ""
    schema_version: int = SCHEMA_VERSION
    engines: dict[str, EngineResult] = field(default_factory=dict)


def now_iso() -> str:
    """UTC, ISO-8601, second precision.

    A real timestamp, unlike cell 18's `datetime.now().strftime('%Y-%m-%d %H:%M:%S')` -- which was
    naive local time in a string column, and which is why the notebook's charts filtered on
    `time > '2026-06-31'`: a date that does not exist, and which only compared at all because
    the column was VARCHAR.
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def host_facts() -> dict[str, Any]:
    """Everything about the machine a reader needs in order to interpret the numbers."""
    import psutil

    return {
        "runner": f"{platform.system().lower()}-{platform.release()}",
        "cpu": psutil.cpu_count(logical=True),
        "mem_gb": round(psutil.virtual_memory().total / 2**30, 1),
        "python": platform.python_version(),
    }


def run_filename(run: Run) -> str:
    """e.g. 2026-09-21T03-17Z_sf10_18234567890.json -- sorts chronologically, unique per run."""
    stamp = run.run_started_at[:16].replace(":", "-")  # YYYY-MM-DDTHH-MM
    return f"{stamp}Z_sf{run.sf}_{run.run_id}.json"


def _encode(run: Run) -> dict[str, Any]:
    payload = asdict(run)
    for name, engine in payload["engines"].items():
        engine["rows"] = [
            {k: v for k, v in row.items() if not (k == "error" and v is None)}
            for row in engine["rows"]
        ]
        payload["engines"][name] = engine
    return payload


def write_run(directory: str | Path, run: Run) -> Path:
    """Write `run` to `directory/run_filename(run)`.

    Refuses to overwrite. Immutability is the property that makes concurrent matrix jobs and
    overlapping workflow runs incapable of producing a merge conflict; silently clobbering would
    give that away for nothing.
    """
    path = Path(directory) / run_filename(run)
    if path.exists():
        raise FileExistsError(f"{path} already exists; runs are immutable")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_encode(run), indent=2) + "\n", encoding="utf-8")
    return path


def read_run(path: str | Path) -> Run:
    """A stored run, read with the same tolerance as read_engine_part: unknown keys are skipped."""
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    known_row = {f.name for f in fields(Row)}
    engines = {
        name: EngineResult(
            version=e["version"],
            status=e.get("status", "ok"),
            rows=[Row(**{k: v for k, v in r.items() if k in known_row}) for r in e["rows"]],
            host=e.get("host", {}),
        )
        for name, e in raw.get("engines", {}).items()
    }
    known_run = {f.name for f in fields(Run)} - {"engines"}
    return Run(**{k: v for k, v in raw.items() if k in known_run}, engines=engines)


def latest_per_engine(directory: str | Path, sf: int, engines: tuple[str, ...], run: Run) -> Run:
    """`run`, with every engine it lacks filled in from that engine's most recent stored run.

    What RESULTS.md shows once a run may carry a single engine: the page is each engine's
    latest result at this scale, not only the engines the newest run happened to include.
    """
    merged = Run(**{f.name: getattr(run, f.name) for f in fields(Run) if f.name != "engines"})
    merged.engines = dict(run.engines)
    for path in sorted(Path(directory).glob(f"*_sf{sf}_*.json"), reverse=True):
        missing = [e for e in engines if e not in merged.engines]
        if not missing:
            break
        stored = read_run(path)
        for engine in missing:
            if engine in stored.engines:
                merged.engines[engine] = stored.engines[engine]
    return merged


def write_engine_part(directory: str | Path, engine: str, result: EngineResult) -> Path:
    """One matrix job's slice, uploaded as an artifact for `publish` to merge."""
    path = Path(directory) / f"{engine}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"engine": engine, **asdict(result)}
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def read_engine_part(path: str | Path) -> tuple[str, EngineResult]:
    """One engine's part, read TOLERANTLY: a key `Row` does not have is ignored.

    THE PART AND THE PUBLISHER CAN BE DIFFERENT CODE, and run 35580753996 is what that costs.
    Its six engine jobs ran while `Row` still had a `verdict` field, so every row in every part
    carried `"verdict": null` -- `write_engine_part` serialises the whole dataclass, Nones
    included, unlike `write_run`. The publish then hit a rejected push, and the regenerate loop
    did what it is designed to do: `reset --hard` onto an origin/main that no longer had the
    field. `Row(**row)` raised TypeError on the second attempt, and a run with six green engines
    committed nothing.

    That is not a one-off. The rejected-push loop deliberately rebuilds on whatever main has
    become, so the publisher is ALWAYS liable to be newer than the parts it is merging, and any
    field added to or removed from `Row` would do the same thing again. `load_all` already reads
    every key with `.get` for exactly this reason; this is the same rule on the other reader.
    """
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    known = {f.name for f in fields(Row)}
    rows = [Row(**{k: v for k, v in row.items() if k in known}) for row in payload["rows"]]
    return payload["engine"], EngineResult(
        version=payload["version"],
        status=payload["status"],
        rows=rows,
        host=payload.get("host", {}),
    )


def load_all(directory: str | Path) -> Any:
    """Every run in `directory`, flattened to one pyarrow row per timed statement.

    Flattening happens HERE, in the one module that knows the file shape, rather than in SQL via
    `read_json_auto('results/*.json')`. The nested `engines` object would otherwise need unnesting
    in every chart query, and a schema change would have to be chased through charts.py.

    Unknown keys are read defensively (`.get`) so an older schema_version still loads -- the whole
    point of keeping history in git is being able to read the old files.
    """
    import pyarrow as pa

    columns: dict[str, list[Any]] = {name: [] for name in ROW_FIELDS}
    for path in sorted(Path(directory).glob("*.json")):
        run = json.loads(path.read_text(encoding="utf-8"))
        for engine_name, engine in run.get("engines", {}).items():
            for row in engine.get("rows", []):
                values = {
                    "run_id": run.get("run_id"),
                    "run_started_at": run.get("run_started_at"),
                    "run_url": run.get("run_url", ""),
                    "git_sha": run.get("git_sha", ""),
                    "sf": run.get("sf"),
                    "test": run.get("test", "tpch"),
                    "runner": run.get("runner", ""),
                    "cpu": run.get("cpu"),
                    "mem_gb": run.get("mem_gb"),
                    "python": run.get("python", ""),
                    "engine": engine_name,
                    "engine_version": engine.get("version", ""),
                    "engine_status": engine.get("status", "ok"),
                    "run_type": row.get("run_type"),
                    "phase": row.get("phase"),
                    "query": row.get("query"),
                    "dur": row.get("dur"),
                    "rows": row.get("rows"),
                    "status": row.get("status", "ok"),
                    "error": row.get("error"),
                }
                for name in ROW_FIELDS:
                    columns[name].append(values[name])

    schema = pa.schema(
        [
            pa.field("run_id", pa.string()),
            pa.field("run_started_at", pa.string()),
            pa.field("run_url", pa.string()),
            pa.field("git_sha", pa.string()),
            pa.field("sf", pa.int32()),
            pa.field("test", pa.string()),
            pa.field("runner", pa.string()),
            pa.field("cpu", pa.int32()),
            pa.field("mem_gb", pa.float64()),
            pa.field("python", pa.string()),
            pa.field("engine", pa.string()),
            pa.field("engine_version", pa.string()),
            pa.field("engine_status", pa.string()),
            pa.field("run_type", pa.string()),
            pa.field("phase", pa.string()),
            pa.field("query", pa.int32()),
            pa.field("dur", pa.float64()),
            pa.field("rows", pa.int64()),
            pa.field("status", pa.string()),
            pa.field("error", pa.string()),
        ]
    )
    return pa.table([pa.array(columns[f.name], type=f.type) for f in schema], schema=schema)
