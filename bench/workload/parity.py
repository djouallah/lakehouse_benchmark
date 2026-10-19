"""Grade the legs' gold-table fingerprints against each other: the workload's acceptance test.

fabric-dbt-examples' .github/scripts/parity.py, as functions. Each leg fingerprints fct_summary
through its OWN adapter (`dbt run-operation parity_fingerprint`, workload/macros/
parity_fingerprint.sql) as the last step of its run; the publish job grades them here.

EXACT fields must be identical: a difference there is a difference in what the pipeline built.
The money sums get REL_TOL, because two engines can compute the same thing and still disagree
in the last place -- DOUBLE -> DECIMAL tie-breaking (HALF_UP on Spark, HALF_EVEN on DuckDB) and
the order ~10^6 doubles are summed in. It absorbs rounding, not logic: a leg past it is a
finding, not a threshold to raise.
"""

from __future__ import annotations

import json
import re

from bench.workload.config import BASELINE

EXACT = ("rows_total", "duids", "days", "date_min", "date_max")
APPROX = ("mw_sum", "price_sum")
REL_TOL = 1e-7


def extract(text: str) -> dict | None:
    """The fingerprint object in a `dbt run-operation parity_fingerprint` log, or None."""
    m = re.search(r"\{\s*\"engine\".*?\n[^\n{}]*\}", text, re.S)
    return json.loads(m.group(0)) if m else None


def rel_diff(a: float, b: float) -> float:
    return abs(a - b) / max(abs(a), abs(b), 1.0)


def grade(fps: dict[str, dict]) -> dict:
    """{baseline, engines, failures, diffs}. `diffs[engine][field]` is the relative difference
    from the baseline on each APPROX field. The baseline is BASELINE when it built, else the
    first engine by name. Fewer than two fingerprints grade nothing and fail nothing."""
    engines = sorted(fps, key=lambda e: (e != BASELINE, e))
    out: dict = {"baseline": engines[0] if engines else None, "engines": engines}
    out["failures"], out["diffs"] = [], {}
    if len(engines) < 2:
        return out
    base_name = engines[0]
    base = fps[base_name]
    for e in engines[1:]:
        fp = fps[e]
        for field in EXACT:
            if fp.get(field) != base.get(field):
                out["failures"].append(
                    f"{field}: {base_name}={base.get(field)!r} but {e}={fp.get(field)!r}"
                )
        out["diffs"][e] = {}
        for field in APPROX:
            a, b = float(base.get(field, 0)), float(fp.get(field, 0))
            d = rel_diff(a, b)
            out["diffs"][e][field] = d
            if d > REL_TOL:
                out["failures"].append(
                    f"{field}: {base_name}={a!r} but {e}={b!r} "
                    f"(relative difference {d:.3e} > {REL_TOL:.0e})"
                )
    return out
