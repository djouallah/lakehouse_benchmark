"""Render the query-suite charts: per query, totals, trend. TPC-H and TPC-DS both come here.

REPLACES cells 20-25. Four of the notebook's five charts survive in some form; one does not.

WHAT CHANGED AND WHY:

* `delta_scan(results)` -> `store.load_all("results")`. The data is local JSON now.
* THE DATE FILTERS ARE GONE. Cells 21 and 22 carried `time > '2026-05-01'` and
  `time > '2026-06-31'`. The second is not a date -- June has 30 days -- and it only ever
  compared because `time` was a VARCHAR and DuckDB fell back to a lexical comparison. With a real
  timestamp it would raise. Runs are filtered on `sf` and `test`, and nothing else.
* CELL 25 IS DROPPED and cell 23 is rewritten. Both plotted duration against CORE COUNT, which on
  a fixed `ubuntu-latest` is a constant: cell 25 becomes one bar, and cell 23's x-axis collapses.
  Cell 23's slot is taken by the chart it was reaching for -- total seconds per engine, sorted,
  which is the "who won" chart.
* THE TREND CHART IS GUARDED. With one day of history a lineplot is a single dot, which reads as
  broken rather than as new. It renders from the second distinct day onwards.
* THE PER-QUERY CHART WRAPS. 22 queries fit one row of grouped bars; 99 do not, so `per_row`
  splits the query axis into stacked bands (TPC-DS: three of 33). With one band the TPC-H chart
  is what it always was.

The palette, the themes and the axis styling are bench/charts.py, shared with the ETL charts.
"""

from __future__ import annotations

import re
from pathlib import Path

import matplotlib.pyplot as plt

from bench.charts import LABEL, RECENT_RUNS, THEMES, _legend, _runs_note, _save, _style
from bench.tpch.config import ENGINES


def _window(con, sf: int, test: str) -> tuple[int, int]:
    """Register `recent`: each engine's last RECENT_RUNS runs of `test` at this scale factor.

    PER ENGINE, not per run. A run may carry one engine or all of them -- adding Gluten needed
    one job, not six -- and a window over whole runs would let three single-engine runs push
    every other engine off the charts. Each engine is drawn from its own latest runs instead.

    Ordered by the run timestamp rather than the id, because a GitHub run id is not monotonic
    across re-runs. Returns the fewest and most runs any engine contributed, so a chart can say.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW recent AS
        SELECT raw.* FROM raw
        JOIN (
            SELECT engine, run_id FROM (
                SELECT engine, run_id,
                       row_number() OVER (
                           PARTITION BY engine ORDER BY max(run_started_at) DESC
                       ) AS rn
                FROM raw
                WHERE sf = {sf} AND test = '{test}'
                GROUP BY engine, run_id
            ) WHERE rn <= {RECENT_RUNS}
        ) latest USING (engine, run_id)
        WHERE sf = {sf} AND test = '{test}'
        """
    )
    low, high = con.execute(
        "SELECT min(n), max(n) FROM "
        "(SELECT engine, count(DISTINCT run_id) AS n FROM recent GROUP BY engine)"
    ).fetchone()
    return low or 0, high or 0


def _present(con, run_type: str) -> list[str]:
    """Engines with at least one successful query, in the fixed palette order."""
    found = {
        row[0]
        for row in con.execute(
            "SELECT DISTINCT engine FROM recent WHERE run_type=? AND phase='query' AND status='ok'",
            [run_type],
        ).fetchall()
    }
    return [e for e in ENGINES if e in found]


def per_query(
    con, sf: int, run_type: str, out_dir: Path, subtitle: str, per_row: int = 22
) -> list[Path]:
    """Grouped bars: seconds per query, one group per query, one bar per engine.

    The headline chart, and the notebook's cell 21 -- except that it only ever plotted cold, so
    the warm twin is new. Averaged over every run at this scale factor, because a single run on a
    shared runner is noisier than the differences being shown.
    """
    engines = _present(con, run_type)
    if not engines:
        return []
    rows = con.execute(
        "SELECT query, engine, AVG(dur) AS dur FROM recent "
        "WHERE run_type=? AND phase='query' AND status='ok' "
        "GROUP BY query, engine ORDER BY query",
        [run_type],
    ).fetchall()
    data: dict[str, dict[int, float]] = {e: {} for e in engines}
    for query, engine, dur in rows:
        if engine in data:
            data[engine][query] = dur
    queries = sorted({q for values in data.values() for q in values})
    bands = [queries[i : i + per_row] for i in range(0, len(queries), per_row)]

    # Queries this engine could not complete. A missing bar and a near-zero bar look identical,
    # so a failure would otherwise read as "extremely fast" -- the opposite of the truth, and
    # exactly the case that matters (Polars and LakeSail on Q9/Q18/Q21 at SF>=10).
    failed = {
        (engine, query)
        for query, engine in con.execute(
            "SELECT DISTINCT query, engine FROM recent "
            "WHERE run_type=? AND phase='query' AND status='error'",
            [run_type],
        ).fetchall()
    }

    paths = []
    for theme in THEMES.values():
        fig, axes = plt.subplots(
            len(bands),
            1,
            figsize=(18, 7 if len(bands) == 1 else 5.5 * len(bands) + 1.5),
            squeeze=False,
        )
        # A 2px surface gap between adjacent bars: total group width 0.82 of the slot.
        width = 0.82 / len(engines)
        for ax, band in zip(axes[:, 0], bands, strict=True):
            for index, engine in enumerate(engines):
                offset = (index - (len(engines) - 1) / 2) * width
                ax.bar(
                    [q + offset for q in band],
                    [data[engine].get(q, 0.0) for q in band],
                    width=width * 0.94,
                    color=theme["colors"][engine],
                    edgecolor=theme["surface"],
                    linewidth=1.0,
                    zorder=3,
                )
                for query in band:
                    if (engine, query) in failed:
                        ax.plot(
                            query + offset,
                            0,
                            marker="x",
                            markersize=7,
                            markeredgewidth=1.8,
                            color=theme["colors"][engine],
                            zorder=5,
                            clip_on=False,
                        )
            ax.set_xticks(band)
            ax.set_xticklabels([f"Q{q}" for q in band])
            ax.set_ylim(bottom=0)
            _style(ax, theme, "", "seconds (lower is better)")
        top, bottom = axes[0, 0], axes[-1, 0]
        top.set_title(
            f"{run_type.title()} run, seconds per query — {subtitle}",
            color=theme["primary"],
            fontsize=13,
            pad=14,
            loc="left",
        )
        _legend(top, theme, engines)
        if failed:
            bottom.text(
                0.0,
                -0.13,
                "✕ = query failed (see RESULTS.md for the error)",
                transform=bottom.transAxes,
                fontsize=9,
                color=theme["secondary"],
            )
        if len(bands) > 1:
            fig.subplots_adjust(hspace=0.3)
        paths.append(_save(fig, out_dir, f"{run_type}_per_query", theme))
    return paths


def totals(con, sf: int, out_dir: Path, subtitle: str, n_queries: int = 22) -> list[Path]:
    """Horizontal bars: total seconds per engine, cold (and warm, for old runs), fastest first.

    Replaces cell 23, which plotted warm totals against core count and then filtered to a single
    core count -- one bar per engine with an axis that carried no information. This is the chart
    it was reaching for.

    Value labels are the relief the light palette's contrast WARN requires; they also make the
    chart readable without the axis.
    """
    rows = con.execute(
        "SELECT engine, run_type, SUM(dur) / COUNT(DISTINCT run_id) AS dur FROM recent "
        "WHERE phase='query' AND status='ok' "
        "GROUP BY engine, run_type",
    ).fetchall()
    if not rows:
        return []
    by_engine: dict[str, dict[str, float]] = {}
    for engine, run_type, dur in rows:
        by_engine.setdefault(engine, {})[run_type] = dur
    order = sorted(
        (e for e in ENGINES if e in by_engine),
        key=lambda e: by_engine[e].get("cold", float("inf")),
        reverse=True,  # fastest ends up at the top of a horizontal axis
    )

    paths = []
    for theme in THEMES.values():
        fig, ax = plt.subplots(figsize=(11, 1.1 * len(order) + 2.2))
        height = 0.36
        # One bar per engine, centred, when there is no warm pass (both suites run cold only now).
        slots = (
            ((height / 2, "cold", 1.0), (-height / 2, "warm", 0.55))
            if any("warm" in values for values in by_engine.values())
            else ((0.0, "cold", 1.0),)
        )
        for index, engine in enumerate(order):
            for offset, run_type, alpha in slots:
                value = by_engine[engine].get(run_type)
                if value is None:
                    continue
                ax.barh(
                    index + offset,
                    value,
                    height=height * 0.94,
                    color=theme["colors"][engine],
                    alpha=alpha,
                    edgecolor=theme["surface"],
                    linewidth=1.0,
                    zorder=3,
                )
                ax.text(
                    value,
                    index + offset,
                    f"  {value:,.0f}s  {run_type}",
                    va="center",
                    ha="left",
                    fontsize=9,
                    color=theme["secondary"],
                    zorder=4,
                )
        ax.set_yticks(range(len(order)))
        ax.set_yticklabels([LABEL[e] for e in order], color=theme["secondary"])
        ax.set_xlim(
            left=0, right=max(v for values in by_engine.values() for v in values.values()) * 1.28
        )
        _style(ax, theme, f"Total seconds for all {n_queries} queries — {subtitle}", "")
        ax.set_xlabel("seconds (lower is better)", color=theme["secondary"], fontsize=10)
        ax.grid(axis="y", visible=False)
        ax.grid(axis="x", linestyle="--", linewidth=0.7, color=theme["grid"], alpha=0.8)
        paths.append(_save(fig, out_dir, "totals", theme))
    return paths


def totals_by_sf(
    con, sfs: tuple[int, ...], out_dir: Path, subtitle: str, test: str, n_queries: int
) -> list[Path]:
    """Grouped horizontal bars: cold total per engine, one group per scale factor.

    Both suites' totals chart.

    THE MEAN OF EACH ENGINE'S LAST RECENT_RUNS COMPLETE RUNS at each scale -- the same three-run
    window the per-query chart uses, because one run on a shared runner is noisier than the
    differences being shown. The label carries no run count: under the time it read as noise.

    EACH BAR SAYS HOW MANY TIMES SLOWER IT IS than the fastest engine AT THAT SCALE -- not a fixed
    baseline engine, because no engine has a bar at every scale (TPC-DS SF 100 has no DuckDB).
    The fastest bar, and a bar alone at its scale, carry the time only: "1x" says nothing.

    ONLY A RUN THAT COMPLETED EVERY STATEMENT COUNTS. A total over the statements that finished
    leaves out the ones that died, so it reads as fast when it is not a total at all. A run that
    lost any statement is skipped, and an engine with no complete run at a scale has no bar there.
    """
    rows = con.execute(
        f"""
        WITH runs AS (
            SELECT engine, sf, run_id, max(run_started_at) AS started,
                   sum(dur) AS dur,
                   count(*) FILTER (WHERE status = 'ok') AS ok
            FROM raw
            WHERE test = ? AND run_type = 'cold' AND phase = 'query'
              AND sf IN ({", ".join(str(s) for s in sfs)})
            GROUP BY engine, sf, run_id
        ), ranked AS (
            SELECT *, row_number() OVER (PARTITION BY engine, sf ORDER BY started DESC) AS rn
            FROM runs
            WHERE ok = {n_queries}
        )
        SELECT engine, sf, avg(dur), count(*) FROM ranked
        WHERE rn <= {RECENT_RUNS}
        GROUP BY engine, sf
        """,
        [test],
    ).fetchall()
    data = {(engine, sf): (dur, n) for engine, sf, dur, n in rows}
    if not data:
        return []
    engines = [e for e in ENGINES if any(k[0] == e for k in data)]
    shown = [s for s in sfs if any(k[1] == s for k in data)]
    height = 0.8 / len(engines)
    top = max(dur for dur, _ in data.values())

    paths = []
    for theme in THEMES.values():
        # HORIZONTAL: the page is wider than it is tall, and a long bar leaves room for its label.
        fig, ax = plt.subplots(figsize=(11, max(5, 0.3 * len(data) + 1.5)))
        for group, sf in enumerate(shown):
            # FASTEST FIRST (topmost) within each scale, and only the engines with a bar here,
            # centred on the tick -- so a missing engine leaves no hole. The label names the
            # engine: seven similar hues matched against a legend were hard to read.
            present = sorted((e for e in engines if (e, sf) in data), key=lambda e: data[(e, sf)])
            fastest = data[(present[0], sf)][0]
            for slot, engine in enumerate(present):
                dur, _ = data[(engine, sf)]
                ratio = dur / fastest
                label = f"{LABEL[engine]}  {dur:,.0f}s"
                if slot:
                    label += f" · {ratio:.1f}×" if ratio < 10 else f" · {ratio:,.0f}×"
                y = group + (slot - (len(present) - 1) / 2) * height
                ax.barh(
                    y,
                    dur,
                    height=height * 0.94,
                    color=theme["colors"][engine],
                    edgecolor=theme["surface"],
                    linewidth=1.0,
                    zorder=3,
                )
                ax.text(
                    dur + top * 0.005,
                    y,
                    label,
                    ha="left",
                    va="center",
                    fontsize=9,
                    color=theme["secondary"],
                    zorder=4,
                )
        ax.set_yticks(range(len(shown)))
        ax.set_yticklabels([f"SF {s}" for s in shown], color=theme["secondary"], fontsize=10)
        ax.set_xlim(0, top * 1.3)  # room for "Spark-OSS  9,303s · 25×" after the longest bar
        ax.invert_yaxis()  # smallest scale, and the fastest engine within it, at the top
        # Two lines: on one, the run's subtitle made the title wider than the plot, and the saved
        # figure grew to fit it, squeezing the bars into the left two thirds.
        title = f"Total seconds for all {n_queries} queries, cold\n{subtitle}"
        _style(ax, theme, title, "", pad=14)
        # _style draws a vertical chart's axes; turn them for this one.
        ax.set_xlabel(
            "seconds (lower is better) · N× = times the fastest engine at that scale",
            color=theme["secondary"],
            fontsize=10,
        )
        ax.grid(axis="y", visible=False)
        ax.grid(axis="x", linestyle="--", linewidth=0.7, color=theme["grid"], alpha=0.8)
        ax.spines["bottom"].set_visible(False)
        ax.spines["left"].set_visible(True)
        ax.tick_params(axis="y", length=0)
        # No legend: every bar names its engine.
        paths.append(_save(fig, out_dir, "totals", theme))
    return paths


def trend(con, sf: int, out_dir: Path, subtitle: str, test: str = "tpch") -> list[Path]:
    """Total seconds per engine over time, cold solid and warm dashed.

    Merges cells 22 and 24, which drew the same thing twice.

    GUARDED: with one day of history this is a single dot per series, which reads as a broken
    chart rather than as a new one. It appears from the second distinct day onwards.
    """
    rows = con.execute(
        "SELECT substr(run_started_at, 1, 10) AS day, engine, run_type, "
        "       SUM(dur) / COUNT(DISTINCT run_id) AS dur "
        "FROM raw WHERE sf=? AND test=? AND phase='query' AND status='ok' "
        "GROUP BY day, engine, run_type ORDER BY day",
        [sf, test],
    ).fetchall()
    days = sorted({row[0] for row in rows})
    if len(days) < 2:
        return []

    series: dict[tuple[str, str], dict[str, float]] = {}
    for day, engine, run_type, dur in rows:
        series.setdefault((engine, run_type), {})[day] = dur

    paths = []
    for theme in THEMES.values():
        fig, ax = plt.subplots(figsize=(14, 6))
        for (engine, run_type), values in series.items():
            if engine not in theme["colors"]:
                continue
            ax.plot(
                days,
                [values.get(d) for d in days],
                color=theme["colors"][engine],
                linewidth=2.0,
                linestyle="-" if run_type == "cold" else "--",
                marker="o" if run_type == "cold" else "s",
                markersize=6,
                markeredgecolor=theme["surface"],
                markeredgewidth=1.5,
                zorder=3,
            )
        ax.set_ylim(bottom=0)
        _style(ax, theme, f"Total seconds over time — {subtitle}", "seconds (lower is better)")
        present = [e for e in ENGINES if any(k[0] == e for k in series)]
        _legend(ax, theme, present)
        # The second encoding: line style, so cold/warm is not carried by opacity alone.
        if any(k[1] == "warm" for k in series):
            ax.text(
                0.0,
                -0.16,
                "solid = cold   ·   dashed = warm",
                transform=ax.transAxes,
                fontsize=9,
                color=theme["secondary"],
            )
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
        paths.append(_save(fig, out_dir, "trend", theme))
    return paths


def render_all(
    table,
    sf: int,
    out_dir: str | Path,
    subtitle: str,
    test: str = "tpch",
    n_queries: int = 22,
    per_row: int = 22,
    totals_sfs: tuple[int, ...] = (),
) -> list[Path]:
    """Every chart that has data, light and dark, for one suite (`test`) at one scale.

    `totals_sfs` swaps the one-scale totals chart for `totals_by_sf` over those scales.
    """
    import duckdb

    con = duckdb.connect()
    con.register("raw", table)
    out_dir = Path(out_dir)

    # The headline charts read `recent` (each engine's last RECENT_RUNS runs); trend reads `raw`.
    low, high = _window(con, sf, test)
    note = _runs_note(high) if low == high else f"each engine's last {low}-{high} runs"
    windowed = f"{subtitle} · {note}"

    paths: list[Path] = []
    paths += per_query(con, sf, "cold", out_dir, windowed, per_row)
    paths += per_query(con, sf, "warm", out_dir, windowed, per_row)
    if totals_sfs:
        # Every scale on one chart: the subtitle drops its own "SF n", and the date is not the
        # date of every bar, so it says whose runs these are instead.
        subtitle_all = re.sub(r" · \d{4}-\d{2}-\d{2}", "", subtitle.replace(f" SF {sf} ·", " ·"))
        subtitle_all += f" · mean of each engine's last {RECENT_RUNS} complete runs"
        paths += totals_by_sf(con, totals_sfs, out_dir, subtitle_all, test, n_queries)
    else:
        paths += totals(con, sf, out_dir, windowed, n_queries)
    paths += trend(con, sf, out_dir, subtitle, test)
    con.close()
    return paths
