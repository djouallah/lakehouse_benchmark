{#-- A query's dates as DATE literals, for an `IN (...)` that Iceberg can prune on.

     `s.DATE IN (SELECT date FROM rebuild_dates)` is a subquery: Iceberg plans the scan before it
     runs, so no data file is skipped and fct_summary read and joined all of fct_scada and
     fct_price every run (DuckDB 332 s, Gluten 300 s, Sail 459 s with nothing new to fold; Trino's
     dynamic filtering pruned at run time, 43 s; run 39913866873). As literals, a date's bounds
     are exact in the manifest (merge_prune.sql), so only the files holding those dates are read.

     Run once, at run time, like merge_prune_predicates. Outside run/build/retry the query text
     itself goes back, so `IN (<query>)` still compiles. No dates: a NULL, which matches nothing. --#}
{% macro date_literals(query) %}
  {%- if not (execute and flags.WHICH in ('run', 'build', 'retry')) -%}{{ return(query) }}{%- endif -%}
  {%- set dates = [] -%}
  {%- for v in run_query(query).columns[0].values() if v is not none -%}
    {%- do dates.append("DATE '" ~ (v | string)[:10] ~ "'") -%}
  {%- endfor -%}
  {{ return(dates | join(', ') if dates else 'CAST(NULL AS DATE)') }}
{% endmacro %}


{#-- DOUBLE -> DECIMAL(18,4) with Spark's last digit, on every engine.

     A plain CAST breaks a tie at the fourth decimal each engine its own way (LEARNING.md, "Why
     the decimals differ in the last place"): Spark and Trino HALF_UP, DuckDB multiplies first and
     sometimes rounds down (duckdb#27503), Polars rounds ties to even (polars#30074). AEMO publishes
     up to five decimals, so DECIMAL(28,10) holds the value exactly (an engine that multiplies by
     10^10 and rounds is exact too below ~9e5; prices and MW are far below). Moved 1e-10 away from
     zero it is no longer a tie, and every engine's nearest rounding then lands on HALF_UP.
     28, not 38: Spark widens a (38,10) + (38,10) to precision 39 and cuts the scale to 9, which
     drops the 1e-10. --#}
{% macro decimal_18_4(col) -%}
  CAST(CASE WHEN {{ col }} < 0
            THEN CAST({{ col }} AS DECIMAL(28, 10)) - CAST('0.0000000001' AS DECIMAL(28, 10))
            ELSE CAST({{ col }} AS DECIMAL(28, 10)) + CAST('0.0000000001' AS DECIMAL(28, 10)) END
       AS DECIMAL(18, 4))
{%- endmacro %}
