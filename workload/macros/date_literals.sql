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

