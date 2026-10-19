{#--
    Emit a comparable fingerprint of the gold table, for parity checking across engines.

    THIS IS THE MEASUREMENT THE REPO EXISTS TO MAKE. Five adapters run the same business
    logic; this is what turns "the same" from a claim into a check.

    Run per engine, and capture stdout:
        dbt run-operation parity_fingerprint --target <engine> --profiles-dir . \
            | python .github/scripts/parity.py capture history/parity
    then compare with `parity.py compare history/parity`.

    Aggregates rather than a row-by-row diff on purpose: the engines write through four
    different stacks (delta-rs, an Iceberg REST catalog, DuckLake parquet, delta-spark) and
    each reports through its OWN adapter. Only the
    numbers are compared.

    A known reason the numbers can differ WITHOUT the logic differing, handled in parity.py
    rather than here:
      * DOUBLE -> DECIMAL tie-breaking is HALF_UP on Spark and HALF_EVEN on DuckDB, so the
        money columns get a relative tolerance, not equality.
--#}

{% macro parity_fingerprint_sql(model='fct_summary') %}
  {%- set rel = ref(model) -%}
  {#-- `dbl` is per-dialect for the same reason the date casts are: Spark SQL has no
       DOUBLE PRECISION and rejects it outright ("extra input 'PRECISION'"), which failed
       the spark leg AFTER a clean 60/62 build -- the fingerprint is the last step, so a
       dialect slip here throws away the whole run's measurement. --#}
  {%- if target.type == 'spark' -%}
    {%- set d, t, mw, price = '`date`', '`time`', 'mw', 'price' -%}
    {%- set dbl = 'DOUBLE' -%}
    {%- set to_text = "CAST(MIN(`date`) AS STRING)" -%}
    {%- set to_text_max = "CAST(MAX(`date`) AS STRING)" -%}
  {%- elif target.type == 'trino' -%}
    {%- set d, t, mw, price = '"date"', '"time"', 'mw', 'price' -%}
    {%- set dbl = 'DOUBLE' -%}
    {%- set to_text = 'CAST(MIN("date") AS VARCHAR)' -%}
    {%- set to_text_max = 'CAST(MAX("date") AS VARCHAR)' -%}
  {%- else -%}
    {%- set d, t, mw, price = 'date', 'time', 'mw', 'price' -%}
    {%- set dbl = 'DOUBLE' -%}
    {%- set to_text = "CAST(MIN(date) AS VARCHAR)" -%}
    {%- set to_text_max = "CAST(MAX(date) AS VARCHAR)" -%}
  {%- endif -%}

  {%- set q -%}
    SELECT
      COUNT(*)                         AS rows_total,
      COUNT(DISTINCT DUID)             AS duids,
      COUNT(DISTINCT {{ d }})          AS days,
      {{ to_text }}                    AS date_min,
      {{ to_text_max }}                AS date_max,
      SUM(CAST({{ mw }} AS {{ dbl }}))    AS mw_sum,
      SUM(CAST({{ price }} AS {{ dbl }})) AS price_sum
    FROM {{ rel }}
  {%- endset -%}
  {{ return(q) }}
{% endmacro %}


{% macro parity_fingerprint(model='fct_summary') %}
  {%- if not execute -%}{{ return('') }}{%- endif -%}
  {%- set q = parity_fingerprint_sql(model) -%}

  {#-- `dbt run-operation` DOES NOT FIRE on-run-start HOOKS. The DuckDB-family targets get
       their OneLake transport from one of those hooks, so this session would otherwise open
       on DuckDB's default transport and fail the OneLake TLS handshake -- observed as
       "could not open file ... Problem with the SSL CA cert (path? access rights?)" on a
       snapshot avro, AFTER a clean 60/62 build. Set it here, the same way
       compact_iceberg.py does for the same reason. --#}
  {#-- 'duckrun' AND 'duckdb': duckrun is its own adapter type, and it reads OneLake
       Delta here, so leaving it out would break it the same way. Same condition as the
       on-run-start hook in dbt_project.yml: `target.type in ['duckdb', 'duckrun']`. --#}
  {%- if target.type in ('duckdb', 'duckrun') and env_var('AZURE_TRANSPORT_OPTION_TYPE', 'default') != 'default' -%}
    {%- do run_query("SET GLOBAL azure_transport_option_type = '" ~ env_var('AZURE_TRANSPORT_OPTION_TYPE') ~ "'") -%}
  {%- endif -%}

  {%- set r = run_query(q).rows[0] -%}
  {%- set out -%}
{
  "engine": "{{ target.name }}",
  "adapter": "{{ target.type }}",
  "model": "{{ model }}",
  "rows_total": {{ r[0] }},
  "duids": {{ r[1] }},
  "days": {{ r[2] }},
  "date_min": "{{ r[3] }}",
  "date_max": "{{ r[4] }}",
  "mw_sum": {{ r[5] }},
  "price_sum": {{ r[6] }}
}
  {%- endset -%}
  {{ log(out, info=True) }}
{% endmacro %}
