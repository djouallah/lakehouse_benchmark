{#-- DuckDB: the incremental batch goes to an on-disk scratch database, not a TEMP table.

     dbt-duckdb builds every incremental batch with CREATE TEMP TABLE ... AS <model>, then merges
     from it. A TEMP table lives in memory UNCOMPRESSED and spills as uncompressed temp files: at
     360 files a run fct_scada (~59M rows x 54 columns) spilled until the OS refused the write
     (run 39403578519). A table in a database file is written as compressed row groups (ALP for
     the doubles, dictionary/FSST for the strings) as it fills: on 20M rows under a 300 MB cap,
     617 MiB of temp files became a 203 MiB table and no spill at all.

     `scratch` is attached in profiles.yml (a file under /mnt). The relation is a real table, so
     the materialization drops it after the merge (`temporary: false`); its name carries the run
     id, as Spark's stage and Trino's __dbt_tmp do. --#}
{% macro duckdb__dispatch_temporary_relation(target_relation, batch_id='') %}
  {%- set relation = api.Relation.create(
        database='scratch',
        schema='main',
        identifier=target_relation.identifier ~ '__dbt_tmp_' ~ invocation_id[:8] ~ (batch_id or ''),
        type='table') -%}
  {% do return({'relation': relation, 'temporary': false}) %}
{% endmacro %}
