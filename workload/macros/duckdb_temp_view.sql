{#-- DuckDB: an incremental model's batch is a TEMP VIEW, not a TEMP TABLE.

     dbt-duckdb inherits dbt-core's incremental recipe: run the model into a temp relation, read
     its columns (expand_target_column_types, on_schema_change), then MERGE from it. As a TEMP
     TABLE the whole batch is materialized first, and DuckDB keeps TEMP objects in its in-memory
     `temp` catalog, uncompressed, spilling uncompressed: at 100 files a run the runner died, at
     360 the spill was refused (runs 39382816547, 39403578519). Measured on 20M rows under a
     300 MB cap: 582 MiB of uncompressed spill for the TEMP TABLE.

     A view gives dbt the same relation to introspect -- DESCRIBE binds a view, it does not run
     it -- and the MERGE then streams the query once, the way dbt-spark's temp view does. Nothing
     model-specific: every temporary SQL relation on DuckDB, everything else through dbt-duckdb's
     own dispatch, other adapters untouched. --#}
{% macro create_table_as(temporary, relation, compiled_code, language='sql', partitioned_by=none, sorted_by=none) -%}
  {%- if target.type == 'duckdb' and temporary and language == 'sql' -%}
    {{ return("create temporary view " ~ relation.include(database=false, schema=false) ~ " as (\n" ~ compiled_code ~ "\n)") }}
  {%- elif target.type == 'duckdb' -%}
    {{ return(adapter.dispatch('create_table_as', 'dbt')(temporary, relation, compiled_code, language, partitioned_by=partitioned_by, sorted_by=sorted_by)) }}
  {%- else -%}
    {{ return(adapter.dispatch('create_table_as', 'dbt')(temporary, relation, compiled_code, language)) }}
  {%- endif -%}
{%- endmacro %}
