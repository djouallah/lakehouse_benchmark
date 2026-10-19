{#-- dbt-spark's table create, without the STAGED create OneLake does not support.

     dbt-spark builds every table, and the first build of every incremental model, as
     `create or replace table <rel> using iceberg as <select>`. Spark runs that through
     SparkCatalog.stageCreateOrReplace, i.e. a REST createTable with stage-create=true, and the
     OneLake Iceberg endpoint answers a staged create with `400 Malformed request` (by design;
     the duckdb leg sets stage_create_tables: 0 in profiles.yml for the same reason). Workload
     run 39334923350 lost the gluten leg to it on its first model.

     So, for an Iceberg table only:
       1. the select as a temp view, and its columns from DESCRIBE
       2. DROP TABLE IF EXISTS            -- what `or replace` did
       3. CREATE TABLE <rel> (<columns>) USING iceberg   -- SparkCatalog.createTable, not staged
       4. INSERT INTO <rel> SELECT * FROM the view         -- returned, run as the model's `main`
     Not atomic like a real CREATE OR REPLACE: a failure between 2 and 4 leaves no table, and the
     next run builds it from scratch. Temporary views and every other file format go to
     dbt-spark's own macro unchanged. --#}
{% macro spark__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if temporary or language != 'sql'
        or config.get('file_format', validator=validation.any[basestring]) != 'iceberg' -%}
    {{ return(dbt_spark.spark__create_table_as(temporary, relation, compiled_code, language)) }}
  {%- endif -%}
  {%- set view = relation.identifier ~ '__dbt_ctas' -%}
  {%- do run_query('create or replace temporary view ' ~ view ~ ' as\n' ~ compiled_code) -%}
  {%- set columns = [] -%}
  {%- for row in run_query('describe ' ~ view) -%}
    {%- if row[0] and not row[0].startswith('#') -%}
      {%- do columns.append('`' ~ row[0] ~ '` ' ~ row[1]) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- do run_query('drop table if exists ' ~ relation) -%}
  {%- do run_query('create table ' ~ relation ~ ' (' ~ columns | join(', ') ~ ') using iceberg') -%}
  insert into {{ relation }} select * from {{ view }}
{%- endmacro %}
