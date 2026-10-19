{#--
    Adapter-level overrides for the ICEBERG target only.

    THE HAZARD THIS FILE IS BUILT AROUND: `iceberg` and `ducklake` are BOTH `type: duckdb`.
    A `duckdb__` macro dispatches on ADAPTER TYPE, so anything defined here applies to the
    ducklake target too. (The direct-lake-parquet-layout repo this scheme is copied from had
    only ONE duckdb-type target, so it could override freely; we cannot.) Every macro here
    therefore branches on `target.name` and reproduces dbt-duckdb's own body verbatim for
    everyone else — and `tests_py/test_adapter_overrides.py` pins those fallback bodies against
    the INSTALLED adapter, so an upstream change fails loudly instead of leaving ducklake
    silently running a stale copy of dbt-duckdb's SQL.

    Reproduced from dbt-duckdb 1.11.0 (dbt/include/duckdb/macros/adapters.sql). Both
    overrides used to be much larger: dbt-duckdb has since upstreamed the DESCRIBE-instead-of-
    information_schema change and the no-CASCADE-for-DuckLake branch, so all that is left
    here is what is genuinely Iceberg-specific.
--#}

{#-- Iceberg delta: filter the hidden '__' column out of DESCRIBE. Everything else is
     dbt-duckdb 1.11.0's body unchanged. --#}
{% macro duckdb__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          column_type as data_type,
          cast(null as bigint) as character_maximum_length,
          cast(null as bigint) as numeric_precision,
          cast(null as bigint) as numeric_scale
      from (describe {{ relation.render() }})
      {%- if target.name == 'duckdb' %}
      {#-- Iceberg surfaces a hidden '__' column that dbt would then try to write. --#}
      where column_name != '__'
      {%- endif %}
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{#-- Iceberg delta: no CASCADE. The DuckDB Iceberg extension does not support
     DROP TABLE ... CASCADE. dbt-duckdb already omits it for DuckLake relations, so the
     else-branch below is its body unchanged. --#}
{% macro duckdb__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {% if target.name == 'duckdb' or adapter.is_ducklake(relation) %}
      drop {{ relation.type }} if exists {{ relation }}
    {% else %}
      drop {{ relation.type }} if exists {{ relation }} cascade
    {% endif %}
  {%- endcall %}
{% endmacro %}
