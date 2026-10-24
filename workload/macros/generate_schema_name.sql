{#-- dbt_<engine>_<layer>: dbt_duckdb_landing, dbt_polars_mart, dbt_gluten_mart.

     The ENGINE is what keeps the legs apart -- they all write into bench's one lakehouse -- and
     the `dbt_` prefix keeps the whole test apart from every other suite's namespace there
     (CH*, DS*, T*, _bench_*; Agent.md). Two legs resolving to one schema would overwrite each
     other's gold layer with every test still green, so this is load-bearing, not cosmetic.
     No DBT_SCHEMA isolation lever as in fabric-dbt-examples: nothing else writes these
     namespaces.

     WORKLOAD_LEG points the names at another leg's tables: the daily tests in table maintenance
     (workload_test.py) run the duckdb target's tests against every leg, dbt_<leg>_*. Never set in
     a build. --#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set layer = (custom_schema_name | trim) if custom_schema_name is not none else 'mart' -%}
    {{ 'dbt_' ~ env_var('WORKLOAD_LEG', target.name) ~ '_' ~ layer }}
{%- endmacro %}
