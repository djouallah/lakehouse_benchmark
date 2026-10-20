{#-- dbt-trino's incremental temporary relation, named per run.

     On this catalog it is a real table (no views on OneLake: views_enabled is off), and its
     stock name, <model>__dbt_tmp, is the same for every run: two concurrent runs would build,
     merge from and drop each other's. The run's invocation id in the name keeps them apart. --#}
{% macro trino__make_temp_relation(base_relation, suffix='__dbt_tmp') %}
  {%- set identifier = base_relation.identifier ~ suffix ~ '_' ~ invocation_id[:8] -%}
  {{ return(base_relation.incorporate(path={'identifier': identifier})) }}
{% endmacro %}
