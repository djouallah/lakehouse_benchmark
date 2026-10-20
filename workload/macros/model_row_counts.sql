{#-- Rows in every enabled model's table, logged as one JSON line for workload_run_engine.py:

        dbt run-operation model_row_counts --target <engine>
        {"model_rows": {"fct_summary": 3086181, ...}}

     The Real workload tab writes them on the DAG's nodes. A model whose relation this session
     cannot see -- the duckdb leg's archive log is a view in DuckDB's in-memory catalog, gone once
     the build's process ends -- is left out rather than counted as zero. --#}
{% macro model_row_counts() %}
  {%- if not execute -%}{{ return('') }}{%- endif -%}
  {%- set out = {} -%}
  {%- for node in graph.nodes.values() if node.resource_type == 'model' -%}
    {%- set rel = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.alias) -%}
    {%- if rel is not none -%}
      {%- do out.update({node.name: run_query('SELECT COUNT(*) FROM ' ~ rel).rows[0][0] | int}) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ log('{"model_rows": ' ~ tojson(out) ~ '}', info=True) }}
{% endmacro %}
