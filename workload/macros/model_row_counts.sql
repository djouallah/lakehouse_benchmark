{#-- Rows in every enabled model's table, logged as one JSON line for workload_run_engine.py:

        dbt run-operation model_row_counts --target <engine>
        {"model_rows": {"fct_summary": 3086181, ...}, "model_files": {"fct_scada": 1295, ...}}

     model_files: the distinct source files each fact holds (its `file` column), i.e. how many
     files that engine has processed. The Real workload tab shows fct_scada's on each card.

     The Real workload tab writes them on the DAG's nodes. A model whose relation this session
     cannot see -- the duckdb leg's archive log is a view in DuckDB's in-memory catalog, gone once
     the build's process ends -- is left out rather than counted as zero. --#}
{% macro model_row_counts() %}
  {%- if not execute -%}{{ return('') }}{%- endif -%}
  {%- set out = {} -%}
  {%- set files = {} -%}
  {%- for node in graph.nodes.values() if node.resource_type == 'model' -%}
    {%- set rel = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.alias) -%}
    {%- if rel is not none -%}
      {%- do out.update({node.name: run_query('SELECT COUNT(*) FROM ' ~ rel).rows[0][0] | int}) -%}
      {%- if node.name in ['fct_scada', 'fct_price', 'fct_scada_today', 'fct_price_today'] -%}
        {%- do files.update({node.name: run_query('SELECT COUNT(DISTINCT file) FROM ' ~ rel).rows[0][0] | int}) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
  {{ log('{"model_rows": ' ~ tojson(out) ~ ', "model_files": ' ~ tojson(files) ~ '}', info=True) }}
{% endmacro %}
