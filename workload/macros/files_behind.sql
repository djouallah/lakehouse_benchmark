{#-- Daily files the leg has not folded yet, logged as one JSON line for workload_run_engine.py:

        dbt run-operation files_behind --target <engine>
        {"files_behind": 0}

     The Real workload tab reads it (how far each leg has got). It used to be
     the assert_all_daily_files_processed_* tests' warning count; the tests now run once a day in
     table maintenance (workload_test.py), so the count is its own query. The larger of fct_scada's
     and fct_price's, as before. EXCEPT rather than NOT IN: a plain set difference every engine
     plans as such (Sail's NOT IN over a big table stayed a hash anti join that ran out of memory,
     LEARNING.md). --#}
{% macro files_behind() %}
  {%- if not execute -%}{{ return('') }}{%- endif -%}
  {%- set log_rel = source('landing', 'csv_raw_archive_log') -%}
  {%- set behind = [] -%}
  {%- for node in graph.nodes.values() if node.resource_type == 'model' and node.name in ['fct_scada', 'fct_price'] -%}
    {%- set rel = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.alias) -%}
    {%- if rel is not none -%}
      {%- set q -%}
        SELECT COUNT(*) FROM (
          SELECT csv_filename FROM {{ log_rel }} WHERE source_type = 'daily'
          EXCEPT
          SELECT {{ adapter.quote('file') }} FROM {{ rel }}
        ) pending
      {%- endset -%}
      {%- do behind.append(run_query(q).rows[0][0] | int) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ log('{"files_behind": ' ~ (behind | max if behind else 'null') ~ '}', info=True) }}
{% endmacro %}
