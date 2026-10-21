{#-- THE MERGE ONLY TOUCHES THE TARGET ROWS THIS BATCH CAN MATCH.

     dbt's merge joins the batch against the WHOLE target: its ON clause is the unique key and
     nothing else, so every run scans every row the table has ever folded, and the scan grows with
     the table. A target row can only match a batch row on the same key, so a key column's batch
     values bound the target exactly: `DBT_INTERNAL_DEST.<col> IN (<this batch's values>)` is an
     incremental predicate that changes no result. As LITERALS, Iceberg prunes on them: each run
     writes its own data files, whose min/max on `file` (or `date`) are this run's, so almost every
     data file is skipped before it is read.

     The column: `file` (every fact's key starts with it) or `date` (fct_summary). dim_duid has
     neither and merges unbounded (a few hundred rows). The values are read from the batch AT RUN
     TIME -- dbt fixes config() at parse time, so a per-run list cannot be an
     `incremental_predicates` config -- from the temp relation the adapter just built.

     Wired into each adapter's merge (below). Polars is not here: dbt-polars refuses
     incremental_predicates on Iceberg, and its merge already prunes data files on the key
     values (one In() per key column) before it reads them. --#}
{% macro merge_prune_predicates(source) %}
  {%- set key = config.get('unique_key') -%}
  {%- set keys = [key] if key is string else (key or []) -%}
  {%- set col = (['file', 'date'] | select('in', keys) | list | first) -%}
  {%- if not col or not execute -%}{{ return([]) }}{%- endif -%}
  {#-- The sail probe target casts the collected column: Sail returned `file` as Arrow string_view
       here, which the Spark Connect client cannot read ("[UNSUPPORTED_DATA_TYPE_FOR_ARROW_CONVERSION]
       string_view is not supported in conversion to Arrow", runs 39687083463 and 39687830566, with
       pyspark-client 4.2.0 and 4.1.1). --#}
  {%- set q = 'SELECT DISTINCT ' ~ col ~ ' FROM ' ~ source -%}
  {%- if target.name == 'sail' and col == 'file' -%}
    {%- set q = 'SELECT CAST(' ~ col ~ ' AS STRING) AS ' ~ col ~ ' FROM (' ~ q ~ ')' -%}
  {%- endif -%}
  {%- set values = run_query(q).columns[0].values() -%}
  {%- set literals = [] -%}
  {%- for v in values if v is not none -%}
    {%- if v is string -%}
      {%- do literals.append("'" ~ v | replace("'", "''") ~ "'") -%}
    {%- else -%}
      {%- do literals.append("DATE '" ~ (v | string)[:10] ~ "'") -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if literals | length == 0 -%}{{ return([]) }}{%- endif -%}
  {{ return(['DBT_INTERNAL_DEST.' ~ col ~ ' IN (' ~ literals | join(', ') ~ ')']) }}
{% endmacro %}


{#-- DuckDB: dbt-duckdb's merge strategy calls duckdb__get_merge_sql directly, not through
     get_merge_sql, so the predicates go in here; the body is dbt-duckdb's own. --#}
{% macro duckdb__get_incremental_merge_sql(args_dict) %}
  {%- set predicates = (normalize_incremental_predicates(args_dict.get('incremental_predicates')) or [])
                       + merge_prune_predicates(args_dict['temp_relation']) -%}
  {{ return(duckdb__get_merge_sql(args_dict['target_relation'], args_dict['temp_relation'],
                                  args_dict['unique_key'], args_dict['dest_columns'], predicates)) }}
{% endmacro %}


{#-- Spark and Trino: both adapters' merge strategies end in get_merge_sql, which dispatches to
     spark__get_merge_sql / trino__get_merge_sql unchanged. --#}
{% macro get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) %}
  {%- set given = [incremental_predicates] if incremental_predicates is string else (incremental_predicates or []) -%}
  {%- set predicates = given + merge_prune_predicates(source) -%}
  {{ return(adapter.dispatch('get_merge_sql', 'dbt')(target, source, unique_key, dest_columns, predicates)) }}
{% endmacro %}
