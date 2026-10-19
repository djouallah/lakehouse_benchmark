{#-- How the trino fact models read the landed AEMO CSVs. Same spec (aemo_columns.sql), same
     record rule, same file choice as every other engine; only the mechanics differ.

       * TRINO HAS NO PATH-BASED FILE READER, so the runner (workload_run_engine.py) declares
         one Hive EXTERNAL table per landing folder in a second catalog, `files`: CSV format,
         every column VARCHAR and named c0, c1, ... by position, wide enough for the widest
         record (price, 130 columns), the first line skipped. A shorter record reads NULL past its
         end, as in the DuckDB legs' null_padding. This is how bench's Trino ETL reads Files/csv
         (bench/etl/engines/trino_iceberg.py).
       * THE FILES of this run are chosen by name on the hidden "$path" column. The list itself is
         polars_pending_files() -- the same parse-time run_query, newest first, process_limit --
         because a Trino hook cannot hold a variable for the model body either.
       * Hive's CSV reader returns an empty field as '', which CAST rejects and every other leg
         reads as NULL: each column goes through NULLIF. --#}

{% macro trino_csv_table(folder) -%}
  files.dbt.{{ folder }}
{%- endmacro %}


{#-- The record-selection predicate, on the record's named (still VARCHAR) columns. --#}
{% macro trino_record_filter(record) %}
  {%- set spec = aemo_spec(record) -%}
  {%- set parts = [] -%}
  {%- for col, val in spec['equals'] %}{% do parts.append('"' ~ col ~ "\" = '" ~ val ~ "'") %}{% endfor -%}
  {%- if spec['nonzero'] %}{% do parts.append('CAST("' ~ spec['nonzero'] ~ '" AS DOUBLE) <> 0') %}{% endif -%}
  {{ parts | join(' AND ') }}
{% endmacro %}


{#-- One record's rows from one folder, restricted to `files` (basenames). `files` is none
     outside run/build/retry, where nothing executes: no file filter then, so it still compiles.
     `_fname` is the file's full path, as the Spark tree's input_file_name(). --#}
{% macro trino_staging(record, folder, files) %}
  {%- if files is none -%}
    {%- set pick = 'TRUE' -%}
  {%- elif files | length == 0 -%}
    {%- set pick = 'FALSE' -%}
  {%- else -%}
    {%- set quoted = [] -%}
    {%- for f in files %}{% do quoted.append("'" ~ f ~ "'") %}{% endfor -%}
    {%- set pick = "element_at(split(\"$path\", '/'), -1) IN (" ~ quoted | join(', ') ~ ")" -%}
  {%- endif %}
  SELECT * FROM (
    SELECT
    {%- for name in aemo_columns(record) %}
      NULLIF(c{{ loop.index0 }}, '') AS "{{ name }}",
    {%- endfor %}
      "$path" AS _fname
    FROM {{ trino_csv_table(folder) }}
    WHERE {{ pick }}
  ) raw
  WHERE {{ trino_record_filter(record) }}
{% endmacro %}


{#-- AEMO's SETTLEMENTDATE / LASTCHANGED text, 'yyyy/MM/dd HH:mm:ss'. --#}
{% macro trino_ts(col) -%}
  date_parse({{ col }}, '%Y/%m/%d %H:%i:%s')
{%- endmacro %}
