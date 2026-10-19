{#-- Basename without extension, from a full path expression.

     This is the ONE macro with a dialect branch inside it, rather than the sibling-macro
     pattern used everywhere else, because the call site must stay identical across the five
     model trees — each dialect passes its own way of naming the source file, and the models
     are otherwise the same text:
       DuckDB / Polars  parse_filename('filename')   -- the reader's file-path column
       Spark            parse_filename('_fname')     -- input_file_name(), kept by the stage
     Keyed on target.type (`spark` for dbt-spark): this is about DIALECT, not engine. --#}
{% macro parse_filename(filepath) %}
  {%- if target.type == 'spark' -%}
    substring_index(element_at(split({{ filepath }}, '/'), -1), '.', 1)
  {%- else -%}
    {#-- DuckDB family (duckrun, iceberg, ducklake) and Polars SQL #}
    split_part(split_part({{ filepath }}, '/', -1), '.', 1)
  {%- endif -%}
{% endmacro %}
