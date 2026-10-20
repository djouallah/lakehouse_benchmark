{#-- The archive log, read where it lands (models/sources.yml): `source()` on every engine that
     can resolve it to the file, the parquet read inline on Polars, whose adapter cannot. Every
     model and test that selects files reads it through here. --#}
{% macro archive_log() -%}
  {%- if target.type == 'polars' -%}
    read_parquet('{{ get_archive_log_path() }}')
  {%- else -%}
    {{ source('landing', 'csv_raw_archive_log') }}
  {%- endif -%}
{%- endmacro %}
