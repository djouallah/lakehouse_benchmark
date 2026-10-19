{#-- The parquet archive log download_aemo.py writes: one row per landed source file.
     Every engine's stg_csv_archive_log reads this same file. --#}
{%- macro get_archive_log_path() -%}
{{ get_root_path() ~ '/csv_raw_archive_log.parquet' }}
{%- endmacro -%}
