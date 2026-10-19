{#-- Where download_aemo.py lands the raw CSVs. ONE folder for all five engines: the repos
     used to disagree (csv/ gzipped for the DuckDB family, csv_raw/ plain for dwh) because
     Fabric OPENROWSET cannot read gzip CSV at all. Everything is landed PLAIN now, so every
     engine reads the same bytes — without that, a parity comparison means nothing. --#}
{%- macro get_csv_archive_path() -%}
{{ get_root_path() ~ '/csv_raw' }}
{%- endmacro -%}
