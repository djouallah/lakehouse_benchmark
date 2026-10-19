{#-- Spark counterpart of new_source_files(): the csv FILENAMES (with extension) a fact model
     ingests THIS RUN, resolved from the archive log AT RUN TIME via run_query. The spark stage
     macros (spark_read_csv.sql) turn the list into an explicit Hadoop brace glob for the
     `USING csv OPTIONS (path ...)` temp view.

     The selection rule is IDENTICAL to the other dialects -- files of this source_type minus
     whatever {{ this }} already holds, NEWEST first, capped at process_limit -- so every engine
     folds the SAME files. process_limit is the same knob duckrun/iceberg/ducklake read in
     their pre_hooks and dwh reads in new_source_files; spark used to be the one engine without
     it, and its first build was a bare-folder scan of the whole archive.

     this_relation is none on a first build / --full-refresh (nothing is ingested yet, so
     every file of the type is new). Returns [] while parsing (execute=false). --#}
{% macro spark_new_files(source_type, this_relation=none) %}
  {%- if not execute -%}{{ return([]) }}{%- endif -%}
  {%- set process_limit = env_var('process_limit', '1000') | int -%}
  {%- set q -%}
    SELECT archive_path
    FROM {{ ref('stg_csv_archive_log') }}
    WHERE source_type = '{{ source_type }}'
    {%- if this_relation is not none %}
      AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this_relation }})
    {%- endif %}
    ORDER BY archive_path DESC
    LIMIT {{ process_limit }}
  {%- endset -%}
  {%- set names = [] -%}
  {#-- archive_path is '/<subfolder>/<name>.CSV'; the glob needs just the real filename. --#}
  {%- for ap in run_query(q).columns[0].values() %}{% do names.append(ap.split('/')[-1]) %}{% endfor -%}
  {{ return(names) }}
{% endmacro %}
