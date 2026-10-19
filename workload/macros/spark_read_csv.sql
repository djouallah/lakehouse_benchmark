{#-- How the gluten (Spark + Gluten/Velox) fact models read the landed CSVs. Ported from
     fabric-dbt-examples' Delta spark leg (dbt1/macros/spark_read_csv.sql); the record rule and
     the file choice are unchanged, the STAGE is not.

       pre_hook 1  spark_stage_view(record)   CREATE OR REPLACE TEMPORARY VIEW raw_<record>
                                              (<explicit STRING schema>) USING csv OPTIONS
                                              (path '<folder>/{this run's files}', header, PERMISSIVE)
       pre_hook 2  spark_stage_table(record)  CREATE OR REPLACE TEMPORARY VIEW stage_<model>
                                              AS SELECT *, input_file_name() FROM raw_<record>
                                              WHERE <record filter>
       model body                             SELECT <casts> FROM stage_<model>
       post_hook   spark_drop_stage()         DROP VIEW IF EXISTS stage_<model>

     THE STAGE IS A TEMPORARY VIEW HERE, NOT A TABLE. The source repo staged into a persistent
     Delta table because dbt-fabricspark builds its __dbt_tmp as a PERSISTENT view, which may not
     reference a temp view. dbt-spark builds __dbt_tmp as a TEMPORARY view, which may, and a CTAS
     may read one too -- so the stage costs no OneLake write and no Iceberg commit (each commit
     is a REST round-trip against the catalog, and a stage table would be two per model per run).

     Both hooks resolve THIS RUN's file list at run time (spark_new_files, capped at
     process_limit). With nothing new the stage is an EMPTY view of the same columns, so the
     body always has something to read; the merge source is zero rows. --#}

{#-- The backtick-quoted STRING schema both the temp view and the empty stage need. --#}
{% macro spark_csv_schema(record) %}
  {%- set parts = [] -%}
  {%- for c in aemo_columns(record) %}{% do parts.append('`' ~ c ~ '` STRING') %}{% endfor -%}
  {{ return(parts | join(', ')) }}
{% endmacro %}


{#-- The files this model folds this run: all of the source type on a first build, else the
     ones {{ this }} does not hold yet. Same rule as every other engine. --#}
{% macro spark_stage_files(record) %}
  {%- set spec = aemo_spec(record) -%}
  {{ return(spark_new_files(spec['source_type'], this if is_incremental() else none)) }}
{% endmacro %}


{#-- stage_<model>: a single-part name, because a temporary view has no schema. --#}
{% macro spark_stage_relation() %}
  {{ return('stage_' ~ this.identifier) }}
{% endmacro %}


{#-- pre_hook 1: the csv temp view over an explicit brace glob of this run's files. --#}
{% macro spark_stage_view(record) %}
  {%- set files = spark_stage_files(record) -%}
  {%- if files | length == 0 -%}{{ return('') }}{%- endif -%}
  {%- set spec = aemo_spec(record) -%}
  {%- set folder = get_csv_archive_path() ~ '/' ~ spec['source_type'] -%}
  {%- if files | length == 1 -%}
    {%- set path = folder ~ '/' ~ files[0] -%}
  {%- else -%}
    {%- set path = folder ~ '/{' ~ files | join(',') ~ '}' -%}
  {%- endif -%}
  CREATE OR REPLACE TEMPORARY VIEW raw_{{ record }} ({{ spark_csv_schema(record) }})
  USING csv OPTIONS (path '{{ path }}', header 'true', mode 'PERMISSIVE')
{% endmacro %}


{#-- pre_hook 2: the record's rows, as a temp view. The record filter runs HERE, on the raw
     strings, so the stage holds only DREGION/DUNIT/PRICE/SCADA rows -- a PUBLIC_DAILY file is
     mostly other report types. input_file_name() sits directly on the csv scan: it is the only
     place the provenance exists, and the models' `file` column is parsed from it. --#}
{% macro spark_stage_table(record) %}
  {%- set files = spark_stage_files(record) -%}
  {%- set stage = spark_stage_relation() -%}
  {%- if files | length == 0 -%}
  CREATE OR REPLACE TEMPORARY VIEW {{ stage }} AS SELECT
  {%- for c in aemo_columns(record) %} CAST(NULL AS STRING) AS `{{ c }}`,{% endfor %} CAST(NULL AS STRING) AS `_fname`
  WHERE 1 = 0
  {%- else -%}
  CREATE OR REPLACE TEMPORARY VIEW {{ stage }} AS
  SELECT *, input_file_name() AS _fname
  FROM raw_{{ record }}
  WHERE {{ spark_record_filter(record) }}
  {%- endif -%}
{% endmacro %}


{#-- post_hook: the stage is scratch, and session-scoped anyway. --#}
{% macro spark_drop_stage() %}
  DROP VIEW IF EXISTS {{ spark_stage_relation() }}
{% endmacro %}


{#-- The record-selection predicate, Spark quoting. Same rule as every other dialect.

     The `nonzero` column MUST be cast before the comparison. This predicate runs on the csv
     temp view, where every column is STRING, and Spark resolves `STRING != 0` by casting the
     STRING to the literal's type -- INT -- and that cast TRUNCATES a decimal fraction
     (TypeCoercion.findCommonTypeForBinaryComparison + UTF8String.toInt). So `'0.5' != 0` and
     `'-0.3' != 0` are both FALSE, and a bare `SCADAVALUE != 0` silently dropped every intraday
     SCADA row with 0 < |value| < 1 -- 12-16% of the non-zero rows (solar and battery aux load,
     wind at low speed). The DuckDB reader types the column as double and dwh TRY_CASTs to
     FLOAT, so only this leg lost them: fct_scada_today was 27,757 rows against 31,803 on the
     other four, and fct_summary's intraday tail ran 200-1,500 rows short on every run. --#}
{% macro spark_record_filter(record, prefix='') %}
  {%- set spec = aemo_spec(record) -%}
  {%- set parts = [] -%}
  {%- for col, val in spec['equals'] %}{% do parts.append(prefix ~ col ~ " = '" ~ val ~ "'") %}{% endfor -%}
  {%- if spec['nonzero'] %}{% do parts.append('CAST(' ~ prefix ~ spec['nonzero'] ~ ' AS DOUBLE) != 0') %}{% endif -%}
  {{ parts | join(' AND ') }}
{% endmacro %}
