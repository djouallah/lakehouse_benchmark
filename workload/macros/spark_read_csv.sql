{#-- How the spark tree's fact models (gluten and sail) read the landed CSVs. Ported from
     fabric-dbt-examples' Delta spark leg (dbt1/macros/spark_read_csv.sql); the record rule and
     the file choice are unchanged.

       pre_hook 1  spark_stage_view(record)   CREATE OR REPLACE TEMPORARY VIEW raw_<record>
                                              (<explicit STRING schema>) USING csv OPTIONS
                                              (path '<folder>/{this run's files}', header, PERMISSIVE)
       pre_hook 2  spark_stage_table(record)  DROP + CREATE TABLE <model>__stage_<run> USING iceberg,
                                              then INSERT ... SELECT *, input_file_name()
                                              FROM raw_<record> WHERE <record filter>
       model body                             SELECT <casts> FROM <model>__stage_<run>
       post_hook   spark_drop_stage()         DROP TABLE IF EXISTS <model>__stage_<run> PURGE

     THE STAGE IS A TABLE, as in the source, and it has to be. Iceberg's MERGE (copy-on-write
     ReplaceData) matches target rows with a subquery over the merge source, and Spark refuses a
     non-deterministic expression there: with the stage as a temp view, `file` was still
     input_file_name() and every incremental run failed INVALID_NON_DETERMINISTIC_EXPRESSIONS
     (workload run 39354663786). Stored in a table, `file` is a column. Created with a plain
     CREATE TABLE and filled with INSERT -- not CREATE OR REPLACE ... AS, which Spark sends as a
     staged create and OneLake refuses (gluten_create_table_as.sql).

     Both hooks resolve THIS RUN's file list at run time (spark_new_files, capped at
     process_limit). With nothing new the stage stays an EMPTY table of the same columns, so the
     body always has something to read; the merge source is zero rows. --#}

{#-- The backtick-quoted STRING schema both the temp view and the empty stage need. --#}
{% macro spark_csv_schema(record) %}
  {%- set parts = [] -%}
  {%- for c in aemo_columns(record) %}{% do parts.append('`' ~ c ~ '` STRING') %}{% endfor -%}
  {{ return(parts | join(', ')) }}
{% endmacro %}


{#-- The sail leg only: the csv view's schema padded to 140 STRING columns, as the Trino
     leg's external tables are (workload_run_engine.py, TRINO_CSV_TABLES) and the Sail ETL's read is
     (bench/etl/engines/_spark_df.py). Sail's csv reader refuses a row LONGER than the schema --
     "incorrect number of fields for line 291, expected 53 got 120" (run 39686575857) -- where
     Spark's PERMISSIVE nulls it and the record filter drops it. The pad columns never leave the
     view: the stage takes the record's own columns by name. --#}
{% macro spark_csv_view_schema(record) %}
  {%- set schema = spark_csv_schema(record) -%}
  {%- if target.name != 'sail' -%}{{ return(schema) }}{%- endif -%}
  {%- set pads = [] -%}
  {%- for i in range(aemo_columns(record) | length, 140) %}{% do pads.append('`_pad_' ~ i ~ '` STRING') %}{% endfor -%}
  {{ return(([schema] + pads) | join(', ')) }}
{% endmacro %}


{#-- The files this model folds this run: all of the source type on a first build, else the
     ones {{ this }} does not hold yet. Same rule as every other engine. --#}
{% macro spark_stage_files(record) %}
  {%- set spec = aemo_spec(record) -%}
  {{ return(spark_new_files(spec['source_type'], this if is_incremental() else none)) }}
{% endmacro %}


{#-- <model>__stage_<run>, in the model's own schema, quoted like {{ this }}. The run's
     invocation id in the name keeps two concurrent runs from dropping or filling each other's
     stage. --#}
{% macro spark_stage_relation() %}
  {{ return(this.incorporate(path={'identifier': this.identifier ~ '__stage_' ~ invocation_id[:8]})) }}
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
  {#-- The sail leg: Sail's csv reader refuses a row shorter than the schema ("Csv error:
       incorrect number of fields for line 1, expected 130 got 10", run 39685977867) unless told
       otherwise; Spark's PERMISSIVE pads it with nulls. bench/etl/engines/_spark_df.py, same option. --#}
  CREATE OR REPLACE TEMPORARY VIEW raw_{{ record }} ({{ spark_csv_view_schema(record) }})
  USING csv OPTIONS (path '{{ path }}', header 'true', mode 'PERMISSIVE'
  {%- if target.name == 'sail' %}, allowTruncatedRows 'true'{% endif %})
{% endmacro %}


{#-- pre_hook 2: the record's rows into the stage table. The record filter runs HERE, on the raw
     strings, so the stage holds only DREGION/DUNIT/PRICE/SCADA rows -- a PUBLIC_DAILY file is
     mostly other report types. input_file_name() is captured now, on the csv scan: it is the only
     place the provenance exists, and the models' `file` column is parsed from it. The drop and the
     create run here, the INSERT is the hook's statement (empty, so skipped, with nothing new). --#}
{% macro spark_stage_table(record) %}
  {%- set files = spark_stage_files(record) -%}
  {%- set stage = spark_stage_relation() -%}
  {%- do run_query('DROP TABLE IF EXISTS ' ~ stage) -%}
  {%- do run_query('CREATE TABLE ' ~ stage ~ ' (' ~ spark_csv_schema(record) ~ ', `_fname` STRING) USING iceberg') -%}
  {%- if files | length > 0 -%}
  INSERT INTO {{ stage }}
  SELECT {% if target.name == 'sail' %}{% for c in aemo_columns(record) %}`{{ c }}`, {% endfor %}{% else %}*, {% endif %}input_file_name() AS _fname
  FROM raw_{{ record }}
  WHERE {{ spark_record_filter(record) }}
  {%- endif -%}
{% endmacro %}


{#-- post_hook: the stage is scratch. A run that dies before this leaves one behind; the next
     run's pre_hook drops it first. --#}
{% macro spark_drop_stage() %}
  DROP TABLE IF EXISTS {{ spark_stage_relation() }} PURGE
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
  {#-- TRY_CAST on the sail leg: Sail's CAST raises on the files' own header row ("Cast error:
       Cannot cast string 'SCADAVALUE' to value of Float64 type", run 39687083463), where Spark's
       gives NULL and `I = 'D'` drops the row. --#}
  {%- set cast = 'TRY_CAST(' if target.name == 'sail' else 'CAST(' -%}
  {%- if spec['nonzero'] %}{% do parts.append(cast ~ prefix ~ spec['nonzero'] ~ ' AS DOUBLE) != 0') %}{% endif -%}
  {{ parts | join(' AND ') }}
{% endmacro %}
