{#-- The DuckDB-family reader (duckrun, iceberg, ducklake): a read_csv() over an explicit
     list of paths, typed from aemo_spec so the column layout lives in exactly one place.

     `paths_expr` is raw SQL that evaluates to a LIST of paths, not a list of strings — the
     fact models set it with a pre-hook SET VARIABLE and pass getvariable('<name>'), which
     keeps the path list out of the compiled SQL (it can run to thousands of files).

     all_varchar / ignore_errors / null_padding are what let one reader handle the ragged
     multi-record AEMO files: a PUBLIC_DAILY file holds several record types of differing
     widths plus 'C'/'I' comment rows, so short rows pad to NULL, extra fields are dropped,
     and the model's WHERE keeps only the rows of interest. auto_detect=false is required —
     with it on, DuckDB samples the file and infers a narrower layout from whichever record
     type it happens to see first. --#}
{% macro duckdb_read_csv(paths_expr, record) %}
  {%- set spec = aemo_spec(record) -%}
  read_csv(
    {{ paths_expr }},
    skip = 1,
    header = 0,
    all_varchar = 1,
    columns = {
      {%- for name, type in spec['columns'] %}
      '{{ name }}': '{{ type }}'{{ "," if not loop.last }}
      {%- endfor %}
    },
    filename = 1,
    null_padding = true,
    ignore_errors = 1,
    auto_detect = false,
    hive_partitioning = false
  )
{% endmacro %}


{#-- The record-selection predicate for the DuckDB family, rendered from aemo_spec's
     `equals`/`nonzero` data so the RULE is shared and only the quoting is per-dialect. --#}
{% macro duckdb_record_filter(record, prefix='') %}
  {%- set spec = aemo_spec(record) -%}
  {%- set parts = [] -%}
  {%- for col, val in spec['equals'] %}{% do parts.append(prefix ~ col ~ " = '" ~ val ~ "'") %}{% endfor -%}
  {%- if spec['nonzero'] %}{% do parts.append(prefix ~ spec['nonzero'] ~ ' != 0') %}{% endif -%}
  {{ parts | join(' AND ') }}
{% endmacro %}
