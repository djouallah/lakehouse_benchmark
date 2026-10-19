{#-- The landing root, for every engine. The ducklake repo used to read ROOT_PATH while the
     other three read FILES_PATH; one name now, so a path bug cannot be engine-specific. --#}
{%- macro get_root_path() -%}
{{ env_var('FILES_PATH', '/tmp') | trim }}
{%- endmacro -%}
