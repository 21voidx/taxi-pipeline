{#
  ============================================================
  macros/generate_schema_name.sql
  Override generate_schema_name bawaan dbt agar setiap
  model masuk ke dataset BigQuery yang benar tanpa prefix
  nama target.

  Tanpa override ini, dbt akan menghasilkan nama seperti:
    "dbt_dev_silver_core"
  Dengan override ini, hasilnya:
    "silver_core"  /  "gold_operations"  dll.

  Di dev, tambahkan prefix developer agar tidak bentrok:
    DEV_USER env var → "myname_silver_core"
  ============================================================
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {# Prod: pakai nama dataset persis seperti yang didefinisikan #}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {# Dev / staging: prefix dengan username agar isolasi per developer #}
        {%- set dev_prefix = env_var('DBT_DEV_PREFIX', 'dev') -%}
        {{ dev_prefix }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
