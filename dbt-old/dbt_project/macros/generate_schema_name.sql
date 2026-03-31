{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set base_schema = custom_schema_name if custom_schema_name is not none else target.schema -%}

    {%- if target.name == 'prod' -%}
        {{ base_schema }}

    {%- else -%}
        {%- set dev_prefix = env_var('DBT_DEV_PREFIX', 'dev') -%}
        {{ dev_prefix }}_{{ base_schema }}

    {%- endif -%}

{%- endmacro %}

