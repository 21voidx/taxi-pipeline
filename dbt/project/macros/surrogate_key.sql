{% macro surrogate_key(column_name) %}
    to_hex(md5(coalesce(cast({{ column_name }} as string), '__NULL__')))
{% endmacro %}
