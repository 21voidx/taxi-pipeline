{% macro safe_rate(numerator, denominator) %}
    safe_divide({{ numerator }}, {{ denominator }})
{% endmacro %}
