-- ============================================================
-- tests/generic/accepted_range.sql
-- Generic test: memastikan nilai kolom berada dalam
-- rentang [min_val, max_val] inklusif.
-- Lebih fleksibel dari dbt_expectations karena mendukung
-- NULL bypass opsional.
-- ============================================================
-- Dipakai via schema.yml:
--   tests:
--     - ride_hailing.accepted_range:
--         min_val: 1
--         max_val: 5
--         include_null: true   # default: false (NULL dianggap pelanggaran)
-- ============================================================

{% test accepted_range(model, column_name, min_val, max_val, include_null=false) %}
    SELECT *
    FROM {{ model }}
    WHERE
        {% if include_null %}
            {{ column_name }} IS NOT NULL
            AND
        {% endif %}
        (
            {{ column_name }} < {{ min_val }}
            OR {{ column_name }} > {{ max_val }}
        )
{% endtest %}
