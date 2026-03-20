-- ============================================================
-- tests/generic/not_negative.sql
-- Generic test: memastikan semua nilai kolom >= 0.
-- Ini adalah implementasi fallback; versi resminya ada di
-- macros/custom_tests.sql ({% test not_negative %}).
-- File ini hanya sebagai referensi / dokumentasi.
-- ============================================================
-- Dipakai via schema.yml:
--   tests:
--     - ride_hailing.not_negative
-- ============================================================

{% test not_negative(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < 0
{% endtest %}
