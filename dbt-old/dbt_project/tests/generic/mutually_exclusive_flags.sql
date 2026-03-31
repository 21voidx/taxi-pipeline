-- ============================================================
-- tests/generic/mutually_exclusive_flags.sql
-- Generic test: memastikan di antara dua flag boolean,
-- tidak boleh keduanya TRUE sekaligus.
-- Cocok untuk is_completed & is_cancelled di fct_trip.
-- ============================================================
-- Dipakai via schema.yml:
--   tests:
--     - ride_hailing.mutually_exclusive_flags:
--         flag_a: is_completed
--         flag_b: is_cancelled
-- ============================================================

{% test mutually_exclusive_flags(model, column_name, flag_a, flag_b) %}
    SELECT *
    FROM {{ model }}
    WHERE
        {{ flag_a }} = TRUE
        AND {{ flag_b }} = TRUE
{% endtest %}
