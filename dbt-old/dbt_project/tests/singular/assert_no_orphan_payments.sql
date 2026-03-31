-- ============================================================
-- tests/singular/assert_no_orphan_payments.sql
-- Setiap payment harus merujuk ke trip yang exists di fct_trip.
-- Test LULUS jika query mengembalikan 0 baris.
-- ============================================================

SELECT
    p.payment_id,
    p.trip_key
FROM {{ ref('fct_payment') }} p
LEFT JOIN {{ ref('fct_trip') }} t
    ON p.trip_key = t.trip_key
WHERE t.trip_key IS NULL
