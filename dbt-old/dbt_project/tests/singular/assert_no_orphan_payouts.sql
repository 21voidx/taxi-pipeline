-- ============================================================
-- tests/singular/assert_no_orphan_payouts.sql
-- Setiap payout driver harus merujuk ke trip completed
-- yang exists di fct_trip.
-- Test LULUS jika query mengembalikan 0 baris.
-- ============================================================

SELECT
    dp.payout_id,
    dp.trip_key
FROM {{ ref('fct_driver_payout') }} dp
LEFT JOIN {{ ref('fct_trip') }} t
    ON dp.trip_key = t.trip_key
WHERE
    t.trip_key IS NULL
    OR t.is_completed = FALSE
