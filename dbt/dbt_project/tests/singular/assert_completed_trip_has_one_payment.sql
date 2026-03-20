-- ============================================================
-- tests/singular/assert_completed_trip_has_one_payment.sql
-- Setiap trip COMPLETED harus memiliki tepat 1 record payment.
-- Test LULUS jika query mengembalikan 0 baris.
-- ============================================================

WITH payment_count AS (
    SELECT
        trip_key,
        COUNT(*) AS n_payments
    FROM {{ ref('fct_payment') }}
    GROUP BY trip_key
)

SELECT
    t.trip_id,
    t.trip_key,
    COALESCE(pc.n_payments, 0) AS n_payments
FROM {{ ref('fct_trip') }} t
LEFT JOIN payment_count pc
    ON t.trip_key = pc.trip_key
WHERE
    t.is_completed = TRUE
    AND (pc.n_payments IS NULL OR pc.n_payments != 1)
