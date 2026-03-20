-- ============================================================
-- tests/singular/assert_trip_dates_in_range.sql
-- Semua trip harus memiliki request_ts dalam rentang
-- periode data yang valid (Jan 2026 – Mar 2026).
-- Trip di luar range ini mengindikasikan data kotor atau bug EL.
-- Test LULUS jika query mengembalikan 0 baris.
-- ============================================================

SELECT
    trip_id,
    request_ts,
    trip_status,
    CASE
        WHEN request_ts < TIMESTAMP('2026-01-01 00:00:00')
            THEN 'before_period_start'
        WHEN request_ts > TIMESTAMP('2026-03-18 23:59:59')
            THEN 'after_period_end'
    END AS violation_reason

FROM {{ ref('fct_trip') }}

WHERE
    request_ts < TIMESTAMP('2026-01-01 00:00:00')
    OR request_ts > TIMESTAMP('2026-03-18 23:59:59')
