-- ============================================================
-- tests/singular/assert_gold_ops_coverage.sql
-- Data mart dm_trip_daily_city harus memiliki data untuk
-- setiap tanggal dalam periode Jan–Mar 2026 dan setiap
-- kota yang aktif.
-- Test ini mendeteksi "missing partitions" / gap data.
-- Test LULUS jika query mengembalikan 0 baris.
-- ============================================================

WITH expected_dates AS (
    -- Hasilkan semua tanggal dalam periode
    SELECT
        CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64) AS date_key
    FROM UNNEST(
        GENERATE_DATE_ARRAY(DATE('2026-01-01'), DATE('2026-03-18'))
    ) AS date_day
),

expected_cities AS (
    SELECT DISTINCT city
    FROM {{ ref('dm_trip_daily_city') }}
    -- Gunakan kota yang pernah muncul di data
),

expected_combinations AS (
    SELECT d.date_key, c.city
    FROM expected_dates d
    CROSS JOIN expected_cities c
),

actual AS (
    SELECT date_key, city
    FROM {{ ref('dm_trip_daily_city') }}
)

SELECT
    ec.date_key,
    ec.city,
    'missing_partition' AS violation_type
FROM expected_combinations ec
LEFT JOIN actual a
    ON  ec.date_key = a.date_key
    AND ec.city     = a.city
WHERE a.date_key IS NULL
