-- ============================================================
-- tests/singular/assert_completion_rate_by_city.sql
-- Completion rate per kota di gold mart tidak boleh turun
-- di bawah 40% atau melebihi 99% (anomali ekstrem).
-- Toleransi untuk kota kecil / hari awal operasional
-- diatur dengan minimum_trip threshold.
-- Test LULUS jika query mengembalikan 0 baris.
-- ============================================================

SELECT
    date_key,
    city,
    total_trip_requested,
    total_trip_completed,
    completion_rate,
    CASE
        WHEN completion_rate < 0.40 THEN 'completion_rate_too_low'
        WHEN completion_rate > 0.99 THEN 'completion_rate_suspiciously_high'
    END AS violation_type

FROM {{ ref('dm_trip_daily_city') }}

WHERE
    total_trip_requested >= 10     -- minimal 10 trip agar statistik bermakna
    AND (
        completion_rate < 0.40
        OR completion_rate > 0.99
    )
