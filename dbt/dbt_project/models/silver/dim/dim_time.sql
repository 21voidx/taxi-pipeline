-- ============================================================
-- silver_core.dim_time
-- Dimensi jam (0–23) untuk analisis pola perjalanan
-- Grain: 1 baris = 1 jam
-- Materialization: table (full refresh)
-- ============================================================

WITH hours AS (
    SELECT hour_num
    FROM UNNEST(GENERATE_ARRAY(0, 23)) AS hour_num
)

SELECT
    hour_num                                AS time_key,   -- 0–23
    hour_num                                AS hour_num,
    {{ classify_time_bucket('hour_num') }}  AS time_bucket,

    -- Label tampilan ramah
    CASE
        WHEN hour_num = 0  THEN '00:00'
        WHEN hour_num < 10 THEN CONCAT('0', CAST(hour_num AS STRING), ':00')
        ELSE CONCAT(CAST(hour_num AS STRING), ':00')
    END                                     AS hour_label,

    -- Flag jam sibuk berdasarkan pola ride-hailing Jakarta
    CASE
        WHEN hour_num BETWEEN 7 AND 9  THEN TRUE
        WHEN hour_num BETWEEN 17 AND 20 THEN TRUE
        ELSE FALSE
    END                                     AS is_peak_hour,

    -- Sesi hari
    CASE
        WHEN hour_num BETWEEN 5 AND 11  THEN 'morning'
        WHEN hour_num BETWEEN 12 AND 16 THEN 'afternoon'
        WHEN hour_num BETWEEN 17 AND 21 THEN 'evening'
        ELSE 'night'
    END                                     AS day_session

FROM hours
ORDER BY time_key
