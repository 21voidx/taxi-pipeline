-- ============================================================
-- silver_core.dim_date
-- Dimensi tanggal untuk seluruh fact table
-- Grain: 1 baris = 1 hari
-- Materialization: table (full refresh)
-- ============================================================

WITH date_spine AS (
    -- Hasilkan semua tanggal dari 2026-01-01 sampai akhir 2027
    -- (cukup untuk proyek ini; perpanjang sesuai kebutuhan)
    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('2026-01-01' as date)",
        end_date   = "cast('2028-01-01' as date)"
    ) }}
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64)              AS date_key,
    date_day                                                     AS full_date,
    EXTRACT(DAY   FROM date_day)                                 AS day_of_month,
    FORMAT_DATE('%A', date_day)                                  AS day_name,
    EXTRACT(WEEK  FROM date_day)                                 AS week_of_year,
    EXTRACT(MONTH FROM date_day)                                 AS month_num,
    FORMAT_DATE('%B', date_day)                                  AS month_name,
    EXTRACT(QUARTER FROM date_day)                               AS quarter_num,
    EXTRACT(YEAR  FROM date_day)                                 AS year_num,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE
        ELSE FALSE
    END                                                          AS is_weekend,
    -- Hari libur nasional Indonesia 2026 (seed dapat di-extend)
    CASE
        WHEN date_day IN (
            '2026-01-01', -- Tahun Baru Masehi
            '2026-01-27', -- Isra Miraj
            '2026-01-29', '2026-01-30', -- Imlek
            '2026-03-21', -- Hari Raya Nyepi
            '2026-03-29', -- Paskah
            '2026-04-02', -- Cuti Bersama
            '2026-05-01', -- Hari Buruh
            '2026-05-14', -- Kenaikan Isa Almasih
            '2026-06-01', -- Hari Pancasila
            '2026-08-17', -- HUT RI
            '2026-12-25'  -- Natal
        ) THEN TRUE
        ELSE FALSE
    END                                                          AS is_national_holiday,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7)
             OR date_day IN ('2026-01-01', '2026-08-17', '2026-12-25')
        THEN FALSE
        ELSE TRUE
    END                                                          AS is_business_day

FROM date_spine
ORDER BY date_key
