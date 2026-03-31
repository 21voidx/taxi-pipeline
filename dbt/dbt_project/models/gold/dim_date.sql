{{
  config(
    materialized = 'table',
    tags         = ['gold', 'dim']
  )
}}

/*
  GOLD DIM: dim_date
  ══════════════════════════════════════════════════════════════
  Generated date dimension dari data_start_date sampai hari ini + 1 tahun.
  Grain: satu baris per hari kalender.
  Includes: hari libur nasional Indonesia (hardcoded / konfigurasi).
*/

WITH date_spine AS (
    {{
      dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('" ~ var('data_start_date') ~ "' as date)",
        end_date   = "date_add(current_date(), interval 365 day)"
      )
    }}
),

holidays AS (
    -- Hari libur nasional Indonesia 2026 (bisa diperluas)
    SELECT date_day, holiday_name FROM UNNEST([
        STRUCT(DATE '2026-01-01' AS date_day, 'Tahun Baru Masehi' AS holiday_name),
        STRUCT(DATE '2026-01-27', 'Isra Miraj'),
        STRUCT(DATE '2026-02-17', 'Tahun Baru Imlek'),
        STRUCT(DATE '2026-03-20', 'Nyepi'),
        STRUCT(DATE '2026-04-03', 'Wafat Isa Almasih'),
        STRUCT(DATE '2026-05-01', 'Hari Buruh'),
        STRUCT(DATE '2026-05-14', 'Kenaikan Isa Almasih'),
        STRUCT(DATE '2026-05-24', 'Hari Raya Waisak'),
        STRUCT(DATE '2026-06-01', 'Hari Pancasila'),
        STRUCT(DATE '2026-08-17', 'HUT Kemerdekaan RI')
    ])
),

final AS (
    SELECT
        FORMAT_DATE('%Y%m%d', ds.date_day)              AS date_key,  -- 20260101
        ds.date_day                                     AS full_date,

        -- ── Calendar fields ──────────────────────────────────────
        EXTRACT(YEAR        FROM ds.date_day)           AS year,
        EXTRACT(QUARTER     FROM ds.date_day)           AS quarter_number,
        CONCAT('Q', EXTRACT(QUARTER FROM ds.date_day))  AS quarter_label,
        EXTRACT(MONTH       FROM ds.date_day)           AS month_number,
        FORMAT_DATE('%B', ds.date_day)                  AS month_name,
        FORMAT_DATE('%b', ds.date_day)                  AS month_abbr,
        EXTRACT(WEEK        FROM ds.date_day)           AS week_of_year,
        EXTRACT(DAYOFYEAR   FROM ds.date_day)           AS day_of_year,
        EXTRACT(DAY         FROM ds.date_day)           AS day_of_month,
        EXTRACT(DAYOFWEEK   FROM ds.date_day)           AS day_of_week,     -- 1=Sun
        FORMAT_DATE('%A', ds.date_day)                  AS day_name,
        FORMAT_DATE('%a', ds.date_day)                  AS day_abbr,

        -- ── Boolean flags ────────────────────────────────────────
        (EXTRACT(DAYOFWEEK FROM ds.date_day) IN (1, 7)) AS is_weekend,
        (h.holiday_name IS NOT NULL)                    AS is_public_holiday,
        h.holiday_name,

        -- ── Period anchors ───────────────────────────────────────
        DATE_TRUNC(ds.date_day, WEEK)                   AS week_start_date,
        DATE_TRUNC(ds.date_day, MONTH)                  AS month_start_date,
        DATE_TRUNC(ds.date_day, QUARTER)                AS quarter_start_date,
        DATE_TRUNC(ds.date_day, YEAR)                   AS year_start_date,

        -- ── Relative flags ───────────────────────────────────────
        (ds.date_day = CURRENT_DATE())                  AS is_today,
        (ds.date_day < CURRENT_DATE())                  AS is_past,
        DATE_DIFF(CURRENT_DATE(), ds.date_day, DAY)     AS days_ago

    FROM date_spine ds
    LEFT JOIN holidays h ON ds.date_day = h.date_day
)

SELECT * FROM final
ORDER BY full_date
