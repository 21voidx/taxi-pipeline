-- ============================================================
-- silver_core.dim_promo
-- Dimensi promosi / voucher
-- Grain: 1 baris = 1 promo
-- Materialization: incremental (merge by promo_id)
-- ============================================================

{{
    config(
        unique_key = 'promo_id'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('bronze_mysql', 'promotions_raw') }}
    {{ incremental_filter('updated_at') }}
)

SELECT
    {{ surrogate_key(['promo_id']) }}    AS promo_key,
    promo_id,
    UPPER(TRIM(promo_code))             AS promo_code,
    promo_name,
    LOWER(promo_type)                   AS promo_type,
    LOWER(discount_type)                AS discount_type,
    CAST(discount_value AS NUMERIC)     AS discount_value,
    start_date,
    end_date,
    DATE_DIFF(end_date, start_date, DAY) + 1  AS promo_duration_days,
    CAST(budget_amount AS NUMERIC)      AS budget_amount,
    LOWER(promo_status)                 AS promo_status,
    -- Flag apakah promo masih dalam periode aktif
    CASE
        WHEN LOWER(promo_status) = 'active'
             AND CURRENT_DATE('Asia/Jakarta') BETWEEN start_date AND end_date
        THEN TRUE
        ELSE FALSE
    END                                 AS is_currently_active,
    created_at,
    updated_at
FROM source
