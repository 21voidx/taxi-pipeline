-- ============================================================
-- silver_core.dim_customer
-- Dimensi customer dengan SCD Type 2 (dari dbt snapshot)
-- Grain: 1 baris = 1 versi customer (aktif atau historis)
-- Materialization: incremental (merge), diisi dari snapshot
-- ============================================================

{{
    config(
        unique_key = 'customer_key'
    )
}}

WITH snapshot_source AS (
    -- Baca dari hasil snapshot dbt (snapshot_dim_customer)
    SELECT
        dbt_scd_id                              AS customer_key_raw,
        customer_id,
        full_name,
        phone_number,
        email,
        gender,
        birth_date,
        created_at                              AS signup_ts,
        updated_at,
        dbt_valid_from                          AS valid_from,
        dbt_valid_to                            AS valid_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM {{ ref('snapshot_dim_customer') }}
),

enriched AS (
    SELECT
        {{ surrogate_key(['customer_id', 'valid_from']) }}  AS customer_key,
        customer_id,
        full_name,
        CASE gender
            WHEN 'male'   THEN 'Pria'
            WHEN 'female' THEN 'Wanita'
            ELSE 'Tidak Diketahui'
        END                                                 AS gender,
        {{ classify_age_group('birth_date') }}              AS age_group,
        DATE(signup_ts, 'Asia/Jakarta')                     AS signup_date,
        email,
        phone_number,
        valid_from,
        valid_to,
        is_current,
        -- Lookup segment terbaru dari bronze_mysql (bisa NULL jika belum ada)
        NULL                                                AS current_segment
    FROM snapshot_source
)

SELECT *
FROM enriched

{% if is_incremental() %}
WHERE valid_from >= (
    SELECT TIMESTAMP_SUB(MAX(valid_from), INTERVAL {{ var('incremental_lookback_days', 3) }} DAY)
    FROM {{ this }}
)
{% endif %}
