-- ============================================================
-- silver_core.dim_driver
-- Dimensi driver dengan SCD Type 2 (dari dbt snapshot)
-- Grain: 1 baris = 1 versi driver (aktif atau historis)
-- Materialization: incremental (merge), diisi dari snapshot
-- ============================================================

{{
    config(
        unique_key = 'driver_key'
    )
}}

WITH snapshot_source AS (
    SELECT
        driver_id,
        full_name,
        driver_status,
        join_date,
        city,
        license_number,
        dbt_valid_from                          AS valid_from,
        dbt_valid_to                            AS valid_to,
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM {{ ref('snapshot_dim_driver') }}
)

SELECT
    {{ surrogate_key(['driver_id', 'valid_from']) }}  AS driver_key,
    driver_id,
    full_name,
    driver_status,
    join_date,
    city,
    license_number,
    valid_from,
    valid_to,
    is_current
FROM snapshot_source

{% if is_incremental() %}
WHERE valid_from >= (
    SELECT TIMESTAMP_SUB(MAX(valid_from), INTERVAL {{ var('incremental_lookback_days', 3) }} DAY)
    FROM {{ this }}
)
{% endif %}
