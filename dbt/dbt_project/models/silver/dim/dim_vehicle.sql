-- ============================================================
-- silver_core.dim_vehicle
-- Dimensi kendaraan
-- Grain: 1 baris = 1 kendaraan
-- Materialization: incremental (merge by vehicle_id)
-- ============================================================

{{
    config(
        unique_key = 'vehicle_id'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('bronze_pg', 'vehicles_raw') }}
    {{ incremental_filter('updated_at') }}
)

SELECT
    {{ surrogate_key(['vehicle_id']) }}  AS vehicle_key,
    vehicle_id,
    driver_id,
    plate_number,
    LOWER(vehicle_type)                 AS vehicle_type,
    brand,
    model,
    CONCAT(brand, ' ', model)           AS vehicle_name,
    production_year,
    seat_capacity,
    -- Kategori umur kendaraan
    CASE
        WHEN (2026 - production_year) <= 2  THEN 'very_new'
        WHEN (2026 - production_year) <= 5  THEN 'new'
        WHEN (2026 - production_year) <= 8  THEN 'normal'
        ELSE 'old'
    END                                 AS age_category,
    created_at,
    updated_at
FROM source
