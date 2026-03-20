-- ============================================================
-- silver_core.dim_location
-- Dimensi area / lokasi yang muncul di trips
-- Grain: 1 baris = 1 kombinasi kota + nama area
-- Materialization: incremental (merge by location_key)
-- ============================================================

{{
    config(
        unique_key = 'location_key'
    )
}}

-- Kumpulkan semua area unik dari pickup & dropoff di trips_raw
WITH pickup_areas AS (
    SELECT DISTINCT
        city,
        pickup_area  AS area_name,
        AVG(pickup_lat) OVER (PARTITION BY city, pickup_area) AS latitude_center,
        AVG(pickup_lng) OVER (PARTITION BY city, pickup_area) AS longitude_center
    FROM {{ source('bronze_pg', 'trips_raw') }}
    WHERE pickup_area IS NOT NULL
),

dropoff_areas AS (
    SELECT DISTINCT
        city,
        dropoff_area AS area_name,
        AVG(dropoff_lat) OVER (PARTITION BY city, dropoff_area) AS latitude_center,
        AVG(dropoff_lng) OVER (PARTITION BY city, dropoff_area) AS longitude_center
    FROM {{ source('bronze_pg', 'trips_raw') }}
    WHERE dropoff_area IS NOT NULL
),

all_areas AS (
    SELECT city, area_name, latitude_center, longitude_center FROM pickup_areas
    UNION DISTINCT
    SELECT city, area_name, latitude_center, longitude_center FROM dropoff_areas
),

aggregated AS (
    SELECT
        city,
        area_name,
        AVG(latitude_center)  AS latitude_center,
        AVG(longitude_center) AS longitude_center
    FROM all_areas
    GROUP BY city, area_name
)

SELECT
    {{ surrogate_key(['city', 'area_name']) }}  AS location_key,
    city,
    area_name,
    ROUND(latitude_center, 6)                  AS latitude_center,
    ROUND(longitude_center, 6)                 AS longitude_center
FROM aggregated
