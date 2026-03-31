-- ============================================================
-- silver_core.fct_trip
-- Fact tabel utama: setiap baris = 1 trip
-- Grain: 1 trip
-- Materialization: incremental (merge by trip_id, partition by created_at)
-- ============================================================

{{
    config(
        unique_key       = 'trip_id',
        partition_by     = {
            'field'      : 'created_at',
            'data_type'  : 'timestamp',
            'granularity': 'day'
        },
        cluster_by       = ['city', 'trip_status']
    )
}}

WITH trips AS (
    SELECT *
    FROM {{ source('bronze_pg', 'trips_raw') }}
    {{ incremental_filter('updated_at') }}
),

-- Ambil surrogate key dari dimensi
dim_customer AS (
    SELECT customer_key, customer_id
    FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE
),

dim_driver AS (
    SELECT driver_key, driver_id
    FROM {{ ref('dim_driver') }}
    WHERE is_current = TRUE
),

dim_vehicle AS (
    SELECT vehicle_key, vehicle_id
    FROM {{ ref('dim_vehicle') }}
),

dim_pickup_loc AS (
    SELECT location_key, city, area_name
    FROM {{ ref('dim_location') }}
),

dim_dropoff_loc AS (
    SELECT location_key, city, area_name
    FROM {{ ref('dim_location') }}
),

dim_trip_status AS (
    SELECT trip_status_key, trip_status_name
    FROM {{ ref('dim_trip_status') }}
),

enriched AS (
    SELECT
        -- Surrogate key
        {{ surrogate_key(['t.trip_id']) }}          AS trip_key,

        -- Natural key
        t.trip_id,

        -- Date / time keys
        {{ date_to_key('t.request_ts') }}            AS request_date_key,
        {{ hour_key('t.request_ts') }}               AS request_time_key,
        {{ date_to_key('t.pickup_ts') }}             AS pickup_date_key,
        {{ date_to_key('t.dropoff_ts') }}            AS dropoff_date_key,

        -- Dimension keys
        dc.customer_key,
        dd.driver_key,
        dv.vehicle_key,
        dpl.location_key                             AS pickup_location_key,
        ddl.location_key                             AS dropoff_location_key,
        ds.trip_status_key,

        -- Dimension degenerasi (disimpan langsung di fact)
        t.city,
        t.trip_status,
        t.surge_multiplier,
        t.cancel_reason,

        -- Measure jarak
        CAST(t.estimated_distance_km AS FLOAT64)     AS estimated_distance_km,
        CAST(t.actual_distance_km    AS FLOAT64)     AS actual_distance_km,

        -- Measure fare (IDR)
        CAST(t.estimated_fare AS NUMERIC)            AS estimated_fare,
        CAST(t.actual_fare    AS NUMERIC)            AS actual_fare,

        -- Measure waktu
        CASE
            WHEN t.pickup_ts IS NOT NULL AND t.request_ts IS NOT NULL
            THEN TIMESTAMP_DIFF(t.pickup_ts, t.request_ts, MINUTE)
            ELSE NULL
        END                                          AS waiting_time_minute,
        CASE
            WHEN t.dropoff_ts IS NOT NULL AND t.pickup_ts IS NOT NULL
            THEN TIMESTAMP_DIFF(t.dropoff_ts, t.pickup_ts, MINUTE)
            ELSE NULL
        END                                          AS trip_duration_minute,

        -- Flag boolean
        (t.trip_status = 'completed')                AS is_completed,
        (t.trip_status IN ('cancelled', 'no_show'))  AS is_cancelled,
        (t.surge_multiplier > 1.0)                   AS has_surge,

        -- Raw timestamps (untuk debugging)
        t.request_ts,
        t.pickup_ts,
        t.dropoff_ts,
        t.created_at,
        t.updated_at

    FROM trips t
    LEFT JOIN dim_customer    dc  ON t.customer_id   = dc.customer_id
    LEFT JOIN dim_driver      dd  ON t.driver_id     = dd.driver_id
    LEFT JOIN dim_vehicle     dv  ON t.vehicle_id    = dv.vehicle_id
    LEFT JOIN dim_pickup_loc  dpl ON t.city = dpl.city AND t.pickup_area  = dpl.area_name
    LEFT JOIN dim_dropoff_loc ddl ON t.city = ddl.city AND t.dropoff_area = ddl.area_name
    LEFT JOIN dim_trip_status ds  ON t.trip_status   = ds.trip_status_name
)

SELECT *
FROM enriched
