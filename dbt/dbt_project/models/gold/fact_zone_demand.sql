{{
  config(
    materialized        = 'incremental',
    unique_key          = ['pickup_zone_id', 'demand_date', 'demand_hour', 'vehicle_type_code'],
    incremental_strategy= 'merge',
    partition_by        = {'field': 'demand_date', 'data_type': 'date'},
    cluster_by          = ['pickup_city', 'vehicle_type_code'],
    tags                = ['gold', 'fact']
  )
}}

/*
  GOLD FACT: fact_zone_demand
  ══════════════════════════════════════════════════════════════
  Grain: zona × tanggal × jam × jenis kendaraan.
  Digunakan untuk: heatmap permintaan, simulasi surge pricing,
                   analisis alokasi penawaran.
*/

WITH rides AS (
    SELECT
        pickup_zone_id,
        pickup_zone_code,
        pickup_zone_name,
        pickup_city,
        vehicle_type_code,
        ride_date                                       AS demand_date,
        request_hour                                    AS demand_hour,
        day_type,
        time_of_day_bucket,

        COUNT(*)                                        AS total_requests,
        SUM(CAST(is_completed AS INT64))                AS completed_rides,
        SUM(CAST(is_cancelled AS INT64))                AS cancelled_rides,
        SUM(CAST(is_surge_ride AS INT64))               AS surge_rides,
        AVG(surge_multiplier)                           AS avg_surge_multiplier,
        SUM(gmv)                                        AS demand_gmv,
        AVG(wait_time_min)                              AS avg_wait_time_min,
        COUNT(DISTINCT driver_id)                       AS unique_drivers,
        COUNT(DISTINCT passenger_id)                    AS unique_passengers,
        MAX(updated_at)                                 AS updated_at

    FROM {{ ref('slv_rides') }}

    {% if is_incremental() %}
    WHERE updated_at > (
        SELECT TIMESTAMP_SUB(MAX(updated_at), INTERVAL 1 HOUR) FROM {{ this }}
    )
    {% endif %}

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

dim_zone AS (
    SELECT zone_id, zone_sk
    FROM {{ ref('dim_zone') }}
),

dim_date AS (
    SELECT full_date, date_key
    FROM {{ ref('dim_date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'r.pickup_zone_id', 'r.demand_date',
        'r.demand_hour', 'r.vehicle_type_code'
    ]) }}                                               AS zone_demand_sk,

    -- ── Keys ─────────────────────────────────────────────────────
    r.pickup_zone_id,
    COALESCE(z.zone_sk, 'UNKNOWN')                      AS pickup_zone_sk,
    dd.date_key                                         AS demand_date_key,

    -- ── Dimensions ───────────────────────────────────────────────
    r.demand_date,
    r.demand_hour,
    r.pickup_zone_code,
    r.pickup_zone_name,
    r.pickup_city,
    r.vehicle_type_code,
    r.day_type,
    r.time_of_day_bucket,

    -- ── Demand measures ──────────────────────────────────────────
    r.total_requests,
    r.completed_rides,
    r.cancelled_rides,
    r.surge_rides,
    r.demand_gmv,

    -- ── Supply measures ──────────────────────────────────────────
    r.unique_drivers,
    r.unique_passengers,

    -- ── Quality measures ─────────────────────────────────────────
    r.avg_surge_multiplier,
    r.avg_wait_time_min,
    ROUND(SAFE_DIVIDE(r.completed_rides,
          NULLIF(r.total_requests, 0)) * 100, 2)        AS fill_rate_pct,

    r.updated_at

FROM rides r
LEFT JOIN dim_zone  z  ON r.pickup_zone_id = z.zone_id
LEFT JOIN dim_date  dd ON r.demand_date    = dd.full_date
