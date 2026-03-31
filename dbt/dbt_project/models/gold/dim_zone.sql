{{
  config(
    materialized = 'table',
    tags         = ['gold', 'dim']
  )
}}

/*
  GOLD DIM: dim_zone
  ══════════════════════════════════════════════════════════════
  Dimensi zona Jabodetabek (SCD-1 / static).
  Digunakan sebagai FK pickup_zone_sk dan dropoff_zone_sk di fact_rides.

  Grain: satu baris per zona aktif.
*/

SELECT
    {{ dbt_utils.generate_surrogate_key(['zone_id']) }}   AS zone_sk,
    zone_id,
    zone_code,
    zone_name,
    city,
    latitude,
    longitude,

    -- Regional grouping
    CASE
      WHEN city = 'Jakarta'    THEN 'DKI Jakarta'
      WHEN city = 'Banten'     THEN 'Banten'
      ELSE 'Jawa Barat'
    END                                                   AS province,

    _loaded_at

FROM {{ ref('brz_zones') }}
