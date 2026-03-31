{{
  config(
    materialized = 'table',
    tags         = ['gold', 'dim']
  )
}}

/*
  GOLD DIM: dim_vehicle_type
  ══════════════════════════════════════════════════════════════
  Dimensi jenis kendaraan dan konfigurasi tarif (SCD-1 / static).
  Grain: satu baris per jenis kendaraan.
*/

SELECT
    {{ dbt_utils.generate_surrogate_key(['vehicle_type_id']) }} AS vehicle_type_sk,
    vehicle_type_id,
    type_code,
    type_name,
    base_fare,
    per_km_rate,
    per_minute_rate,
    capacity,

    -- Tier grouping for analytics
    CASE
      WHEN type_code IN ('PREMIUM', 'SUV') THEN 'PREMIUM'
      WHEN type_code = 'COMFORT'           THEN 'STANDARD'
      ELSE 'EKONOMI'
    END                                                 AS tier_group,

    CURRENT_TIMESTAMP()                                 AS _loaded_at

FROM {{ source('postgres_raw', 'vehicle_types') }}
