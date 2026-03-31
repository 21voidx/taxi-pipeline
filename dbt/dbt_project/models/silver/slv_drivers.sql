{{
  config(
    materialized        = 'incremental',
    unique_key          = 'driver_id',
    incremental_strategy= 'merge',
    tags                = ['silver', 'regex_cleaned']
  )
}}

/*
  SILVER: slv_drivers
  ══════════════════════════════════════════════════════════════
  Applies:
    ✓ REGEX  – normalise phone (→ 08xxx), NIK (→ 16 digits),
               license plate (→ uppercase, standard format)
    ✓ CLEAN  – trim whitespace, handle NULLs
    ✓ JOIN   – enrich with zone name & vehicle type label
    ✓ DERIVE – is_phone_valid, is_nik_valid, driver_tier
*/

WITH raw AS (
    SELECT * FROM {{ ref('brz_drivers') }}
),

zones AS (
    SELECT zone_id, zone_code, zone_name, city
    FROM {{ ref('brz_zones') }}
),

vehicle_types AS (
    SELECT vehicle_type_id, type_code, type_name,
           base_fare, per_km_rate
    FROM {{ source('postgres_raw', 'vehicle_types') }}
),

cleaned AS (
    SELECT
        d.driver_id,
        d.driver_code,
        TRIM(INITCAP(d.full_name))                  AS full_name,

        -- ── Phone normalisation ──────────────────────────────────────
        -- Pattern: strip +62 prefix → replace with 0
        --          remove dashes, spaces
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(TRIM(d.phone_number), r'^\+62', '0'),
          r'[\s\-\.]', ''),
        r'[^0-9]', '')                               AS phone_normalised,

        REGEXP_CONTAINS(
          REGEXP_REPLACE(
            REGEXP_REPLACE(TRIM(d.phone_number), r'^\+62', '0'),
          r'[\s\-]',''),
          r'^08[0-9]{8,12}$'
        )                                            AS is_phone_valid,

        LOWER(TRIM(d.email))                         AS email,

        -- ── NIK normalisation (16-digit national ID) ──────────────────
        REGEXP_REPLACE(TRIM(d.nik), r'[\s\-]', '')  AS nik_normalised,

        REGEXP_CONTAINS(
          REGEXP_REPLACE(TRIM(d.nik), r'[\s\-]', ''),
          r'^\d{16}$'
        )                                            AS is_nik_valid,

        d.sim_number,

        -- ── License plate normalisation ───────────────────────────────
        -- Format: "B 1234 ABC" (uppercase, single spaces)
        UPPER(
          REGEXP_REPLACE(
            TRIM(d.license_plate),
          r'\s+', ' ')
        )                                            AS license_plate_normalised,

        -- ── Vehicle info ───────────────────────────────────────────────
        d.vehicle_type_id,
        vt.type_code                                 AS vehicle_type_code,
        vt.type_name                                 AS vehicle_type_name,
        TRIM(d.vehicle_brand)                        AS vehicle_brand,
        TRIM(d.vehicle_model)                        AS vehicle_model,
        d.vehicle_year,
        TRIM(d.vehicle_color)                        AS vehicle_color,

        -- ── Zone enrichment ────────────────────────────────────────────
        d.home_zone_id,
        z.zone_code                                  AS home_zone_code,
        z.zone_name                                  AS home_zone_name,
        z.city                                       AS home_city,

        -- ── Rating & status ────────────────────────────────────────────
        COALESCE(d.rating, 5.0)                      AS rating,
        d.total_trips,
        UPPER(d.status)                              AS status,

        -- ── Derived: driver tier ───────────────────────────────────────
        CASE
          WHEN d.total_trips >= 1000 AND d.rating >= 4.8 THEN 'PLATINUM'
          WHEN d.total_trips >= 500  AND d.rating >= 4.5 THEN 'GOLD'
          WHEN d.total_trips >= 100  AND d.rating >= 4.0 THEN 'SILVER'
          ELSE 'BRONZE'
        END                                          AS driver_tier,

        -- ── Driver age on platform ─────────────────────────────────────
        DATE_DIFF(CURRENT_DATE(), DATE(d.joined_at), DAY) AS days_on_platform,

        d.joined_at,
        d.updated_at,
        d._loaded_at,
        d._source_table,
        d._row_hash

    FROM raw d
    LEFT JOIN zones z        ON d.home_zone_id    = z.zone_id
    LEFT JOIN vehicle_types vt ON d.vehicle_type_id = vt.vehicle_type_id
)

SELECT *
FROM cleaned
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
