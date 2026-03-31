{{
  config(
    materialized = 'table',
    tags         = ['gold', 'dim', 'scd2']
  )
}}

/*
  GOLD DIM: dim_driver
  ══════════════════════════════════════════════════════════════
  SCD Type 2 — membaca dari snapshot snp_drivers.
  Setiap perubahan profil driver (status, tier, rating, zone)
  menghasilkan baris baru; baris lama di-close dengan dbt_valid_to.

  Grain: satu baris per versi profil pengemudi.
  driver_sk digunakan sebagai FK di fact_rides.
*/

WITH snp AS (
    SELECT * FROM {{ ref('snp_drivers') }}
)

SELECT
    -- ── Surrogate key ─────────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['driver_id', 'dbt_scd_id']) }}
                                                    AS driver_sk,

    -- ── Natural key ───────────────────────────────────────────────
    driver_id,
    driver_code,

    -- ── Descriptive attributes ────────────────────────────────────
    full_name,
    phone_normalised                                AS phone_number,
    email,
    is_phone_valid,
    is_nik_valid,
    license_plate_normalised                        AS license_plate,

    -- ── Vehicle ───────────────────────────────────────────────────
    vehicle_type_code,
    vehicle_type_name,
    vehicle_brand,
    vehicle_model,
    vehicle_year,
    vehicle_color,

    -- ── Geography ─────────────────────────────────────────────────
    home_zone_code,
    home_zone_name,
    home_city,

    -- ── Performance ───────────────────────────────────────────────
    rating,
    total_trips,
    driver_tier,
    status,

    -- ── Platform tenure ───────────────────────────────────────────
    days_on_platform,
    DATE(joined_at)                                 AS joined_date,

    -- ── SCD-2 metadata ────────────────────────────────────────────
    dbt_scd_id,
    DATE(dbt_valid_from)                            AS valid_from,
    DATE(dbt_valid_to)                              AS valid_to,
    dbt_valid_to IS NULL                            AS is_current

FROM snp
