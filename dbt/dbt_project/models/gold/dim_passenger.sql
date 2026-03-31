{{
  config(
    materialized = 'table',
    tags         = ['gold', 'dim', 'scd2']
  )
}}

/*
  GOLD DIM: dim_passenger
  ══════════════════════════════════════════════════════════════
  SCD Type 2 — membaca dari snapshot snp_passengers.
  Melacak perubahan loyalty_tier dan status penumpang dari waktu ke waktu.

  Grain: satu baris per versi profil penumpang.
  passenger_sk digunakan sebagai FK di fact_rides.
*/

WITH snp AS (
    SELECT * FROM {{ ref('snp_passengers') }}
)

SELECT
    -- ── Surrogate key ─────────────────────────────────────────────
    {{ dbt_utils.generate_surrogate_key(['passenger_id', 'dbt_scd_id']) }}
                                                    AS passenger_sk,

    -- ── Natural key ───────────────────────────────────────────────
    passenger_id,
    passenger_code,

    -- ── Descriptive attributes ────────────────────────────────────
    full_name,
    phone_normalised                                AS phone_number,
    email,
    is_phone_valid,
    is_email_valid,
    birth_date,
    age_bucket,
    gender,

    -- ── Geography ─────────────────────────────────────────────────
    home_zone_code,
    home_zone_name,
    home_city,

    -- ── Status & loyalty ──────────────────────────────────────────
    is_verified,
    total_trips,
    status,
    loyalty_tier,
    referral_code,

    -- ── Platform tenure ───────────────────────────────────────────
    DATE(registered_at)                             AS registered_date,

    -- ── SCD-2 metadata ────────────────────────────────────────────
    dbt_scd_id,
    DATE(dbt_valid_from)                            AS valid_from,
    DATE(dbt_valid_to)                              AS valid_to,
    dbt_valid_to IS NULL                            AS is_current

FROM snp
