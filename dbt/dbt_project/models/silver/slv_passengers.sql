{{
  config(
    materialized        = 'incremental',
    unique_key          = 'passenger_id',
    incremental_strategy= 'merge',
    tags                = ['silver', 'regex_cleaned']
  )
}}

/*
  SILVER: slv_passengers
  ══════════════════════════════════════════════════════════════
  Applies:
    ✓ REGEX  – normalise phone, validate email format
    ✓ CLEAN  – trim whitespace, handle NULLs
    ✓ JOIN   – enrich dengan zone name
    ✓ DERIVE – age_bucket, loyalty_tier, is_email_valid
*/

WITH raw AS (
    SELECT * FROM {{ ref('brz_passengers') }}
),

zones AS (
    SELECT zone_id, zone_code, zone_name, city
    FROM {{ ref('brz_zones') }}
),

cleaned AS (
    SELECT
        p.passenger_id,
        p.passenger_code,
        TRIM(INITCAP(p.full_name))                      AS full_name,

        -- ── Phone normalisation ──────────────────────────────────
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(TRIM(p.phone_number), r'^\+62', '0'),
          r'[\s\-\.]', ''),
        r'[^0-9]', '')                                   AS phone_normalised,

        REGEXP_CONTAINS(
          REGEXP_REPLACE(
            REGEXP_REPLACE(TRIM(p.phone_number), r'^\+62', '0'),
          r'[\s\-]',''),
          r'^08[0-9]{8,12}$'
        )                                                AS is_phone_valid,

        -- ── Email validation ─────────────────────────────────────
        LOWER(TRIM(p.email))                             AS email,

        REGEXP_CONTAINS(
          LOWER(TRIM(p.email)),
          r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
        )                                                AS is_email_valid,

        -- ── Demographics ─────────────────────────────────────────
        p.birth_date,

        CASE
          WHEN p.birth_date IS NULL THEN 'UNKNOWN'
          WHEN DATE_DIFF(CURRENT_DATE(), p.birth_date, YEAR) < 18 THEN 'UNDER_18'
          WHEN DATE_DIFF(CURRENT_DATE(), p.birth_date, YEAR) < 25 THEN '18-24'
          WHEN DATE_DIFF(CURRENT_DATE(), p.birth_date, YEAR) < 35 THEN '25-34'
          WHEN DATE_DIFF(CURRENT_DATE(), p.birth_date, YEAR) < 45 THEN '35-44'
          WHEN DATE_DIFF(CURRENT_DATE(), p.birth_date, YEAR) < 55 THEN '45-54'
          ELSE '55+'
        END                                              AS age_bucket,

        UPPER(COALESCE(p.gender, 'UNKNOWN'))             AS gender,

        -- ── Zone enrichment ──────────────────────────────────────
        p.home_zone_id,
        z.zone_code                                      AS home_zone_code,
        z.zone_name                                      AS home_zone_name,
        z.city                                           AS home_city,

        -- ── Status ───────────────────────────────────────────────
        p.is_verified,
        p.total_trips,
        UPPER(p.status)                                  AS status,
        p.referral_code,

        -- ── Derived: loyalty tier ────────────────────────────────
        CASE
          WHEN p.total_trips >= 200  THEN 'PLATINUM'
          WHEN p.total_trips >= 50   THEN 'GOLD'
          WHEN p.total_trips >= 10   THEN 'SILVER'
          ELSE 'BRONZE'
        END                                              AS loyalty_tier,

        p.registered_at,
        p.updated_at,
        p._loaded_at,
        p._source_table,
        p._row_hash

    FROM raw p
    LEFT JOIN zones z ON p.home_zone_id = z.zone_id
)

SELECT *
FROM cleaned
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
