{{
  config(
    materialized = 'view',
    tags         = ['silver', 'staging', 'drivers']
  )
}}

-- ══════════════════════════════════════════════════════════════
--  stg_drivers
--  Purpose : Clean & standardise driver data from Bronze.
--  Cleaning :
--    - phone_number  → strip +62, dashes → canonical 08XXXXXXXXX
--    - nik           → remove spaces, pad/truncate to 16 digits
--    - license_plate → UPPER() + trim
--    - rating        → cap between 1.0 – 5.0
-- ══════════════════════════════════════════════════════════════

with source as (

    select * from {{ source('bronze_pg', 'drivers') }}

),

cleaned as (

    select
        -- ── Identity ──────────────────────────────────────────────
        driver_id,
        driver_code,
        full_name,

        -- ── Phone: +62XXXXXXXXXX → 08XXXXXXXXXX, remove dashes ───
        {{ clean_phone_id('phone_number') }}                        as phone_number_clean,
        phone_number                                                as phone_number_raw,

        email,

        -- ── NIK: remove spaces, validate 16 digits ────────────────
        {{ clean_nik('nik') }}                                      as nik_clean,
        nik                                                         as nik_raw,

        -- ── SIM & plate ───────────────────────────────────────────
        sim_number,
        upper(trim(regexp_replace(license_plate, r'\s+', ' ')))    as license_plate_clean,
        license_plate                                               as license_plate_raw,

        -- ── Vehicle ───────────────────────────────────────────────
        vehicle_type_id,
        vehicle_brand,
        vehicle_model,
        vehicle_year,
        vehicle_color,

        -- ── Zone & metrics ────────────────────────────────────────
        home_zone_id,
        greatest(1.0, least(5.0, coalesce(rating, 5.0)))           as rating,
        coalesce(total_trips, 0)                                    as total_trips,

        -- ── Status ────────────────────────────────────────────────
        upper(status)                                               as driver_status,

        -- ── Timestamps ────────────────────────────────────────────
        joined_at,
        updated_at,
        _source_system,

        -- ── Data quality flags ────────────────────────────────────
        case
            when regexp_contains(phone_number, r'^\+62')      then true
            when regexp_contains(phone_number, r'-')           then true
            else false
        end                                                         as phone_was_dirty,

        case
            when length(regexp_replace(nik, r'\s', '')) != 16  then true
            else false
        end                                                         as nik_was_dirty,

        case
            when lower(license_plate) = license_plate             then true
            else false
        end                                                         as plate_was_dirty

    from source

)

select * from cleaned
