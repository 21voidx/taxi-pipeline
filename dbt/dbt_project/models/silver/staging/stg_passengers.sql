{{
  config(
    materialized = 'view',
    tags         = ['silver', 'staging', 'passengers']
  )
}}

-- ══════════════════════════════════════════════════════════════
--  stg_passengers
--  Purpose : Clean & standardise passenger data from Bronze.
-- ══════════════════════════════════════════════════════════════

with source as (

    select * from {{ source('bronze_pg', 'passengers') }}

),

cleaned as (

    select
        -- ── Identity ──────────────────────────────────────────────
        passenger_id,
        passenger_code,
        full_name,

        -- ── Phone: canonical 08XXXXXXXXX ──────────────────────────
        {{ clean_phone_id('phone_number') }}    as phone_number_clean,
        phone_number                            as phone_number_raw,

        email,

        -- ── Demographics ──────────────────────────────────────────
        birth_date,
        date_diff(current_date(), birth_date, year) as age_years,
        case
            when date_diff(current_date(), birth_date, year) between 18 and 25 then '18-25'
            when date_diff(current_date(), birth_date, year) between 26 and 35 then '26-35'
            when date_diff(current_date(), birth_date, year) between 36 and 45 then '36-45'
            when date_diff(current_date(), birth_date, year) between 46 and 55 then '46-55'
            else '56+'
        end                                     as age_bucket,
        upper(coalesce(gender, 'UNKNOWN'))       as gender,

        -- ── Home zone & status ────────────────────────────────────
        home_zone_id,
        referral_code,
        is_verified,
        coalesce(total_trips, 0)                as total_trips,
        upper(status)                           as passenger_status,

        -- ── Timestamps ────────────────────────────────────────────
        registered_at,
        updated_at,
        _source_system,

        -- ── DQ flags ──────────────────────────────────────────────
        case
            when regexp_contains(phone_number, r'^\+62') then true
            when regexp_contains(phone_number, r'-')      then true
            else false
        end                                     as phone_was_dirty

    from source

)

select * from cleaned
