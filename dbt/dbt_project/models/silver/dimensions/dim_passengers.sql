{{
  config(
    materialized = 'table',
    tags         = ['silver', 'dimension', 'passengers']
  )
}}

-- ══════════════════════════════════════════════════════════════
--  dim_passengers
--  Purpose : Clean passenger dimension with zone join.
-- ══════════════════════════════════════════════════════════════

with passengers as (

    select * from {{ ref('stg_passengers') }}

),

zones as (

    select * from {{ ref('dim_zones') }}

),

joined as (

    select
        p.passenger_id,
        p.passenger_code,
        p.full_name                     as passenger_name,
        p.phone_number_clean            as phone_number,
        p.email,
        p.birth_date,
        p.age_years,
        p.age_bucket,
        p.gender,
        p.referral_code,
        p.is_verified,
        p.total_trips,
        p.passenger_status,
        p.registered_at,
        p.updated_at,

        -- ── DQ traceability ───────────────────────────────────────
        p.phone_was_dirty,

        -- ── Loyalty tier ──────────────────────────────────────────
        case
            when p.total_trips >= 200  then 'Platinum'
            when p.total_trips >= 50   then 'Gold'
            when p.total_trips >= 10   then 'Silver'
            else 'New'
        end                             as loyalty_tier,

        -- ── Home zone ─────────────────────────────────────────────
        z.zone_code                     as home_zone_code,
        z.zone_name                     as home_zone_name,
        z.city                          as home_city,
        z.province                      as home_province

    from passengers p
    left join zones z
           on p.home_zone_id = z.zone_id

)

select * from joined
