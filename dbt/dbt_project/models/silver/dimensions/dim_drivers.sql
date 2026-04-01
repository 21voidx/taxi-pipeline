{{
  config(
    materialized = 'table',
    tags         = ['silver', 'dimension', 'drivers']
  )
}}

-- ══════════════════════════════════════════════════════════════
--  dim_drivers
--  Purpose : Clean driver dimension with vehicle type & zone joins.
--  Source  : stg_drivers → joins vehicle_types + dim_zones
-- ══════════════════════════════════════════════════════════════

with drivers as (

    select * from {{ ref('stg_drivers') }}

),

vehicle_types as (

    select * from {{ source('bronze_pg', 'vehicle_types') }}

),

zones as (

    select * from {{ ref('dim_zones') }}

),

joined as (

    select
        -- ── Driver core ───────────────────────────────────────────
        d.driver_id,
        d.driver_code,
        d.full_name                                     as driver_name,
        d.phone_number_clean                            as phone_number,
        d.email,
        d.nik_clean                                     as nik,
        d.sim_number,
        d.license_plate_clean                           as license_plate,
        d.driver_status,
        d.rating,
        d.total_trips,
        d.joined_at,
        d.updated_at,

        -- ── DQ traceability ───────────────────────────────────────
        d.phone_was_dirty,
        d.nik_was_dirty,
        d.plate_was_dirty,

        -- ── Vehicle info (denormalised) ───────────────────────────
        vt.vehicle_type_id,
        vt.type_code                                    as vehicle_type_code,
        vt.type_name                                    as vehicle_type_name,
        vt.capacity                                     as vehicle_capacity,
        d.vehicle_brand,
        d.vehicle_model,
        d.vehicle_year,
        d.vehicle_color,

        -- ── Driver experience tier ────────────────────────────────
        case
            when d.total_trips >= 1000 then 'Platinum'
            when d.total_trips >= 500  then 'Gold'
            when d.total_trips >= 100  then 'Silver'
            else 'Bronze'
        end                                             as experience_tier,

        -- ── Home zone ─────────────────────────────────────────────
        z.zone_code                                     as home_zone_code,
        z.zone_name                                     as home_zone_name,
        z.city                                          as home_city,
        z.province                                      as home_province,
        z.area_type                                     as home_area_type

    from drivers d
    left join vehicle_types vt
           on d.vehicle_type_id = vt.vehicle_type_id
    left join zones z
           on d.home_zone_id = z.zone_id

)

select * from joined
