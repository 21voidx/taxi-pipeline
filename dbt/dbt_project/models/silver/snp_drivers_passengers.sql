-- ════════════════════════════════════════════════════════════════
--  snapshots/snp_drivers.sql
--  SCD Type 2 snapshot — tracks changes to driver profile
--  (status, rating, tier, zone) over time
-- ════════════════════════════════════════════════════════════════
{% snapshot snp_drivers %}

{{
  config(
    target_schema  = 'silver_snapshots',
    unique_key     = 'driver_id',
    strategy       = 'timestamp',
    updated_at     = 'updated_at',
    invalidate_hard_deletes = True
  )
}}

SELECT
    driver_id,
    driver_code,
    full_name,
    phone_normalised,
    email,
    is_phone_valid,
    is_nik_valid,
    license_plate_normalised,
    vehicle_type_code,
    vehicle_type_name,
    vehicle_brand,
    vehicle_model,
    vehicle_year,
    vehicle_color,
    home_zone_code,
    home_zone_name,
    home_city,
    rating,
    total_trips,
    status,
    driver_tier,
    days_on_platform,
    joined_at,
    updated_at

FROM {{ ref('slv_drivers') }}

{% endsnapshot %}


-- ════════════════════════════════════════════════════════════════
--  snapshots/snp_passengers.sql
--  SCD Type 2 snapshot — tracks loyalty tier, status changes
-- ════════════════════════════════════════════════════════════════
{% snapshot snp_passengers %}

{{
  config(
    target_schema  = 'silver_snapshots',
    unique_key     = 'passenger_id',
    strategy       = 'timestamp',
    updated_at     = 'updated_at',
    invalidate_hard_deletes = True
  )
}}

SELECT
    passenger_id,
    passenger_code,
    full_name,
    phone_normalised,
    email,
    is_phone_valid,
    is_email_valid,
    birth_date,
    age_bucket,
    gender,
    home_zone_code,
    home_zone_name,
    home_city,
    is_verified,
    total_trips,
    status,
    loyalty_tier,
    referral_code,
    registered_at,
    updated_at

FROM {{ ref('slv_passengers') }}

{% endsnapshot %}
