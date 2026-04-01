{% snapshot scd2_drivers %}

{{
    config(
        target_schema  = 'dev_snapshots',
        unique_key     = 'driver_id',

        -- ── Strategy: timestamp ──────────────────────────────────
        -- updated_at is maintained by Postgres trigger on every UPDATE.
        -- Airflow loads the full table (or CDC delta) to Bronze on each run.
        -- When dbt snapshot detects a new updated_at for the same driver_id,
        -- it closes the old row (dbt_valid_to = NOW()) and opens a new one.
        strategy       = 'timestamp',
        updated_at     = 'updated_at',

        -- ── Retention: keep all history (SCD Type 2) ────────────
        invalidate_hard_deletes = true,

        tags           = ['snapshot', 'scd2', 'drivers']
    )
}}

-- ══════════════════════════════════════════════════════════════
--  scd2_drivers
--
--  Tracks historical changes to driver attributes over time,
--  most importantly: status, rating, vehicle assignments, and zone.
--
--  dbt adds these columns automatically:
--    dbt_scd_id      → unique surrogate key per version row
--    dbt_updated_at  → when dbt processed this version
--    dbt_valid_from  → timestamp this version became active
--    dbt_valid_to    → timestamp this version was superseded (NULL = current)
--
--  Common analytical queries:
--    - How many drivers transitioned from ACTIVE → SUSPENDED in Q1?
--    - What was driver X's status on a specific date?
--    - Which drivers changed vehicle type more than once?
-- ══════════════════════════════════════════════════════════════

select
    driver_id,
    driver_code,
    full_name,
    phone_number,
    email,
    nik,
    sim_number,
    license_plate,
    vehicle_type_id,
    vehicle_brand,
    vehicle_model,
    vehicle_year,
    vehicle_color,
    home_zone_id,
    rating,
    total_trips,
    status,
    joined_at,
    updated_at,
    _source_system

from {{ source('bronze_pg', 'drivers') }}

{% endsnapshot %}
