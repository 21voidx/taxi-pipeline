{% snapshot snapshot_dim_driver %}

{{
    config(
        unique_key    = 'driver_id',
        strategy      = 'timestamp',
        updated_at    = 'updated_at',
        invalidate_hard_deletes = True
    )
}}

-- Perubahan driver_status (active → suspended dll.) akan
-- menghasilkan baris SCD2 baru secara otomatis
SELECT
    driver_id,
    TRIM(full_name)         AS full_name,
    phone_number,
    LOWER(TRIM(email))      AS email,
    UPPER(license_number)   AS license_number,
    LOWER(driver_status)    AS driver_status,
    join_date,
    TRIM(city)              AS city,
    created_at,
    updated_at
FROM {{ source('bronze_pg', 'drivers_raw') }}

{% endsnapshot %}
