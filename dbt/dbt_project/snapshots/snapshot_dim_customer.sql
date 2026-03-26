{% snapshot snapshot_dim_customer %}

{{
    config(
        unique_key    = 'customer_id',
        strategy      = 'timestamp',
        updated_at    = 'updated_at',
        invalidate_hard_deletes = True
    )
}}

-- Baca dari bronze, normalisasi minimal sebelum di-snapshot
SELECT
    customer_id,
    TRIM(full_name)         AS full_name,
    phone_number,
    LOWER(TRIM(email))      AS email,
    LOWER(gender)           AS gender,
    birth_date,
    created_at,
    updated_at,
    _ingested_at,
    _source_system
FROM {{ source('bronze_pg', 'customers_raw') }}

{% endsnapshot %}
