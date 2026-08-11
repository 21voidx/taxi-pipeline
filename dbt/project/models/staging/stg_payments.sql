with ranked as (
    select
        payment_id,
        ride_id,
        upper(trim(payment_method)) as payment_method,
        upper(trim(payment_status)) as payment_status,
        cast(payment_amount as numeric) as payment_amount,
        cast(platform_fee as numeric) as platform_fee,
        cast(driver_earning as numeric) as driver_earning,
        upper(trim(failure_reason)) as failure_reason,

        paid_at as paid_at_utc,
        datetime(paid_at, 'Asia/Jakarta') as paid_at_jakarta,
        date(paid_at, 'Asia/Jakarta') as paid_date,

        created_at as created_at_utc,
        datetime(created_at, 'Asia/Jakarta') as created_at_jakarta,
        date(created_at, 'Asia/Jakarta') as created_date,

        updated_at as updated_at_utc,
        datetime(updated_at, 'Asia/Jakarta') as updated_at_jakarta,

        _ingested_at as _ingested_at_utc,
        datetime(_ingested_at, 'Asia/Jakarta') as _ingested_at_jakarta,
        _batch_id,
        _source_system,
        _source_updated_at as _source_updated_at_utc,
        datetime(_source_updated_at, 'Asia/Jakarta') as _source_updated_at_jakarta,
        _airflow_run_id,

        row_number() over (
            partition by payment_id
            order by updated_at desc, _ingested_at desc
        ) as _row_number
    from {{ source('raw_ride_hailing', 'raw_payments') }}
)

select * except (_row_number)
from ranked
where _row_number = 1
