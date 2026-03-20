-- ============================================================
-- silver_core.fct_driver_payout
-- Fact payout driver per trip
-- Grain: 1 baris = 1 payout per trip
-- Materialization: incremental (merge by payout_id)
-- ============================================================

{{
    config(
        unique_key       = 'payout_id',
        partition_by     = {
            'field'      : 'created_at',
            'data_type'  : 'timestamp',
            'granularity': 'day'
        },
        cluster_by       = ['payout_status']
    )
}}

WITH payouts AS (
    SELECT *
    FROM {{ source('bronze_pg', 'driver_payouts_raw') }}
    {{ incremental_filter('updated_at') }}
),

dim_trip AS (
    SELECT trip_key, trip_id
    FROM {{ ref('fct_trip') }}
),

dim_driver AS (
    SELECT driver_key, driver_id
    FROM {{ ref('dim_driver') }}
    WHERE is_current = TRUE
)

SELECT
    {{ surrogate_key(['p.payout_id']) }}                AS payout_key,
    p.payout_id,
    dt.trip_key,
    dd.driver_key,

    -- Date key
    CAST(FORMAT_DATE('%Y%m%d', p.payout_date) AS INT64) AS payout_date_key,

    -- Degenerate dimension
    p.payout_status,

    -- Measures (IDR)
    CAST(p.base_earning        AS NUMERIC)              AS base_earning,
    CAST(p.incentive_amount    AS NUMERIC)              AS incentive_amount,
    CAST(p.bonus_amount        AS NUMERIC)              AS bonus_amount,
    CAST(p.deduction_amount    AS NUMERIC)              AS deduction_amount,
    CAST(p.final_payout_amount AS NUMERIC)              AS final_payout_amount,

    -- Derived: total tambahan di atas base
    CAST(p.incentive_amount + p.bonus_amount AS NUMERIC) AS total_additional_earning,

    -- Flags
    (p.payout_status = 'paid')    AS is_paid,
    (p.bonus_amount > 0)          AS has_bonus,
    (p.incentive_amount > 0)      AS has_incentive,
    (p.deduction_amount > 0)      AS has_deduction,

    p.payout_date,
    p.created_at,
    p.updated_at

FROM payouts p
LEFT JOIN dim_trip   dt ON p.trip_id   = dt.trip_id
LEFT JOIN dim_driver dd ON p.driver_id = dd.driver_id
