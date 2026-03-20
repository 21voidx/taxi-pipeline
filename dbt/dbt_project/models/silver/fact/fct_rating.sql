-- ============================================================
-- silver_core.fct_rating
-- Fact rating customer terhadap driver
-- Grain: 1 baris = 1 rating per trip
-- Materialization: incremental (merge by rating_id)
-- ============================================================

{{
    config(
        unique_key       = 'rating_id',
        partition_by     = {
            'field'      : 'created_at',
            'data_type'  : 'timestamp',
            'granularity': 'day'
        },
        cluster_by       = ['rating_score']
    )
}}

WITH ratings AS (
    SELECT *
    FROM {{ source('bronze_pg', 'ratings_raw') }}
    {{ incremental_filter('updated_at') }}
),

dim_trip AS (
    SELECT trip_key, trip_id, city
    FROM {{ ref('fct_trip') }}
),

dim_customer AS (
    SELECT customer_key, customer_id
    FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE
),

dim_driver AS (
    SELECT driver_key, driver_id
    FROM {{ ref('dim_driver') }}
    WHERE is_current = TRUE
)

SELECT
    {{ surrogate_key(['r.rating_id']) }}     AS rating_key,
    r.rating_id,
    dt.trip_key,
    dc.customer_key,
    dd.driver_key,

    -- Date key
    {{ date_to_key('r.rated_ts') }}          AS rating_date_key,

    -- Measure
    r.rating_score,

    -- Kategori rating
    CASE
        WHEN r.rating_score = 5 THEN 'excellent'
        WHEN r.rating_score = 4 THEN 'good'
        WHEN r.rating_score = 3 THEN 'neutral'
        WHEN r.rating_score = 2 THEN 'poor'
        ELSE 'very_poor'
    END                                      AS rating_category,

    -- Flags
    (r.review_text IS NOT NULL AND TRIM(r.review_text) != '')  AS has_review_text,
    (r.rating_score >= 4)                   AS is_positive,
    (r.rating_score <= 2)                   AS is_negative,

    r.rated_ts,
    r.created_at,
    r.updated_at

FROM ratings r
LEFT JOIN dim_trip     dt ON r.trip_id     = dt.trip_id
LEFT JOIN dim_customer dc ON r.customer_id = dc.customer_id
LEFT JOIN dim_driver   dd ON r.driver_id   = dd.driver_id
