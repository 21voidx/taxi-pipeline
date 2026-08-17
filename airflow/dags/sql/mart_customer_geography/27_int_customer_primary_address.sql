-- Grain: 1 row = 1 customer with a non-empty main address.
--
-- Important privacy/design choice:
-- - address_raw/address_normalized remain internal in STG.
-- - geocode_query removes house number and RT/RW before being sent to the
--   external geocoder. The dashboard is intentionally street/area level.

CREATE OR REPLACE TABLE
`{{ params.project_id }}.{{ params.stg_dataset }}.int_customer_primary_address`
AS

WITH unnested_addresses AS (
    SELECT
        c.customer_uuid,
        c.customer_name,
        JSON_VALUE(address_item, '$.id') AS address_id,
        TRIM(JSON_VALUE(address_item, '$.address')) AS address_raw,
        LOWER(TRIM(JSON_VALUE(address_item, '$.is_main'))) AS is_main
    FROM
        `{{ params.project_id }}.{{ params.stg_dataset }}.stg_customer` AS c
    CROSS JOIN UNNEST(
        JSON_QUERY_ARRAY(c.detail_customer_addresses)
    ) AS address_item
),

main_address AS (
    SELECT
        customer_uuid,
        customer_name,
        address_id,
        address_raw
    FROM unnested_addresses
    WHERE
        is_main = 'true'
        AND NULLIF(TRIM(address_raw), '') IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_uuid
        ORDER BY address_id, address_raw
    ) = 1
),

normalized AS (
    SELECT
        customer_uuid,
        customer_name,
        address_id,
        address_raw,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            LOWER(address_raw),
                            r'\b(jln|jl)\.?',
                            'jalan'
                        ),
                        r'[,.;]+',
                        ' '
                    ),
                    r'\s+',
                    ' '
                ),
                r'\bnomer\b',
                'nomor'
            )
        ) AS address_normalized
    FROM main_address
),

classified AS (
    SELECT
        *,
        CASE
            WHEN REGEXP_CONTAINS(
                address_normalized,
                r'\b(no|nomor)\s*[a-z0-9]'
            ) THEN 'EXACT'

            WHEN REGEXP_CONTAINS(
                address_normalized,
                r'\b(perumahan|perum|komplek|kompleks|cluster|residence|griya|apartemen|apartment)\b'
            ) THEN 'COMPLEX'

            WHEN REGEXP_CONTAINS(address_normalized, r'\bjalan\b')
                THEN 'STREET'

            ELSE 'AREA'
        END AS input_location_precision
    FROM normalized
),

privacy_minimized AS (
    SELECT
        *,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        address_normalized,
                        r'\b(no|nomor)\s*[a-z0-9/-]+',
                        ' '
                    ),
                    r'\b(rt|rw)\s*[0-9/-]+',
                    ' '
                ),
                r'\s+',
                ' '
            )
        ) AS geocode_query
    FROM classified
)

SELECT
    customer_uuid,
    customer_name,
    address_id,
    address_raw,
    address_normalized,
    NULLIF(geocode_query, '') AS geocode_query,
    CASE
        WHEN NULLIF(geocode_query, '') IS NULL THEN NULL
        ELSE TO_HEX(SHA256(geocode_query))
    END AS address_hash,
    input_location_precision
FROM privacy_minimized;