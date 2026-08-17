-- Dashboard-ready geography mart.
-- Grain: 1 row = 1 customer from mart_customer_analytics.
--
-- This mart intentionally preserves customers without usable addresses so that
-- Total Customers and Geocode Coverage can be calculated from one source.

CREATE OR REPLACE TABLE
`{{ params.project_id }}.{{ params.mart_dataset }}.mart_customer_geography`
AS

WITH cache_latest AS (
    SELECT *
    FROM `{{ params.project_id }}.{{ params.stg_dataset }}.customer_geocode_cache`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY address_hash
        ORDER BY geocoded_at DESC
    ) = 1
),

outlet AS (
    SELECT *
    FROM `{{ params.project_id }}.{{ params.stg_dataset }}.geography_outlet_reference`
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        ORDER BY geocoded_at DESC
    ) = 1
),

joined AS (
    SELECT
        ca.*,

        -- Address-quality metadata. Raw/full address is intentionally not
        -- exposed to the dashboard mart.
        a.input_location_precision,
        a.geocode_query,

        CASE
            WHEN a.address_hash IS NULL THEN 'NO_MAIN_ADDRESS'
            WHEN g.address_hash IS NULL THEN 'PENDING'
            ELSE g.geocode_status
        END AS geocode_status,

        COALESCE(g.location_precision, 'UNKNOWN') AS location_precision,
        g.location_label,
        g.formatted_address AS geocoded_formatted_address,
        g.street,
        g.suburb,
        g.district,
        g.city,
        g.state,
        g.postcode,
        g.country_code,
        g.result_type AS geocode_result_type,
        g.geocode_confidence,
        g.street_confidence,
        g.city_confidence,
        g.geocode_provider,
        g.geocode_attribution,

        g.latitude AS map_latitude,
        g.longitude AS map_longitude,
        CASE
            WHEN g.location_label IS NULL THEN NULL
            ELSE CONCAT(
                g.location_label,
                IF(g.suburb IS NOT NULL AND g.suburb != g.location_label, CONCAT(', ', g.suburb), ''),
                IF(g.district IS NOT NULL AND g.district != g.location_label, CONCAT(', ', g.district), ''),
                IF(g.city IS NOT NULL AND g.city != g.location_label, CONCAT(', ', g.city), ''),
                IF(g.state IS NOT NULL, CONCAT(', ', g.state), ''),
                ', Indonesia'
            )
        END AS map_location,

        o.outlet_name,
        o.outlet_address,
        o.latitude AS outlet_latitude,
        o.longitude AS outlet_longitude,

        CASE
            WHEN
                g.geocode_status = 'SUCCESS'
                AND g.latitude IS NOT NULL
                AND g.longitude IS NOT NULL
                AND o.latitude IS NOT NULL
                AND o.longitude IS NOT NULL
            THEN ST_DISTANCE(
                ST_GEOGPOINT(g.longitude, g.latitude),
                ST_GEOGPOINT(o.longitude, o.latitude)
            ) / 1000.0
        END AS estimated_distance_km

    FROM
        `{{ params.project_id }}.{{ params.mart_dataset }}.mart_customer_analytics` AS ca

    LEFT JOIN
        `{{ params.project_id }}.{{ params.stg_dataset }}.int_customer_primary_address` AS a
        USING (customer_uuid)

    LEFT JOIN cache_latest AS g
        USING (address_hash)

    LEFT JOIN outlet AS o
        ON TRUE
),

final AS (
    SELECT
        *,

        geocode_status = 'SUCCESS'
            AND map_latitude IS NOT NULL
            AND map_longitude IS NOT NULL
            AS is_geocoded,

        CASE location_precision
            WHEN 'EXACT' THEN 'HIGH'
            WHEN 'COMPLEX' THEN 'MEDIUM'
            WHEN 'STREET' THEN 'MEDIUM'
            WHEN 'AREA' THEN 'LOW'
            ELSE 'NONE'
        END AS distance_reliability,

        CASE
            WHEN estimated_distance_km IS NULL THEN 'UNKNOWN'
            WHEN estimated_distance_km <= 1 THEN '0-1 KM'
            WHEN estimated_distance_km <= 3 THEN '1-3 KM'
            WHEN estimated_distance_km <= 5 THEN '3-5 KM'
            ELSE '>5 KM'
        END AS distance_band,

        CASE
            WHEN estimated_distance_km IS NULL THEN 5
            WHEN estimated_distance_km <= 1 THEN 1
            WHEN estimated_distance_km <= 3 THEN 2
            WHEN estimated_distance_km <= 5 THEN 3
            ELSE 4
        END AS distance_band_order,

        CASE
            WHEN estimated_distance_km IS NOT NULL
            THEN estimated_distance_km <= 5
            ELSE FALSE
        END AS is_within_estimated_5km

    FROM joined
)

SELECT
    * REPLACE (
        ROUND(estimated_distance_km, 2) AS estimated_distance_km
    )
FROM final;