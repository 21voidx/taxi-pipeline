from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

from helpers.hallolaundry import (
    BQ_LOCATION,
    BQ_STG_DATASET,
    GCP_CONN_ID,
    GCP_PROJECT_ID,
    REQUEST_TIMEOUT,
    build_http_session,
    pause,
    required_variable,
)

LOG = logging.getLogger(__name__)

GEOAPIFY_API_URL = "https://api.geoapify.com/v1/geocode/search"
GEOAPIFY_API_KEY_VARIABLE = "GEOAPIFY_API_KEY"

OUTLET_NAME = "Haghi Laundry Express & Self-Service"
OUTLET_ADDRESS = (
    "Jl. Nusantara Raya No.94, Beji, Kecamatan Beji, "
    "Kota Depok, Jawa Barat 16421, Indonesia"
)

# Primary customer-address context.
PRIMARY_CITY = "Depok"
PRIMARY_STATE = "Jawa Barat"
PRIMARY_COUNTRY = "Indonesia"
PRIMARY_COUNTRY_CODE = "id"

GEOCODE_DELAY_SECONDS = float(os.getenv("HAGHI_GEOCODE_DELAY_SECONDS", "0.25"))
GEOCODE_MAX_PER_RUN = int(os.getenv("HAGHI_GEOCODE_MAX_PER_RUN", "1000"))

# Fallback search is intentionally local. 10 km is wider than Haghi's 5 km
# free-pickup policy, while preventing generic street names from resolving
# to far-away Jakarta/Bogor/Bekasi locations.
GEOCODE_SEARCH_RADIUS_METERS = int(
    os.getenv("HAGHI_GEOCODE_SEARCH_RADIUS_METERS", "10000")
)

# Bump this value whenever the matching/geocoding strategy changes materially.
# Rows with an older/null version are automatically reprocessed.
GEOCODE_ALGORITHM_VERSION = os.getenv(
    "HAGHI_GEOCODE_ALGORITHM_VERSION",
    "v2_depok_first_10km_fallback",
)

MERGE_BATCH_SIZE = int(os.getenv("HAGHI_GEOCODE_MERGE_BATCH_SIZE", "100"))
GEOAPIFY_RESULT_LIMIT = int(os.getenv("HAGHI_GEOAPIFY_RESULT_LIMIT", "5"))

CACHE_TABLE = "customer_geocode_cache"
OUTLET_TABLE = "geography_outlet_reference"
PRIMARY_ADDRESS_TABLE = "int_customer_primary_address"


def _table_id(table_name: str) -> str:
    return f"{GCP_PROJECT_ID}.{BQ_STG_DATASET}.{table_name}"


def _client():
    hook = BigQueryHook(
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        use_legacy_sql=False,
    )
    return hook.get_client(project_id=GCP_PROJECT_ID, location=BQ_LOCATION)


def _ensure_tables(client) -> None:
    """Create geography tables and migrate cache schema when needed."""
    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{_table_id(CACHE_TABLE)}` (
            address_hash STRING,
            geocode_query STRING,
            input_location_precision STRING,
            geocode_version STRING,
            geocode_strategy STRING,
            geocode_status STRING,
            latitude FLOAT64,
            longitude FLOAT64,
            formatted_address STRING,
            location_label STRING,
            street STRING,
            suburb STRING,
            district STRING,
            city STRING,
            state STRING,
            postcode STRING,
            country_code STRING,
            result_type STRING,
            match_type STRING,
            location_precision STRING,
            is_depok_result BOOL,
            geocoder_distance_m FLOAT64,
            geocode_confidence FLOAT64,
            street_confidence FLOAT64,
            city_confidence FLOAT64,
            geocode_provider STRING,
            geocode_attribution STRING,
            geocoded_at TIMESTAMP
        )
        CLUSTER BY geocode_status, location_precision
        """
    ).result()

    # Existing installations may already have the v1 cache table. BigQuery
    # leaves old rows NULL in the newly added fields; geocode_version then
    # causes those rows to be reprocessed automatically.
    client.query(
        f"""
        ALTER TABLE `{_table_id(CACHE_TABLE)}`
          ADD COLUMN IF NOT EXISTS input_location_precision STRING,
          ADD COLUMN IF NOT EXISTS geocode_version STRING,
          ADD COLUMN IF NOT EXISTS geocode_strategy STRING,
          ADD COLUMN IF NOT EXISTS match_type STRING,
          ADD COLUMN IF NOT EXISTS is_depok_result BOOL,
          ADD COLUMN IF NOT EXISTS geocoder_distance_m FLOAT64
        """
    ).result()

    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{_table_id(OUTLET_TABLE)}` (
            outlet_name STRING,
            outlet_address STRING,
            latitude FLOAT64,
            longitude FLOAT64,
            formatted_address STRING,
            geocode_provider STRING,
            geocode_attribution STRING,
            geocoded_at TIMESTAMP
        )
        """
    ).result()


def _request_geoapify(session, *, api_key: str, params: dict) -> list[dict]:
    request_params = {
        "format": "json",
        "lang": "id",
        "limit": GEOAPIFY_RESULT_LIMIT,
        "apiKey": api_key,
        **params,
    }

    response = session.get(
        GEOAPIFY_API_URL,
        params=request_params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    payload = response.json()
    results = payload.get("results") or []
    return [result for result in results if isinstance(result, dict)]


def _request_depok_primary(
    session,
    *,
    api_key: str,
    geocode_query: str,
    input_location_precision: str,
    proximity: tuple[float, float],
) -> tuple[list[dict], str]:
    """Attempt 1: search using explicit Depok/Jawa Barat context."""
    longitude, latitude = proximity

    if input_location_precision in {"EXACT", "STREET"}:
        # House number has already been privacy-minimized in SQL. Structured
        # street + city/state/country is much less ambiguous than free text.
        params = {
            "street": geocode_query,
            "city": PRIMARY_CITY,
            "state": PRIMARY_STATE,
            "country": PRIMARY_COUNTRY,
            "type": "street",
            "filter": f"countrycode:{PRIMARY_COUNTRY_CODE}",
            "bias": f"proximity:{longitude},{latitude}",
        }
        strategy = "DEPOK_STRUCTURED"
    else:
        # Complex/area inputs are not reliably a "street", so keep them as
        # contextual free text while still stating the city/state/country.
        params = {
            "text": (
                f"{geocode_query}, {PRIMARY_CITY}, "
                f"{PRIMARY_STATE}, {PRIMARY_COUNTRY}"
            ),
            "filter": f"countrycode:{PRIMARY_COUNTRY_CODE}",
            "bias": f"proximity:{longitude},{latitude}",
        }
        strategy = "DEPOK_CONTEXT_TEXT"

    return _request_geoapify(session, api_key=api_key, params=params), strategy


def _request_local_fallback(
    session,
    *,
    api_key: str,
    geocode_query: str,
    proximity: tuple[float, float],
) -> tuple[list[dict], str]:
    """Attempt 2: allow nearby non-Depok results but restrict them to 10 km."""
    longitude, latitude = proximity
    params = {
        "text": geocode_query,
        "filter": (
            f"countrycode:{PRIMARY_COUNTRY_CODE}|"
            f"circle:{longitude},{latitude},{GEOCODE_SEARCH_RADIUS_METERS}"
        ),
        "bias": f"proximity:{longitude},{latitude}",
    }
    return (
        _request_geoapify(session, api_key=api_key, params=params),
        "LOCAL_RADIUS_FALLBACK",
    )


def _as_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_depok_result(result: dict) -> bool:
    """Return True when administrative result clearly belongs to Depok."""
    city = str(result.get("city") or "").casefold()
    county = str(result.get("county") or "").casefold()

    if "depok" in city or "depok" in county:
        return True

    # Some OSM records can omit city/county. Only use formatted address as a
    # fallback administrative signal when both structured components are absent.
    if not city and not county:
        formatted = str(result.get("formatted") or "").casefold()
        return "depok" in formatted

    return False


def _location_precision(result: dict) -> str:
    result_type = str(result.get("result_type") or "").lower()
    housenumber = result.get("housenumber")
    street = result.get("street")

    if housenumber and result_type in {"building", "amenity"}:
        return "EXACT"

    if result_type == "amenity" and not housenumber:
        return "COMPLEX"

    if street:
        return "STREET"

    if result_type in {
        "suburb",
        "district",
        "city",
        "postcode",
        "county",
        "state",
        "country",
        "locality",
    }:
        return "AREA"

    return "UNKNOWN"


def _location_label(result: dict, geocode_query: str, precision: str) -> str | None:
    if precision == "COMPLEX":
        return geocode_query.title()

    return (
        result.get("street")
        or result.get("suburb")
        or result.get("district")
        or result.get("city")
        or result.get("formatted")
    )


def _status_for_result(result: dict, input_location_precision: str) -> str:
    country_code = str(result.get("country_code") or "").lower()
    rank = result.get("rank") or {}

    confidence = _as_float(rank.get("confidence"))
    street_confidence = _as_float(rank.get("confidence_street_level"))
    city_confidence = _as_float(rank.get("confidence_city_level"))

    if country_code and country_code != PRIMARY_COUNTRY_CODE:
        return "AMBIGUOUS"

    if input_location_precision in {"EXACT", "STREET"}:
        # A street input must resolve to a street. Do not silently downgrade a
        # failed street lookup to a city centroid.
        if not result.get("street"):
            return "AMBIGUOUS"

        if street_confidence is None or street_confidence >= 0.50:
            return "SUCCESS"

        return "AMBIGUOUS"

    if input_location_precision == "COMPLEX":
        if (
            result.get("name")
            or result.get("street")
            or result.get("suburb")
            or result.get("district")
            or result.get("city")
        ):
            if confidence is None or confidence >= 0.50:
                return "SUCCESS"

        return "AMBIGUOUS"

    # AREA inputs can legitimately resolve only to suburb/district/city.
    if result.get("city") or result.get("suburb") or result.get("district"):
        best_area_confidence = (
            city_confidence if city_confidence is not None else confidence
        )
        if best_area_confidence is None or best_area_confidence >= 0.50:
            return "SUCCESS"

    if confidence is not None and confidence >= 0.70:
        return "SUCCESS"

    return "AMBIGUOUS"


def _candidate_score(result: dict, input_location_precision: str) -> tuple:
    rank = result.get("rank") or {}
    confidence = _as_float(rank.get("confidence")) or 0.0
    street_confidence = _as_float(rank.get("confidence_street_level")) or 0.0
    city_confidence = _as_float(rank.get("confidence_city_level")) or 0.0
    distance = _as_float(result.get("distance"))
    distance_score = -(distance if distance is not None else 10**12)

    has_street = 1 if result.get("street") else 0
    expects_street = input_location_precision in {"EXACT", "STREET"}

    return (
        has_street if expects_street else 1,
        street_confidence if expects_street else city_confidence,
        confidence,
        distance_score,
    )


def _best_candidate(
    results: list[dict],
    *,
    input_location_precision: str,
    require_depok: bool,
) -> tuple[dict | None, str]:
    candidates: list[dict] = []

    for result in results:
        country_code = str(result.get("country_code") or "").lower()
        if country_code and country_code != PRIMARY_COUNTRY_CODE:
            continue

        if require_depok and not _is_depok_result(result):
            continue

        candidates.append(result)

    if not candidates:
        return None, "ZERO_RESULTS"

    candidates.sort(
        key=lambda result: _candidate_score(result, input_location_precision),
        reverse=True,
    )
    best = candidates[0]
    return best, _status_for_result(best, input_location_precision)


def _cache_row(
    *,
    address_hash: str,
    geocode_query: str,
    input_location_precision: str,
    result: dict | None,
    geocode_strategy: str,
    geocode_status: str,
) -> dict:
    now = datetime.now(timezone.utc).isoformat()

    if result is None:
        return {
            "address_hash": address_hash,
            "geocode_query": geocode_query,
            "input_location_precision": input_location_precision,
            "geocode_version": GEOCODE_ALGORITHM_VERSION,
            "geocode_strategy": geocode_strategy,
            "geocode_status": geocode_status,
            "latitude": None,
            "longitude": None,
            "formatted_address": None,
            "location_label": None,
            "street": None,
            "suburb": None,
            "district": None,
            "city": None,
            "state": None,
            "postcode": None,
            "country_code": None,
            "result_type": None,
            "match_type": None,
            "location_precision": "UNKNOWN",
            "is_depok_result": None,
            "geocoder_distance_m": None,
            "geocode_confidence": None,
            "street_confidence": None,
            "city_confidence": None,
            "geocode_provider": "geoapify",
            "geocode_attribution": None,
            "geocoded_at": now,
        }

    rank = result.get("rank") or {}
    datasource = result.get("datasource") or {}
    precision = _location_precision(result)

    return {
        "address_hash": address_hash,
        "geocode_query": geocode_query,
        "input_location_precision": input_location_precision,
        "geocode_version": GEOCODE_ALGORITHM_VERSION,
        "geocode_strategy": geocode_strategy,
        "geocode_status": geocode_status,
        "latitude": _as_float(result.get("lat")),
        "longitude": _as_float(result.get("lon")),
        "formatted_address": result.get("formatted"),
        "location_label": _location_label(result, geocode_query, precision),
        "street": result.get("street"),
        "suburb": result.get("suburb"),
        "district": result.get("district") or result.get("county"),
        "city": result.get("city"),
        "state": result.get("state"),
        "postcode": result.get("postcode"),
        "country_code": result.get("country_code"),
        "result_type": result.get("result_type"),
        "match_type": rank.get("match_type"),
        "location_precision": precision,
        "is_depok_result": _is_depok_result(result),
        "geocoder_distance_m": _as_float(result.get("distance")),
        "geocode_confidence": _as_float(rank.get("confidence")),
        "street_confidence": _as_float(rank.get("confidence_street_level")),
        "city_confidence": _as_float(rank.get("confidence_city_level")),
        "geocode_provider": "geoapify",
        "geocode_attribution": datasource.get("attribution"),
        "geocoded_at": now,
    }


def _merge_cache_rows(client, rows: list[dict]) -> None:
    if not rows:
        return

    payload = json.dumps(
        rows,
        ensure_ascii=False,
        allow_nan=False,
    )

    sql = f"""
    MERGE `{_table_id(CACHE_TABLE)}` AS target
    USING (
        SELECT
            JSON_VALUE(item, '$.address_hash') AS address_hash,
            JSON_VALUE(item, '$.geocode_query') AS geocode_query,
            JSON_VALUE(item, '$.input_location_precision') AS input_location_precision,
            JSON_VALUE(item, '$.geocode_version') AS geocode_version,
            JSON_VALUE(item, '$.geocode_strategy') AS geocode_strategy,
            JSON_VALUE(item, '$.geocode_status') AS geocode_status,
            SAFE_CAST(JSON_VALUE(item, '$.latitude') AS FLOAT64) AS latitude,
            SAFE_CAST(JSON_VALUE(item, '$.longitude') AS FLOAT64) AS longitude,
            JSON_VALUE(item, '$.formatted_address') AS formatted_address,
            JSON_VALUE(item, '$.location_label') AS location_label,
            JSON_VALUE(item, '$.street') AS street,
            JSON_VALUE(item, '$.suburb') AS suburb,
            JSON_VALUE(item, '$.district') AS district,
            JSON_VALUE(item, '$.city') AS city,
            JSON_VALUE(item, '$.state') AS state,
            JSON_VALUE(item, '$.postcode') AS postcode,
            JSON_VALUE(item, '$.country_code') AS country_code,
            JSON_VALUE(item, '$.result_type') AS result_type,
            JSON_VALUE(item, '$.match_type') AS match_type,
            JSON_VALUE(item, '$.location_precision') AS location_precision,
            SAFE_CAST(JSON_VALUE(item, '$.is_depok_result') AS BOOL) AS is_depok_result,
            SAFE_CAST(JSON_VALUE(item, '$.geocoder_distance_m') AS FLOAT64) AS geocoder_distance_m,
            SAFE_CAST(JSON_VALUE(item, '$.geocode_confidence') AS FLOAT64) AS geocode_confidence,
            SAFE_CAST(JSON_VALUE(item, '$.street_confidence') AS FLOAT64) AS street_confidence,
            SAFE_CAST(JSON_VALUE(item, '$.city_confidence') AS FLOAT64) AS city_confidence,
            JSON_VALUE(item, '$.geocode_provider') AS geocode_provider,
            JSON_VALUE(item, '$.geocode_attribution') AS geocode_attribution,
            SAFE_CAST(JSON_VALUE(item, '$.geocoded_at') AS TIMESTAMP) AS geocoded_at
        FROM UNNEST(
            JSON_QUERY_ARRAY(
                PARSE_JSON(@payload, wide_number_mode=>'round')
            )
        ) AS item
    ) AS source
    ON target.address_hash = source.address_hash

    WHEN MATCHED THEN UPDATE SET
        geocode_query = source.geocode_query,
        input_location_precision = source.input_location_precision,
        geocode_version = source.geocode_version,
        geocode_strategy = source.geocode_strategy,
        geocode_status = source.geocode_status,
        latitude = source.latitude,
        longitude = source.longitude,
        formatted_address = source.formatted_address,
        location_label = source.location_label,
        street = source.street,
        suburb = source.suburb,
        district = source.district,
        city = source.city,
        state = source.state,
        postcode = source.postcode,
        country_code = source.country_code,
        result_type = source.result_type,
        match_type = source.match_type,
        location_precision = source.location_precision,
        is_depok_result = source.is_depok_result,
        geocoder_distance_m = source.geocoder_distance_m,
        geocode_confidence = source.geocode_confidence,
        street_confidence = source.street_confidence,
        city_confidence = source.city_confidence,
        geocode_provider = source.geocode_provider,
        geocode_attribution = source.geocode_attribution,
        geocoded_at = source.geocoded_at

    WHEN NOT MATCHED THEN INSERT (
        address_hash,
        geocode_query,
        input_location_precision,
        geocode_version,
        geocode_strategy,
        geocode_status,
        latitude,
        longitude,
        formatted_address,
        location_label,
        street,
        suburb,
        district,
        city,
        state,
        postcode,
        country_code,
        result_type,
        match_type,
        location_precision,
        is_depok_result,
        geocoder_distance_m,
        geocode_confidence,
        street_confidence,
        city_confidence,
        geocode_provider,
        geocode_attribution,
        geocoded_at
    ) VALUES (
        source.address_hash,
        source.geocode_query,
        source.input_location_precision,
        source.geocode_version,
        source.geocode_strategy,
        source.geocode_status,
        source.latitude,
        source.longitude,
        source.formatted_address,
        source.location_label,
        source.street,
        source.suburb,
        source.district,
        source.city,
        source.state,
        source.postcode,
        source.country_code,
        source.result_type,
        source.match_type,
        source.location_precision,
        source.is_depok_result,
        source.geocoder_distance_m,
        source.geocode_confidence,
        source.street_confidence,
        source.city_confidence,
        source.geocode_provider,
        source.geocode_attribution,
        source.geocoded_at
    )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("payload", "STRING", payload),
        ]
    )
    client.query(sql, job_config=job_config).result()


def _ensure_outlet_reference(client, session, api_key: str) -> tuple[float, float]:
    existing = list(
        client.query(
            f"""
            SELECT longitude, latitude
            FROM `{_table_id(OUTLET_TABLE)}`
            WHERE longitude IS NOT NULL AND latitude IS NOT NULL
            ORDER BY geocoded_at DESC
            LIMIT 1
            """
        ).result()
    )

    if existing:
        return float(existing[0]["longitude"]), float(existing[0]["latitude"])

    LOG.info("Geocoding fixed Haghi Laundry outlet reference point.")

    results = _request_geoapify(
        session,
        api_key=api_key,
        params={
            "text": OUTLET_ADDRESS,
            "filter": f"countrycode:{PRIMARY_COUNTRY_CODE}",
        },
    )

    depok_results = [result for result in results if _is_depok_result(result)]
    result = depok_results[0] if depok_results else None

    if result is None:
        raise AirflowException(
            "Could not geocode Haghi Laundry outlet to a Depok result."
        )

    latitude = _as_float(result.get("lat"))
    longitude = _as_float(result.get("lon"))

    if latitude is None or longitude is None:
        raise AirflowException(
            "Outlet geocoder response did not contain latitude/longitude."
        )

    datasource = result.get("datasource") or {}
    row = {
        "outlet_name": OUTLET_NAME,
        "outlet_address": OUTLET_ADDRESS,
        "latitude": latitude,
        "longitude": longitude,
        "formatted_address": result.get("formatted"),
        "geocode_provider": "geoapify",
        "geocode_attribution": datasource.get("attribution"),
        "geocoded_at": datetime.now(timezone.utc).isoformat(),
    }

    payload = json.dumps(
        [row],
        ensure_ascii=False,
        allow_nan=False,
    )

    sql = f"""
    INSERT INTO `{_table_id(OUTLET_TABLE)}` (
        outlet_name,
        outlet_address,
        latitude,
        longitude,
        formatted_address,
        geocode_provider,
        geocode_attribution,
        geocoded_at
    )
    SELECT
        JSON_VALUE(item, '$.outlet_name'),
        JSON_VALUE(item, '$.outlet_address'),
        SAFE_CAST(JSON_VALUE(item, '$.latitude') AS FLOAT64),
        SAFE_CAST(JSON_VALUE(item, '$.longitude') AS FLOAT64),
        JSON_VALUE(item, '$.formatted_address'),
        JSON_VALUE(item, '$.geocode_provider'),
        JSON_VALUE(item, '$.geocode_attribution'),
        SAFE_CAST(JSON_VALUE(item, '$.geocoded_at') AS TIMESTAMP)
    FROM UNNEST(
        JSON_QUERY_ARRAY(
            PARSE_JSON(@payload, wide_number_mode=>'round')
        )
    ) AS item
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("payload", "STRING", payload),
        ]
    )
    client.query(sql, job_config=job_config).result()

    return longitude, latitude


def _pending_addresses(client) -> list[dict]:
    query = f"""
    SELECT DISTINCT
        a.address_hash,
        a.geocode_query,
        a.input_location_precision
    FROM `{_table_id(PRIMARY_ADDRESS_TABLE)}` AS a
    LEFT JOIN `{_table_id(CACHE_TABLE)}` AS cache
        USING (address_hash)
    WHERE
        a.address_hash IS NOT NULL
        AND a.geocode_query IS NOT NULL
        AND (
            cache.address_hash IS NULL
            OR COALESCE(cache.geocode_version, '') != @geocode_version
        )
    ORDER BY a.geocode_query
    LIMIT {GEOCODE_MAX_PER_RUN}
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "geocode_version",
                "STRING",
                GEOCODE_ALGORITHM_VERSION,
            )
        ]
    )

    return [
        dict(row.items())
        for row in client.query(query, job_config=job_config).result()
    ]


def _resolve_address(
    session,
    *,
    api_key: str,
    geocode_query: str,
    input_location_precision: str,
    proximity: tuple[float, float],
) -> tuple[dict | None, str, str]:
    """Return (result, strategy, final_status) using Depok-first fallback logic."""

    primary_results, primary_strategy = _request_depok_primary(
        session,
        api_key=api_key,
        geocode_query=geocode_query,
        input_location_precision=input_location_precision,
        proximity=proximity,
    )

    primary_result, primary_status = _best_candidate(
        primary_results,
        input_location_precision=input_location_precision,
        require_depok=True,
    )

    if primary_result is not None and primary_status == "SUCCESS":
        return primary_result, primary_strategy, "SUCCESS"

    # Avoid issuing both requests back-to-back too aggressively.
    pause(GEOCODE_DELAY_SECONDS)

    fallback_results, fallback_strategy = _request_local_fallback(
        session,
        api_key=api_key,
        geocode_query=geocode_query,
        proximity=proximity,
    )

    fallback_result, fallback_status = _best_candidate(
        fallback_results,
        input_location_precision=input_location_precision,
        require_depok=False,
    )

    if fallback_result is not None and fallback_status == "SUCCESS":
        return fallback_result, fallback_strategy, "SUCCESS"

    # Preserve a real candidate for QA instead of discarding it as ZERO_RESULTS.
    if fallback_result is not None:
        return fallback_result, fallback_strategy, "AMBIGUOUS"

    if primary_result is not None:
        return primary_result, primary_strategy, "AMBIGUOUS"

    return None, "NO_MATCH", "ZERO_RESULTS"


def geocode_new_customer_addresses() -> dict:
    """Geocode stale/new unique addresses and persist results in BigQuery.

    Privacy:
    - INT SQL strips house number and RT/RW before this helper sees the query.
    - Customer name, phone and UUID are never sent to Geoapify.

    Matching:
    1. Prefer Depok/Jawa Barat/Indonesia context.
    2. If no reliable Depok match exists, retry within 10 km of the outlet.
    3. Keep ambiguous/zero-results explicit instead of fabricating coordinates.
    """

    api_key = required_variable(GEOAPIFY_API_KEY_VARIABLE)
    client = _client()
    _ensure_tables(client)

    session = build_http_session()
    outlet_longitude, outlet_latitude = _ensure_outlet_reference(
        client,
        session,
        api_key,
    )
    proximity = (outlet_longitude, outlet_latitude)

    pending = _pending_addresses(client)
    if not pending:
        LOG.info(
            "No customer addresses require geocoding for version=%s.",
            GEOCODE_ALGORITHM_VERSION,
        )
        return {
            "pending": 0,
            "processed": 0,
            "success": 0,
            "ambiguous": 0,
            "zero_results": 0,
            "depok_primary": 0,
            "local_fallback": 0,
        }

    LOG.info(
        "Geocoding %s stale/new unique customer street/area queries "
        "(version=%s, fallback_radius_m=%s, max_per_run=%s).",
        len(pending),
        GEOCODE_ALGORITHM_VERSION,
        GEOCODE_SEARCH_RADIUS_METERS,
        GEOCODE_MAX_PER_RUN,
    )

    counters = {
        "pending": len(pending),
        "processed": 0,
        "success": 0,
        "ambiguous": 0,
        "zero_results": 0,
        "depok_primary": 0,
        "local_fallback": 0,
    }
    batch: list[dict] = []

    for item in pending:
        address_hash = item["address_hash"]
        geocode_query = (item["geocode_query"] or "").strip()
        input_location_precision = (
            item.get("input_location_precision") or "AREA"
        ).upper()

        if len(geocode_query) < 3:
            row = _cache_row(
                address_hash=address_hash,
                geocode_query=geocode_query,
                input_location_precision=input_location_precision,
                result=None,
                geocode_strategy="NO_MATCH",
                geocode_status="INVALID_INPUT",
            )
        else:
            try:
                result, strategy, status = _resolve_address(
                    session,
                    api_key=api_key,
                    geocode_query=geocode_query,
                    input_location_precision=input_location_precision,
                    proximity=proximity,
                )
            except Exception as exc:
                # Transient HTTP/API errors must not become permanent cache rows.
                raise AirflowException(
                    "Geoapify request failed for "
                    f"address_hash={address_hash}: {exc}"
                ) from exc

            row = _cache_row(
                address_hash=address_hash,
                geocode_query=geocode_query,
                input_location_precision=input_location_precision,
                result=result,
                geocode_strategy=strategy,
                geocode_status=status,
            )

        batch.append(row)
        counters["processed"] += 1

        status = row["geocode_status"]
        strategy = row["geocode_strategy"]

        if status == "SUCCESS":
            counters["success"] += 1
        elif status == "AMBIGUOUS":
            counters["ambiguous"] += 1
        elif status == "ZERO_RESULTS":
            counters["zero_results"] += 1

        if strategy in {"DEPOK_STRUCTURED", "DEPOK_CONTEXT_TEXT"}:
            counters["depok_primary"] += 1
        elif strategy == "LOCAL_RADIUS_FALLBACK":
            counters["local_fallback"] += 1

        if len(batch) >= MERGE_BATCH_SIZE:
            _merge_cache_rows(client, batch)
            batch.clear()

        pause(GEOCODE_DELAY_SECONDS)

    _merge_cache_rows(client, batch)

    LOG.info("Customer geocoding completed: %s", counters)
    return counters