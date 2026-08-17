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

GEOCODE_DELAY_SECONDS = float(os.getenv("HAGHI_GEOCODE_DELAY_SECONDS", "0.25"))
GEOCODE_MAX_PER_RUN = int(os.getenv("HAGHI_GEOCODE_MAX_PER_RUN", "1000"))
GEOCODE_SEARCH_RADIUS_METERS = int(
    os.getenv("HAGHI_GEOCODE_SEARCH_RADIUS_METERS", "25000")
)
MERGE_BATCH_SIZE = int(os.getenv("HAGHI_GEOCODE_MERGE_BATCH_SIZE", "100"))

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
    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{_table_id(CACHE_TABLE)}` (
            address_hash STRING,
            geocode_query STRING,
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
            location_precision STRING,
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


def _request_geocode(
    session,
    *,
    api_key: str,
    text: str,
    proximity: tuple[float, float] | None = None,
) -> dict | None:
    params = {
        "text": text,
        "format": "json",
        "filter": "countrycode:id",
        "lang": "id",
        "limit": 1,
        "apiKey": api_key,
    }

    if proximity is not None:
        longitude, latitude = proximity
        # Customer addresses are local-business data. A configurable radius
        # prevents generic street names such as "Jalan Jawa" from resolving
        # to a distant city while still allowing customers outside Depok's
        # administrative boundary.
        params["filter"] = (
            "countrycode:id|"
            f"circle:{longitude},{latitude},{GEOCODE_SEARCH_RADIUS_METERS}"
        )
        params["bias"] = f"proximity:{longitude},{latitude}"

    response = session.get(
        GEOAPIFY_API_URL,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()

    payload = response.json()
    results = payload.get("results") or []
    if not results:
        return None
    return results[0]


def _as_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _location_precision(result: dict) -> str:
    result_type = str(result.get("result_type") or "").lower()
    housenumber = result.get("housenumber")
    street = result.get("street")

    if housenumber and result_type in {"building", "amenity"}:
        return "EXACT"

    if result_type in {"amenity"} and not housenumber:
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


def _status_for_result(result: dict) -> str:
    country_code = str(result.get("country_code") or "").lower()
    rank = result.get("rank") or {}
    confidence = _as_float(rank.get("confidence"))
    street_confidence = _as_float(rank.get("confidence_street_level"))
    city_confidence = _as_float(rank.get("confidence_city_level"))

    if country_code and country_code != "id":
        return "AMBIGUOUS"

    # Street-level customer analysis tolerates incomplete house-level matches.
    # A solid street match is sufficient for this dashboard use case.
    if result.get("street"):
        if street_confidence is None or street_confidence >= 0.50:
            return "SUCCESS"

    # Area-only inputs (e.g. kelurahan/kecamatan) can still be useful if the
    # returned city/area confidence is reasonable.
    if result.get("city") or result.get("suburb") or result.get("district"):
        best_area_confidence = city_confidence if city_confidence is not None else confidence
        if best_area_confidence is None or best_area_confidence >= 0.50:
            return "SUCCESS"

    if confidence is not None and confidence >= 0.70:
        return "SUCCESS"

    return "AMBIGUOUS"


def _cache_row(address_hash: str, geocode_query: str, result: dict | None) -> dict:
    now = datetime.now(timezone.utc).isoformat()

    if result is None:
        return {
            "address_hash": address_hash,
            "geocode_query": geocode_query,
            "geocode_status": "ZERO_RESULTS",
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
            "location_precision": "UNKNOWN",
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
        "geocode_status": _status_for_result(result),
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
        "location_precision": precision,
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

    payload = json.dumps(rows, ensure_ascii=False, allow_nan=False)
    sql = f"""
    MERGE `{_table_id(CACHE_TABLE)}` AS target
    USING (
        SELECT
            JSON_VALUE(item, '$.address_hash') AS address_hash,
            JSON_VALUE(item, '$.geocode_query') AS geocode_query,
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
            JSON_VALUE(item, '$.location_precision') AS location_precision,
            SAFE_CAST(JSON_VALUE(item, '$.geocode_confidence') AS FLOAT64) AS geocode_confidence,
            SAFE_CAST(JSON_VALUE(item, '$.street_confidence') AS FLOAT64) AS street_confidence,
            SAFE_CAST(JSON_VALUE(item, '$.city_confidence') AS FLOAT64) AS city_confidence,
            JSON_VALUE(item, '$.geocode_provider') AS geocode_provider,
            JSON_VALUE(item, '$.geocode_attribution') AS geocode_attribution,
            SAFE_CAST(JSON_VALUE(item, '$.geocoded_at') AS TIMESTAMP) AS geocoded_at
        FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@payload, wide_number_mode=>'round'))) AS item
    ) AS source
    ON target.address_hash = source.address_hash

    WHEN MATCHED THEN UPDATE SET
        geocode_query = source.geocode_query,
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
        location_precision = source.location_precision,
        geocode_confidence = source.geocode_confidence,
        street_confidence = source.street_confidence,
        city_confidence = source.city_confidence,
        geocode_provider = source.geocode_provider,
        geocode_attribution = source.geocode_attribution,
        geocoded_at = source.geocoded_at

    WHEN NOT MATCHED THEN INSERT (
        address_hash,
        geocode_query,
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
        location_precision,
        geocode_confidence,
        street_confidence,
        city_confidence,
        geocode_provider,
        geocode_attribution,
        geocoded_at
    ) VALUES (
        source.address_hash,
        source.geocode_query,
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
        source.location_precision,
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
            LIMIT 1
            """
        ).result()
    )

    if existing:
        return float(existing[0]["longitude"]), float(existing[0]["latitude"])

    LOG.info("Geocoding fixed Haghi Laundry outlet reference point.")
    result = _request_geocode(
        session,
        api_key=api_key,
        text=OUTLET_ADDRESS,
        proximity=None,
    )
    if result is None:
        raise AirflowException("Could not geocode Haghi Laundry outlet address.")

    latitude = _as_float(result.get("lat"))
    longitude = _as_float(result.get("lon"))
    if latitude is None or longitude is None:
        raise AirflowException("Outlet geocoder response did not contain latitude/longitude.")

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

    payload = json.dumps([row], ensure_ascii=False, allow_nan=False)
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
    FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@payload, wide_number_mode=>'round'))) AS item
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("payload", "STRING", payload)]
    )
    client.query(sql, job_config=job_config).result()

    return longitude, latitude


def _pending_addresses(client) -> list[dict]:
    query = f"""
    SELECT DISTINCT
        a.address_hash,
        a.geocode_query
    FROM `{_table_id(PRIMARY_ADDRESS_TABLE)}` AS a
    LEFT JOIN `{_table_id(CACHE_TABLE)}` AS cache
        USING (address_hash)
    WHERE
        a.address_hash IS NOT NULL
        AND a.geocode_query IS NOT NULL
        AND cache.address_hash IS NULL
    ORDER BY a.geocode_query
    LIMIT {GEOCODE_MAX_PER_RUN}
    """

    return [dict(row.items()) for row in client.query(query).result()]


def geocode_new_customer_addresses() -> dict:
    """Geocode only new unique street/area queries and persist results in BigQuery.

    The INT SQL intentionally strips house number and RT/RW before this helper
    sees the address. Customer name, phone and UUID are never sent to Geoapify.
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

    pending = _pending_addresses(client)
    if not pending:
        LOG.info("No new customer addresses require geocoding.")
        return {
            "pending": 0,
            "processed": 0,
            "success": 0,
            "ambiguous": 0,
            "zero_results": 0,
        }

    LOG.info(
        "Geocoding %s new unique customer street/area queries (max_per_run=%s).",
        len(pending),
        GEOCODE_MAX_PER_RUN,
    )

    counters = {
        "pending": len(pending),
        "processed": 0,
        "success": 0,
        "ambiguous": 0,
        "zero_results": 0,
    }
    batch: list[dict] = []

    for item in pending:
        address_hash = item["address_hash"]
        geocode_query = (item["geocode_query"] or "").strip()

        if len(geocode_query) < 3:
            row = _cache_row(address_hash, geocode_query, None)
            row["geocode_status"] = "INVALID_INPUT"
        else:
            try:
                result = _request_geocode(
                    session,
                    api_key=api_key,
                    text=geocode_query,
                    proximity=(outlet_longitude, outlet_latitude),
                )
            except Exception as exc:
                # Do not persist transient HTTP/API errors as a permanent cache result.
                # Raising allows the Airflow retry policy to retry safely.
                raise AirflowException(
                    f"Geoapify request failed for address_hash={address_hash}: {exc}"
                ) from exc

            row = _cache_row(address_hash, geocode_query, result)

        batch.append(row)
        counters["processed"] += 1

        status = row["geocode_status"]
        if status == "SUCCESS":
            counters["success"] += 1
        elif status == "AMBIGUOUS":
            counters["ambiguous"] += 1
        elif status == "ZERO_RESULTS":
            counters["zero_results"] += 1

        if len(batch) >= MERGE_BATCH_SIZE:
            _merge_cache_rows(client, batch)
            batch.clear()

        pause(GEOCODE_DELAY_SECONDS)

    _merge_cache_rows(client, batch)

    LOG.info("Customer geocoding completed: %s", counters)
    return counters