CREATE TEMP TABLE temp_zones AS
WITH raw_zones AS (
  SELECT 
    zone_id,
    zone_code,
    zone_name,
    city,
    latitude,
    longitude,
    is_active,
    created_at
  FROM `{{ params.project_id }}.dev_bronze_pg.zones`
)

, deduplicated_zones AS (
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY zone_code 
        ORDER BY created_at DESC
      ) AS rn
    FROM raw_zones
  )
  WHERE rn = 1
)

SELECT 
  zone_id,
  zone_code,
  zone_name,
  city,
  latitude,
  longitude,
  is_active,
  created_at,
  CURRENT_TIMESTAMP() AS _ingested_at,
  'postgresql' AS _source_system
FROM deduplicated_zones;

MERGE INTO `{{ params.project_id }}.dev_label.zones` T
USING temp_zones S
ON T.zone_code = S.zone_code

WHEN MATCHED THEN
  UPDATE SET
    T.zone_name = S.zone_name,
    T.city = S.city,
    T.latitude = S.latitude,
    T.longitude = S.longitude,
    T.is_active = S.is_active,
    T.created_at = S.created_at,
    T._ingested_at = S._ingested_at,
    T._source_system = S._source_system

WHEN NOT MATCHED THEN
  INSERT (
    zone_id,
    zone_code,
    zone_name,
    city,
    latitude,
    longitude,
    is_active,
    created_at,
    _ingested_at,
    _source_system
  )
  VALUES (
    S.zone_id,
    S.zone_code,
    S.zone_name,
    S.city,
    S.latitude,
    S.longitude,
    S.is_active,
    S.created_at,
    S._ingested_at,
    S._source_system
  );