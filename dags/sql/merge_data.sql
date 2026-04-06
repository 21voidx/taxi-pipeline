CREATE TEMP TABLE temp_ride_events AS
WITH raw_ride_events AS (
  SELECT
    event_id,
    ride_id,
    event_type,
    event_payload,
    occurred_at,
    COALESCE(_source_system, 'postgres') AS _source_system
  FROM `{{ params.project_id }}.dev_bronze_pg.ride_events`
),

deduplicated_ride_events AS (
  -- Ambil record terakhir per event_id
  -- Cocok jika source bronze bersifat append-only / CDC
  SELECT * EXCEPT(rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY occurred_at DESC
      ) AS rn
    FROM raw_ride_events
  )
  WHERE rn = 1
)

SELECT
  event_id,
  ride_id,
  event_type,
  event_payload,
  occurred_at,
  _source_system
FROM deduplicated_ride_events;

MERGE INTO `{{ params.project_id }}.dev_label.ride_events` T
USING temp_ride_events S
ON T.event_id = S.event_id

WHEN MATCHED THEN
  UPDATE SET
    T.ride_id = S.ride_id,
    T.event_type = S.event_type,
    T.event_payload = S.event_payload,
    T.occurred_at = S.occurred_at,
    T._source_system = S._source_system

WHEN NOT MATCHED THEN
  INSERT (
    event_id,
    ride_id,
    event_type,
    event_payload,
    occurred_at,
    _source_system
  )
  VALUES (
    S.event_id,
    S.ride_id,
    S.event_type,
    S.event_payload,
    S.occurred_at,
    S._source_system
  );