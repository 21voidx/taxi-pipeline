-- Grain: one row per IoT scan event.
--
-- Verified raw structure:
-- iot_machine, modelable, root mode, created_by.
--
-- Important:
-- mode.timer is at the root, not under modelable.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_machine_scan_events`
AS

SELECT
  CAST(s.id AS STRING) AS scan_event_id,
  CAST(s.scanning_type AS STRING) AS scanning_type,
  CAST(s.modelable_type AS STRING) AS modelable_type,

  CAST(s.iot_machine_id AS STRING) AS source_machine_id,
  CAST(s.iot_machine.id AS STRING) AS machine_id,
  CAST(s.iot_machine.name AS STRING) AS machine_name,
  LOWER(CAST(s.iot_machine.machine_type AS STRING))
    AS machine_type,
  CAST(s.iot_machine.device_id AS STRING)
    AS machine_device_id,
  CAST(s.iot_machine.machine_command_id AS STRING)
    AS machine_command_id,
  CAST(s.iot_machine.machine_command_name AS STRING)
    AS machine_command_name,

  CAST(s.modelable_id AS STRING) AS source_modelable_id,
  CAST(s.modelable.id AS STRING) AS machine_activation_id,

  CAST(s.modelable.transaction_relation_id AS STRING)
    AS transaction_id,

  CAST(s.modelable.transaction_relation_service_id AS STRING)
    AS transaction_service_operation_id,

  LOWER(CAST(
    s.modelable.transaction_relation.transaction_type AS STRING
  )) AS source_transaction_type,

  LOWER(CAST(
    s.modelable.transaction_relation.payment_status AS STRING
  )) AS source_payment_status,

  LOWER(CAST(
    s.modelable.transaction_relation.transaction_status AS STRING
  )) AS source_transaction_status,

  SAFE_CAST(
    s.modelable.transaction_relation.cancelled_at AS TIMESTAMP
  ) AS source_transaction_cancelled_at_utc,

  CAST(s.modelable.regular_service.id AS STRING)
    AS regular_service_id,
  CAST(s.modelable.regular_service.name AS STRING)
    AS regular_service_name,

  SAFE_CAST(
    s.modelable.regular_service.multiply_scanning AS INT64
  ) AS configured_multiply_scanning,

  SAFE_CAST(s.modelable.total_scanning AS INT64)
    AS source_total_scanning,

  CAST(s.modelable.mode_id AS STRING) AS mode_id,
  CAST(s.mode.name AS STRING) AS mode_name,

  -- Correct verified path.
  SAFE_CAST(s.mode.timer AS INT64) AS mode_timer_minutes,
  SAFE_CAST(s.mode.delay AS INT64) AS mode_delay_seconds,

  CAST(s.created_by.id AS STRING) AS operator_id,
  CAST(s.created_by.name AS STRING) AS operator_name,

  SAFE_CAST(s.created_at AS TIMESTAMP) AS scan_created_at_utc,

  DATETIME(
    SAFE_CAST(s.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS scan_created_datetime_jakarta,

  DATE(
    SAFE_CAST(s.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS scan_date,

  SAFE_CAST(s.updated_at AS TIMESTAMP)
    AS source_updated_at_utc

FROM
  `{{ params.project_id }}.{{ params.raw_dataset }}.raw_history_scanning_iot` AS s

WHERE s.id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(s.id AS STRING)
  ORDER BY
    SAFE_CAST(s.updated_at AS TIMESTAMP) DESC,
    SAFE_CAST(s._ingested_at AS TIMESTAMP) DESC
) = 1;
