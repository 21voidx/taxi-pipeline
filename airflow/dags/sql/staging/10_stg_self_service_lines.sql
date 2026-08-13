-- Grain: one row per services[] item within a self-service transaction.
--
-- Verified array:
-- raw_transaction_self_service.services[]
--
-- Because services[].amount and services[].quantity can be NULL:
-- - quantity defaults to 1 cycle;
-- - header total is allocated evenly across service lines when line amount
--   is unavailable. This preserves transaction revenue in service reporting.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_self_service_lines`
AS

WITH latest_parent AS (
  SELECT *
  FROM `{{ params.project_id }}.{{ params.raw_dataset }}.raw_transaction_self_service`
  WHERE id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CAST(id AS STRING)
    ORDER BY
      SAFE_CAST(updated_at AS TIMESTAMP) DESC,
      SAFE_CAST(_ingested_at AS TIMESTAMP) DESC
  ) = 1
),
exploded AS (
  SELECT
    t,
    line,
    COUNT(*) OVER (
      PARTITION BY CAST(t.id AS STRING)
    ) AS service_line_count
  FROM latest_parent AS t
  CROSS JOIN UNNEST(t.services) AS line
)

SELECT
  CAST(line.id AS STRING) AS self_service_line_id,
  CAST(t.id AS STRING) AS self_service_transaction_id,

  CAST(NULL AS STRING) AS customer_uuid,

  DATE(
    SAFE_CAST(t.created_at AS TIMESTAMP),
    'Asia/Jakarta'
  ) AS transaction_date,

  CAST(line.iot_machine_id AS STRING) AS machine_id,
  CAST(line.iot_machine.name AS STRING) AS machine_name,

  LOWER(CAST(line.iot_machine.machine_type AS STRING))
    AS machine_type,

  CAST(line.iot_machine.device_id AS STRING)
    AS machine_device_id,

  CAST(line.iot_machine.machine_command_id AS STRING)
    AS machine_command_id,

  CAST(line.iot_machine.machine_command_name AS STRING)
    AS machine_command_name,

  COALESCE(
    CAST(line.iot_machine_service_id AS STRING),
    CAST(line.iot_machine.id AS STRING)
  ) AS service_id,

  COALESCE(
    CAST(line.iot_machine.machine_command_name AS STRING),
    CAST(line.iot_machine.name AS STRING),
    'Self Service'
  ) AS service_name,

  LOWER(CAST(line.service_type AS STRING)) AS service_type,

  COALESCE(
    SAFE_CAST(line.quantity AS NUMERIC),
    CAST(1 AS NUMERIC)
  ) AS quantity,

  SAFE_CAST(line.amount AS NUMERIC) AS source_line_amount,

  COALESCE(
    SAFE_CAST(line.amount AS NUMERIC),
    SAFE_DIVIDE(
      SAFE_CAST(t.total AS NUMERIC),
      NULLIF(service_line_count, 0)
    )
  ) AS line_amount,

  LOWER(CAST(line.machine_callback.status AS STRING))
    AS callback_status,

  CAST(line.machine_callback.message AS STRING)
    AS callback_message,

  LOWER(CAST(line.machine_callback.status AS STRING))
    IN ('success', 'successful', 'valid', 'ok', 'processed', '200')
    AS is_callback_success,

  SAFE_CAST(line.used_at AS DATETIME)
    AS used_datetime_jakarta,

  TIMESTAMP(
    SAFE_CAST(line.used_at AS DATETIME),
    'Asia/Jakarta'
  ) AS used_at_utc,

  SAFE_CAST(line.expired_at AS DATETIME)
    AS expired_datetime_jakarta,

  SAFE_CAST(line.created_at AS TIMESTAMP)
    AS line_created_at_utc,

  SAFE_CAST(line.updated_at AS TIMESTAMP)
    AS source_updated_at_utc

FROM exploded

WHERE line.id IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
  PARTITION BY CAST(line.id AS STRING)
  ORDER BY SAFE_CAST(line.updated_at AS TIMESTAMP) DESC
) = 1;
