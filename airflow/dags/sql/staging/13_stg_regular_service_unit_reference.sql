-- Official Unit Regular Services reference.
-- Source: Unit Regular Services API supplied by the user.
--
-- Grain: one row per official unit_id.

CREATE OR REPLACE TABLE
  `{{ params.project_id }}.{{ params.stg_dataset }}.stg_regular_service_unit_reference`
AS

SELECT 1 AS unit_id, 'KG' AS unit_name UNION ALL
SELECT 2, 'M2' UNION ALL
SELECT 3, 'BARANG' UNION ALL
SELECT 4, 'PCS' UNION ALL
SELECT 5, 'LEMBAR' UNION ALL
SELECT 6, 'STEL' UNION ALL
SELECT 7, 'M' UNION ALL
SELECT 8, 'PAKET' UNION ALL
SELECT 9, 'UNIT' UNION ALL
SELECT 10, 'DUDUKAN' UNION ALL
SELECT 11, 'PASANG' UNION ALL
SELECT 12, 'CM2' UNION ALL
SELECT 13, 'KOIN' UNION ALL
SELECT 14, 'LOAD' UNION ALL
SELECT 15, 'HELAI' UNION ALL
SELECT 16, 'MILI' UNION ALL
SELECT 17, 'BIJI' UNION ALL
SELECT 18, 'CM';
