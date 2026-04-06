CREATE temporary table temp_zones AS
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
  -- Opsional: Jika tabelnya besar, aktifkan filter tanggal di bawah ini.
  -- Karena tabel zones ini kecil (master data), full-scan biasanya tidak masalah dan lebih aman.
  -- WHERE date(created_at, 'Asia/Jakarta') >= DATE('{{ task_instance.xcom_pull(task_ids="params_eval", key="start_date") }}')
)

, deduplicated_zones AS (
  -- Mengambil data paling terakhir (latest state) per zone_code
  -- Ini sangat berguna untuk data hasil CDC yang sifatnya append-only di staging
  SELECT * EXCEPT(rn)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY zone_code 
        ORDER BY created_at DESC
      ) as rn
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
  created_at
FROM deduplicated_zones;


MERGE INTO `{{ params.project_id }}.dev_label.zones` T
  USING temp_zones S
  -- zone_code digunakan sebagai key karena sifatnya UNIQUE dan tidak berubah
  ON T.zone_code = S.zone_code 
  WHEN MATCHED THEN
    UPDATE SET
      T.zone_name = S.zone_name,
      T.city = S.city,
      T.latitude = S.latitude,
      T.longitude = S.longitude,
      T.is_active = S.is_active,
      T.created_at = S.created_at
        
  WHEN NOT MATCHED THEN
    INSERT (
      zone_id, 
      zone_code, 
      zone_name, 
      city, 
      latitude, 
      longitude, 
      is_active, 
      created_at
    )
    VALUES (
      S.zone_id, 
      S.zone_code, 
      S.zone_name, 
      S.city, 
      S.latitude, 
      S.longitude, 
      S.is_active, 
      S.created_at
    );