# Ride-Hailing Taxi Data Platform

## Overview

Dokumen ini berisi contoh desain sederhana namun lengkap untuk pipeline data **ride-hailing taxi** dengan stack berikut:

- **Source system**: PostgreSQL dan MySQL
- **Extract & Load**: Airflow
- **Query engine**: Trino
- **Data warehouse**: BigQuery
- **Transformation**: dbt-bigquery
- **Layering**: Bronze, Silver, Gold

Struktur desain:

```text
PostgreSQL + MySQL
        |
        v
   Airflow EL
        |
        v
 BigQuery Bronze
        |
        v
 dbt-bigquery Silver (Star Schema)
        |
        v
 dbt-bigquery Gold (Data Mart)
        |
        v
      Trino
```

---

## 1. Architecture

### End-to-End Flow

1. Data operasional berasal dari **PostgreSQL** dan **MySQL**.
2. **Airflow** melakukan extract dan load ke **BigQuery Bronze**.
3. **dbt-bigquery** melakukan transformasi dari Bronze ke **Silver**.
4. Pada **Silver**, data dibentuk menjadi **star schema**.
5. Dari Silver, dbt membangun **Gold data mart** untuk kebutuhan domain bisnis.
6. **Trino** dipakai untuk query dan analisis.

### Penilaian ide: 9/10
**Alasan:** arsitekturnya rapi, mudah dijelaskan, dan cukup dekat dengan praktik modern analytics engineering.

---

## 2. Business Scope: Ride-Hailing Taxi

Bisnis yang dimodelkan:

- customer memesan taxi
- driver menerima order
- trip berjalan dan selesai / batal
- pembayaran dilakukan
- promo dapat digunakan
- customer memberi rating
- driver menerima payout

---

## 3. Source Systems

### 3.1 PostgreSQL (`ride_ops_pg`)

Tabel sumber operasional utama:

1. `customers`
2. `drivers`
3. `vehicles`
4. `trips`
5. `trip_status_logs`
6. `payments`
7. `driver_payouts`
8. `ratings`

### 3.2 MySQL (`ride_marketing_mysql`)

Tabel sumber tambahan marketing dan CRM:

1. `promotions`
2. `promo_redemptions`
3. `customer_segments`
4. `campaign_spend`

---

## 4. Bronze Layer

Bronze adalah raw data hasil load dari source ke BigQuery. Isi tabel dijaga sedekat mungkin dengan bentuk sumber.

### Dataset contoh

- `bronze_pg`
- `bronze_mysql`

---

## 4.1 Bronze PostgreSQL Tables

### `bronze_pg.customers_raw`

| Column | Type | Keterangan |
|---|---|---|
| customer_id | STRING | id customer dari source |
| full_name | STRING | nama customer |
| phone_number | STRING | nomor hp |
| email | STRING | email |
| gender | STRING | gender |
| birth_date | DATE | tanggal lahir |
| created_at | TIMESTAMP | waktu daftar |
| updated_at | TIMESTAMP | waktu update |
| _ingested_at | TIMESTAMP | waktu masuk bronze |
| _source_system | STRING | postgres |

### `bronze_pg.drivers_raw`

| Column | Type |
|---|---|
| driver_id | STRING |
| full_name | STRING |
| phone_number | STRING |
| email | STRING |
| license_number | STRING |
| driver_status | STRING |
| join_date | DATE |
| city | STRING |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |
| _ingested_at | TIMESTAMP |
| _source_system | STRING |

### `bronze_pg.vehicles_raw`

| Column | Type |
|---|---|
| vehicle_id | STRING |
| driver_id | STRING |
| plate_number | STRING |
| vehicle_type | STRING |
| brand | STRING |
| model | STRING |
| production_year | INT64 |
| seat_capacity | INT64 |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |
| _ingested_at | TIMESTAMP |
| _source_system | STRING |

### `bronze_pg.trips_raw`

| Column | Type |
|---|---|
| trip_id | STRING |
| customer_id | STRING |
| driver_id | STRING |
| vehicle_id | STRING |
| request_ts | TIMESTAMP |
| pickup_ts | TIMESTAMP |
| dropoff_ts | TIMESTAMP |
| pickup_lat | FLOAT64 |
| pickup_lng | FLOAT64 |
| dropoff_lat | FLOAT64 |
| dropoff_lng | FLOAT64 |
| pickup_area | STRING |
| dropoff_area | STRING |
| city | STRING |
| estimated_distance_km | FLOAT64 |
| actual_distance_km | FLOAT64 |
| estimated_fare | NUMERIC |
| actual_fare | NUMERIC |
| surge_multiplier | FLOAT64 |
| trip_status | STRING |
| cancel_reason | STRING |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |
| _ingested_at | TIMESTAMP |
| _source_system | STRING |

### `bronze_pg.trip_status_logs_raw`

| Column | Type |
|---|---|
| trip_status_log_id | STRING |
| trip_id | STRING |
| status_code | STRING |
| status_ts | TIMESTAMP |
| actor_type | STRING |
| created_at | TIMESTAMP |
| _ingested_at | TIMESTAMP |
| _source_system | STRING |

### `bronze_pg.payments_raw`

| Column | Type |
|---|---|
| payment_id | STRING |
| trip_id | STRING |
| customer_id | STRING |
| payment_method | STRING |
| payment_status | STRING |
| gross_amount | NUMERIC |
| discount_amount | NUMERIC |
| tax_amount | NUMERIC |
| toll_amount | NUMERIC |
| tip_amount | NUMERIC |
| net_amount | NUMERIC |
| paid_ts | TIMESTAMP |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |
| _ingested_at | TIMESTAMP |
| _source_system | STRING |

### `bronze_pg.driver_payouts_raw`

| Column | Type |
|---|---|
| payout_id | STRING |
| driver_id | STRING |
| trip_id | STRING |
| payout_date | DATE |
| base_earning | NUMERIC |
| incentive_amount | NUMERIC |
| bonus_amount | NUMERIC |
| deduction_amount | NUMERIC |
| final_payout_amount | NUMERIC |
| payout_status | STRING |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |
| _ingested_at | TIMESTAMP |
| _source_system | STRING |

### `bronze_pg.ratings_raw`

| Column | Type |
|---|---|
| rating_id | STRING |
| trip_id | STRING |
| customer_id | STRING |
| driver_id | STRING |
| rating_score | INT64 |
| review_text | STRING |
| rated_ts | TIMESTAMP |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |
| _ingested_at | TIMESTAMP |
| _source_system | STRING |

---

## 4.2 Bronze MySQL Tables

### `bronze_mysql.promotions_raw`

| Column | Type |
|---|---|
| promo_id | STRING |
| promo_code | STRING |
| promo_name | STRING |
| promo_type | STRING |
| discount_type | STRING |
| discount_value | NUMERIC |
| start_date | DATE |
| end_date | DATE |
| budget_amount | NUMERIC |
| promo_status | STRING |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |
| _ingested_at | TIMESTAMP |
| _source_system | STRING |

### `bronze_mysql.promo_redemptions_raw`

| Column | Type |
|---|---|
| redemption_id | STRING |
| promo_id | STRING |
| customer_id | STRING |
| trip_id | STRING |
| redeemed_ts | TIMESTAMP |
| discount_amount | NUMERIC |
| redemption_status | STRING |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |
| _ingested_at | TIMESTAMP |
| _source_system | STRING |

### `bronze_mysql.customer_segments_raw`

| Column | Type |
|---|---|
| customer_id | STRING |
| segment_name | STRING |
| segment_score | FLOAT64 |
| snapshot_date | DATE |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |
| _ingested_at | TIMESTAMP |
| _source_system | STRING |

### `bronze_mysql.campaign_spend_raw`

| Column | Type |
|---|---|
| campaign_id | STRING |
| campaign_name | STRING |
| channel_name | STRING |
| spend_date | DATE |
| spend_amount | NUMERIC |
| impressions | INT64 |
| clicks | INT64 |
| installs | INT64 |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |
| _ingested_at | TIMESTAMP |
| _source_system | STRING |

---

## 5. Bronze Refresh Strategy

### Incremental

Cocok untuk tabel transaksi atau master yang terus bertambah dan berubah:

- `customers_raw`
- `drivers_raw`
- `vehicles_raw`
- `trips_raw`
- `trip_status_logs_raw`
- `payments_raw`
- `driver_payouts_raw`
- `ratings_raw`
- `promotions_raw`
- `promo_redemptions_raw`
- `campaign_spend_raw`

**Dasar incremental:** `updated_at` atau `created_at`

### Snapshot

- `customer_segments_raw`

Dipakai karena segment customer bisa berubah dari waktu ke waktu dan jejak historinya berguna untuk analisis.

### Full Refresh

Boleh dipakai untuk tabel kecil yang sangat stabil, misalnya `promotions_raw` jika volume sangat kecil. Meski begitu, incremental tetap lebih efisien.

### Penilaian ide: 8.8/10
**Alasan:** cukup realistis untuk implementasi awal dan tetap efisien di BigQuery.

---

## 6. Silver Layer: Star Schema

Pada Silver, data dibersihkan dan dibentuk menjadi **fact** serta **dimension**.

### Dataset contoh

- `silver_core`

---

## 6.1 Grain Silver

### Fact tables

#### `fct_trip`
**Grain:** 1 baris = 1 trip

#### `fct_payment`
**Grain:** 1 baris = 1 payment transaction untuk 1 trip

#### `fct_driver_payout`
**Grain:** 1 baris = 1 payout driver untuk 1 trip

#### `fct_rating`
**Grain:** 1 baris = 1 rating pada 1 trip

#### `fct_promo_redemption`
**Grain:** 1 baris = 1 redemption promo pada 1 trip/customer

### Dimension tables

#### `dim_date`
**Grain:** 1 baris = 1 tanggal

#### `dim_time`
**Grain:** 1 baris = 1 jam

#### `dim_customer`
**Grain:** 1 baris = 1 customer versi aktif atau historis sesuai SCD

#### `dim_driver`
**Grain:** 1 baris = 1 driver versi aktif atau historis sesuai SCD

#### `dim_vehicle`
**Grain:** 1 baris = 1 vehicle

#### `dim_location`
**Grain:** 1 baris = 1 area standar

#### `dim_payment_method`
**Grain:** 1 baris = 1 metode bayar

#### `dim_trip_status`
**Grain:** 1 baris = 1 jenis status trip

#### `dim_promo`
**Grain:** 1 baris = 1 promo

#### `dim_customer_segment`
**Grain:** 1 baris = 1 segment customer pada satu snapshot period

### Penilaian grain: 9.5/10
**Alasan:** jelas, aman dari double counting, dan enak diturunkan ke data mart.

---

## 6.2 Silver Dimension Tables

### `silver_core.dim_date`

| Column | Type |
|---|---|
| date_key | INT64 |
| full_date | DATE |
| day_of_month | INT64 |
| day_name | STRING |
| week_of_year | INT64 |
| month_num | INT64 |
| month_name | STRING |
| quarter_num | INT64 |
| year_num | INT64 |
| is_weekend | BOOL |

### `silver_core.dim_time`

| Column | Type |
|---|---|
| time_key | INT64 |
| hour_num | INT64 |
| time_bucket | STRING |

### `silver_core.dim_customer`

| Column | Type |
|---|---|
| customer_key | INT64 |
| customer_id | STRING |
| full_name | STRING |
| gender | STRING |
| age_group | STRING |
| signup_date | DATE |
| email | STRING |
| phone_number | STRING |
| current_segment | STRING |
| city | STRING |
| valid_from | TIMESTAMP |
| valid_to | TIMESTAMP |
| is_current | BOOL |

### `silver_core.dim_driver`

| Column | Type |
|---|---|
| driver_key | INT64 |
| driver_id | STRING |
| full_name | STRING |
| driver_status | STRING |
| join_date | DATE |
| city | STRING |
| license_number | STRING |
| valid_from | TIMESTAMP |
| valid_to | TIMESTAMP |
| is_current | BOOL |

### `silver_core.dim_vehicle`

| Column | Type |
|---|---|
| vehicle_key | INT64 |
| vehicle_id | STRING |
| driver_id | STRING |
| plate_number | STRING |
| vehicle_type | STRING |
| brand | STRING |
| model | STRING |
| production_year | INT64 |
| seat_capacity | INT64 |

### `silver_core.dim_location`

| Column | Type |
|---|---|
| location_key | INT64 |
| city | STRING |
| area_name | STRING |
| latitude_center | FLOAT64 |
| longitude_center | FLOAT64 |

### `silver_core.dim_payment_method`

| Column | Type |
|---|---|
| payment_method_key | INT64 |
| payment_method_name | STRING |

### `silver_core.dim_trip_status`

| Column | Type |
|---|---|
| trip_status_key | INT64 |
| trip_status_name | STRING |

### `silver_core.dim_promo`

| Column | Type |
|---|---|
| promo_key | INT64 |
| promo_id | STRING |
| promo_code | STRING |
| promo_name | STRING |
| promo_type | STRING |
| discount_type | STRING |
| discount_value | NUMERIC |
| start_date | DATE |
| end_date | DATE |
| budget_amount | NUMERIC |
| promo_status | STRING |

---

## 6.3 Silver Fact Tables

### `silver_core.fct_trip`
**Grain: 1 row = 1 trip**

| Column | Type |
|---|---|
| trip_key | INT64 |
| trip_id | STRING |
| request_date_key | INT64 |
| request_time_key | INT64 |
| pickup_date_key | INT64 |
| dropoff_date_key | INT64 |
| customer_key | INT64 |
| driver_key | INT64 |
| vehicle_key | INT64 |
| pickup_location_key | INT64 |
| dropoff_location_key | INT64 |
| trip_status_key | INT64 |
| estimated_distance_km | FLOAT64 |
| actual_distance_km | FLOAT64 |
| estimated_fare | NUMERIC |
| actual_fare | NUMERIC |
| surge_multiplier | FLOAT64 |
| trip_duration_minute | INT64 |
| waiting_time_minute | INT64 |
| is_completed | BOOL |
| is_cancelled | BOOL |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |

### `silver_core.fct_payment`
**Grain: 1 row = 1 payment transaction**

| Column | Type |
|---|---|
| payment_key | INT64 |
| payment_id | STRING |
| trip_key | INT64 |
| customer_key | INT64 |
| payment_date_key | INT64 |
| payment_method_key | INT64 |
| gross_amount | NUMERIC |
| discount_amount | NUMERIC |
| tax_amount | NUMERIC |
| toll_amount | NUMERIC |
| tip_amount | NUMERIC |
| net_amount | NUMERIC |
| payment_status | STRING |
| paid_ts | TIMESTAMP |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |

### `silver_core.fct_driver_payout`
**Grain: 1 row = 1 driver payout for 1 trip**

| Column | Type |
|---|---|
| payout_key | INT64 |
| payout_id | STRING |
| trip_key | INT64 |
| driver_key | INT64 |
| payout_date_key | INT64 |
| base_earning | NUMERIC |
| incentive_amount | NUMERIC |
| bonus_amount | NUMERIC |
| deduction_amount | NUMERIC |
| final_payout_amount | NUMERIC |
| payout_status | STRING |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |

### `silver_core.fct_rating`
**Grain: 1 row = 1 rating**

| Column | Type |
|---|---|
| rating_key | INT64 |
| rating_id | STRING |
| trip_key | INT64 |
| customer_key | INT64 |
| driver_key | INT64 |
| rating_date_key | INT64 |
| rating_score | INT64 |
| has_review_text | BOOL |
| rated_ts | TIMESTAMP |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |

### `silver_core.fct_promo_redemption`
**Grain: 1 row = 1 promo redemption**

| Column | Type |
|---|---|
| promo_redemption_key | INT64 |
| redemption_id | STRING |
| promo_key | INT64 |
| trip_key | INT64 |
| customer_key | INT64 |
| redemption_date_key | INT64 |
| discount_amount | NUMERIC |
| redemption_status | STRING |
| redeemed_ts | TIMESTAMP |
| created_at | TIMESTAMP |
| updated_at | TIMESTAMP |

### Penilaian star schema: 9/10
**Alasan:** sederhana, reusable, dan kuat untuk kebutuhan analitik lintas fungsi.

---

## 7. Gold Layer: Data Mart

Gold adalah layer siap pakai untuk dashboard, reporting, dan query bisnis.

### Dataset contoh

- `gold_operations`
- `gold_finance`
- `gold_marketing`
- `gold_driver`

---

## 7.1 Operations Data Mart

### `gold_operations.dm_trip_daily_city`
**Grain: 1 row = 1 tanggal per city**

| Column | Type |
|---|---|
| date_key | INT64 |
| city | STRING |
| total_trip_requested | INT64 |
| total_trip_completed | INT64 |
| total_trip_cancelled | INT64 |
| completion_rate | FLOAT64 |
| cancellation_rate | FLOAT64 |
| avg_estimated_distance_km | FLOAT64 |
| avg_actual_distance_km | FLOAT64 |
| avg_trip_duration_minute | FLOAT64 |
| avg_waiting_time_minute | FLOAT64 |
| avg_actual_fare | NUMERIC |

### `gold_operations.dm_trip_hourly_city`
**Grain: 1 row = 1 tanggal + jam + city**

| Column | Type |
|---|---|
| date_key | INT64 |
| time_key | INT64 |
| city | STRING |
| total_trip_requested | INT64 |
| total_trip_completed | INT64 |
| total_trip_cancelled | INT64 |
| avg_surge_multiplier | FLOAT64 |

---

## 7.2 Finance Data Mart

### `gold_finance.dm_finance_daily_city`
**Grain: 1 row = 1 tanggal per city**

| Column | Type |
|---|---|
| date_key | INT64 |
| city | STRING |
| total_gross_amount | NUMERIC |
| total_discount_amount | NUMERIC |
| total_tax_amount | NUMERIC |
| total_toll_amount | NUMERIC |
| total_tip_amount | NUMERIC |
| total_net_amount | NUMERIC |
| total_driver_payout | NUMERIC |
| estimated_platform_revenue | NUMERIC |
| completed_trip_count | INT64 |
| avg_revenue_per_trip | NUMERIC |

### `gold_finance.dm_payment_method_daily`
**Grain: 1 row = 1 tanggal per payment method**

| Column | Type |
|---|---|
| date_key | INT64 |
| payment_method_name | STRING |
| payment_count | INT64 |
| total_net_amount | NUMERIC |
| avg_net_amount | NUMERIC |
| success_payment_count | INT64 |
| failed_payment_count | INT64 |

---

## 7.3 Marketing Data Mart

### `gold_marketing.dm_promo_daily`
**Grain: 1 row = 1 tanggal per promo**

| Column | Type |
|---|---|
| date_key | INT64 |
| promo_code | STRING |
| promo_name | STRING |
| total_redemption | INT64 |
| total_discount_amount | NUMERIC |
| unique_customer_count | INT64 |
| total_trip_count | INT64 |
| completed_trip_count | INT64 |

### `gold_marketing.dm_campaign_daily_channel`
**Grain: 1 row = 1 tanggal per channel**

| Column | Type |
|---|---|
| date_key | INT64 |
| channel_name | STRING |
| campaign_name | STRING |
| spend_amount | NUMERIC |
| impressions | INT64 |
| clicks | INT64 |
| installs | INT64 |
| ctr | FLOAT64 |
| cpi | NUMERIC |

### `gold_marketing.dm_customer_segment_daily`
**Grain: 1 row = 1 snapshot_date per segment**

| Column | Type |
|---|---|
| snapshot_date | DATE |
| segment_name | STRING |
| customer_count | INT64 |
| avg_segment_score | FLOAT64 |

---

## 7.4 Driver Data Mart

### `gold_driver.dm_driver_daily_performance`
**Grain: 1 row = 1 tanggal per driver**

| Column | Type |
|---|---|
| date_key | INT64 |
| driver_id | STRING |
| city | STRING |
| total_trip_assigned | INT64 |
| total_trip_completed | INT64 |
| total_trip_cancelled | INT64 |
| completion_rate | FLOAT64 |
| avg_rating_score | FLOAT64 |
| total_actual_distance_km | FLOAT64 |
| total_working_revenue | NUMERIC |
| total_payout_amount | NUMERIC |

### `gold_driver.dm_driver_monthly_summary`
**Grain: 1 row = 1 bulan per driver**

| Column | Type |
|---|---|
| month_key | STRING |
| driver_id | STRING |
| city | STRING |
| total_trip_completed | INT64 |
| avg_rating_score | FLOAT64 |
| total_payout_amount | NUMERIC |
| total_bonus_amount | NUMERIC |
| total_incentive_amount | NUMERIC |

---

## 8. Refresh Strategy: Silver and Gold

## 8.1 Silver Strategy

### Full Refresh

- `dim_date`
- `dim_time`
- `dim_payment_method`
- `dim_trip_status`

Dipakai karena kecil dan stabil.

### Incremental

- `dim_vehicle`
- `dim_location`
- `dim_promo`
- `fct_trip`
- `fct_payment`
- `fct_driver_payout`
- `fct_rating`
- `fct_promo_redemption`

### Snapshot / SCD Type 2

- `dim_customer`
- `dim_driver`

Alasannya, data ini berpeluang berubah dan jejak perubahannya berguna untuk analisis historis.

## 8.2 Gold Strategy

### Incremental Partition Overwrite

Sangat cocok untuk data mart berbasis tanggal:

- `dm_trip_daily_city`
- `dm_trip_hourly_city`
- `dm_finance_daily_city`
- `dm_payment_method_daily`
- `dm_promo_daily`
- `dm_campaign_daily_channel`
- `dm_driver_daily_performance`

### Incremental Overwrite per Month

- `dm_driver_monthly_summary`

### Full Refresh atau Incremental Append

- `dm_customer_segment_daily`

Tergantung bagaimana snapshot segment disimpan.

### Penilaian strategi materialization: 9.2/10
**Alasan:** efisien, aman untuk late-arriving data, dan sesuai dengan BigQuery.

---

## 9. Recommended dbt Materialization

## 9.1 Silver

### Full Refresh / Table
- `dim_date`
- `dim_time`
- `dim_payment_method`
- `dim_trip_status`

### Incremental
- `dim_vehicle`
- `dim_location`
- `dim_promo`
- `fct_trip`
- `fct_payment`
- `fct_driver_payout`
- `fct_rating`
- `fct_promo_redemption`

### Snapshot
- `dim_customer`
- `dim_driver`

## 9.2 Gold

Hampir semua data mart cocok memakai `incremental` dengan partition berbasis `date_key`.

---

## 10. Example dbt Project Structure

```text
models/
  bronze/
    sources.yml

  silver/
    dim/
      dim_date.sql
      dim_time.sql
      dim_customer.sql
      dim_driver.sql
      dim_vehicle.sql
      dim_location.sql
      dim_payment_method.sql
      dim_trip_status.sql
      dim_promo.sql
    fact/
      fct_trip.sql
      fct_payment.sql
      fct_driver_payout.sql
      fct_rating.sql
      fct_promo_redemption.sql

  gold/
    operations/
      dm_trip_daily_city.sql
      dm_trip_hourly_city.sql
    finance/
      dm_finance_daily_city.sql
      dm_payment_method_daily.sql
    marketing/
      dm_promo_daily.sql
      dm_campaign_daily_channel.sql
      dm_customer_segment_daily.sql
    driver/
      dm_driver_daily_performance.sql
      dm_driver_monthly_summary.sql

snapshots/
  snapshot_dim_customer.sql
  snapshot_dim_driver.sql
```

---

## 11. Example Airflow DAG Sequence

Urutan task yang wajar:

1. extract PostgreSQL tables
2. load ke BigQuery `bronze_pg`
3. extract MySQL tables
4. load ke BigQuery `bronze_mysql`
5. jalankan dbt snapshot
6. jalankan dbt models silver
7. jalankan dbt models gold
8. jalankan data quality check
9. logging dan monitoring

### Jadwal yang masuk akal

- Bronze load: setiap 15 menit atau 1 jam
- Silver: setelah Bronze selesai
- Gold: setelah Silver selesai

---

## 12. Example Trino Queries

### Operations

```sql
SELECT
    date_key,
    city,
    total_trip_requested,
    total_trip_completed,
    total_trip_cancelled,
    completion_rate,
    cancellation_rate
FROM bigquery.gold_operations.dm_trip_daily_city
WHERE date_key BETWEEN 20260101 AND 20260131
ORDER BY date_key, city;
```

### Finance

```sql
SELECT
    date_key,
    city,
    total_net_amount,
    total_driver_payout,
    estimated_platform_revenue
FROM bigquery.gold_finance.dm_finance_daily_city
WHERE date_key = 20260115;
```

### Marketing

```sql
SELECT
    date_key,
    promo_code,
    total_redemption,
    total_discount_amount,
    unique_customer_count
FROM bigquery.gold_marketing.dm_promo_daily
WHERE date_key BETWEEN 20260101 AND 20260131
ORDER BY total_redemption DESC;
```

### Driver

```sql
SELECT
    date_key,
    driver_id,
    city,
    total_trip_completed,
    avg_rating_score,
    total_payout_amount
FROM bigquery.gold_driver.dm_driver_daily_performance
WHERE date_key = 20260115
ORDER BY total_trip_completed DESC;
```

---

## 13. Why Bronze, Silver, Gold Split Matters

### Bronze
Dipakai untuk menyimpan raw source. Cocok untuk audit, debugging, dan reprocessing.

### Silver
Dipakai untuk model analitik inti. Pada layer ini fact dan dimension disusun agar konsisten dan reusable.

### Gold
Dipakai untuk kebutuhan dashboard, laporan, dan KPI per domain bisnis.

### Penilaian pembagian layer: 10/10
**Alasan:** paling mudah dijelaskan, paling rapi untuk pengembangan, dan aman untuk scale bertahap.

---

## 14. Ringkasan Tabel per Layer

### Bronze
- `customers_raw`
- `drivers_raw`
- `vehicles_raw`
- `trips_raw`
- `trip_status_logs_raw`
- `payments_raw`
- `driver_payouts_raw`
- `ratings_raw`
- `promotions_raw`
- `promo_redemptions_raw`
- `customer_segments_raw`
- `campaign_spend_raw`

### Silver
- `dim_date`
- `dim_time`
- `dim_customer`
- `dim_driver`
- `dim_vehicle`
- `dim_location`
- `dim_payment_method`
- `dim_trip_status`
- `dim_promo`
- `fct_trip`
- `fct_payment`
- `fct_driver_payout`
- `fct_rating`
- `fct_promo_redemption`

### Gold
- `dm_trip_daily_city`
- `dm_trip_hourly_city`
- `dm_finance_daily_city`
- `dm_payment_method_daily`
- `dm_promo_daily`
- `dm_campaign_daily_channel`
- `dm_customer_segment_daily`
- `dm_driver_daily_performance`
- `dm_driver_monthly_summary`

---

## 15. Final Evaluation

### Nilai keseluruhan rancangan: 9.3/10
**Alasan singkat:**
- pemisahan layer jelas
- grain fact dan dimension kuat
- star schema di silver cocok untuk reuse
- data mart di gold sudah terarah per domain
- strategi incremental, snapshot, dan full refresh cukup masuk akal

Nilainya belum 10 karena pada implementasi produksi besar masih bisa ditambah:

- CDC delete handling
- monitoring freshness dan SLA
- dbt test yang lebih ketat
- lineage dan observability yang lebih lengkap

---

## 16. Key Recommendation

Kalau desain ini mau dipakai untuk tugas, presentasi, atau mini project portfolio, bagian yang paling kuat untuk ditekankan adalah:

1. **bronze menyimpan raw data**
2. **silver menjadi star schema reusable**
3. **gold menjadi data mart per domain**
4. **customer dan driver cocok memakai snapshot / SCD Type 2**
5. **fact transactional lebih cocok incremental**

Itu fondasi yang paling sering dicari saat pembahasan data warehouse modern.

