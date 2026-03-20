# dbt-BigQuery Project: Ride-Hailing Taxi

Proyek dbt untuk transformasi data ride-hailing dari **Bronze → Silver → Gold** di Google BigQuery.

---

## Struktur Project

```
dbt/
├── dbt_profiles/
│   └── profiles.yml              ← koneksi BigQuery (dev & prod)
└── dbt_project/
    ├── dbt_project.yml           ← konfigurasi utama, materialization, schema
    ├── packages.yml              ← dbt_utils, dbt_expectations, audit_helper
    ├── .sqlfluff                 ← linting SQL
    ├── .dbtignore
    │
    ├── macros/
    │   ├── helpers.sql           ← surrogate_key, date_to_key, safe_div, dll.
    │   ├── custom_tests.sql      ← generic tests tambahan (not_negative, dll.)
    │   └── generate_schema_name.sql  ← override dataset naming per env
    │
    ├── models/
    │   ├── bronze/
    │   │   └── sources.yml       ← declare raw tables dari Airflow load
    │   │
    │   ├── silver/
    │   │   ├── dim/
    │   │   │   ├── dim_date.sql
    │   │   │   ├── dim_time.sql
    │   │   │   ├── dim_customer.sql      ← SCD Type 2 (dari snapshot)
    │   │   │   ├── dim_driver.sql        ← SCD Type 2 (dari snapshot)
    │   │   │   ├── dim_vehicle.sql
    │   │   │   ├── dim_location.sql
    │   │   │   ├── dim_payment_method.sql
    │   │   │   ├── dim_trip_status.sql
    │   │   │   ├── dim_promo.sql
    │   │   │   └── schema.yml
    │   │   └── fact/
    │   │       ├── fct_trip.sql
    │   │       ├── fct_payment.sql
    │   │       ├── fct_driver_payout.sql
    │   │       ├── fct_rating.sql
    │   │       ├── fct_promo_redemption.sql
    │   │       └── schema.yml
    │   │
    │   └── gold/
    │       ├── operations/
    │       │   ├── dm_trip_daily_city.sql
    │       │   ├── dm_trip_hourly_city.sql
    │       │   └── schema.yml
    │       ├── finance/
    │       │   ├── dm_finance_daily_city.sql
    │       │   ├── dm_payment_method_daily.sql
    │       │   └── schema.yml
    │       ├── marketing/
    │       │   ├── dm_promo_daily.sql
    │       │   ├── dm_campaign_daily_channel.sql
    │       │   ├── dm_customer_segment_daily.sql
    │       │   └── schema.yml
    │       └── driver/
    │           ├── dm_driver_daily_performance.sql
    │           ├── dm_driver_monthly_summary.sql
    │           └── schema.yml
    │
    ├── snapshots/
    │   ├── snapshot_dim_customer.sql     ← SCD Type 2 untuk customer
    │   └── snapshot_dim_driver.sql       ← SCD Type 2 untuk driver
    │
    ├── seeds/
    │   ├── national_holidays.csv
    │   ├── city_master.csv
    │   ├── cancel_reason_lookup.csv
    │   └── schema.yml
    │
    └── tests/
        ├── generic/
        │   ├── not_negative.sql
        │   ├── accepted_range.sql
        │   └── mutually_exclusive_flags.sql
        └── singular/
            ├── assert_no_orphan_payments.sql
            ├── assert_completed_trip_has_one_payment.sql
            ├── assert_no_orphan_payouts.sql
            ├── assert_payment_amount_logic.sql
            ├── assert_platform_revenue_sanity.sql
            ├── assert_trip_dates_in_range.sql
            ├── assert_gold_ops_coverage.sql
            ├── assert_scd2_no_overlap_customer.sql
            └── assert_completion_rate_by_city.sql
```

---

## Dataset BigQuery (per Environment)

| Layer | Dev Dataset | Prod Dataset |
|---|---|---|
| Bronze PG | `dev_bronze_pg` | `bronze_pg` |
| Bronze MySQL | `dev_bronze_mysql` | `bronze_mysql` |
| Silver | `dev_silver_core` | `silver_core` |
| Gold – Operations | `dev_gold_operations` | `gold_operations` |
| Gold – Finance | `dev_gold_finance` | `gold_finance` |
| Gold – Marketing | `dev_gold_marketing` | `gold_marketing` |
| Gold – Driver | `dev_gold_driver` | `gold_driver` |

> Di dev, prefix `dev_` ditambahkan otomatis via `generate_schema_name.sql`.
> Set `DBT_DEV_PREFIX=namadev` untuk isolasi per developer.

---

## Quick Start

### 1. Persiapan

```bash
# Install dbt-bigquery
pip install dbt-bigquery

# Set profiles directory
export DBT_PROFILES_DIR=/path/to/dbt/dbt_profiles

# (Opsional) Autentikasi GCP
gcloud auth application-default login
```

### 2. Install packages

```bash
cd dbt_project
dbt deps
```

### 3. Seed static data

```bash
dbt seed
```

### 4. Jalankan snapshot (SCD Type 2)

```bash
dbt snapshot
```

### 5. Build Silver + Gold

```bash
# Full build semua model
dbt run

# Atau per layer:
dbt run --select tag:silver
dbt run --select tag:gold

# Atau per domain gold:
dbt run --select tag:operations
dbt run --select tag:finance
dbt run --select tag:marketing
dbt run --select tag:driver
```

### 6. Test

```bash
# Semua test
dbt test

# Test per layer
dbt test --select tag:silver
dbt test --select tag:gold

# Singular tests saja
dbt test --select test_type:singular

# Generic tests saja
dbt test --select test_type:generic
```

### 7. Generate docs

```bash
dbt docs generate
dbt docs serve
```

---

## Urutan Run yang Benar

Sesuai lineage dbt:

```
dbt seed
    ↓
dbt snapshot           # SCD2: snapshot_dim_customer, snapshot_dim_driver
    ↓
dbt run --select tag:dim    # dim_date, dim_time, dim_customer, ...
    ↓
dbt run --select tag:fact   # fct_trip, fct_payment, ...
    ↓
dbt run --select tag:gold   # dm_trip_daily_city, dm_finance_daily_city, ...
    ↓
dbt test
```

> dbt mengelola dependency ini secara otomatis via `ref()`.
> Cukup jalankan `dbt run` dan dbt akan menemukan urutan yang benar.

---

## Materialization per Layer

| Model | Strategi | Keterangan |
|---|---|---|
| `dim_date`, `dim_time`, `dim_payment_method`, `dim_trip_status` | `table` | Kecil, stabil, full refresh |
| `dim_customer`, `dim_driver` | `incremental` + snapshot | Diisi dari SCD2 snapshot |
| `dim_vehicle`, `dim_location`, `dim_promo` | `incremental` merge | Update by unique_key |
| Semua `fct_*` | `incremental` merge | Partisi harian by created_at |
| `dm_*` (kecuali segment) | `incremental` insert_overwrite | Overwrite partisi date_key |
| `dm_customer_segment_daily` | `incremental` merge | Unique per snapshot_date + segment |
| `dm_driver_monthly_summary` | `incremental` merge | Unique per month_key + driver_id |

---

## Custom Macros

| Macro | Kegunaan |
|---|---|
| `surrogate_key([cols])` | Hash surrogate key |
| `date_to_key('col')` | Timestamp → INT64 YYYYMMDD (WIB) |
| `hour_key('col')` | Timestamp → jam WIB (0-23) |
| `safe_div(num, den)` | Pembagian aman (tidak error div/0) |
| `round_idr('col')` | Bulatkan ke satuan IDR |
| `incremental_filter('col')` | WHERE clause incremental otomatis |
| `classify_age_group('col')` | birth_date → bucket usia |
| `classify_time_bucket('col')` | jam → bucket peak/off-peak |

---

## Custom Generic Tests

| Test | Validasi |
|---|---|
| `not_negative` | Nilai ≥ 0 |
| `timestamp_order` | col_a ≤ col_b |
| `completed_trip_has_timestamps` | Trip completed wajib punya timestamps |
| `date_key_format` | INT64 YYYYMMDD valid |
| `net_amount_consistency` | net = gross − disc + tax + toll + tip |
| `is_current_unique_per_id` | Max 1 `is_current=TRUE` per natural key |
| `rating_score_range` | Score antara 1–5 |
| `referential_integrity` | FK ada di parent table |
| `accepted_range` | Nilai dalam [min, max] |
| `mutually_exclusive_flags` | Dua flag tidak boleh TRUE bersamaan |

---

## Airflow DAG Integration

Urutan task yang disarankan di Airflow setelah EL selesai:

```
extract_pg → load_bronze_pg ─┐
                              ├─→ dbt_snapshot → dbt_run_silver → dbt_run_gold → dbt_test
extract_mysql → load_bronze_mysql ─┘
```

Gunakan `DbtRunOperator` atau `BashOperator` dengan env var:

```bash
DBT_PROFILES_DIR=/opt/airflow/dbt_profiles \
DBT_DEV_PREFIX=airflow \
dbt run --target prod --select tag:silver tag:gold
```
