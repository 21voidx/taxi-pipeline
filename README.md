# 🚕 Taxi Ride Hailing

A **end-to-end data engineering playground** that simulates a ride-hailing platform — from synthetic data generation all the way to analytics-ready Gold marts in BigQuery.

The project is intentionally self-contained: spin up the Docker Compose stacks, register the Kafka Connect connectors, trigger the Airflow DAGs, and you have a fully running modern data stack on VPS.

---

## 📐 Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          Data Sources                                        │
│  PostgreSQL (ride_ops_pg)               MongoDB (ride_ops_mg)                │
│  ├── drivers        ◄─── batch          ├── ride_events     ◄─── CDC         │
│  ├── passengers     ◄─── batch          └── driver_location_stream ◄─ CDC    │
│  ├── vehicle_types  ◄─── batch                                               │
│  ├── zones          ◄─── batch                                               │
│  └── rides          ◄─── CDC                                                 │
└───────────┬────────────────────────────────────┬─────────────────────────────┘
            │ batch (daily)                      │ CDC (real-time)
            ▼                                    ▼
  ┌──────────────────┐              ┌────────────────────────┐
  │  Trino           │              │  Debezium → Kafka      │
  │  (PG → BQ CTAS)  │              │  (KRaft + Schema Reg.) │
  └────────┬─────────┘              │  → Kafka Connect       │
           │                        │  → GCS Sink (Parquet)  │
           │                        └────────────┬───────────┘
           │                                     │
           │           ┌─────────────────────────┘
           │           │
    ┌──────▼───────┐  ┌▼─────────────────┐    ┌──────────────────┐
    │ Airflow DAG  │  │  Airflow DAG     │    │  Airflow DAG     │
    │ Postgres →   │  │  GCS → BQ        │    │  dbt Pipeline    │
    │ BQ via Trino │  │  (hourly CDC)    │    │                  │
    └──────┬───────┘  └────────┬─────────┘    └────────┬─────────┘
           │                   │                        │
           └───────────────────▼────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │     BigQuery        │
                    │  ┌───────────────┐  │
                    │  │    Bronze     │  │  Raw / source views
                    │  ├───────────────┤  │
                    │  │    Silver     │  │  Dims + Facts (star schema)
                    │  ├───────────────┤  │
                    │  │     Gold      │  │  Analytics marts
                    │  └───────────────┘  │
                    └─────────────────────┘
```

---

## 🗂️ Project Structure

```
dummy-taxi-pipeline/
├── config/                        # Airflow configuration (airflow.cfg)
├── connectors/                    # Kafka Connect connector configs
│   ├── debezium-mongodb-source.json
│   ├── debezium-postgres.json
│   ├── gcs-sink.json
│   └── minio-sink.json            # Alternative: MinIO instead of GCS
├── credentials/
│   └── service-account.json       # GCP service account (not committed)
├── dags/
│   ├── helpers/                   # Reusable Trino & BQ task-group utilities
│   ├── gcs_to_bigquery_cdc.py     # Hourly CDC loader: GCS → BigQuery
│   ├── postgres_to_bq_trino_multi_table_V3_with_label.py  # Batch ingest
│   ├── dbt_pipeline.py            # dbt run orchestration
│   └── sql/merge_data.sql
├── data-generator/
│   ├── generator/
│   │   ├── generator_v3.py        # Simulation engine (Python + Faker)
│   │   ├── Dockerfile
│   │   └── requirements-generator.txt
│   ├── mongodb/init_mongodb.js    # MongoDB replica set & schema init
│   └── postgres/init_postgres.sql # PostgreSQL schema & seed data
├── dbt/
│   ├── dbt_project/               # dbt project: ride_hailing
│   │   ├── models/
│   │   │   ├── bronze/            # Source views
│   │   │   ├── silver/
│   │   │   │   ├── dimensions/    # dim_drivers, dim_passengers, dim_zones…
│   │   │   │   └── facts/         # fct_rides, fct_ride_events
│   │   │   └── gold/
│   │   │       ├── mart_driver/
│   │   │       ├── mart_finance/
│   │   │       └── mart_operations/
│   │   ├── snapshots/             # SCD Type 2 for drivers & passengers
│   │   ├── macros/
│   │   └── tests/
│   └── docker/Dockerfile
├── kafka-connect/Dockerfile       # Custom Kafka Connect image with plugins
├── trino/
│   ├── catalog/                   # bigquery, postgresql, mysql, mongodb
│   └── etc/                       # Trino config & JVM settings
├── docker-compose-celery.yaml     # Main stack: Airflow + Trino + databases
├── docker-compose-cdc.yaml        # CDC stack: Kafka + Debezium + connectors
├── docker-compose-generator.yaml  # Data generator service
├── Dockerfile                     # Custom Airflow image
├── requirements.txt               # Airflow provider dependencies
└── .env                           # Environment variables (see setup)
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Data Generation | Python · Faker (`id_ID`) · psycopg2 · pymongo |
| Operational DB | PostgreSQL 14 · MongoDB (replica set) · MySQL 8 |
| CDC / Streaming | Apache Kafka (KRaft, Confluent 7.6) · Debezium · Schema Registry (Avro) |
| Object Storage | Google Cloud Storage (or MinIO for local) |
| Orchestration | Apache Airflow 3.x · CeleryExecutor · Redis |
| Query Federation | Trino (catalogs: PostgreSQL · BigQuery · MySQL · MongoDB) |
| Data Warehouse | Google BigQuery |
| Transformation | dbt (BigQuery adapter) |
| Containerisation | Docker · Docker Compose |

---

## ⚙️ Prerequisites

- Docker & Docker Compose v2
- GCP project with BigQuery and GCS enabled
- A GCP service account JSON key with roles:
  - `BigQuery Data Editor`
  - `BigQuery Job User`
  - `Storage Object Admin`
- Python 3.10+ (for local dbt runs, optional)

---

## 🚀 Quick Start

### 1. Clone & configure environment

```bash
git clone https://github.com/<your-org>/dummy-taxi-pipeline.git
cd dummy-taxi-pipeline

cp .env.example .env
# Edit .env — fill in GCP_PROJECT_ID, GCS_BUCKET, Fernet/secret keys, etc.
```

Place your GCP service account key at:

```
credentials/service-account.json
```

Generate required Airflow secrets if you haven't already:

```bash
# Fernet key
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Secret key
python3 -c "import secrets; print(secrets.token_hex(32))"
```

### 2. Start the main stack (Airflow + Trino + databases)

```bash
docker compose -f docker-compose-celery.yaml up -d
```

Wait for all services to be healthy, then open the Airflow UI at `http://localhost:8080`.

### 3. Start the CDC stack (Kafka + Debezium + Kafka Connect)

```bash
docker compose -f docker-compose-cdc.yaml up -d
```

Once Kafka Connect is ready, register the connectors:

```bash
# PostgreSQL CDC source
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-postgres.json

# MongoDB CDC source
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-mongodb-source.json

# GCS sink (writes Parquet, partitioned hourly)
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/gcs-sink.json
```

### 4. Start the data generator

```bash
docker compose -f docker-compose-generator.yaml up -d
```

The generator seeds the databases with **150 drivers** and **600 passengers**, then continuously simulates ride activity at **60× real-time speed** (configurable via `SIM_SPEED`).

### 5. Trigger the Airflow DAGs

| DAG | Schedule | Description |
|---|---|---|
| `postgres_to_bq_trino_multi_table_V3_with_label` | `@daily` | Batch ingest `drivers`, `passengers`, `vehicle_types`, `zones` → BigQuery Bronze via Trino |
| `gcs_to_bigquery_cdc_hourly` | `0 * * * *` | Load hourly CDC Parquet files from GCS → BigQuery `raw_cdc` |
| `dbt_pipeline` | After ingest | Run dbt models: Bronze → Silver → Gold |

---

## 📊 Data Model (dbt)

The dbt project (`ride_hailing`) follows a **Medallion architecture** targeting BigQuery.

### Bronze
Raw source views over BigQuery tables loaded from CDC and batch ingest. No transformation applied.

### Silver — Star Schema

**Dimensions**

| Model | Description |
|---|---|
| `dim_drivers` | Driver profiles with vehicle and rating data |
| `dim_passengers` | Passenger profiles |
| `dim_zones` | Geographic zones (Jakarta areas) |
| `dim_vehicle_types` | Vehicle categories and base fares |
| `dim_date` | Date spine |

**Facts**

| Model | Materialization | Description |
|---|---|---|
| `fct_rides` | Incremental (merge) | Completed ride transactions with fare breakdown |
| `fct_ride_events` | Incremental (merge) | Per-ride status events (REQUESTED → ACCEPTED → PICKED_UP → COMPLETED) |

**Snapshots (SCD Type 2)**
- `snapshot_dim_driver`
- `snapshot_dim_passengers`
- `snapshot_dim_vehicle_types`
- `snapshot_dim_zones`

### Gold — Analytics Marts

| Mart | Models |
|---|---|
| `mart_operations` | `mart_daily_ride_summary`, `mart_cancellation_analysis`, `mart_zone_heatmap` |
| `mart_finance` | `mart_revenue_daily`, `mart_surge_analysis` |
| `mart_driver` | `mart_driver_performance`, `mart_driver_daily_activity` |

---

## 🔄 CDC Pipeline Detail

```
PostgreSQL (rides) / MongoDB (ride_events, driver_location_stream)
       │  (WAL / oplog)
       ▼
  Debezium Source Connector
  (Avro + Schema Registry)
       │
       ▼
  Kafka Topics  (prefix: cdc.)
  e.g. cdc.public.rides
       │
       ▼
  Kafka Connect GCS Sink
  Format : Parquet (Snappy)
  Path   : raw/cdc/topic=<topic>/year=YYYY/month=MM/day=dd/hour=HH/
       │
       ▼
  Airflow DAG: gcs_to_bigquery_cdc_hourly
  → BigQuery dataset: raw_cdc
```

Topics captured:

| Source | Topics | Ingestion |
|---|---|---|
| PostgreSQL | `cdc.public.rides` | CDC → GCS → BQ |
| MongoDB | `cdc.ride_ops_mg.ride_events`, `cdc.ride_ops_mg.driver_location_stream` | CDC → GCS → BQ |
| PostgreSQL | `drivers`, `passengers`, `vehicle_types`, `zones` | Batch (Trino) |

---

## 🧩 Trino Federation

Trino acts as a unified query layer across all data sources. The batch ingest DAG uses Trino to `INSERT INTO` BigQuery directly from PostgreSQL — no intermediate staging files needed.

Configured catalogs:

| Catalog | Backend |
|---|---|
| `postgresql` | PostgreSQL 14 (ride_ops_pg) |
| `bigquery` | Google BigQuery |
| `mysql` | MySQL 8 (ride_marketing_mysql) |
| `mongodb` | MongoDB |

---

## 🌱 Data Generator

The generator (`data-generator/generator/generator_v3.py`) simulates a realistic Indonesian ride-hailing operation:

- **Locale**: `id_ID` (Indonesian names, addresses in Jakarta zones: JKT-SEL, JKT-PUS, etc.)
- **Simulation clock**: configurable start date and speed multiplier (`SIM_SPEED=60` → 1 real second = 1 simulated minute)
- **Ride lifecycle**: `REQUESTED → ACCEPTED → PICKED_UP → COMPLETED / CANCELLED / NO_DRIVER`
- **Driver locations**: streamed to MongoDB at configurable intervals (default 2/min)
- **Recovery**: on restart, looks back `RECOVERY_LOOKBACK_DAYS` to resume open rides

Key environment variables:

| Variable | Default | Description |
|---|---|---|
| `SIM_SPEED` | `60` | Simulation speed multiplier |
| `INITIAL_DRIVERS` | `150` | Number of drivers to seed |
| `INITIAL_PASSENGERS` | `600` | Number of passengers to seed |
| `RIDES_PER_TICK_WEEKDAY_BASE` | `2` | New rides per tick on weekdays |
| `RIDES_PER_TICK_WEEKEND_BASE` | `3` | New rides per tick on weekends |
| `MAX_OPEN_RIDES` | `400` | Max concurrent open rides |
| `SIM_START_AT` | `2026-01-01T00:00:00+07:00` | Simulated start timestamp |

---

## 🔐 Environment Variables

Copy `.env` and fill in all values before running. Key variables:

```dotenv
# PostgreSQL
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=ride_ops_pg

# GCP
GCP_PROJECT_ID=your-gcp-project
GCS_BUCKET=your-gcs-bucket
BQ_LOCATION=us
BQ_DATASET_BRONZE=bronze_taxi
BQ_DATASET_SILVER=silver_taxi
BQ_DATASET_GOLD=gold_taxi

# Airflow
AIRFLOW__CORE__FERNET_KEY=<generated>
AIRFLOW__CORE__SECRET_KEY=<generated>
AIRFLOW_UID=50000

# dbt
DBT_TARGET=prod
```

> ⚠️ **Never commit `.env` or `credentials/service-account.json` to version control.**

---

## 🧪 dbt Tests

Custom data quality tests included:

| Test | Description |
|---|---|
| `assert_fare_consistency` | Fare amount must be ≥ base fare for completed rides |
| `assert_ride_timestamp_order` | `pickup_at` must be after `accepted_at` |
| `assert_phone_canonical` | Phone numbers match canonical format |

Run tests:

```bash
dbt test --profiles-dir ./dbt/dbt_profiles --project-dir ./dbt/dbt_project
```

---

## 📦 Airflow Providers

```
apache-airflow-providers-postgres
apache-airflow-providers-mysql
apache-airflow-providers-google
apache-airflow-providers-docker
apache-airflow-providers-trino
psycopg2-binary
```

---

## 📝 License

This project is for **educational and portfolio purposes**. Feel free to fork and adapt it.
