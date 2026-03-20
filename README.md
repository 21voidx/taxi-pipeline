# 🏦 Banking Data Platform — End-to-End Data Engineering Portfolio

> **Production-grade batch analytics pipeline for banking domain**  
> Multi-source ingestion → Trino federation → BigQuery → dbt → Looker

---

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SOURCE SYSTEMS                                     │
│                                                                             │
│  ┌────────────────────── ┐        ┌──────────────────────────────────────┐  │
│  │  PostgreSQL 15        │        │  MySQL 8.0                           │  │
│  │  (Core Banking)       │        │  (Transaction System)                │  │
│  │                       │        │                                      │  │
│  │  • customers          │        │  • transactions                      │  │
│  │  • accounts           │        │  • merchants                         │  │
│  │  • branches           │        │  • fraud_flags                       │  │
│  │  • employees          │        │  • payment_methods                   │  │
│  │  • loan_applications  │        │  • transaction_types                 │  │
│  │  • credit_scores      │        │                                      │  │
│  │                       │        │  [WAL/binlog pre-configured          │  │
│  │  [wal_level=logical   │        │   for future CDC via Debezium]       │  │
│  │   pre-configured]     │        │                                      │  │
│  └──────────┬─────────── ┘        └───────────────┬──────────────────────┘  │
│             │                                     │                         │
└─────────────┼─────────────────────────────────────┼─────────────────────────┘
              │                                     │
              ▼                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         QUERY FEDERATION LAYER                              │
│                                                                             │
│                    ┌──────────────────────────┐                             │
│                    │   Trino (Query Engine)    │                            │
│                    │                           │                            │
│                    │  Catalogs:                │                            │
│                    │  • postgresql (connector) │                            │
│                    │  • mysql     (connector)  │                            │
│                    │  • bigquery  (connector)  │                            │
│                    └──────────────┬────────────┘                            │
│                                   │                                         │
└───────────────────────────────────┼─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATION (Apache Airflow 2.8)                      │
│                                                                             │
│  DAGs:                                                                      │
│  ├── dag_core_banking       (Postgres → BigQuery raw)                       │
│  ├── dag_transactions       (MySQL → BigQuery raw)                          │
│  └── dbt_transformation     (BigQuery raw → staging → marts)                │  │                                                                             │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                   DATA WAREHOUSE (Google BigQuery)                          │
│                                                                             │
│  Dataset: raw_core_banking      ← Airflow (Postgres extract)                │
│  Dataset: raw_transactions      ← Airflow (MySQL extract)                   │
│  Dataset: staging               ← dbt (cleaned, typed, documented)          │
│  Dataset: intermediate          ← dbt (business logic joins)                │
│  Dataset: marts                 ← dbt (analytics-ready aggregations)        │
│           ├── customer/                                                     │
│           ├── risk/                                                         │
│           └── finance/                                                      │
│                                                                             │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BI & ANALYTICS (Looker / LookML)                         │
│                                                                             │
│  • Customer 360 Dashboard                                                   │
│  • Fraud & Risk Analytics                                                   │
│  • Transaction Volume & Revenue                                             │
│  • Loan Portfolio Performance                                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│              🔮 FUTURE: CDC Real-Time Streaming (Phase 2)                  │
│                                                                             │
│  PostgreSQL (WAL) ──► Debezium ──► Kafka ──► ClickHouse ──► Grafana         │
│  MySQL (binlog)   ──►                                                       │
│                                                                             │
│  [Infrastructure pre-configured in this project — not yet activated]        │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🗂️ Project Structure

```
banking-data-platform/
│
├── README.md                          # This file
├── SETUP_GCP.md                       # Step-by-step GCP setup guide
├── docker-compose.yml                 # All local services
├── docker-compose.override.yml        # Dev-only overrides
├── .env.example                       # Environment variables template
├── .gitignore
├── Makefile                           # Common dev commands
│
├── docs/
│   ├── architecture.md                # Detailed architecture decisions
│   ├── data-dictionary.md             # All table definitions
│   └── cdc-roadmap.md                 # Phase 2 CDC planning
│
├── data-generator/                    # Synthetic banking data generator
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                        # Entry point: --mode full/incremental/truncate
│   ├── config/
│   │   └── settings.py                # Env var config + date range settings
│   ├── generators/
│   │   ├── db_connections.py          # Postgres & MySQL connection helpers
│   │   ├── core_banking/              # PostgreSQL generators
│   │   │   ├── customers.py           # Indonesian customer profiles (KYC, NIK)
│   │   │   ├── accounts.py            # Savings, checking, loan, credit card accounts
│   │   │   ├── branches.py            # Bank branch master data
│   │   │   ├── employees.py           # Employee profiles per branch
│   │   │   └── loan_applications.py   # Loan lifecycle (submitted → disbursed)
│   │   └── transaction/               # MySQL generators
│   │       ├── transactions.py        # IDR transactions with realistic patterns
│   │       ├── merchants.py           # Merchant profiles with MCC codes
│   │       └── fraud_flags.py         # Fraud detection flags (0.8% rate)
│   └── schemas/
│       ├── postgres_schema.sql        # DDL with WAL config + WIB timestamps
│       └── mysql_schema.sql           # DDL with binlog config + WIB timestamps
│
├── ingestion/
│   ├── trino/
│   │   ├── config/config.properties
│   │   └── catalogs/
│   │       ├── postgresql.properties
│   │       ├── mysql.properties
│   │       └── bigquery.properties
│   └── scripts/
│       ├── extract_postgres.py        # Incremental extract via Trino
│       └── extract_mysql.py           # Incremental extract via Trino
│
├── orchestration/
│   └── airflow/
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── dags/
│       │   ├── dag_core_banking.py
│       │   ├── dag_transactions.py
│       │   └── dbt_transformation_dag.py
│       └── plugins/
│
├── transformation/
│   └── dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── packages.yml
│       ├── models/
│       │   ├── staging/
│       │   │   ├── _sources.yml
│       │   │   ├── core_banking/      # stg_customers, stg_accounts, ...
│       │   │   └── transactions/      # stg_transactions, stg_merchants, ...
│       │   ├── intermediate/          # Business logic joins
│       │   └── marts/
│       │       ├── customer/          # mart_customer_360, mart_clv
│       │       ├── risk/              # mart_fraud_analytics, mart_credit_risk
│       │       └── finance/           # mart_transaction_analytics
│       ├── tests/
│       ├── macros/
│       │   ├── generate_schema_name.sql
│       │   ├── audit_columns.sql
│       │   └── banking_utils.sql
│       └── snapshots/                 # SCD Type 2 for customers & accounts
│
├── monitoring/
│   ├── great_expectations/            # Data quality suite
│   └── alerts/alert_config.yml        # Alerting rules
│
└── scripts/
    ├── setup.sh                       # Local dev bootstrap
    ├── setup_gcp.sh                   # GCP project bootstrap
    └── init_cdc.sh                    # CDC pre-configuration validator
```

---


---

## 🧪 Data Generator

Synthetic data generator yang menghasilkan data perbankan Indonesia realistis untuk mengisi PostgreSQL (Core Banking) dan MySQL (Transaction System) sebagai source data pipeline.

### Fitur Utama

- **Timezone WIB** — Semua timestamp (`created_at`, `updated_at`, `transaction_at`, dll.) menggunakan Asia/Jakarta (UTC+7), bukan UTC
- **Controlled date range** — Full load dan incremental load menggunakan rentang tanggal yang dapat dikonfigurasi, bukan `NOW()` real-time
- **Dua mode operasi** — `full` untuk initial load historis, `incremental` untuk simulasi daily load
- **Data Indonesia realistis** — NIK 16 digit, nama WNI, provinsi, kota, mata uang IDR, pola transaksi QRIS/mobile banking
- **Deterministik** — `SEED=42` menghasilkan data yang sama setiap kali dijalankan (reproducible)
- **Fraud rate realistis** — 0.8% transaksi di-flag sebagai fraud, sesuai industri perbankan

### Date Range Design

```
FULL LOAD
─────────────────────────────────────────────────────────────────
  2025-01-01                                          2025-12-31
      │◄──────────── data tersebar merata ──────────────────►│
      │                                                       │
      │  transactions      → spread across full year          │
      │  customers         → onboarded throughout 2025        │
      │  accounts          → opened throughout 2025           │
      │  loan_applications → submitted throughout 2025        │
      │                                                       │
      │  branches    → pre-2025 (bank sudah lama berdiri)     │
      │  employees   → pre-2025 (hire date bisa lebih lama)   │
      │  merchants   → pre-2025 (merchant sudah existing)     │

INCREMENTAL LOAD (daily, mulai 2026-01-02)
─────────────────────────────────────────────────────────────────
  2026-01-02   2026-01-03   2026-01-04   ...
      │             │             │
   1 hari       1 hari        1 hari     ← tiap run = 1 hari data baru
   ~333 txn    ~333 txn     ~333 txn     ← (num_transactions / 30)
```

### Modes

| Mode | Deskripsi | Kapan Digunakan |
|------|-----------|-----------------|
| `full` | Generate semua tabel dari nol, data tersebar di `FULL_RANGE_START` s/d `FULL_START_DATE` | Initial setup, reset environment |
| `incremental` | Tambah data baru untuk 1 hari (customers baru, transaksi harian, loan baru) | Daily pipeline testing, Airflow DAG |
| `truncate` | Hapus semua generated data *(belum diimplementasi)* | Reset data |

### Cara Menjalankan

#### Via Docker (recommended)

```bash
# Full load — default range 2025-01-01 s/d 2025-12-31
docker compose run --rm data-generator python main.py --mode full

# Full load dengan range custom
docker compose run --rm data-generator \
  python main.py --mode full \
  --range-start 2025-01-01 \
  --start-date 2025-12-31

# Incremental load di tanggal custom (tidak perlu tunggu hari berikutnya)
docker compose run --rm data-generator \
  python main.py --mode incremental \
  --incremental-date 2026-01-02
```

#### Via Environment Variables (untuk Airflow DAG)

```bash
# Set via env var, cocok untuk DockerOperator di Airflow
FULL_RANGE_START=2025-01-01 \
FULL_START_DATE=2025-12-31 \
python main.py --mode full

# Incremental dengan tanggal dari Airflow execution_date
INCREMENTAL_DATE={{ ds }} python main.py --mode incremental
```

#### Contoh Airflow DockerOperator

```python
from airflow.providers.docker.operators.docker import DockerOperator

incremental_load = DockerOperator(
    task_id="incremental_data_load",
    image="banking-data-generator:latest",
    command="python main.py --mode incremental --incremental-date {{ ds }}",
    environment={
        "POSTGRES_HOST": "postgres-core",
        "MYSQL_HOST": "mysql-txn",
        # ... credentials dari Airflow connections
    },
    network_mode="banking-net",
)
```

### Environment Variables

| Variable | Default | Deskripsi |
|----------|---------|-----------|
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `core_banking` | Database name |
| `POSTGRES_USER` | `banking_core` | Username |
| `POSTGRES_PASSWORD` | *(required)* | Password |
| `MYSQL_HOST` | `localhost` | MySQL host |
| `MYSQL_PORT` | `3306` | MySQL port |
| `MYSQL_DB` | `transaction_db` | Database name |
| `MYSQL_USER` | `banking_txn` | Username |
| `MYSQL_PASSWORD` | *(required)* | Password |
| `NUM_CUSTOMERS` | `10000` | Jumlah customer yang di-generate |
| `NUM_TRANSACTIONS` | `100000` | Jumlah transaksi untuk full load |
| `NUM_MERCHANTS` | `500` | Jumlah merchant |
| `NUM_BRANCHES` | `50` | Jumlah cabang |
| `NUM_EMPLOYEES` | `200` | Jumlah karyawan |
| `SEED` | `42` | Random seed (reproducibility) |
| `BATCH_SIZE` | `500` | Ukuran batch insert |
| `FULL_RANGE_START` | `2025-01-01` | Tanggal **awal** range full load |
| `FULL_START_DATE` | `2025-12-31` | Tanggal **akhir** range full load |
| `INCREMENTAL_DATE` | `2026-01-02` | Tanggal simulasi incremental load |
| `TZ` | `Asia/Jakarta` | Timezone container (set di docker-compose) |

### Data yang Dihasilkan

#### PostgreSQL — Core Banking (`core_banking`)

| Tabel | Volume (default) | Deskripsi |
|-------|-----------------|-----------|
| `branches` | 50 rows | Cabang bank di kota-kota Indonesia |
| `employees` | 200 rows | Karyawan per cabang (teller, CS, RM, dll.) |
| `product_types` | 7 rows | Produk perbankan (tabungan, giro, KPR, KTA, CC) |
| `customers` | 10.000 rows | Profil nasabah dengan NIK, KYC status, segmen |
| `accounts` | ~15.000 rows | Rekening nasabah (1–3 akun per nasabah) |
| `loan_applications` | ~2.000 rows | Aplikasi pinjaman dengan lifecycle lengkap |

#### MySQL — Transaction System (`transaction_db`)

| Tabel | Volume (default) | Deskripsi |
|-------|-----------------|-----------|
| `transaction_types` | 8 types | DEBIT_PURCHASE, ATM_WITHDRAWAL, QRIS_PAYMENT, dll. |
| `payment_methods` | 6 methods | MOBILE_BANKING, DEBIT_CARD, QRIS, dll. |
| `merchant_categories` | 15 MCC | ISO 18245 Merchant Category Codes |
| `merchants` | 500 rows | Merchant dengan MCC, kota, risk level |
| `transactions` | 100.000 rows | Transaksi IDR dengan channel, status, fee |
| `fraud_flags` | ~800 rows | Fraud flags (0.8% rate, 4 severity levels) |

### Distribusi Data Realistis

**Transaction channels:**
```
mobile banking  40%  │████████████████████
ATM             20%  │██████████
web banking     15%  │████████
POS             15%  │████████
QRIS             5%  │███
branch           5%  │███
```

**Transaction types (IDR):**
```
DEBIT_PURCHASE    30%  │  Rp 10.000 – Rp 5.000.000
CREDIT_PURCHASE   10%  │  Rp 50.000 – Rp 25.000.000
ATM_WITHDRAWAL    15%  │  Rp 100.000 – Rp 3.000.000
TRANSFER_OUT      15%  │  Rp 50.000 – Rp 50.000.000
TRANSFER_IN       12%  │  Rp 50.000 – Rp 50.000.000
SALARY_CREDIT      5%  │  Rp 3.000.000 – Rp 50.000.000
BILL_PAYMENT       8%  │  Rp 50.000 – Rp 5.000.000
QRIS_PAYMENT       5%  │  Rp 5.000 – Rp 500.000
```

**Transaction status:**
```
completed  95%  │  fraud_flags rate: 0.8% dari completed
failed      3%  │  severity: low 40% / medium 35% / high 18% / critical 7%
pending     1%
reversed    1%
```

**Customer segments:**
```
retail     70%  │  KYC: verified 85% / pending 8% / rejected 4% / expired 3%
priority   15%
premier     8%
sme         4%
private     2%
corporate   1%
```

### Timestamp Design

Semua timestamp disimpan dalam **WIB (Asia/Jakarta, UTC+7)**. Generator mengirim nilai `created_at` dan `updated_at` secara eksplisit ke database — tidak mengandalkan `DEFAULT NOW()` — sehingga data historis ter-backfill dengan benar sesuai `range_start` s/d `range_end`.

```
PostgreSQL → TIMESTAMPTZ  → stored as UTC, displayed as WIB (+07)
MySQL      → DATETIME(6)  → stored as WIB naive string (no tz offset)
```

## ⚡ Quick Start (Local Development)

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Docker | ≥ 24.0 | Container runtime |
| Docker Compose | ≥ 2.20 | Service orchestration |
| Python | ≥ 3.11 | Scripts & generators |
| `gcloud` CLI | latest | GCP interaction |
| `dbt-bigquery` | ≥ 1.7 | Transformations |
| Make | any | Dev shortcuts |

### 1. Clone & Configure

```bash
git clone https://github.com/yourname/banking-data-platform.git
cd banking-data-platform

# Copy and edit environment variables
cp .env.example .env
# Edit .env with your actual values (GCP project ID, credentials, etc.)
```

### 2. Bootstrap Local Services

```bash
# Start all local services (Postgres, MySQL, Trino, Airflow)
make up

# Verify all services are healthy
make health

# Generate synthetic banking data
make generate-data

# Verify data was loaded
make verify-data
```

### 3. Run Ingestion Pipeline

```bash
# Trigger ingestion DAGs via Airflow UI
open http://localhost:8080   # admin / admin

# Or trigger manually via CLI
make trigger-ingest
```

### 4. Run dbt Transformations

```bash
cd transformation/dbt

# Install dependencies
dbt deps

# Run staging + intermediate + marts
dbt run --profiles-dir . --target dev

# Run data quality tests
dbt test --profiles-dir . --target dev

# Generate documentation
dbt docs generate && dbt docs serve
```

---

## 🏛️ Data Models

### Source: Core Banking (PostgreSQL)

```sql
-- Key tables
customers          → 500K rows  (PII encrypted at rest)
accounts           → 1.2M rows  (savings, checking, loans, credit)
branches           → 150 rows
employees          → 2K rows
loan_applications  → 200K rows
credit_scores      → 500K rows
```

### Source: Transaction System (MySQL)

```sql
transactions       → 10M+ rows (incremental daily load)
merchants          → 50K rows
fraud_flags        → 100K rows
payment_methods    → 8 types
transaction_types  → 15 types
```

### BigQuery Marts

| Mart | Description | Refresh |
|------|-------------|---------|
| `mart_customer_360` | Unified customer profile with all products | Daily |
| `mart_customer_lifetime_value` | CLV segmentation & scoring | Weekly |
| `mart_transaction_analytics` | Daily transaction volume, revenue | Daily |
| `mart_fraud_analytics` | Fraud patterns, risk scoring | Daily |
| `mart_credit_risk` | Loan performance, NPL ratios | Daily |
| `mart_product_performance` | Product adoption, cross-sell metrics | Weekly |

---

## 🔒 Banking-Grade Best Practices

### Security
- **PII Masking**: Customer PII (name, NIK, phone) masked in `staging` layer using BigQuery column-level security
- **Encryption**: All GCS buckets use CMEK (Customer-Managed Encryption Keys)
- **IAM**: Least-privilege service accounts per component
- **Secret Management**: All credentials via GCP Secret Manager (never in `.env` in production)
- **Network**: VPC-native setup, Private Google Access enabled
- **Audit Log**: BigQuery Data Access audit logs enabled

### Data Quality
- **Source freshness** checks in dbt (`source freshness`)
- **Not-null, unique, accepted-values** tests on all primary & foreign keys
- **Great Expectations** suites for raw data validation before ingestion
- **Referential integrity** tests across source joins

### Reliability
- **Idempotent** DAGs: safe to re-run without duplicating data
- **Incremental loads** with `updated_at` watermark tracking
- **Dead-letter queue**: Failed records written to `raw_errors` dataset
- **Alerting**: Airflow email + Slack alerts on SLA miss

### Observability
- **dbt artifacts** (manifest, run_results) stored to GCS for lineage
- **Airflow metrics** via StatsD → Prometheus → Grafana (optional)
- **BigQuery slot usage** dashboards in Looker

### Compliance (Banking)
- **Data lineage** tracked end-to-end via dbt lineage graph
- **Row-level security** in BigQuery for multi-branch access
- **Data retention** policies on raw datasets (90 days)
- **GDPR/right-to-be-forgotten** workflow via dbt macro

---

## 🔮 Phase 2: CDC Real-Time Streaming (Roadmap)

The infrastructure is **pre-configured** for CDC but not yet active:

```
PostgreSQL  ──► (WAL logical replication configured)
MySQL       ──► (binlog ROW format configured)
                        │
                        ▼
                   Debezium (Kafka Connect)
                        │
                        ▼
                   Apache Kafka
                        │
                        ▼
                   ClickHouse (OLAP)
                        │
                        ▼
                   Grafana (Real-time dashboards)
```

**What's already done for CDC readiness:**
- PostgreSQL: `wal_level=logical`, `max_replication_slots=5`, `max_wal_senders=5`
- MySQL: `binlog_format=ROW`, `binlog_row_image=FULL`, `expire_logs_days=7`
- Tables have `created_at`, `updated_at`, and `deleted_at` (soft-delete) columns
- Primary keys defined on all tables
- `scripts/init_cdc.sh` validates CDC readiness

---

## 🌐 GCP Services Used

| Service | Purpose |
|---------|---------|
| BigQuery | Data warehouse, SQL transformations |
| Cloud Storage (GCS) | Raw data lake landing zone |
| Cloud Composer (optional) | Managed Airflow |
| Artifact Registry | Docker image storage |
| Secret Manager | Credentials management |
| Cloud Run / GCE | Airflow worker (self-managed) |
| IAM & VPC | Security & networking |

> **Full GCP setup guide → [`SETUP_GCP.md`](./SETUP_GCP.md)**

---

## 🛠️ Make Commands Reference

```bash
make up              # Start all Docker services
make down            # Stop all services
make health          # Check service health
make generate-data   # Run data generator (Postgres + MySQL)
make verify-data     # Count rows in source tables
make trigger-ingest  # Trigger Airflow ingestion DAGs
make dbt-run         # Run all dbt models
make dbt-test        # Run all dbt tests
make dbt-docs        # Generate & serve dbt docs
make lint            # Run SQLFluff linter on dbt models
make clean           # Remove volumes and containers
make logs            # Tail logs for all services
```

---