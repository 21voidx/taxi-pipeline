# CDC + Kafka sebagai Solusi Keterbatasan SCD2

## Daftar Isi

1. [Konteks: Mengapa SCD2 Tidak Cukup](#1-konteks-mengapa-scd2-tidak-cukup)
2. [Kelemahan Mendasar SCD2](#2-kelemahan-mendasar-scd2)
3. [SCD2 vs Event Log: Perbedaan Konsep](#3-scd2-vs-event-log-perbedaan-konsep)
4. [Apa itu CDC dan Bagaimana Cara Kerjanya](#4-apa-itu-cdc-dan-bagaimana-cara-kerjanya)
5. [Bentuk Data yang Ditangkap CDC](#5-bentuk-data-yang-ditangkap-cdc)
6. [Arsitektur CDC Kafka untuk Ride-Hailing](#6-arsitektur-cdc-kafka-untuk-ride-hailing)
7. [Perbandingan: SCD2 vs Airflow Batch vs CDC Kafka](#7-perbandingan-scd2-vs-airflow-batch-vs-cdc-kafka)
8. [Mengapa trip_status_logs Tetap Dibutuhkan](#8-mengapa-trip_status_logs-tetap-dibutuhkan)
9. [Rekomendasi per Tabel](#9-rekomendasi-per-tabel)
10. [Arsitektur Hybrid yang Ideal](#10-arsitektur-hybrid-yang-ideal)
11. [Ringkasan Keputusan](#11-ringkasan-keputusan)

---

## 1. Konteks: Mengapa SCD2 Tidak Cukup

Pertanyaan yang wajar muncul ketika merancang fact table:

> *"Kalau SCD2 bisa melacak perubahan, kenapa tidak langsung pakai SCD2 di `fct_trip` saja dan buang `trip_status_logs`?"*

Secara sekilas terlihat masuk akal. Tapi jawabannya ada pada **perbedaan mendasar antara apa yang SCD2 simpan vs apa yang event log simpan**.

---

## 2. Kelemahan Mendasar SCD2

### SCD2 Hanya Menyimpan Nilai Atribut, Bukan Waktu Kejadian

SCD2 bekerja dengan cara **menangkap snapshot nilai kolom setiap kali ada perubahan**. Yang tersimpan adalah *nilai saat itu*, bukan *kapan transisi terjadi secara presisi*.

### dbt Snapshot Hanya Melihat Nilai di Source SAAT INI

Setiap kali `dbt snapshot` berjalan, ia melakukan query biasa ke source:

```sql
SELECT * FROM customers_raw
```

Ia hanya melihat **nilai yang ada sekarang**. Tidak ada cara baginya melihat nilai yang sudah berubah dan tertimpa sebelum snapshot berjalan.

### Ilustrasi Konkret: Perubahan yang Hilang

Misalnya `dbt snapshot` dijadwal setiap jam. Dalam satu jam, Budi berubah segment sebanyak 3 kali:

```
Jam 09:00  Budi masuk → status = bronze
Jam 09:20  Budi beli lebih banyak → status = silver   (UPDATE row yang sama di Postgres)
Jam 09:45  Budi beli lagi → status = gold             (UPDATE row yang sama di Postgres)
Jam 10:00  dbt snapshot berjalan
```

Yang dilihat dbt snapshot jam 10:00:

```sql
SELECT * FROM customers_raw WHERE customer_id = 'C001'

-- Postgres sudah menimpa nilai bronze dan silver.
-- dbt hanya melihat nilai terbaru:
customer_id  full_name  status  updated_at
C001         Budi       gold    09:45:00
```

Hasil SCD2 yang terbentuk:

```
customer_id  status  dbt_valid_from  dbt_valid_to
C001         bronze  09:00:00        10:00:00      ← snapshot sebelumnya
C001         gold    10:00:00        NULL          ← snapshot baru

Status silver = TIDAK PERNAH TEREKAM, hilang selamanya
```

### Kelemahan Tambahan: `dbt_valid_from` Bukan Waktu Kejadian Sebenarnya

```
Status driver_accepted terjadi jam 08:02:00 di Postgres
dbt snapshot berjalan  jam 09:00:00

SCD2 mencatat:
  dbt_valid_from = 09:00:00   ← waktu snapshot, BUKAN waktu kejadian
  dbt_valid_to   = 10:00:00   ← snapshot berikutnya

Yang tersimpan adalah kapan dbt mendeteksi perubahan,
bukan kapan perubahan itu benar-benar terjadi.
```

Akibatnya jika kamu menghitung durasi antar status dari SCD2:

```
Kejadian nyata:
  driver_accepted terjadi jam 08:02
  cancelled       terjadi jam 08:25
  Durasi sebenarnya = 23 menit

SCD2 dengan snapshot setiap jam:
  dbt_valid_from driver_accepted = 09:00  (snapshot pertama yang menangkap)
  dbt_valid_to   driver_accepted = 10:00  (snapshot berikutnya sudah cancelled)
  Durasi menurut SCD2             = 60 menit  ← SALAH, 2.6x lipat dari kenyataan
```

### Kelemahan Fatal: Lompatan Status dalam Satu Interval

Dalam satu interval snapshot, satu trip bisa melewati banyak status sekaligus:

```
Snapshot jam 09:00: T002 = requested
Snapshot jam 10:00: T002 = completed  ← lompat langsung, 4 status terlewat

SCD2 yang terbentuk:
trip_id  status      dbt_valid_from  dbt_valid_to
T002     requested   09:00:00        10:00:00
T002     completed   10:00:00        NULL

Status driver_accepted, driver_arrived, trip_started = TIDAK PERNAH ADA
```

---

## 3. SCD2 vs Event Log: Perbedaan Konsep

Ini adalah perbedaan yang paling penting untuk dipahami.

```
SCD2 menjawab:
  "Nilai atribut X pada entitas Y di waktu T adalah apa?"
  Contoh: "Budi segment-nya apa di bulan Januari?"

Event log menjawab:
  "Kejadian X terjadi jam berapa?"
  Contoh: "Driver T001 diterima jam berapa? Cancelled jam berapa?"
```

### Perbandingan Data untuk Trip yang Sama

Kejadian nyata di Postgres untuk trip T001:

```
08:00 → requested
08:02 → driver_accepted
08:18 → driver_arrived
08:20 → trip_started
08:55 → trip_completed
```

**Hasil SCD2** (snapshot tiap jam):

```
trip_id  status          dbt_valid_from  dbt_valid_to   durasi hitung
T001     requested       08:00           09:00          60 mnt  ← SALAH
T001     trip_completed  09:00           NULL           -

Status driver_accepted, driver_arrived, trip_started = HILANG
```

**Hasil trip_status_logs** (event log):

```
log_id  trip_id  status            status_ts   durasi ke status berikutnya
LOG001  T001     requested         08:00:00    2 menit   ✓
LOG002  T001     driver_accepted   08:02:00    16 menit  ✓
LOG003  T001     driver_arrived    08:18:00    2 menit   ✓
LOG004  T001     trip_started      08:20:00    35 menit  ✓
LOG005  T001     trip_completed    08:55:00    -         ✓
```

### Kapan SCD2 Aman Dipakai

SCD2 aman hanya jika perubahan terjadi **lebih lambat dari interval snapshot**:

```
Interval snapshot = 1 jam

AMAN:
  Customer ganti nomor HP   → 1 kali sehari      → snapshot pasti menangkap
  Driver ganti kota domisili → 1 kali seminggu   → snapshot pasti menangkap

TIDAK AMAN:
  Trip berubah status → 5 kali dalam 55 menit    → snapshot melewatkan semua
                                                    kecuali yang terakhir
```

---

## 4. Apa itu CDC dan Bagaimana Cara Kerjanya

**CDC (Change Data Capture)** adalah teknik menangkap setiap perubahan data di database secara real-time dengan membaca **Write-Ahead Log (WAL)** Postgres — bukan dengan melakukan SELECT ke tabel.

### Cara Kerja WAL Postgres

Setiap operasi INSERT, UPDATE, DELETE di Postgres dicatat di WAL sebelum diterapkan ke tabel. WAL adalah log internal yang digunakan Postgres untuk recovery. CDC tool seperti **Debezium** membaca WAL ini secara streaming:

```
Postgres Internal:
  1. Query UPDATE customers SET status='silver'
  2. Postgres tulis ke WAL dulu: "UPDATE customers, row C001, before=bronze, after=silver"
  3. Baru terapkan ke tabel

Debezium CDC:
  → Membaca WAL secara streaming
  → Menangkap event sebelum dan sesudah perubahan
  → Publish ke Kafka topic
```

### Perbandingan: Cara Kerja dbt Snapshot vs CDC

```
dbt snapshot                          CDC Kafka
────────────────────────────────      ────────────────────────────────
SELECT * FROM table                   Baca WAL Postgres
Lihat nilai SAAT INI                  Lihat SETIAP perubahan
Jalankan per jadwal (tiap jam/hari)   Berjalan terus-menerus (streaming)
Perubahan antar jadwal = HILANG       Tidak ada yang bisa lolos
Waktu tersimpan = waktu snapshot      Waktu tersimpan = waktu kejadian exact
Kompleksitas rendah                   Kompleksitas menengah-tinggi
```

---

## 5. Bentuk Data yang Ditangkap CDC

Debezium menangkap setiap perubahan dalam format berikut:

```json
{
  "op": "u",
  "ts_ms": 1736916120000,
  "before": {
    "customer_id": "C001",
    "status": "silver",
    "updated_at": "2026-01-15 09:20:00"
  },
  "after": {
    "customer_id": "C001",
    "status": "gold",
    "updated_at": "2026-01-15 09:45:00"
  },
  "source": {
    "db": "ride_ops_pg",
    "table": "customers",
    "lsn": 12345678
  }
}
```

Keterangan field `op`:

```
c = create  (INSERT baris baru)
u = update  (UPDATE baris yang sudah ada)
d = delete  (DELETE baris)
r = read    (snapshot awal saat CDC pertama kali diaktifkan)
```

### Bentuk Tabel di BigQuery Bronze CDC

Setiap event CDC menjadi satu baris di BigQuery:

```
op  customer_id  status_before  status_after  event_ts              lsn
c   C001         NULL           bronze        2026-01-15 09:00:00   1000  ← INSERT awal
u   C001         bronze         silver        2026-01-15 09:20:00   1001  ← UPDATE pertama
u   C001         silver         gold          2026-01-15 09:45:00   1002  ← UPDATE kedua
u   C001         gold           platinum      2026-02-01 10:00:00   1003  ← UPDATE bulan depan
```

Tidak ada yang hilang meskipun snapshot dbt berjalan setiap jam, karena semua perubahan sudah tersimpan sebagai baris terpisah sebelum dbt berjalan.

---

## 6. Arsitektur CDC Kafka untuk Ride-Hailing

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgreSQL (ride_ops_pg)                  │
│                                                             │
│   WAL (Write-Ahead Log)                                     │
│   ┌──────────────────────────────────────────────────────┐  │
│   │ UPDATE trips SET status='completed' WHERE id='T001'  │  │
│   │ INSERT INTO trip_status_logs VALUES (...)            │  │
│   │ UPDATE payments SET status='success' WHERE ...       │  │
│   └──────────────────────────────────────────────────────┘  │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        │  Debezium CDC Connector
                        │  (membaca WAL secara streaming)
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                      Apache Kafka                           │
│                                                             │
│   Topics:                                                   │
│   ├── ride_ops.public.trips                                 │
│   ├── ride_ops.public.trip_status_logs                      │
│   ├── ride_ops.public.payments                              │
│   ├── ride_ops.public.customers          (batch cukup)      │
│   └── ride_ops.public.drivers            (batch cukup)      │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        │  Kafka Consumer / Kafka Connect BigQuery Sink
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                  BigQuery Bronze CDC Layer                  │
│                                                             │
│   bronze_cdc.trips_events          (op, before, after, ts) │
│   bronze_cdc.payments_events       (op, before, after, ts) │
│   bronze_cdc.trip_status_events    (op, before, after, ts) │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        │  dbt (micro-batch atau streaming)
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Silver & Gold (seperti sebelumnya)             │
│                                                             │
│   fct_trip              (nilai terbaru, merge)              │
│   fct_trip_timeline     (durasi antar status, dari CDC)     │
│   dm_trip_daily_city    (agregat, insert_overwrite)         │
└─────────────────────────────────────────────────────────────┘
```

---

## 7. Perbandingan: SCD2 vs Airflow Batch vs CDC Kafka

| | SCD2 (dbt snapshot) | Airflow Batch EL | CDC Kafka |
|---|---|---|---|
| **Cara kerja** | SELECT + bandingkan versi | SELECT + MERGE | Baca WAL Postgres |
| **Granularitas** | Per jadwal snapshot | Per interval schedule | Setiap perubahan row |
| **Latency data** | Jam (sesuai jadwal snapshot) | Menit hingga jam | Detik hingga milidetik |
| **Perubahan terlewat** | Ya, jika 2x update dalam 1 interval | Ya, jika 2x update dalam 1 interval | Tidak pernah |
| **Historis lengkap** | Tidak (hanya saat snapshot jalan) | Tidak | Ya, setiap versi ada |
| **Timestamp akurat** | Tidak (waktu snapshot, bukan kejadian) | Tidak | Ya (waktu kejadian exact) |
| **Nilai before/after** | Tidak ada before | Tidak ada before | Ada keduanya |
| **Beban source DB** | Query reguler | Query reguler | Minimal (baca WAL saja) |
| **Kompleksitas setup** | Rendah | Rendah | Menengah-Tinggi |
| **Biaya infra** | Rendah | Rendah | Lebih tinggi (Kafka cluster) |
| **Cocok untuk** | Dimensi berubah lambat | Batch analytics harian | Realtime + historis lengkap |

---

## 8. Mengapa trip_status_logs Tetap Dibutuhkan

Meskipun CDC sudah menangkap setiap perubahan, `trip_status_logs` tetap relevan dan direkomendasikan karena beberapa alasan:

### trip_status_logs adalah Sumber Kebenaran di Level Aplikasi

`trip_status_logs` di-insert oleh **aplikasi backend** pada saat kejadian terjadi — bukan oleh CDC atau dbt. Ini artinya:

```
trip_status_logs                    CDC dari trips table
────────────────────────────────    ────────────────────────────────
Dibuat oleh aplikasi                Dibuat oleh Debezium otomatis
Berisi actor_type (siapa yang       Tidak punya informasi actor
  melakukan perubahan)
Berisi status_ts yang akurat        event_ts = waktu WAL ditulis
Merupakan intent bisnis yang        Side effect dari UPDATE di tabel trips
  eksplisit dicatat
```

### Kombinasi yang Ideal

Dengan keduanya (trip_status_logs + CDC), kamu punya lapisan validasi berlapis:

```sql
-- Validasi: bandingkan event dari aplikasi vs event dari CDC
SELECT
    tsl.trip_id,
    tsl.status_code,
    tsl.status_ts                           AS app_recorded_ts,
    cdc.event_ts                            AS wal_recorded_ts,
    TIMESTAMP_DIFF(cdc.event_ts,
        tsl.status_ts, SECOND)              AS lag_seconds

FROM trip_status_logs tsl
JOIN bronze_cdc.trips_events cdc
    ON  tsl.trip_id    = cdc.after_trip_id
    AND tsl.status_code = cdc.after_trip_status
```

---

## 9. Rekomendasi per Tabel

Tidak perlu semua tabel pakai CDC. Gunakan CDC hanya untuk tabel yang benar-benar butuh historis real-time:

| Tabel | Rekomendasi | Alasan |
|---|---|---|
| `trips` | **CDC** ✅ | Status berubah cepat (detik), historis setiap transisi bernilai tinggi |
| `trip_status_logs` | **CDC** ✅ | Event log high-frequency, append-only, butuh realtime |
| `payments` | **CDC** ✅ | Status pending → success penting ditrack exact waktunya |
| `customers` | **Airflow Batch** ✅ | Berubah lambat (harian), batch lebih dari cukup |
| `drivers` | **Airflow Batch** ✅ | Berubah lambat, batch cukup |
| `vehicles` | **Airflow Batch** ✅ | Sangat jarang berubah |
| `promotions` | **Airflow Batch** ✅ | Berubah lambat, jumlah kecil |
| `promo_redemptions` | **Airflow Batch** ✅ | Append-only, tidak urgent realtime |
| `driver_payouts` | **Airflow Batch** ✅ | Batch harian cukup untuk payroll |
| `ratings` | **Airflow Batch** ✅ | Append-only setelah dibuat, tidak urgent |
| `customer_segments` | **Airflow Batch** ✅ | Snapshot harian memang by design |
| `campaign_spend` | **Airflow Batch** ✅ | Data harian, batch lebih dari cukup |

---

## 10. Arsitektur Hybrid yang Ideal

Gabungkan CDC untuk tabel kritis dan Airflow Batch untuk tabel yang berubah lambat:

```
PostgreSQL (ride_ops_pg)
          │
          ├─── Debezium CDC ──────────────────────────────────────────┐
          │    (trips, trip_status_logs, payments)                    │
          │                                                           ▼
          │                                                    Kafka Topics
          │                                                           │
          │                                                           │ Kafka Connect
          │                                                           │ BigQuery Sink
          │                                                           ▼
          │                                              bronze_cdc dataset
          │                                              (setiap event = 1 baris)
          │                                                           │
          └─── Airflow Batch EL ──────────────────────────────────────┤
               (customers, drivers,                                   │
                vehicles, ratings, dll.)                              │
                          │                                           │
                          ▼                                           │
                  bronze_pg dataset                                   │
                  (nilai terbaru saja)                                │
                          │                                           │
                          └──────────────────┬────────────────────────┘
                                             │
                                             ▼
                                    dbt Transformation
                                             │
                          ┌──────────────────┼──────────────────┐
                          ▼                  ▼                  ▼
                    Silver Dim         Silver Fact          Silver Fact
                    (dari batch)    fct_trip (merge)    fct_trip_timeline
                                    nilai terbaru        durasi antar status
                                                         (dari CDC events)
                                             │
                                             ▼
                                       Gold Data Mart
                                  dm_trip_daily_city
                                  dm_cancel_analysis
                                  dm_finance_daily_city
```

### Contoh Model dbt yang Memanfaatkan CDC

Dengan CDC, kamu bisa membuat `fct_trip_timeline` yang jauh lebih akurat:

```sql
-- models/silver/fact/fct_trip_status_timeline.sql
-- Memanfaatkan CDC events untuk mendapatkan timestamp presisi setiap status

WITH cdc_events AS (
    SELECT
        JSON_VALUE(after, '$.trip_id')      AS trip_id,
        JSON_VALUE(after, '$.trip_status')  AS status_code,
        TIMESTAMP_MILLIS(ts_ms)             AS event_ts,   -- waktu WAL, bukan snapshot
        op
    FROM {{ source('bronze_cdc', 'trips_events') }}
    WHERE op IN ('c', 'u')                 -- insert dan update saja
),

pivoted AS (
    SELECT
        trip_id,
        MIN(CASE WHEN status_code = 'requested'       THEN event_ts END) AS ts_requested,
        MIN(CASE WHEN status_code = 'driver_accepted' THEN event_ts END) AS ts_driver_accepted,
        MIN(CASE WHEN status_code = 'driver_arrived'  THEN event_ts END) AS ts_driver_arrived,
        MIN(CASE WHEN status_code = 'trip_started'    THEN event_ts END) AS ts_trip_started,
        MIN(CASE WHEN status_code = 'completed'       THEN event_ts END) AS ts_trip_completed,
        MIN(CASE WHEN status_code = 'cancelled'       THEN event_ts END) AS ts_trip_cancelled
    FROM cdc_events
    GROUP BY trip_id
)

SELECT
    trip_id,
    ts_requested,
    ts_driver_accepted,
    ts_driver_arrived,
    ts_trip_started,
    ts_trip_completed,
    ts_trip_cancelled,

    -- Durasi presisi antar tahap (dalam detik)
    TIMESTAMP_DIFF(ts_driver_accepted, ts_requested,      SECOND) AS sec_request_to_accepted,
    TIMESTAMP_DIFF(ts_driver_arrived,  ts_driver_accepted,SECOND) AS sec_accepted_to_arrived,
    TIMESTAMP_DIFF(ts_trip_started,    ts_driver_arrived, SECOND) AS sec_arrived_to_started,
    TIMESTAMP_DIFF(ts_trip_completed,  ts_trip_started,   SECOND) AS sec_trip_duration,

    -- Khusus trip cancelled: berapa lama dari accepted ke cancel
    TIMESTAMP_DIFF(ts_trip_cancelled, ts_driver_accepted, SECOND) AS sec_accepted_to_cancelled,

    -- Klasifikasi pola cancel berdasarkan timing yang akurat
    CASE
        WHEN ts_trip_cancelled IS NULL
            THEN NULL
        WHEN ts_driver_arrived IS NOT NULL
             AND TIMESTAMP_DIFF(ts_trip_cancelled, ts_driver_arrived, MINUTE) <= 5
            THEN 'no_show'
        WHEN TIMESTAMP_DIFF(ts_trip_cancelled, ts_driver_accepted, MINUTE) <= 3
            THEN 'early_cancel'
        WHEN TIMESTAMP_DIFF(ts_trip_cancelled, ts_driver_accepted, MINUTE) > 15
            THEN 'long_wait_cancel'
        ELSE 'normal_cancel'
    END AS cancel_pattern

FROM pivoted
```

---

## 11. Ringkasan Keputusan

### Gunakan SCD2 (dbt snapshot) untuk:

```
✅ Dimensi yang berubah lambat (harian/mingguan)
✅ Perlu menjawab "nilai atribut X di waktu T adalah apa?"
✅ Contoh: dim_customer (segment), dim_driver (status aktif/suspended)
✅ Tim tidak perlu infrastruktur tambahan
```

### Gunakan Airflow Batch EL untuk:

```
✅ Tabel yang berubah lambat dan tidak butuh realtime
✅ Analytics dengan latency 1-6 jam masih acceptable
✅ Tim ingin setup yang sederhana
✅ Contoh: customers, drivers, vehicles, promotions
```

### Gunakan CDC Kafka untuk:

```
✅ Tabel yang berubah sangat cepat (detik/menit)
✅ Butuh historis SETIAP perubahan, tidak boleh ada yang terlewat
✅ Butuh nilai "before" dan "after" setiap perubahan
✅ Butuh timestamp presisi saat kejadian terjadi (bukan saat snapshot)
✅ Butuh data realtime di BigQuery (latency < 1 menit)
✅ Contoh: trips, payments, trip_status_logs
```

### Aturan Sederhana

```
Pertanyaan yang ingin dijawab:

"Nilai atribut X pada entitas Y di waktu T adalah apa?"
  → SCD2 (dbt snapshot)

"Kejadian X terjadi jam berapa dengan presisi detik?"
  → Event log (trip_status_logs) + CDC Kafka

"Berapa nilai terbaru dan hanya butuh batch update?"
  → Airflow Batch EL + incremental merge

SCD2 pada fact table transaksi yang berubah cepat:
  → Akan kehilangan transisi antar snapshot
  → Salah menghitung durasi (waktu snapshot ≠ waktu kejadian)
  → Tidak cocok, gunakan event log + CDC sebagai gantinya
```

---

## Referensi Teknologi

| Teknologi | Fungsi | Link |
|---|---|---|
| **Debezium** | CDC connector untuk Postgres, MySQL, MongoDB | https://debezium.io |
| **Apache Kafka** | Message broker untuk streaming events | https://kafka.apache.org |
| **Kafka Connect** | Framework untuk sink/source connector (termasuk BigQuery) | https://docs.confluent.io/kafka-connectors |
| **BigQuery Kafka Connector** | Sink CDC events langsung ke BigQuery | https://github.com/GoogleCloudPlatform/kafka-connector-bigquery |
| **dbt snapshot** | SCD Type 2 untuk dimensi berubah lambat | https://docs.getdbt.com/docs/build/snapshots |