# Ride-Hailing Data Generator

Generator data realistis untuk source system ride-hailing.
Mengisi **PostgreSQL** (`ride_ops_pg`) dan **MySQL** (`ride_marketing_mysql`)
dengan data periode **1 Januari 2026 – 18 Maret 2026**.

---

## Struktur Project

```
ride-hailing-generator/
├── docker-compose.yml
├── postgres/
│   └── init.sql          ← schema PostgreSQL (ride_ops_pg)
├── mysql/
│   └── init.sql          ← schema MySQL (ride_marketing_mysql)
└── generator/
    ├── Dockerfile
    ├── requirements.txt
    └── generate.py       ← script generator utama
```

---

## Cara Menjalankan

```bash
# Clone / copy folder ini, lalu:
docker compose up --build
```

Generator akan:
1. Menunggu hingga PostgreSQL dan MySQL siap (health check).
2. Men-generate data secara in-memory.
3. Menyisipkan data ke kedua database.
4. Exit dengan log ringkasan.

> **Estimasi waktu:** ±2–4 menit tergantung spesifikasi mesin.

---

## Volume Data yang Di-generate

| Tabel | Jumlah (approx.) |
|---|---|
| `customers` | 350 |
| `drivers` | 110 |
| `vehicles` | 110 |
| `trips` | 13.500 |
| `trip_status_logs` | ~54.000 |
| `payments` | ~9.700 |
| `driver_payouts` | ~9.700 |
| `ratings` | ~7.600 |
| `promotions` | 12 |
| `promo_redemptions` | ~1.940 |
| `customer_segments` | ~10.780 (77 hari × 40% customer) |
| `campaign_spend` | ~350 |

---

## Logika Data (Business Rules)

### Trip Lifecycle
```
request_ts  →  [driver_accepted]  →  pickup_ts  →  dropoff_ts
```
- **completed** (72%): semua timestamp terisi, ada payment + payout + rating
- **cancelled** (22%): hanya `request_ts`, memiliki `cancel_reason`
- **no_show** (6%): driver datang tapi penumpang tidak ada

### Distribusi Kota
| Kota | Bobot |
|---|---|
| Jakarta | 55% |
| Surabaya | 18% |
| Bandung | 13% |
| Medan | 8% |
| Bali | 6% |

### Pola Jam Ramai
Peak jam **07–09** (pagi), **12–14** (siang), **17–20** (sore).

### Harga Trip (IDR)
- Base fare: Rp 4.000–9.000
- Per km: Rp 2.500–4.500
- Surge: 1.0× (60%) / 1.2× (22%) / 1.5× (13%) / 2.0× (5%)
- PPN 11% ditambahkan ke payment

### Payment
- GoPay (30%) / OVO (25%) / DANA (15%) / kartu kredit (10%) / kartu debit (8%) / tunai (12%)
- Success rate: 90%+

### Driver Payout
- Base earning: 70–80% dari actual fare
- Incentive + bonus: berdasarkan performa
- Status: paid (91%) / pending (7%) / failed (2%)

### Promo
- 12 promo aktif selama periode
- ~20% trip completed menggunakan promo
- Diskon: fixed IDR atau persentase dengan cap

### Growth Trend
Distribusi trip condong ke akhir periode (bisnis makin berkembang) —
menggunakan distribusi triangular pada pemilihan hari.

---

## Koneksi Database

### PostgreSQL
```
Host:     localhost:5432
Database: ride_ops_pg
User:     postgres
Password: postgres
```

### MySQL
```
Host:     localhost:3306
Database: ride_marketing_mysql
User:     root
Password: root
```

---

## Menjalankan Ulang Generator

Jika ingin generate ulang (data lama akan di-skip karena `ON CONFLICT DO NOTHING` / `INSERT IGNORE`):

```bash
docker compose run --rm generator
```

Atau hapus volume dan mulai dari awal:

```bash
docker compose down -v
docker compose up --build
```

---

## Koneksi ke Airflow

Tambahkan connection di Airflow UI:

**PostgreSQL:**
- Conn Id: `postgres_ride_ops`
- Conn Type: Postgres
- Host: `ride_ops_pg` (nama container)
- Schema: `ride_ops_pg`
- Login: `postgres`
- Password: `postgres`
- Port: `5432`

**MySQL:**
- Conn Id: `mysql_ride_marketing`
- Conn Type: MySQL
- Host: `ride_marketing_mysql`
- Schema: `ride_marketing_mysql`
- Login: `root`
- Password: `root`
- Port: `3306`

> Pastikan Airflow berada dalam Docker network yang sama:
> tambahkan `networks` di `docker-compose.yml` atau gunakan `--network host`.
