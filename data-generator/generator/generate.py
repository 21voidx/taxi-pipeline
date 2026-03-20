#!/usr/bin/env python3
"""
=============================================================
Ride-Hailing Data Generator
=============================================================
Mengisi PostgreSQL (ride_ops_pg) dan MySQL (ride_marketing_mysql)
dengan data realistis periode 1 Jan 2026 – 18 Mar 2026.

Logika data:
  - Customers & drivers sebagian besar sudah ada sebelum Jan 2026
    (established business), sebagian kecil registrasi selama periode.
  - Trips dibuat dengan pola jam sibuk (pagi/siang/sore) dan
    distribusi kota sesuai bobot populasi.
  - Status trip: completed (72%) → cancelled (22%) → no_show (6%)
  - Hanya trip COMPLETED yang memiliki payment, payout, rating.
  - ~20% trip completed menggunakan promo.
  - customer_segments di-snapshot harian (seperti batch job CRM).
  - campaign_spend harian per channel marketing.
=============================================================
"""

import os
import math
import random
import uuid
import time
import logging
from datetime import datetime, date, timedelta

import psycopg2
import psycopg2.extras
import mysql.connector
from mysql.connector import errorcode

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIG  (bisa di-override via environment variable)
# ─────────────────────────────────────────────────────────────
PG_CONFIG = dict(
    host=os.getenv("PG_HOST", "postgres-ops"),
    port=int(os.getenv("PG_PORT", 5432)),
    dbname=os.getenv("PG_DB", "ride_ops_pg"),
    user=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASSWORD", "postgres"),
)

MY_CONFIG = dict(
    host=os.getenv("MYSQL_HOST", "mysql"),
    port=int(os.getenv("MYSQL_PORT", 3306)),
    database=os.getenv("MYSQL_DB", "ride_marketing_mysql"),
    user=os.getenv("MYSQL_USER", "mysql"),
    password=os.getenv("MYSQL_PASSWORD", "mysql"),
)

START_DT = datetime(2026, 1, 1, 0, 0, 0)
END_DT   = datetime(2026, 3, 18, 23, 59, 59)
TOTAL_DAYS = (END_DT.date() - START_DT.date()).days + 1  # 77 hari

RANDOM_SEED  = 42
N_CUSTOMERS  = 350
N_DRIVERS    = 110
N_TRIPS      = 13_500

# ─────────────────────────────────────────────────────────────
# KOTA & AREA
# ─────────────────────────────────────────────────────────────
CITIES = {
    "Jakarta": {
        "weight": 0.55,
        "lat": (-6.35, -6.10),
        "lng": (106.70, 106.97),
        "areas": [
            "Sudirman", "Kuningan", "Kemang", "Senayan", "Blok M",
            "Grogol", "Kelapa Gading", "Cibubur", "Bekasi Barat",
            "Tangerang Selatan", "Menteng", "Tebet", "Mampang Prapatan",
            "Cikini", "Harmoni", "Mangga Dua", "Pluit", "Pantai Indah Kapuk",
            "BSD City", "Alam Sutera", "Kalideres", "Tanjung Priok",
        ],
    },
    "Surabaya": {
        "weight": 0.18,
        "lat": (-7.37, -7.15),
        "lng": (112.63, 112.83),
        "areas": [
            "Gubeng", "Genteng", "Wonokromo", "Sukolilo", "Mulyorejo",
            "Rungkut", "Tenggilis Mejoyo", "Wiyung", "Lakarsantri", "Tandes",
        ],
    },
    "Bandung": {
        "weight": 0.13,
        "lat": (-6.98, -6.85),
        "lng": (107.55, 107.70),
        "areas": [
            "Dago", "Buah Batu", "Antapani", "Cicendo", "Coblong",
            "Sumur Bandung", "Regol", "Lengkong", "Batununggal", "Arcamanik",
        ],
    },
    "Medan": {
        "weight": 0.08,
        "lat": (3.50, 3.70),
        "lng": (98.60, 98.82),
        "areas": [
            "Medan Kota", "Medan Baru", "Medan Sunggal",
            "Medan Petisah", "Medan Helvetia", "Medan Denai",
        ],
    },
    "Bali": {
        "weight": 0.06,
        "lat": (-8.75, -8.55),
        "lng": (115.10, 115.32),
        "areas": [
            "Kuta", "Seminyak", "Ubud", "Sanur",
            "Nusa Dua", "Denpasar", "Canggu", "Jimbaran",
        ],
    },
}

CITY_LIST    = list(CITIES.keys())
CITY_WEIGHTS = [CITIES[c]["weight"] for c in CITY_LIST]

# ─────────────────────────────────────────────────────────────
# MASTER DATA  (nama, kendaraan, dll.)
# ─────────────────────────────────────────────────────────────
MALE_FIRST = [
    "Budi", "Andi", "Rudi", "Deni", "Hendra", "Fajar", "Rizky",
    "Ahmad", "Muhammad", "Wahyu", "Joko", "Slamet", "Agus", "Bambang",
    "Surya", "Hadi", "Eko", "Yusuf", "Irfan", "Bagas", "Gilang",
    "Dimas", "Reza", "Arif", "Dian", "Rian", "Fandi", "Lutfi",
]
FEMALE_FIRST = [
    "Siti", "Dewi", "Rina", "Linda", "Yuni", "Nadia", "Putri",
    "Sri", "Ani", "Wati", "Indah", "Maya", "Lestari", "Desi",
    "Fitri", "Ayu", "Ratna", "Nurul", "Hesti", "Dwi", "Tuti",
    "Wulandari", "Citra", "Mega", "Rini", "Lia", "Dina",
]
LAST_NAMES = [
    "Santoso", "Wijaya", "Setiawan", "Purnomo", "Susanto", "Wibowo",
    "Pratama", "Hidayat", "Rahmad", "Saputra", "Kurniawan", "Nugroho",
    "Utomo", "Haryanto", "Sari", "Fitriani", "Rahayu", "Astuti",
    "Kusuma", "Permata", "Prasetyo", "Sulistyo", "Andriani", "Firmansyah",
]

VEHICLE_CATALOG = {
    "economy": [
        ("Toyota", "Avanza", 7), ("Toyota", "Rush", 7), ("Honda", "Brio", 5),
        ("Daihatsu", "Xenia", 7), ("Daihatsu", "Sigra", 7),
        ("Suzuki", "Ertiga", 7), ("Mitsubishi", "Expander", 7),
        ("Honda", "Mobilio", 7),
    ],
    "standard": [
        ("Toyota", "Innova Reborn", 7), ("Honda", "HRV", 5),
        ("Hyundai", "Tucson", 5), ("Suzuki", "XL7", 7),
        ("Toyota", "Corolla Cross", 5), ("Nissan", "Terra", 7),
    ],
    "premium": [
        ("Toyota", "Alphard", 7), ("BMW", "520i", 5),
        ("Mercedes-Benz", "E300", 5), ("Lexus", "ES300h", 5),
        ("Volvo", "S90", 5), ("Toyota", "Vellfire", 7),
    ],
    "motor": [
        ("Honda", "Beat", 2), ("Honda", "Vario 160", 2),
        ("Yamaha", "NMAX 160", 2), ("Yamaha", "Mio M3", 2),
        ("Suzuki", "Address", 2), ("Honda", "Scoopy", 2),
    ],
}
VEHICLE_TYPE_WEIGHTS = [0.38, 0.32, 0.10, 0.20]

PAYMENT_METHODS  = ["gopay", "ovo", "dana", "credit_card", "debit_card", "cash"]
PAYMENT_WEIGHTS  = [0.30, 0.25, 0.15, 0.10, 0.08, 0.12]

REVIEWS = [
    "Driver sangat ramah dan tepat waktu",
    "Perjalanan nyaman, sopir profesional",
    "Mantap, akan order lagi",
    "Lumayan, tapi agak lama menunggunya",
    "Driver tidak tahu jalan dengan baik",
    "Mobil bersih dan nyaman",
    "Pelayanan sangat memuaskan",
    "Cukup oke",
    "Bisa lebih cepat lagi",
    "Recommended!",
    "Sopir ramah, mobil harum",
    "Agak susah menemukan lokasi penjemputan",
    "Perjalanan lancar, terima kasih",
    "Driver sigap, komunikasi baik",
    None, None, None,  # beberapa trip tidak meninggalkan review
]

CANCEL_REASONS = [
    "Driver terlalu jauh",
    "Ganti rencana perjalanan",
    "Menemukan transportasi lain",
    "Driver terlalu lama datang",
    "Lokasi penjemputan salah",
    "Harga terlalu mahal",
    "Salah pilih tipe kendaraan",
]

# ─────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────

def rand_dt(start: datetime, end: datetime) -> datetime:
    """Random datetime dalam rentang [start, end]."""
    delta_sec = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta_sec))


def weighted_hour() -> int:
    """Jam dengan bobot pola perjalanan: pagi / siang / sore lebih ramai."""
    r = random.random()
    if r < 0.06:   return random.randint(5, 6)    # subuh
    elif r < 0.22: return random.randint(7, 9)    # peak pagi
    elif r < 0.38: return random.randint(9, 12)   # menjelang siang
    elif r < 0.54: return random.randint(12, 14)  # jam makan siang
    elif r < 0.64: return random.randint(14, 17)  # siang biasa
    elif r < 0.84: return random.randint(17, 20)  # peak sore
    else:          return random.randint(20, 23)  # malam


def make_name(gender: str) -> str:
    first = random.choice(MALE_FIRST if gender == "M" else FEMALE_FIRST)
    last  = random.choice(LAST_NAMES)
    return f"{first} {last}"


def make_phone() -> str:
    prefix = random.choice(["0811","0812","0813","0821","0822","0851","0852","0878","0896","0897"])
    return prefix + "".join(str(random.randint(0, 9)) for _ in range(7))


def make_email(name: str) -> str:
    parts  = name.lower().replace(" ", "")
    sep    = random.choice(["", ".", "_"])
    domain = random.choice(["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"])
    suffix = str(random.randint(1, 999)) if random.random() < 0.45 else ""
    return f"{parts[:8]}{sep}{suffix}@{domain}"


def rand_coords(city_name: str):
    c    = CITIES[city_name]
    lat  = round(random.uniform(*c["lat"]), 6)
    lng  = round(random.uniform(*c["lng"]), 6)
    area = random.choice(c["areas"])
    return lat, lng, area


def haversine_km(lat1, lng1, lat2, lng2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a    = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2)**2
    return 2 * R * math.asin(math.sqrt(a))


def round_idr(amount: float, nearest: int = 500) -> float:
    """Bulatkan ke nearest IDR (misal 500 atau 1000)."""
    return round(amount / nearest) * nearest


# ─────────────────────────────────────────────────────────────
# WAIT FOR DB
# ─────────────────────────────────────────────────────────────

def wait_for_pg(cfg, retries=40, delay=4):
    log.info("Menunggu PostgreSQL siap...")
    for i in range(retries):
        try:
            conn = psycopg2.connect(**cfg)
            conn.close()
            log.info("PostgreSQL ✓")
            return
        except Exception as e:
            log.info(f"  Retry {i+1}/{retries}: {e}")
            time.sleep(delay)
    raise RuntimeError("PostgreSQL tidak siap setelah timeout.")


def wait_for_mysql(cfg, retries=40, delay=4):
    log.info("Menunggu MySQL siap...")
    for i in range(retries):
        try:
            conn = mysql.connector.connect(**cfg)
            conn.close()
            log.info("MySQL ✓")
            return
        except Exception as e:
            log.info(f"  Retry {i+1}/{retries}: {e}")
            time.sleep(delay)
    raise RuntimeError("MySQL tidak siap setelah timeout.")


# ─────────────────────────────────────────────────────────────
# DATA GENERATORS
# ─────────────────────────────────────────────────────────────

def gen_customers(n: int) -> list[dict]:
    """300+ customers. 80% registrasi sebelum Jan 2026 (existing users)."""
    log.info(f"  Generating {n} customers...")
    rows = []
    for _ in range(n):
        gender  = random.choice(["M", "F"])
        name    = make_name(gender)
        # 80% existing, 20% new user selama periode
        if random.random() < 0.80:
            created = rand_dt(datetime(2022, 6, 1), datetime(2025, 12, 28))
        else:
            created = rand_dt(START_DT, END_DT - timedelta(days=1))
        updated = created + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
        birth_y = random.randint(1975, 2004)
        rows.append({
            "customer_id":  str(uuid.uuid4()),
            "full_name":    name,
            "phone_number": make_phone(),
            "email":        make_email(name),
            "gender":       "male" if gender == "M" else "female",
            "birth_date":   date(birth_y, random.randint(1, 12), random.randint(1, 28)),
            "created_at":   created,
            "updated_at":   updated if updated <= END_DT else END_DT,
        })
    return rows


def gen_drivers(n: int) -> list[dict]:
    """Driver dengan distribusi kota sesuai bobot."""
    log.info(f"  Generating {n} drivers...")
    rows = []
    city_pool = random.choices(CITY_LIST, weights=CITY_WEIGHTS, k=n)
    for i in range(n):
        gender  = random.choices(["M", "F"], weights=[0.88, 0.12])[0]
        name    = make_name(gender)
        # 85% driver sudah bergabung sebelum Jan 2026
        if random.random() < 0.85:
            created = rand_dt(datetime(2022, 1, 1), datetime(2025, 12, 28))
        else:
            created = rand_dt(START_DT, END_DT - timedelta(days=10))
        updated = created + timedelta(days=random.randint(0, 60))
        # Sebagian besar aktif; sedikit inactive/suspended
        status = random.choices(
            ["active", "active", "active", "inactive", "suspended"],
            weights=[0.76, 0.05, 0.04, 0.10, 0.05],
        )[0]
        rows.append({
            "driver_id":      str(uuid.uuid4()),
            "full_name":      name,
            "phone_number":   make_phone(),
            "email":          make_email(name),
            "license_number": f"SIM-{random.randint(100000, 999999)}",
            "driver_status":  status,
            "join_date":      created.date(),
            "city":           city_pool[i],
            "created_at":     created,
            "updated_at":     updated if updated <= END_DT else END_DT,
        })
    return rows


def gen_vehicles(drivers: list[dict]) -> list[dict]:
    """1 kendaraan per driver. Tipe sesuai distribusi pasar."""
    log.info(f"  Generating {len(drivers)} vehicles...")
    rows = []
    used_plates = set()
    for driver in drivers:
        vtype_key = random.choices(
            list(VEHICLE_CATALOG.keys()),
            weights=VEHICLE_TYPE_WEIGHTS,
        )[0]
        brand, model, seat = random.choice(VEHICLE_CATALOG[vtype_key])
        year = random.randint(2017, 2024)

        # Buat nomor polisi unik
        for _ in range(10):
            prefix = random.choice(["B", "D", "L", "BK", "DK", "F", "G"])
            digits = random.randint(1000, 9999)
            suffix = "".join(random.choices("ABCDEFGHJKLMNPRSTUVWXYZ", k=2))
            plate  = f"{prefix} {digits} {suffix}"
            if plate not in used_plates:
                used_plates.add(plate)
                break

        created = driver["created_at"] + timedelta(days=random.randint(1, 14))
        rows.append({
            "vehicle_id":      str(uuid.uuid4()),
            "driver_id":       driver["driver_id"],
            "plate_number":    plate,
            "vehicle_type":    vtype_key,
            "brand":           brand,
            "model":           model,
            "production_year": year,
            "seat_capacity":   seat,
            "created_at":      created,
            "updated_at":      created,
        })
    return rows


def gen_trips(
    customers: list[dict],
    drivers: list[dict],
    vehicles: list[dict],
    n: int,
) -> list[dict]:
    """Trip dengan logika urutan waktu yang konsisten."""
    log.info(f"  Generating {n} trips...")

    active_drivers   = [d for d in drivers if d["driver_status"] == "active"]
    driver_to_vehicle = {v["driver_id"]: v for v in vehicles}

    # Pra-hitung bobot hari: bisnis makin ramai (growth trend)
    # Distribusi triangular condong ke akhir periode
    rows = []
    for _ in range(n):
        day_idx   = int(random.triangular(0, TOTAL_DAYS - 1, TOTAL_DAYS - 1))
        trip_date = START_DT.date() + timedelta(days=day_idx)

        hour      = weighted_hour()
        minute    = random.randint(0, 59)
        second    = random.randint(0, 59)
        request_ts = datetime(trip_date.year, trip_date.month, trip_date.day, hour, minute, second)
        if request_ts > END_DT:
            request_ts = END_DT - timedelta(minutes=random.randint(30, 120))

        customer = random.choice(customers)
        driver   = random.choice(active_drivers)
        vehicle  = driver_to_vehicle.get(driver["driver_id"])
        if not vehicle:
            continue

        city = driver["city"]
        p_lat, p_lng, p_area = rand_coords(city)
        d_lat, d_lng, d_area = rand_coords(city)
        # Pastikan pickup ≠ dropoff area
        for _ in range(5):
            if p_area != d_area:
                break
            d_lat, d_lng, d_area = rand_coords(city)

        # Jarak & ongkos (IDR)
        raw_dist = haversine_km(p_lat, p_lng, d_lat, d_lng)
        # Faktor jalan raya lebih berkelok (road factor 1.2–1.5)
        est_dist = round(max(raw_dist * random.uniform(1.20, 1.50), 1.0), 2)

        base_fare   = random.randint(4_000, 9_000)
        per_km_rate = random.randint(2_500, 4_500)
        surge       = random.choices([1.0, 1.2, 1.5, 2.0], weights=[0.60, 0.22, 0.13, 0.05])[0]
        est_fare    = round_idr((base_fare + est_dist * per_km_rate) * surge, nearest=1_000)

        status = random.choices(
            ["completed", "cancelled", "no_show"],
            weights=[0.72, 0.22, 0.06],
        )[0]

        # Waktu tunggu driver: 3–18 menit
        wait_min = random.randint(3, 18)
        pickup_ts  = request_ts + timedelta(minutes=wait_min) if status != "no_show" else None
        dropoff_ts = None
        act_dist   = None
        act_fare   = None
        cancel_rsn = None

        if status == "completed":
            act_dist   = round(est_dist * random.uniform(0.88, 1.18), 2)
            trip_min   = max(int(act_dist * random.uniform(3.5, 6.5)), 3)
            dropoff_ts = pickup_ts + timedelta(minutes=trip_min)
            act_fare   = round_idr(est_fare * random.uniform(0.92, 1.08), nearest=500)
            if dropoff_ts > END_DT:
                dropoff_ts = END_DT
        elif status == "cancelled":
            cancel_rsn = random.choice(CANCEL_REASONS)
        else:  # no_show
            cancel_rsn = "Penumpang tidak kunjung muncul"

        created_at = request_ts
        updated_at = dropoff_ts or (request_ts + timedelta(minutes=random.randint(5, 25)))
        if updated_at > END_DT:
            updated_at = END_DT

        rows.append({
            "trip_id":               str(uuid.uuid4()),
            "customer_id":           customer["customer_id"],
            "driver_id":             driver["driver_id"],
            "vehicle_id":            vehicle["vehicle_id"],
            "request_ts":            request_ts,
            "pickup_ts":             pickup_ts,
            "dropoff_ts":            dropoff_ts,
            "pickup_lat":            p_lat,
            "pickup_lng":            p_lng,
            "dropoff_lat":           d_lat,
            "dropoff_lng":           d_lng,
            "pickup_area":           p_area,
            "dropoff_area":          d_area,
            "city":                  city,
            "estimated_distance_km": est_dist,
            "actual_distance_km":    act_dist,
            "estimated_fare":        est_fare,
            "actual_fare":           act_fare,
            "surge_multiplier":      surge,
            "trip_status":           status,
            "cancel_reason":         cancel_rsn,
            "created_at":            created_at,
            "updated_at":            updated_at,
        })

    return rows


def gen_trip_status_logs(trips: list[dict]) -> list[dict]:
    """
    Setiap trip memiliki log status sesuai alurnya:
      completed  → requested → driver_accepted → driver_arrived → trip_started → trip_completed
      cancelled  → requested → driver_accepted → trip_cancelled
      no_show    → requested → driver_accepted → driver_arrived → trip_cancelled
    """
    STATUS_FLOWS = {
        "completed": [
            "requested", "driver_accepted", "driver_arrived",
            "trip_started", "trip_completed",
        ],
        "cancelled": ["requested", "driver_accepted", "trip_cancelled"],
        "no_show":   ["requested", "driver_accepted", "driver_arrived", "trip_cancelled"],
    }
    ACTOR_MAP = {
        "requested":      "system",
        "driver_accepted":"driver",
        "driver_arrived": "driver",
        "trip_started":   "driver",
        "trip_completed": "system",
        "trip_cancelled": None,  # acak
    }
    rows = []
    for trip in trips:
        flow = STATUS_FLOWS.get(trip["trip_status"], ["requested"])
        ts   = trip["request_ts"]

        for i, status_code in enumerate(flow):
            if status_code == "driver_accepted":
                ts = ts + timedelta(seconds=random.randint(30, 120))
            elif status_code == "driver_arrived" and trip["pickup_ts"]:
                ts = trip["pickup_ts"] - timedelta(seconds=random.randint(30, 90))
            elif status_code == "trip_started" and trip["pickup_ts"]:
                ts = trip["pickup_ts"]
            elif status_code == "trip_completed" and trip["dropoff_ts"]:
                ts = trip["dropoff_ts"]
            elif status_code == "trip_cancelled":
                ts = ts + timedelta(minutes=random.randint(1, 8))

            actor = ACTOR_MAP.get(status_code)
            if actor is None:
                actor = random.choice(["customer", "driver", "system"])

            rows.append({
                "trip_status_log_id": str(uuid.uuid4()),
                "trip_id":            trip["trip_id"],
                "status_code":        status_code,
                "status_ts":          ts,
                "actor_type":         actor,
                "created_at":         ts,
            })
    return rows


def gen_payments(trips: list[dict]) -> list[dict]:
    """Payment hanya untuk trip completed."""
    log.info("  Generating payments...")
    rows = []
    for trip in trips:
        if trip["trip_status"] != "completed":
            continue

        method   = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]
        gross    = trip["actual_fare"]
        tax      = round_idr(gross * 0.11, nearest=500)   # PPN 11%
        toll     = random.choices([0, 0, 0, 5_000, 10_000, 15_000, 20_000],
                                  weights=[0.50, 0.10, 0.10, 0.12, 0.10, 0.05, 0.03])[0]
        tip      = random.choices([0, 0, 0, 2_000, 5_000, 10_000],
                                  weights=[0.50, 0.15, 0.10, 0.12, 0.09, 0.04])[0]
        discount = 0  # akan di-update oleh promo_redemption jika ada promo
        net      = gross + tax + toll + tip - discount

        # Mayoritas sukses; kecil kemungkinan gagal/pending
        p_status = random.choices(
            ["success", "success", "success", "failed", "refunded", "pending"],
            weights=[0.90, 0.02, 0.02, 0.03, 0.02, 0.01],
        )[0]

        paid_ts  = trip["dropoff_ts"] + timedelta(seconds=random.randint(15, 180))

        rows.append({
            "payment_id":     str(uuid.uuid4()),
            "trip_id":        trip["trip_id"],
            "customer_id":    trip["customer_id"],
            "payment_method": method,
            "payment_status": p_status,
            "gross_amount":   gross,
            "discount_amount":discount,
            "tax_amount":     tax,
            "toll_amount":    toll,
            "tip_amount":     tip,
            "net_amount":     net,
            "paid_ts":        paid_ts,
            "created_at":     paid_ts,
            "updated_at":     paid_ts,
        })
    return rows


def gen_driver_payouts(trips: list[dict]) -> list[dict]:
    """Payout driver hanya untuk trip completed."""
    log.info("  Generating driver payouts...")
    rows = []
    for trip in trips:
        if trip["trip_status"] != "completed":
            continue

        fare      = trip["actual_fare"]
        # Driver dapat 70–80% dari actual fare sebagai base earning
        base      = round_idr(fare * random.uniform(0.70, 0.80), nearest=500)
        incentive = random.choices(
            [0, 0, 5_000, 10_000, 20_000, 30_000],
            weights=[0.48, 0.12, 0.18, 0.12, 0.07, 0.03],
        )[0]
        bonus     = random.choices(
            [0, 0, 0, 10_000, 25_000, 50_000],
            weights=[0.60, 0.10, 0.10, 0.11, 0.06, 0.03],
        )[0]
        deduction = random.choices(
            [0, 0, 1_000, 2_000, 5_000],
            weights=[0.72, 0.10, 0.10, 0.05, 0.03],
        )[0]
        final     = base + incentive + bonus - deduction

        # Payout T+0 sampai T+2 hari setelah trip
        payout_date = trip["dropoff_ts"].date() + timedelta(days=random.randint(0, 2))
        created_at  = trip["dropoff_ts"] + timedelta(hours=random.randint(1, 6))
        updated_at  = created_at + timedelta(hours=random.randint(1, 18))

        rows.append({
            "payout_id":           str(uuid.uuid4()),
            "driver_id":           trip["driver_id"],
            "trip_id":             trip["trip_id"],
            "payout_date":         payout_date,
            "base_earning":        base,
            "incentive_amount":    incentive,
            "bonus_amount":        bonus,
            "deduction_amount":    deduction,
            "final_payout_amount": final,
            "payout_status": random.choices(
                ["paid", "pending", "failed"],
                weights=[0.91, 0.07, 0.02],
            )[0],
            "created_at": created_at,
            "updated_at": updated_at,
        })
    return rows


def gen_ratings(trips: list[dict]) -> list[dict]:
    """Rating hanya untuk trip completed, ~78% dari mereka memberi rating."""
    log.info("  Generating ratings...")
    rows = []
    for trip in trips:
        if trip["trip_status"] != "completed":
            continue
        if random.random() > 0.78:
            continue

        score   = random.choices([1, 2, 3, 4, 5], weights=[0.02, 0.04, 0.09, 0.25, 0.60])[0]
        rated_ts = trip["dropoff_ts"] + timedelta(minutes=random.randint(2, 120))

        rows.append({
            "rating_id":    str(uuid.uuid4()),
            "trip_id":      trip["trip_id"],
            "customer_id":  trip["customer_id"],
            "driver_id":    trip["driver_id"],
            "rating_score": score,
            "review_text":  random.choice(REVIEWS),
            "rated_ts":     rated_ts,
            "created_at":   rated_ts,
            "updated_at":   rated_ts,
        })
    return rows


# ─── MySQL generators ──────────────────────────────────────────

def gen_promotions() -> list[dict]:
    """12 promo dengan berbagai tipe dan periode."""
    log.info("  Generating promotions...")
    catalog = [
        # (code, name, ptype, dtype, dval, start, end)
        ("NEWUSER26",   "New User Special Jan 2026",        "new_user",     "percentage", 50,     "2026-01-01", "2026-01-31"),
        ("WEEKEND10K",  "Weekend Hemat 10K",                "general",      "fixed",      10_000, "2026-01-01", "2026-03-31"),
        ("DOMPETDIGITAL","Cashback Dompet Digital 20%",     "payment_method","percentage",20,     "2026-01-15", "2026-02-28"),
        ("HEMAT20FEB",  "Hemat 20% Sepanjang Februari",     "general",      "percentage", 20,     "2026-02-01", "2026-02-28"),
        ("RAMADAN26",   "Promo Spesial Ramadan 2026",       "seasonal",     "percentage", 30,     "2026-02-15", "2026-03-18"),
        ("FLASHSALE50", "Flash Sale 50% – Satu Hari Saja",  "flash",        "percentage", 50,     "2026-01-20", "2026-01-21"),
        ("MALAMHEMAT",  "Promo Perjalanan Malam",           "time_based",   "fixed",      15_000, "2026-01-01", "2026-03-31"),
        ("PREMIUM15",   "Diskon Mobil Premium 15%",         "vehicle_type", "percentage", 15,     "2026-01-01", "2026-03-31"),
        ("REFERJOY",    "Referral Teman Dapat Bonus",       "referral",     "fixed",      5_000,  "2026-01-01", "2026-03-31"),
        ("BDAY25",      "Diskon Ulang Tahun 25%",           "birthday",     "percentage", 25,     "2026-01-01", "2026-03-31"),
        ("JAKFEST26",   "Jakarta City Festival Feb",        "city",         "fixed",      20_000, "2026-02-01", "2026-02-14"),
        ("EARLYBIRD10", "Early Bird Pagi Hari 10%",         "time_based",   "percentage", 10,     "2026-01-01", "2026-03-31"),
    ]
    rows = []
    for code, name, ptype, dtype, dval, start_s, end_s in catalog:
        start_d = date.fromisoformat(start_s)
        end_d   = date.fromisoformat(end_s)
        created = datetime(2025, 12, random.randint(1, 28), random.randint(8, 18))
        status  = "active" if end_d >= date(2026, 3, 18) else "expired"
        rows.append({
            "promo_id":      str(uuid.uuid4()),
            "promo_code":    code,
            "promo_name":    name,
            "promo_type":    ptype,
            "discount_type": dtype,
            "discount_value":dval,
            "start_date":    start_d,
            "end_date":      end_d,
            "budget_amount": random.randint(10, 200) * 1_000_000,
            "promo_status":  status,
            "created_at":    created,
            "updated_at":    created + timedelta(hours=random.randint(1, 24)),
        })
    return rows


def gen_promo_redemptions(
    trips: list[dict],
    promos: list[dict],
) -> list[dict]:
    """~20% trip completed menggunakan promo yang berlaku di tanggal itu."""
    log.info("  Generating promo redemptions...")
    rows = []
    for trip in trips:
        if trip["trip_status"] != "completed":
            continue
        if random.random() > 0.20:
            continue

        trip_date = trip["dropoff_ts"].date()
        eligible  = [
            p for p in promos
            if p["start_date"] <= trip_date <= p["end_date"]
        ]
        if not eligible:
            continue

        promo = random.choice(eligible)
        fare  = trip["actual_fare"]

        if promo["discount_type"] == "percentage":
            raw_disc = fare * promo["discount_value"] / 100
            # Cap diskon maksimal per tipe promo
            cap = {
                "new_user": 50_000, "general": 30_000, "payment_method": 25_000,
                "seasonal": 40_000, "flash": 75_000, "time_based": 20_000,
                "vehicle_type": 35_000, "referral": 10_000, "birthday": 30_000,
                "city": 25_000,
            }.get(promo["promo_type"], 30_000)
            disc = round_idr(min(raw_disc, cap), nearest=500)
        else:
            disc = promo["discount_value"]

        redeemed_ts = trip["dropoff_ts"] - timedelta(minutes=random.randint(1, 10))

        rows.append({
            "redemption_id":    str(uuid.uuid4()),
            "promo_id":         promo["promo_id"],
            "customer_id":      trip["customer_id"],
            "trip_id":          trip["trip_id"],
            "redeemed_ts":      redeemed_ts,
            "discount_amount":  disc,
            "redemption_status":"success",
            "created_at":       redeemed_ts,
            "updated_at":       redeemed_ts,
        })
    return rows


def gen_customer_segments(customers: list[dict]) -> list[dict]:
    """
    Snapshot harian CRM: setiap hari di-snapshot ~40% customer.
    Segment bisa berubah dari waktu ke waktu.
    """
    log.info(f"  Generating customer_segments ({TOTAL_DAYS} hari × ~40% customer)...")
    SEGMENTS = ["platinum", "gold", "silver", "bronze", "new"]
    WEIGHTS  = [0.05, 0.15, 0.25, 0.30, 0.25]
    SCORE_RANGE = {
        "platinum": (85, 100),
        "gold":     (65, 85),
        "silver":   (45, 65),
        "bronze":   (25, 45),
        "new":      (5,  25),
    }
    rows = []
    for day_idx in range(TOTAL_DAYS):
        snap_date = START_DT.date() + timedelta(days=day_idx)
        sampled   = random.sample(customers, k=int(len(customers) * 0.40))
        snap_ts   = datetime(snap_date.year, snap_date.month, snap_date.day,
                             random.randint(1, 4), random.randint(0, 59))
        for cust in sampled:
            seg   = random.choices(SEGMENTS, weights=WEIGHTS)[0]
            lo, hi = SCORE_RANGE[seg]
            score  = round(random.uniform(lo, hi), 2)
            rows.append({
                "customer_id":  cust["customer_id"],
                "segment_name": seg,
                "segment_score":score,
                "snapshot_date":snap_date,
                "created_at":   snap_ts,
                "updated_at":   snap_ts,
            })
    return rows


def gen_campaign_spend() -> list[dict]:
    """Daily spend per channel dari Jan 1 s/d Mar 18."""
    log.info(f"  Generating campaign_spend ({TOTAL_DAYS} hari × beberapa channel)...")
    CHANNELS = {
        "google_ads":  {
            "campaigns": ["Brand Search Q1 2026", "Competitor Conquest", "Remarketing App Users"],
            "base_range": (5_000_000, 30_000_000),
        },
        "meta_ads": {
            "campaigns": ["Awareness Jan 2026", "Conversion Feb 2026", "Retargeting Q1"],
            "base_range": (3_000_000, 20_000_000),
        },
        "tiktok_ads": {
            "campaigns": ["Gen Z Acquisition", "Viral Challenge Q1"],
            "base_range": (2_000_000, 15_000_000),
        },
        "influencer": {
            "campaigns": ["Mega Influencer Q1", "Nano Influencer Network"],
            "base_range": (5_000_000, 50_000_000),
        },
        "sms_blast": {
            "campaigns": ["Weekly Promo Blast", "Retention SMS"],
            "base_range": (1_000_000, 5_000_000),
        },
        "email_marketing": {
            "campaigns": ["Newsletter Jan", "Re-engagement Q1", "Newsletter Feb"],
            "base_range": (500_000, 2_000_000),
        },
        "tv_spot": {
            "campaigns": ["Q1 Brand TVC"],
            "base_range": (50_000_000, 200_000_000),
        },
    }
    rows = []
    for day_idx in range(TOTAL_DAYS):
        spend_date = START_DT.date() + timedelta(days=day_idx)
        is_weekend = spend_date.weekday() >= 5  # Sabtu/Minggu

        for channel, meta in CHANNELS.items():
            # ~15% chance channel tidak aktif hari itu
            if random.random() < 0.15:
                continue
            # TV spot hanya sekali seminggu
            if channel == "tv_spot" and spend_date.weekday() != 1:  # Selasa
                continue

            campaign = random.choice(meta["campaigns"])
            lo, hi   = meta["base_range"]
            spend    = random.randint(lo, hi)
            if is_weekend:
                spend = int(spend * random.uniform(0.45, 0.75))

            impr     = int(spend / random.uniform(800, 3_000))
            clicks   = int(impr * random.uniform(0.01, 0.08))
            installs = int(clicks * random.uniform(0.04, 0.20))

            created_ts = datetime(
                spend_date.year, spend_date.month, spend_date.day,
                random.randint(0, 5), random.randint(0, 59),
            )
            rows.append({
                "campaign_id":  str(uuid.uuid4()),
                "campaign_name":campaign,
                "channel_name": channel,
                "spend_date":   spend_date,
                "spend_amount": spend,
                "impressions":  impr,
                "clicks":       clicks,
                "installs":     installs,
                "created_at":   created_ts,
                "updated_at":   created_ts,
            })
    return rows


# ─────────────────────────────────────────────────────────────
# BATCH INSERT HELPERS
# ─────────────────────────────────────────────────────────────

def pg_insert(conn, table: str, rows: list[dict], cols: list[str], batch: int = 500):
    if not rows:
        return
    cur = conn.cursor()
    ph  = ",".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({ph}) ON CONFLICT DO NOTHING"
    buf = []
    for row in rows:
        buf.append(tuple(row[c] for c in cols))
        if len(buf) >= batch:
            psycopg2.extras.execute_batch(cur, sql, buf)
            conn.commit()
            buf = []
    if buf:
        psycopg2.extras.execute_batch(cur, sql, buf)
        conn.commit()
    cur.close()
    log.info(f"    ↳ {table}: {len(rows):,} rows inserted")


def my_insert(conn, table: str, rows: list[dict], cols: list[str], batch: int = 500):
    if not rows:
        return
    cur = conn.cursor()
    ph  = ",".join(["%s"] * len(cols))
    sql = f"INSERT IGNORE INTO {table} ({','.join(cols)}) VALUES ({ph})"
    buf = []
    for row in rows:
        buf.append(tuple(row[c] for c in cols))
        if len(buf) >= batch:
            cur.executemany(sql, buf)
            conn.commit()
            buf = []
    if buf:
        cur.executemany(sql, buf)
        conn.commit()
    cur.close()
    log.info(f"    ↳ {table}: {len(rows):,} rows inserted")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("  Ride-Hailing Data Generator  |  Jan–Mar 2026")
    log.info("=" * 60)

    wait_for_pg(PG_CONFIG)
    wait_for_mysql(MY_CONFIG)

    random.seed(RANDOM_SEED)

    # ── Generate ──────────────────────────────────────────────
    log.info("[1/8] Generating master & transaction data...")
    customers   = gen_customers(N_CUSTOMERS)
    drivers     = gen_drivers(N_DRIVERS)
    vehicles    = gen_vehicles(drivers)
    trips       = gen_trips(customers, drivers, vehicles, N_TRIPS)

    log.info("[2/8] Generating trip status logs...")
    status_logs = gen_trip_status_logs(trips)

    log.info("[3/8] Generating payments...")
    payments    = gen_payments(trips)

    log.info("[4/8] Generating driver payouts...")
    payouts     = gen_driver_payouts(trips)

    log.info("[5/8] Generating ratings...")
    ratings     = gen_ratings(trips)

    log.info("[6/8] Generating promotions & redemptions...")
    promotions  = gen_promotions()
    redemptions = gen_promo_redemptions(trips, promotions)

    log.info("[7/8] Generating customer segments (daily snapshot)...")
    segments    = gen_customer_segments(customers)

    log.info("[8/8] Generating campaign spend...")
    campaign_spend = gen_campaign_spend()

    # ── Insert PostgreSQL ─────────────────────────────────────
    log.info("")
    log.info("Inserting ke PostgreSQL (ride_ops_pg)...")
    pg = psycopg2.connect(**PG_CONFIG)

    pg_insert(pg, "customers", customers, [
        "customer_id","full_name","phone_number","email",
        "gender","birth_date","created_at","updated_at",
    ])
    pg_insert(pg, "drivers", drivers, [
        "driver_id","full_name","phone_number","email","license_number",
        "driver_status","join_date","city","created_at","updated_at",
    ])
    pg_insert(pg, "vehicles", vehicles, [
        "vehicle_id","driver_id","plate_number","vehicle_type","brand",
        "model","production_year","seat_capacity","created_at","updated_at",
    ])
    pg_insert(pg, "trips", trips, [
        "trip_id","customer_id","driver_id","vehicle_id",
        "request_ts","pickup_ts","dropoff_ts",
        "pickup_lat","pickup_lng","dropoff_lat","dropoff_lng",
        "pickup_area","dropoff_area","city",
        "estimated_distance_km","actual_distance_km",
        "estimated_fare","actual_fare","surge_multiplier",
        "trip_status","cancel_reason","created_at","updated_at",
    ])
    pg_insert(pg, "trip_status_logs", status_logs, [
        "trip_status_log_id","trip_id","status_code",
        "status_ts","actor_type","created_at",
    ])
    pg_insert(pg, "payments", payments, [
        "payment_id","trip_id","customer_id","payment_method","payment_status",
        "gross_amount","discount_amount","tax_amount","toll_amount","tip_amount",
        "net_amount","paid_ts","created_at","updated_at",
    ])
    pg_insert(pg, "driver_payouts", payouts, [
        "payout_id","driver_id","trip_id","payout_date",
        "base_earning","incentive_amount","bonus_amount","deduction_amount",
        "final_payout_amount","payout_status","created_at","updated_at",
    ])
    pg_insert(pg, "ratings", ratings, [
        "rating_id","trip_id","customer_id","driver_id",
        "rating_score","review_text","rated_ts","created_at","updated_at",
    ])
    pg.close()

    # ── Insert MySQL ──────────────────────────────────────────
    log.info("")
    log.info("Inserting ke MySQL (ride_marketing_mysql)...")
    my = mysql.connector.connect(**MY_CONFIG)

    my_insert(my, "promotions", promotions, [
        "promo_id","promo_code","promo_name","promo_type","discount_type",
        "discount_value","start_date","end_date","budget_amount",
        "promo_status","created_at","updated_at",
    ])
    my_insert(my, "promo_redemptions", redemptions, [
        "redemption_id","promo_id","customer_id","trip_id","redeemed_ts",
        "discount_amount","redemption_status","created_at","updated_at",
    ])
    my_insert(my, "customer_segments", segments, [
        "customer_id","segment_name","segment_score",
        "snapshot_date","created_at","updated_at",
    ])
    my_insert(my, "campaign_spend", campaign_spend, [
        "campaign_id","campaign_name","channel_name","spend_date",
        "spend_amount","impressions","clicks","installs",
        "created_at","updated_at",
    ])
    my.close()

    # ── Summary ───────────────────────────────────────────────
    completed = sum(1 for t in trips if t["trip_status"] == "completed")
    cancelled = sum(1 for t in trips if t["trip_status"] == "cancelled")
    no_show   = sum(1 for t in trips if t["trip_status"] == "no_show")

    log.info("")
    log.info("=" * 60)
    log.info("  DATA GENERATION SELESAI ✓")
    log.info("=" * 60)
    log.info(f"  PostgreSQL (ride_ops_pg):")
    log.info(f"    customers        : {len(customers):>8,}")
    log.info(f"    drivers          : {len(drivers):>8,}")
    log.info(f"    vehicles         : {len(vehicles):>8,}")
    log.info(f"    trips            : {len(trips):>8,}  (completed={completed:,} | cancelled={cancelled:,} | no_show={no_show:,})")
    log.info(f"    trip_status_logs : {len(status_logs):>8,}")
    log.info(f"    payments         : {len(payments):>8,}")
    log.info(f"    driver_payouts   : {len(payouts):>8,}")
    log.info(f"    ratings          : {len(ratings):>8,}")
    log.info(f"  MySQL (ride_marketing_mysql):")
    log.info(f"    promotions       : {len(promotions):>8,}")
    log.info(f"    promo_redemptions: {len(redemptions):>8,}")
    log.info(f"    customer_segments: {len(segments):>8,}")
    log.info(f"    campaign_spend   : {len(campaign_spend):>8,}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
