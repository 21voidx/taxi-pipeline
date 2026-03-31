"""
╔══════════════════════════════════════════════════════════════╗
║     TAXI RIDE-HAILING  –  Real-time Data Generator          ║
║     PostgreSQL  →  drivers | passengers | rides | events    ║
║     MySQL       →  payments | promotions | incentives        ║
╚══════════════════════════════════════════════════════════════╝
"""

import os, sys, time, random, uuid, json, threading, signal
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg2
import psycopg2.extras
import mysql.connector
from faker import Faker
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich import box
from dotenv import load_dotenv

load_dotenv()
fake = Faker("id_ID")
console = Console()

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
PG_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST",     "postgres-ops"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB",       "ride_ops_pg"),
    "user":     os.getenv("POSTGRES_USER",     "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

MYSQL_CONFIG = {
    "host":     os.getenv("MYSQL_HOST",     "mysql"),
    "port":     int(os.getenv("MYSQL_PORT", "3306")),
    "database": os.getenv("MYSQL_DB",       "ride_marketing_mysql"),
    "user":     os.getenv("MYSQL_USER",     "mysql"),
    "password": os.getenv("MYSQL_PASSWORD", "mysql"),
}

# Generator tunables
DRIVER_SEED_COUNT      = int(os.getenv("DRIVER_SEED",    "50"))
PASSENGER_SEED_COUNT   = int(os.getenv("PASSENGER_SEED", "200"))
RIDES_PER_BATCH        = int(os.getenv("RIDES_PER_BATCH","3"))
INTERVAL_SECONDS       = float(os.getenv("INTERVAL_SEC", "5"))

# ─────────────────────────────────────────
#  LOOKUP DATA  (matches DB seeds)
# ─────────────────────────────────────────
ZONES = [
    (1, "JKT-SEL", "Jakarta Selatan", -6.2615, 106.8106),
    (2, "JKT-PUS", "Jakarta Pusat",   -6.1745, 106.8227),
    (3, "JKT-BAR", "Jakarta Barat",   -6.1682, 106.7632),
    (4, "JKT-TIM", "Jakarta Timur",   -6.2250, 106.9004),
    (5, "JKT-UTA", "Jakarta Utara",   -6.1382, 106.8833),
    (6, "TGR",     "Tangerang",       -6.1783, 106.6319),
    (7, "BKS",     "Bekasi",          -6.2383, 106.9756),
    (8, "DPK",     "Depok",           -6.4025, 106.7942),
    (9, "BSD",     "BSD City",        -6.2994, 106.6535),
   (10, "KWN",    "Kawasan Niaga",   -6.2088, 106.8456),
]

VEHICLE_TYPES = [
    (1, "MOTOR",   4000,  1500, 200),
    (2, "ECONOMI", 8000,  2500, 400),
    (3, "COMFORT", 12000, 3500, 500),
    (4, "SUV",     15000, 4500, 600),
    (5, "PREMIUM", 25000, 6000, 800),
]

VEHICLE_BRANDS = {
    "MOTOR":   [("Honda", "Beat"),("Honda", "Vario"),("Yamaha","NMAX"),("Yamaha","Aerox")],
    "ECONOMI": [("Toyota","Avanza"),("Daihatsu","Xenia"),("Suzuki","Ertiga")],
    "COMFORT": [("Toyota","Innova"),("Honda","CRV"),("Mitsubishi","Xpander")],
    "SUV":     [("Toyota","Fortuner"),("Mitsubishi","Pajero"),("Chevrolet","Trailblazer")],
    "PREMIUM": [("Toyota","Alphard"),("Mercedes","E-Class"),("BMW","5 Series")],
}

COLORS = ["Hitam","Putih","Silver","Merah","Biru","Abu-abu","Coklat"]
GATEWAYS = ["MIDTRANS","XENDIT","DOKU","FASPAY"]
PAYMENT_METHODS = [1,2,3,4,5,6,7,8,9,10]  # method_id from DB

INCENTIVE_TYPES = ["BONUS_TRIP","PEAK_HOUR","SURGE","REFERRAL"]
CANCELLATION_REASONS = [
    "Driver terlalu jauh", "Penumpang tidak ditemukan",
    "Permintaan penumpang", "Kendaraan bermasalah",
    "Cuaca buruk", "Aplikasi error",
]

# ─────────────────────────────────────────
#  STATS (shared state, thread-safe-ish)
# ─────────────────────────────────────────
stats = {
    "drivers_created":     0,
    "passengers_created":  0,
    "rides_created":       0,
    "payments_created":    0,
    "events_created":      0,
    "incentives_created":  0,
    "errors":              0,
    "batches":             0,
    "start_time":          datetime.now(),
}
driver_ids     = []
passenger_ids  = []
lock           = threading.Lock()


# ═══════════════════════════════════════════════
#  DB CONNECTIONS
# ═══════════════════════════════════════════════
def get_pg_conn():
    return psycopg2.connect(**PG_CONFIG)

def get_mysql_conn():
    return mysql.connector.connect(**MYSQL_CONFIG)


# ═══════════════════════════════════════════════
#  HELPER GENERATORS
# ═══════════════════════════════════════════════
def gen_phone_id():
    """Generate Indonesian phone number – sometimes dirty (for Silver cleaning)."""
    prefixes = ["0812","0813","0852","0853","0821","0822","0811",
                "0878","0877","0858","0819","0859"]
    number = random.choice(prefixes) + "".join([str(random.randint(0,9)) for _ in range(8)])
    if random.random() < 0.08:  # 8% dirty: +62 prefix
        number = "+62" + number[1:]
    if random.random() < 0.04:  # 4% dirty: dashes
        number = number[:4] + "-" + number[4:8] + "-" + number[8:]
    return number

def gen_nik():
    """16-digit NIK, sometimes malformed (for Silver regex cleaning)."""
    nik = "".join([str(random.randint(0,9)) for _ in range(16)])
    if random.random() < 0.10:  # dirty: spaces
        nik = nik[:6] + " " + nik[6:12] + " " + nik[12:]
    elif random.random() < 0.05:  # dirty: only 15 digits
        nik = nik[:15]
    return nik

def gen_license_plate():
    """Indonesian license plate, sometimes lowercase/dirty."""
    areas   = ["B","D","F","Z","T","BE","BG","BP","DA","DB","DC","DD"]
    numbers = str(random.randint(1000, 9999))
    suffix  = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=random.randint(2,3)))
    plate   = f"{random.choice(areas)} {numbers} {suffix}"
    if random.random() < 0.10:
        plate = plate.lower()  # dirty lowercase
    return plate

def gen_coordinates_near(lat, lon, radius_km=5.0):
    """Random point near a base coordinate."""
    dlat = random.uniform(-radius_km/111, radius_km/111)
    dlon = random.uniform(-radius_km/111, radius_km/111)
    return round(lat + dlat, 7), round(lon + dlon, 7)

def calc_fare(vehicle_type_id, distance_km, duration_min, surge=1.0):
    vt = next(v for v in VEHICLE_TYPES if v[0] == vehicle_type_id)

    _, vehicle_name, base_rate, distance_rate, time_rate = vt

    base = Decimal(str(base_rate))
    dist_fare = Decimal(str(distance_rate)) * Decimal(str(distance_km))
    time_fare = Decimal(str(time_rate)) * Decimal(str(duration_min))
    total = (base + dist_fare + time_fare) * Decimal(str(surge))

    return float(base), float(dist_fare), float(time_fare), round(float(total), -2)

def is_peak_hour(dt: datetime) -> bool:
    return dt.hour in range(7, 10) or dt.hour in range(17, 20)


# ═══════════════════════════════════════════════
#  SEED: DRIVERS
# ═══════════════════════════════════════════════
def seed_drivers(pg_conn, count: int):
    cur = pg_conn.cursor()
    inserted = 0
    for i in range(count):
        vt_code, vt_row = random.choice(list(VEHICLE_BRANDS.items()))
        vt_id = next(v[0] for v in VEHICLE_TYPES if v[1] == vt_code)
        brand, model  = random.choice(vt_row)
        zone_id = random.choice(ZONES)[0]
        driver_id   = str(uuid.uuid4())
        driver_code = f"DRV-{fake.random_number(digits=8, fix_len=True)}"

        try:
            cur.execute("""
                INSERT INTO drivers
                  (driver_id, driver_code, full_name, phone_number, email,
                   nik, sim_number, license_plate,
                   vehicle_type_id, vehicle_brand, vehicle_model,
                   vehicle_year, vehicle_color, home_zone_id,
                   rating, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (driver_code) DO NOTHING
            """, (
                driver_id, driver_code,
                fake.name(), gen_phone_id(),
                fake.email() if random.random() > 0.15 else None,
                gen_nik(),
                f"SIM{fake.random_number(digits=12, fix_len=True)}",
                gen_license_plate(),
                vt_id, brand, model,
                random.randint(2018, 2024),
                random.choice(COLORS),
                zone_id,
                round(random.uniform(4.0, 5.0), 2),
                random.choices(
                    ["ACTIVE","ACTIVE","ACTIVE","INACTIVE","SUSPENDED"],
                    weights=[70, 10, 10, 5, 5]
                )[0],
            ))
            driver_ids.append(driver_id)
            inserted += 1
        except Exception as e:
            pg_conn.rollback()
            continue

    pg_conn.commit()
    cur.close()
    return inserted


# ═══════════════════════════════════════════════
#  SEED: PASSENGERS
# ═══════════════════════════════════════════════
def seed_passengers(pg_conn, count: int):
    cur = pg_conn.cursor()
    inserted = 0
    for i in range(count):
        passenger_id   = str(uuid.uuid4())
        passenger_code = f"PAX-{fake.random_number(digits=9, fix_len=True)}"
        zone_id = random.choice(ZONES)[0]

        try:
            cur.execute("""
                INSERT INTO passengers
                  (passenger_id, passenger_code, full_name, phone_number, email,
                   birth_date, gender, home_zone_id, is_verified, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (passenger_code) DO NOTHING
            """, (
                passenger_id, passenger_code,
                fake.name(), gen_phone_id(),
                fake.email() if random.random() > 0.2 else None,
                fake.date_of_birth(minimum_age=18, maximum_age=65),
                random.choices(["M","F","OTHER","UNKNOWN"],
                               weights=[45,45,5,5])[0],
                zone_id,
                random.random() > 0.25,
                "ACTIVE",
            ))
            passenger_ids.append(passenger_id)
            inserted += 1
        except Exception as e:
            pg_conn.rollback()
            continue

    pg_conn.commit()
    cur.close()
    return inserted


# ═══════════════════════════════════════════════
#  INSERT: ONE RIDE  (full lifecycle)
# ═══════════════════════════════════════════════
def insert_ride(pg_conn, mysql_conn, run_promos: list) -> dict:
    pg_cur    = pg_conn.cursor()
    my_cur    = mysql_conn.cursor()
    result    = {}

    # --- pick actors ---
    driver_id    = random.choice(driver_ids)
    passenger_id = random.choice(passenger_ids)
    vt           = random.choice(VEHICLE_TYPES)
    pickup_zone  = random.choice(ZONES)
    dropoff_zone = random.choice(ZONES)

    # --- ride timing ---
    now           = datetime.utcnow()
    requested_at  = now - timedelta(minutes=random.uniform(0, 30))
    is_peak       = is_peak_hour(requested_at)
    surge         = round(random.choice([1.0, 1.0, 1.0, 1.2, 1.5, 2.0])
                          if is_peak else 1.0, 2)

    # --- geo ---
    p_lat, p_lon = gen_coordinates_near(pickup_zone[3],  pickup_zone[4])
    d_lat, d_lon = gen_coordinates_near(dropoff_zone[3], dropoff_zone[4])

    # --- ride metrics ---
    distance_km  = round(random.uniform(1.5, 45.0), 2)
    duration_min = int(distance_km * random.uniform(3, 8) + random.uniform(2, 10))

    base_fare, dist_fare, time_fare, total_fare = calc_fare(
        vt[0], distance_km, duration_min, surge
    )

    # --- ride status ---
    ride_status_weights = {
        "COMPLETED": 70, "CANCELLED": 15,
        "ACCEPTED": 8, "PICKED_UP": 5, "NO_DRIVER": 2,
    }
    ride_status = random.choices(
        list(ride_status_weights.keys()),
        weights=list(ride_status_weights.values())
    )[0]

    accepted_at   = None; picked_up_at = None
    completed_at  = None; cancelled_at = None
    pax_rating    = None; drv_rating   = None
    cancel_reason = None

    if ride_status in ("ACCEPTED","PICKED_UP","COMPLETED"):
        accepted_at  = requested_at + timedelta(minutes=random.uniform(1,5))
    if ride_status in ("PICKED_UP","COMPLETED"):
        picked_up_at = accepted_at  + timedelta(minutes=random.uniform(2,10))
    if ride_status == "COMPLETED":
        completed_at = picked_up_at + timedelta(minutes=duration_min)
        pax_rating   = round(random.choices([3,4,4,5,5,5], k=1)[0] + random.uniform(0,.9), 1)
        drv_rating   = round(random.choices([3,4,4,5,5,5], k=1)[0] + random.uniform(0,.9), 1)
    if ride_status == "CANCELLED":
        cancelled_at  = requested_at + timedelta(minutes=random.uniform(0,8))
        cancel_reason = random.choice(CANCELLATION_REASONS)
        total_fare    = 0.0

    # --- promo ---
    promo_discount = 0.0
    used_promo     = None
    if run_promos and ride_status == "COMPLETED" and random.random() < 0.25:
        used_promo     = random.choice(run_promos)
        if used_promo["promo_type"] == "FLAT":
            promo_discount = min(used_promo["discount_value"], total_fare)
        else:
            promo_discount = min(
                total_fare * used_promo["discount_value"] / 100,
                used_promo["max_discount"] or 999999
            )
        total_fare = max(0, total_fare - promo_discount)

    ride_id   = str(uuid.uuid4())
    ride_code = f"RIDE-{fake.random_number(digits=12, fix_len=True)}"

    # ── INSERT RIDE (Postgres) ──────────────────────────
    try:
        pg_cur.execute("""
            INSERT INTO rides (
              ride_id, ride_code, driver_id, passenger_id, vehicle_type_id,
              pickup_zone_id, dropoff_zone_id,
              pickup_address, dropoff_address,
              pickup_lat, pickup_lon, dropoff_lat, dropoff_lon,
              requested_at, accepted_at, picked_up_at, completed_at, cancelled_at,
              distance_km, duration_minutes,
              base_fare, distance_fare, time_fare, surge_multiplier,
              promo_discount, total_fare, ride_status,
              cancellation_reason, passenger_rating, driver_rating
            ) VALUES (
              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
              %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
        """, (
            ride_id, ride_code, driver_id, passenger_id, vt[0],
            pickup_zone[0], dropoff_zone[0],
            fake.street_address(), fake.street_address(),
            p_lat, p_lon, d_lat, d_lon,
            requested_at, accepted_at, picked_up_at, completed_at, cancelled_at,
            distance_km, duration_min,
            base_fare, dist_fare, time_fare, surge,
            promo_discount, total_fare, ride_status,
            cancel_reason, pax_rating, drv_rating,
        ))

        # ── INSERT RIDE EVENTS ──────────────────────
        events = [("RIDE_REQUESTED", {"surge": surge, "zone": pickup_zone[1]}, requested_at)]
        if accepted_at:
            events.append(("RIDE_ACCEPTED", {"driver_id": driver_id}, accepted_at))
        if picked_up_at:
            events.append(("PASSENGER_PICKED_UP", {}, picked_up_at))
        if completed_at:
            events.append(("RIDE_COMPLETED", {
                "fare": total_fare, "distance": distance_km
            }, completed_at))
        if cancelled_at:
            events.append(("RIDE_CANCELLED", {"reason": cancel_reason}, cancelled_at))

        for etype, payload, etime in events:
            pg_cur.execute("""
                INSERT INTO ride_events (event_id, ride_id, event_type, event_payload, occurred_at)
                VALUES (%s,%s,%s,%s,%s)
            """, (str(uuid.uuid4()), ride_id, etype, json.dumps(payload), etime))

        pg_conn.commit()
        result["ride_status"] = ride_status
        result["events"]      = len(events)

    except Exception as e:
        pg_conn.rollback()
        result["error"] = str(e)
        return result

    # ── INSERT PAYMENT (MySQL) ──────────────────────────
    if ride_status == "COMPLETED" and total_fare > 0:
        try:
            payment_id   = str(uuid.uuid4())
            payment_code = f"PAY-{fake.random_number(digits=12, fix_len=True)}"
            method_id    = random.choice(PAYMENT_METHODS)
            platform_fee = round(total_fare * 0.20, 2)
            drv_earning  = round(total_fare * 0.80, 2)
            gateway      = random.choice(GATEWAYS)
            pay_status   = random.choices(
                ["SUCCESS","SUCCESS","SUCCESS","FAILED","PENDING"],
                weights=[80,5,5,5,5]
            )[0]
            paid_at = completed_at if pay_status == "SUCCESS" else None

            # Simulate dirty gateway response JSON
            gateway_raw = json.dumps({
                "gateway":     gateway,
                "reference":   f"TXN{fake.random_number(digits=16,fix_len=True)}",
                "status":      pay_status,
                "amount":      total_fare,
                "timestamp":   str(completed_at),
                "metadata":    {"ip": fake.ipv4(), "device": random.choice(["android","ios"])},
            })

            my_cur.execute("""
                INSERT INTO payments (
                  payment_id, payment_code, ride_id, passenger_id, method_id,
                  amount, platform_fee, driver_earning, promo_discount,
                  payment_status, payment_gateway, gateway_ref,
                  gateway_raw_response, paid_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                payment_id, payment_code, ride_id, passenger_id, method_id,
                total_fare, platform_fee, drv_earning, promo_discount,
                pay_status, gateway, f"TXN{fake.random_number(digits=16,fix_len=True)}",
                gateway_raw, paid_at,
            ))

            # ── PROMO USAGE ──────────────────────
            if used_promo and promo_discount > 0:
                my_cur.execute("""
                    INSERT INTO promo_usage (promo_id, passenger_id, ride_id, discount_given)
                    VALUES (%s,%s,%s,%s)
                """, (used_promo["promo_id"], passenger_id, ride_id, promo_discount))

            # ── DRIVER INCENTIVE ──────────────────
            if random.random() < 0.30:
                inc_type = random.choice(INCENTIVE_TYPES)
                inc_amt  = random.choice([5000, 10000, 15000, 20000, 25000])
                if inc_type == "SURGE" and surge > 1:
                    inc_amt = round(drv_earning * (surge - 1) * 0.1)
                my_cur.execute("""
                    INSERT INTO driver_incentives
                      (driver_id, incentive_type, reference_id, amount,
                       description, period_date, is_paid)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                """, (
                    driver_id, inc_type, ride_id, inc_amt,
                    f"Insentif {inc_type} - {ride_code}",
                    (completed_at or requested_at).date(),
                    random.random() < 0.7,
                ))
                result["incentive"] = inc_amt

            mysql_conn.commit()
            result["payment"] = pay_status

        except Exception as e:
            mysql_conn.rollback()
            result["payment_error"] = str(e)

    pg_cur.close()
    my_cur.close()
    return result


# ═══════════════════════════════════════════════
#  SEED PROMOTIONS IN MYSQL
# ═══════════════════════════════════════════════
def seed_promotions(mysql_conn) -> list:
    now = datetime.utcnow()
    promos = [
        ("NEWUSER50",  "New User 50%",        "PERCENT", 50.0, 30000, 0,    "ALL",    now - timedelta(days=30), now + timedelta(days=60)),
        ("FLAT10K",    "Diskon 10rb",          "FLAT",    10000,None,  5000, "ALL",    now - timedelta(days=10), now + timedelta(days=20)),
        ("WEEKEND25",  "Weekend 25%",          "PERCENT", 25.0, 25000, 0,    "ALL",    now - timedelta(days=5),  now + timedelta(days=10)),
        ("MOTOR5K",    "Motor Hemat 5rb",      "FLAT",    5000, None,  0,    "MOTOR",  now - timedelta(days=7),  now + timedelta(days=7)),
        ("GOWORK15",   "GoWork 15%",           "PERCENT", 15.0, 20000, 8000, "COMFORT,SUV", now, now + timedelta(days=30)),
    ]
    cur     = mysql_conn.cursor()
    out     = []
    for p in promos:
        try:
            cur.execute("""
                INSERT IGNORE INTO promotions
                  (promo_code, promo_name, promo_type, discount_value,
                   max_discount, min_fare, applicable_vehicle_types,
                   valid_from, valid_until)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, p)
            mysql_conn.commit()
        except:
            mysql_conn.rollback()

    cur.execute("SELECT promo_id, promo_code, promo_type, discount_value, max_discount FROM promotions WHERE is_active=1")
    for row in cur.fetchall():
        out.append({
            "promo_id":       row[0],
            "promo_code":     row[1],
            "promo_type":     row[2],
            "discount_value": float(row[3]),
            "max_discount":   float(row[4]) if row[4] else None,
        })
    cur.close()
    return out


# ═══════════════════════════════════════════════
#  RICH DASHBOARD
# ═══════════════════════════════════════════════
def make_dashboard() -> Panel:
    elapsed = (datetime.now() - stats["start_time"]).seconds
    rps     = stats["rides_created"] / max(elapsed, 1)

    tbl = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold cyan")
    tbl.add_column("Metric",  style="bold white", min_width=22)
    tbl.add_column("Value",   style="bold green",  justify="right")
    tbl.add_column("Source",  style="dim")

    tbl.add_row("🚗  Rides Generated",       f"{stats['rides_created']:,}",      "PostgreSQL")
    tbl.add_row("💳  Payments Inserted",     f"{stats['payments_created']:,}",   "MySQL")
    tbl.add_row("📋  Ride Events",           f"{stats['events_created']:,}",     "PostgreSQL")
    tbl.add_row("🎁  Incentives Created",    f"{stats['incentives_created']:,}", "MySQL")
    tbl.add_row("👤  Drivers (seeded)",      f"{stats['drivers_created']:,}",    "PostgreSQL")
    tbl.add_row("🧑  Passengers (seeded)",   f"{stats['passengers_created']:,}", "PostgreSQL")
    tbl.add_row("⚡  Rides/sec",             f"{rps:.2f}",                       "—")
    tbl.add_row("🔄  Batches",               f"{stats['batches']:,}",            "—")
    tbl.add_row("❌  Errors",                f"{stats['errors']:,}",             "—")
    tbl.add_row("⏱️   Elapsed",              f"{elapsed}s",                      "—")

    return Panel(
        tbl,
        title="[bold yellow]🚕  TAXI RIDE-HAILING  –  Real-time Data Generator[/]",
        subtitle="[dim]Ctrl+C to stop | Generating for dbt Bronze → Silver → Gold[/]",
        border_style="yellow",
    )


# ═══════════════════════════════════════════════
#  MAIN LOOP
# ═══════════════════════════════════════════════
def main():
    console.print("\n[bold yellow]🚕 TAXI Data Generator Starting...[/]\n")

    # Connect
    console.print("[cyan]Connecting to PostgreSQL...[/]", end=" ")
    pg_conn = get_pg_conn()
    console.print("[green]✓[/]")

    console.print("[cyan]Connecting to MySQL...[/]", end=" ")
    mysql_conn = get_mysql_conn()
    console.print("[green]✓[/]")

    # Seed reference data
    console.print(f"\n[bold]Seeding {DRIVER_SEED_COUNT} drivers...[/]")
    n = seed_drivers(pg_conn, DRIVER_SEED_COUNT)
    stats["drivers_created"] = n
    console.print(f"  [green]✓ {n} drivers ready[/]")

    console.print(f"[bold]Seeding {PASSENGER_SEED_COUNT} passengers...[/]")
    n = seed_passengers(pg_conn, PASSENGER_SEED_COUNT)
    stats["passengers_created"] = n
    console.print(f"  [green]✓ {n} passengers ready[/]")

    console.print("[bold]Seeding promotions...[/]")
    active_promos = seed_promotions(mysql_conn)
    console.print(f"  [green]✓ {len(active_promos)} active promotions[/]")

    console.print(f"\n[bold green]Streaming rides every {INTERVAL_SECONDS}s — {RIDES_PER_BATCH} rides/batch[/]\n")

    # Run loop
    running = True
    def handle_sig(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT,  handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    with Live(make_dashboard(), refresh_per_second=2, console=console) as live:
        while running:
            for _ in range(RIDES_PER_BATCH):
                if not running:
                    break
                try:
                    res = insert_ride(pg_conn, mysql_conn, active_promos)

                    if "error" in res:
                        console.print(f"[red]RIDE INSERT ERROR:[/] {res['error']}")
                        with lock:
                            stats["errors"] += 1
                        continue

                    if "payment_error" in res:
                        console.print(f"[yellow]PAYMENT INSERT ERROR:[/] {res['payment_error']}")
                        with lock:
                            stats["errors"] += 1

                    with lock:
                        stats["rides_created"] += 1
                        stats["events_created"] += res.get("events", 0)

                        if "payment" in res:
                            stats["payments_created"] += 1
                        if "incentive" in res:
                            stats["incentives_created"] += 1

                except Exception as e:
                    console.print(f"[bold red]UNHANDLED ERROR:[/] {e}")
                    with lock:
                        stats["errors"] += 1

            with lock:
                stats["batches"] += 1
            live.update(make_dashboard())
            time.sleep(INTERVAL_SECONDS)

    console.print("\n[bold yellow]Generator stopped.[/]")
    pg_conn.close()
    mysql_conn.close()


if __name__ == "__main__":
    main()
