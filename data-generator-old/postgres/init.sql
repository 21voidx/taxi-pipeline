-- ============================================================
-- Schema: ride_ops_pg  (PostgreSQL)
-- Source system operasional ride-hailing
-- ============================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id     VARCHAR(36) PRIMARY KEY,
    full_name       VARCHAR(100) NOT NULL,
    phone_number    VARCHAR(20),
    email           VARCHAR(100),
    gender          VARCHAR(10),
    birth_date      DATE,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id       VARCHAR(36) PRIMARY KEY,
    full_name       VARCHAR(100) NOT NULL,
    phone_number    VARCHAR(20),
    email           VARCHAR(100),
    license_number  VARCHAR(30),
    driver_status   VARCHAR(20) NOT NULL DEFAULT 'active',
    join_date       DATE,
    city            VARCHAR(50),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id      VARCHAR(36) PRIMARY KEY,
    driver_id       VARCHAR(36) NOT NULL REFERENCES drivers(driver_id),
    plate_number    VARCHAR(20) UNIQUE,
    vehicle_type    VARCHAR(20),
    brand           VARCHAR(50),
    model           VARCHAR(50),
    production_year INTEGER,
    seat_capacity   INTEGER,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS trips (
    trip_id                 VARCHAR(36) PRIMARY KEY,
    customer_id             VARCHAR(36) NOT NULL REFERENCES customers(customer_id),
    driver_id               VARCHAR(36) NOT NULL REFERENCES drivers(driver_id),
    vehicle_id              VARCHAR(36) NOT NULL REFERENCES vehicles(vehicle_id),
    request_ts              TIMESTAMP NOT NULL,
    pickup_ts               TIMESTAMP,
    dropoff_ts              TIMESTAMP,
    pickup_lat              DOUBLE PRECISION,
    pickup_lng              DOUBLE PRECISION,
    dropoff_lat             DOUBLE PRECISION,
    dropoff_lng             DOUBLE PRECISION,
    pickup_area             VARCHAR(100),
    dropoff_area            VARCHAR(100),
    city                    VARCHAR(50),
    estimated_distance_km   NUMERIC(8,2),
    actual_distance_km      NUMERIC(8,2),
    estimated_fare          NUMERIC(14,2),
    actual_fare             NUMERIC(14,2),
    surge_multiplier        NUMERIC(4,2) DEFAULT 1.0,
    trip_status             VARCHAR(20) NOT NULL,
    cancel_reason           TEXT,
    created_at              TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trips_customer   ON trips(customer_id);
CREATE INDEX IF NOT EXISTS idx_trips_driver     ON trips(driver_id);
CREATE INDEX IF NOT EXISTS idx_trips_request_ts ON trips(request_ts);
CREATE INDEX IF NOT EXISTS idx_trips_status     ON trips(trip_status);
CREATE INDEX IF NOT EXISTS idx_trips_city       ON trips(city);

CREATE TABLE IF NOT EXISTS trip_status_logs (
    trip_status_log_id  VARCHAR(36) PRIMARY KEY,
    trip_id             VARCHAR(36) NOT NULL REFERENCES trips(trip_id),
    status_code         VARCHAR(30) NOT NULL,
    status_ts           TIMESTAMP NOT NULL,
    actor_type          VARCHAR(20),
    created_at          TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tsl_trip_id ON trip_status_logs(trip_id);

CREATE TABLE IF NOT EXISTS payments (
    payment_id      VARCHAR(36) PRIMARY KEY,
    trip_id         VARCHAR(36) NOT NULL REFERENCES trips(trip_id),
    customer_id     VARCHAR(36) NOT NULL REFERENCES customers(customer_id),
    payment_method  VARCHAR(30),
    payment_status  VARCHAR(20),
    gross_amount    NUMERIC(14,2),
    discount_amount NUMERIC(14,2) DEFAULT 0,
    tax_amount      NUMERIC(14,2) DEFAULT 0,
    toll_amount     NUMERIC(14,2) DEFAULT 0,
    tip_amount      NUMERIC(14,2) DEFAULT 0,
    net_amount      NUMERIC(14,2),
    paid_ts         TIMESTAMP,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pay_trip_id     ON payments(trip_id);
CREATE INDEX IF NOT EXISTS idx_pay_customer_id ON payments(customer_id);
CREATE INDEX IF NOT EXISTS idx_pay_method      ON payments(payment_method);

CREATE TABLE IF NOT EXISTS driver_payouts (
    payout_id           VARCHAR(36) PRIMARY KEY,
    driver_id           VARCHAR(36) NOT NULL REFERENCES drivers(driver_id),
    trip_id             VARCHAR(36) NOT NULL REFERENCES trips(trip_id),
    payout_date         DATE,
    base_earning        NUMERIC(14,2),
    incentive_amount    NUMERIC(14,2) DEFAULT 0,
    bonus_amount        NUMERIC(14,2) DEFAULT 0,
    deduction_amount    NUMERIC(14,2) DEFAULT 0,
    final_payout_amount NUMERIC(14,2),
    payout_status       VARCHAR(20),
    created_at          TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dp_driver_id ON driver_payouts(driver_id);
CREATE INDEX IF NOT EXISTS idx_dp_trip_id   ON driver_payouts(trip_id);
CREATE INDEX IF NOT EXISTS idx_dp_date      ON driver_payouts(payout_date);

CREATE TABLE IF NOT EXISTS ratings (
    rating_id       VARCHAR(36) PRIMARY KEY,
    trip_id         VARCHAR(36) NOT NULL REFERENCES trips(trip_id),
    customer_id     VARCHAR(36) NOT NULL REFERENCES customers(customer_id),
    driver_id       VARCHAR(36) NOT NULL REFERENCES drivers(driver_id),
    rating_score    INTEGER CHECK (rating_score BETWEEN 1 AND 5),
    review_text     TEXT,
    rated_ts        TIMESTAMP,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rat_trip_id     ON ratings(trip_id);
CREATE INDEX IF NOT EXISTS idx_rat_driver_id   ON ratings(driver_id);
CREATE INDEX IF NOT EXISTS idx_rat_customer_id ON ratings(customer_id);
