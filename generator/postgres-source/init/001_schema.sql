SET TIME ZONE 'UTC';

CREATE TABLE IF NOT EXISTS cities (
    city_id BIGSERIAL PRIMARY KEY,
    city_code VARCHAR(10) NOT NULL UNIQUE,
    city_name VARCHAR(100) NOT NULL,
    timezone VARCHAR(50) NOT NULL DEFAULT 'Asia/Jakarta',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
    updated_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now())
);

CREATE TABLE IF NOT EXISTS zones (
    zone_id BIGSERIAL PRIMARY KEY,
    city_id BIGINT NOT NULL REFERENCES cities(city_id),
    zone_code VARCHAR(30) NOT NULL UNIQUE,
    zone_name VARCHAR(100) NOT NULL,
    zone_type VARCHAR(30) NOT NULL,
    is_hotspot BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
    updated_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now())
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGSERIAL PRIMARY KEY,
    customer_name VARCHAR(150) NOT NULL,
    registered_city_id BIGINT NOT NULL REFERENCES cities(city_id),
    customer_status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
    updated_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now())
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id BIGSERIAL PRIMARY KEY,
    driver_name VARCHAR(150) NOT NULL,
    city_id BIGINT NOT NULL REFERENCES cities(city_id),
    service_type VARCHAR(10) NOT NULL CHECK (service_type IN ('BIKE', 'CAR')),
    driver_status VARCHAR(20) NOT NULL DEFAULT 'AVAILABLE',
    rating NUMERIC(3,2) NOT NULL DEFAULT 4.80,
    created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
    updated_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now())
);

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id BIGSERIAL PRIMARY KEY,
    driver_id BIGINT NOT NULL UNIQUE REFERENCES drivers(driver_id),
    vehicle_type VARCHAR(20) NOT NULL CHECK (vehicle_type IN ('MOTORCYCLE', 'CAR')),
    vehicle_year SMALLINT NOT NULL,
    vehicle_status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
    updated_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now())
);

CREATE TABLE IF NOT EXISTS rides (
    ride_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    driver_id BIGINT REFERENCES drivers(driver_id),
    city_id BIGINT NOT NULL REFERENCES cities(city_id),
    service_type VARCHAR(10) NOT NULL CHECK (service_type IN ('BIKE', 'CAR')),
    pickup_zone_id BIGINT NOT NULL REFERENCES zones(zone_id),
    dropoff_zone_id BIGINT NOT NULL REFERENCES zones(zone_id),
    ride_status VARCHAR(30) NOT NULL CHECK (
        ride_status IN (
            'REQUESTED', 'ACCEPTED', 'DRIVER_ARRIVED', 'IN_PROGRESS',
            'COMPLETED', 'CANCELLED', 'NO_DRIVER'
        )
    ),
    requested_at TIMESTAMP NOT NULL,
    accepted_at TIMESTAMP,
    driver_arrived_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    cancelled_by VARCHAR(20),
    cancellation_reason VARCHAR(100),
    estimated_distance_km NUMERIC(10,2) NOT NULL,
    actual_distance_km NUMERIC(10,2),
    estimated_duration_min NUMERIC(10,2) NOT NULL,
    actual_duration_min NUMERIC(10,2),
    base_fare NUMERIC(14,2) NOT NULL,
    distance_fare NUMERIC(14,2) NOT NULL,
    time_fare NUMERIC(14,2) NOT NULL,
    surge_multiplier NUMERIC(5,2) NOT NULL DEFAULT 1.00,
    gross_fare NUMERIC(14,2) NOT NULL,
    discount_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
    final_fare NUMERIC(14,2) NOT NULL,
    status_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
    updated_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now())
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id BIGSERIAL PRIMARY KEY,
    ride_id BIGINT NOT NULL UNIQUE REFERENCES rides(ride_id),
    payment_method VARCHAR(20) NOT NULL CHECK (payment_method IN ('CASH', 'EWALLET', 'CARD')),
    payment_status VARCHAR(20) NOT NULL CHECK (payment_status IN ('PENDING', 'PAID', 'FAILED', 'REFUNDED')),
    payment_amount NUMERIC(14,2) NOT NULL,
    platform_fee NUMERIC(14,2) NOT NULL,
    driver_earning NUMERIC(14,2) NOT NULL,
    failure_reason VARCHAR(100),
    paid_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
    updated_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now())
);

CREATE INDEX IF NOT EXISTS idx_zones_city ON zones(city_id);
CREATE INDEX IF NOT EXISTS idx_customers_city ON customers(registered_city_id);
CREATE INDEX IF NOT EXISTS idx_drivers_city_service ON drivers(city_id, service_type);
CREATE INDEX IF NOT EXISTS idx_rides_updated_at ON rides(updated_at);
CREATE INDEX IF NOT EXISTS idx_rides_requested_at ON rides(requested_at);
CREATE INDEX IF NOT EXISTS idx_rides_city_service ON rides(city_id, service_type);
CREATE INDEX IF NOT EXISTS idx_rides_status ON rides(ride_status);
CREATE INDEX IF NOT EXISTS idx_payments_updated_at ON payments(updated_at);
CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(payment_status);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    -- Preserve an explicit event-time supplied by the generator or backfill.
    -- For ordinary application updates that do not set updated_at, use server UTC.
    IF NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at THEN
        NEW.updated_at = timezone('UTC', now());
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['cities', 'zones', 'customers', 'drivers', 'vehicles', 'rides', 'payments']
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%I_updated_at ON %I', table_name, table_name);
        EXECUTE format(
            'CREATE TRIGGER trg_%I_updated_at BEFORE UPDATE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION set_updated_at()',
            table_name,
            table_name
        );
    END LOOP;
END;
$$;
