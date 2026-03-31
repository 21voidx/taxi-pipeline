-- ============================================================
--  ride_marketing_mysql  (MySQL)
--  Tables: payment_methods, payments, promotions,
--          promo_usage, driver_incentives
-- ============================================================

USE ride_marketing_mysql;

-- ─────────────────────
--  PAYMENT METHODS
-- ─────────────────────
CREATE TABLE IF NOT EXISTS payment_methods (
    method_id      INT          AUTO_INCREMENT PRIMARY KEY,
    method_code    VARCHAR(20)  NOT NULL UNIQUE,
    method_name    VARCHAR(50)  NOT NULL,
    method_type    VARCHAR(20)  NOT NULL COMMENT 'CASH, EWALLET, CARD, BANK',
    provider       VARCHAR(50),
    is_active      TINYINT(1)   NOT NULL DEFAULT 1,
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT IGNORE INTO payment_methods (method_code, method_name, method_type, provider) VALUES
  ('CASH',       'Tunai',            'CASH',    NULL),
  ('OVO',        'OVO',              'EWALLET', 'OVO'),
  ('GOPAY',      'GoPay',            'EWALLET', 'Gojek'),
  ('DANA',       'DANA',             'EWALLET', 'DANA'),
  ('SHOPEEPAY',  'ShopeePay',        'EWALLET', 'Shopee'),
  ('BRIVA',      'BRI Virtual Acct', 'BANK',    'BRI'),
  ('MANDIRI',    'Mandiri Virtual',  'BANK',    'Mandiri'),
  ('CC_VISA',    'Kartu Kredit Visa','CARD',    'Visa'),
  ('CC_MC',      'Mastercard',       'CARD',    'Mastercard'),
  ('LINKAJA',    'LinkAja',          'EWALLET', 'Telkom');

-- ─────────────────────
--  PAYMENTS
-- ─────────────────────
CREATE TABLE IF NOT EXISTS payments (
    payment_id         CHAR(36)      NOT NULL PRIMARY KEY,
    payment_code       VARCHAR(30)   NOT NULL UNIQUE,
    ride_id            CHAR(36)      NOT NULL,
    passenger_id       CHAR(36)      NOT NULL,
    method_id          INT           NOT NULL REFERENCES payment_methods(method_id),
    amount             DECIMAL(12,2) NOT NULL,
    platform_fee       DECIMAL(10,2) DEFAULT 0.00,
    driver_earning     DECIMAL(12,2) NOT NULL,
    promo_discount     DECIMAL(10,2) DEFAULT 0.00,
    currency           VARCHAR(5)    NOT NULL DEFAULT 'IDR',
    payment_status     VARCHAR(20)   NOT NULL DEFAULT 'PENDING'
                         COMMENT 'PENDING, SUCCESS, FAILED, REFUNDED, EXPIRED',
    payment_gateway    VARCHAR(50),
    gateway_ref        VARCHAR(100),
    gateway_raw_response TEXT        COMMENT 'raw JSON from gateway, cleaned in Silver',
    paid_at            DATETIME,
    expired_at         DATETIME,
    refunded_at        DATETIME,
    refund_reason      VARCHAR(200),
    created_at         DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
                         ON UPDATE CURRENT_TIMESTAMP,
    _source_system     VARCHAR(20)   DEFAULT 'mysql'
);

CREATE INDEX idx_payments_ride      ON payments(ride_id);
CREATE INDEX idx_payments_passenger ON payments(passenger_id);
CREATE INDEX idx_payments_status    ON payments(payment_status);
CREATE INDEX idx_payments_updated   ON payments(updated_at);

-- ─────────────────────
--  PROMOTIONS
-- ─────────────────────
CREATE TABLE IF NOT EXISTS promotions (
    promo_id       INT           AUTO_INCREMENT PRIMARY KEY,
    promo_code     VARCHAR(30)   NOT NULL UNIQUE,
    promo_name     VARCHAR(100)  NOT NULL,
    description    TEXT,
    promo_type     VARCHAR(20)   NOT NULL COMMENT 'FLAT, PERCENT, FREE_RIDE',
    discount_value DECIMAL(10,2) NOT NULL COMMENT 'amount or percent',
    max_discount   DECIMAL(10,2) COMMENT 'cap for PERCENT type',
    min_fare       DECIMAL(10,2) DEFAULT 0,
    applicable_vehicle_types VARCHAR(100) COMMENT 'comma-separated: MOTOR,ECONOMI',
    applicable_zones VARCHAR(200) COMMENT 'comma-separated zone_codes',
    max_usage_total INT          DEFAULT 999999,
    max_usage_per_user INT       DEFAULT 1,
    current_usage  INT           DEFAULT 0,
    valid_from     DATETIME      NOT NULL,
    valid_until    DATETIME      NOT NULL,
    is_active      TINYINT(1)    DEFAULT 1,
    created_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ─────────────────────
--  PROMO USAGE
-- ─────────────────────
CREATE TABLE IF NOT EXISTS promo_usage (
    usage_id       BIGINT        AUTO_INCREMENT PRIMARY KEY,
    promo_id       INT           NOT NULL REFERENCES promotions(promo_id),
    passenger_id   CHAR(36)      NOT NULL,
    ride_id        CHAR(36)      NOT NULL,
    discount_given DECIMAL(10,2) NOT NULL,
    used_at        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_system VARCHAR(20)   DEFAULT 'mysql'
);

CREATE INDEX idx_promo_usage_promo     ON promo_usage(promo_id);
CREATE INDEX idx_promo_usage_passenger ON promo_usage(passenger_id);

-- ─────────────────────
--  DRIVER INCENTIVES
-- ─────────────────────
CREATE TABLE IF NOT EXISTS driver_incentives (
    incentive_id   BIGINT        AUTO_INCREMENT PRIMARY KEY,
    driver_id      CHAR(36)      NOT NULL,
    incentive_type VARCHAR(30)   NOT NULL COMMENT 'BONUS_TRIP, PEAK_HOUR, SURGE, REFERRAL',
    reference_id   CHAR(36)      COMMENT 'ride_id or null',
    amount         DECIMAL(10,2) NOT NULL,
    description    VARCHAR(200),
    period_date    DATE          NOT NULL,
    is_paid        TINYINT(1)    DEFAULT 0,
    paid_at        DATETIME,
    created_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _source_system VARCHAR(20)   DEFAULT 'mysql'
);

CREATE INDEX idx_incentives_driver ON driver_incentives(driver_id);
CREATE INDEX idx_incentives_period ON driver_incentives(period_date);
CREATE INDEX idx_incentives_type   ON driver_incentives(incentive_type);
