-- ============================================================
-- Schema: ride_marketing_mysql  (MySQL 8)
-- Source system marketing & CRM ride-hailing
-- ============================================================

CREATE TABLE IF NOT EXISTS promotions (
    promo_id        VARCHAR(36)     PRIMARY KEY,
    promo_code      VARCHAR(30)     NOT NULL UNIQUE,
    promo_name      VARCHAR(100),
    promo_type      VARCHAR(30),
    discount_type   VARCHAR(20),
    discount_value  DECIMAL(12,2),
    start_date      DATE,
    end_date        DATE,
    budget_amount   DECIMAL(16,2),
    promo_status    VARCHAR(20),
    created_at      DATETIME        NOT NULL,
    updated_at      DATETIME        NOT NULL,
    INDEX idx_promo_status (promo_status),
    INDEX idx_promo_dates  (start_date, end_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS promo_redemptions (
    redemption_id       VARCHAR(36)     PRIMARY KEY,
    promo_id            VARCHAR(36)     NOT NULL,
    customer_id         VARCHAR(36)     NOT NULL,
    trip_id             VARCHAR(36)     NOT NULL,
    redeemed_ts         DATETIME,
    discount_amount     DECIMAL(14,2),
    redemption_status   VARCHAR(20),
    created_at          DATETIME        NOT NULL,
    updated_at          DATETIME        NOT NULL,
    INDEX idx_pr_promo_id    (promo_id),
    INDEX idx_pr_customer_id (customer_id),
    INDEX idx_pr_trip_id     (trip_id),
    INDEX idx_pr_redeemed_ts (redeemed_ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS customer_segments (
    id              BIGINT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    customer_id     VARCHAR(36)     NOT NULL,
    segment_name    VARCHAR(30)     NOT NULL,
    segment_score   DECIMAL(6,2),
    snapshot_date   DATE            NOT NULL,
    created_at      DATETIME        NOT NULL,
    updated_at      DATETIME        NOT NULL,
    INDEX idx_cs_customer_id  (customer_id),
    INDEX idx_cs_snapshot     (snapshot_date),
    INDEX idx_cs_segment_name (segment_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS campaign_spend (
    campaign_id     VARCHAR(36)     PRIMARY KEY,
    campaign_name   VARCHAR(100),
    channel_name    VARCHAR(50),
    spend_date      DATE,
    spend_amount    DECIMAL(16,2),
    impressions     BIGINT,
    clicks          BIGINT,
    installs        BIGINT,
    created_at      DATETIME        NOT NULL,
    updated_at      DATETIME        NOT NULL,
    INDEX idx_csp_spend_date (spend_date),
    INDEX idx_csp_channel    (channel_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
