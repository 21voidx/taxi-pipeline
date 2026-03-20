-- ============================================================
-- silver_core.dim_payment_method
-- Dimensi metode pembayaran (static lookup)
-- Grain: 1 baris = 1 metode bayar
-- Materialization: table (full refresh)
-- ============================================================

SELECT
    ROW_NUMBER() OVER (ORDER BY payment_method_name) AS payment_method_key,
    payment_method_name,
    payment_method_category,
    is_digital_wallet

FROM (
    SELECT 'cash'          AS payment_method_name, 'cash'           AS payment_method_category, FALSE AS is_digital_wallet
    UNION ALL
    SELECT 'credit_card',                          'card',                                       FALSE
    UNION ALL
    SELECT 'debit_card',                           'card',                                       FALSE
    UNION ALL
    SELECT 'gopay',                                'digital_wallet',                             TRUE
    UNION ALL
    SELECT 'ovo',                                  'digital_wallet',                             TRUE
    UNION ALL
    SELECT 'dana',                                 'digital_wallet',                             TRUE
    UNION ALL
    SELECT 'shopeepay',                            'digital_wallet',                             TRUE
    UNION ALL
    SELECT 'unknown',                              'unknown',                                    FALSE
) AS t
