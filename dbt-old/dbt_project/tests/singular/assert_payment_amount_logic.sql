-- ============================================================
-- tests/singular/assert_payment_amount_logic.sql
-- Validasi kelogisan amount di fct_payment:
--   1. net_amount tidak boleh negatif untuk payment success
--   2. discount_amount tidak boleh melebihi gross_amount
--   3. tax_amount harus dalam kisaran 0–20% dari gross_amount
-- Test LULUS jika query mengembalikan 0 baris.
-- ============================================================

SELECT
    payment_id,
    payment_status,
    gross_amount,
    discount_amount,
    tax_amount,
    net_amount,
    CASE
        WHEN is_success AND net_amount < 0
            THEN 'net_amount_negative_on_success'
        WHEN discount_amount > gross_amount
            THEN 'discount_exceeds_gross'
        WHEN gross_amount > 0
             AND (tax_amount / gross_amount) NOT BETWEEN 0 AND 0.20
            THEN 'tax_rate_out_of_range'
        ELSE NULL
    END AS violation_type

FROM {{ ref('fct_payment') }}

WHERE
    (is_success AND net_amount < 0)
    OR (discount_amount > gross_amount)
    OR (
        gross_amount > 0
        AND (tax_amount / NULLIF(gross_amount, 0)) NOT BETWEEN 0 AND 0.20
    )
