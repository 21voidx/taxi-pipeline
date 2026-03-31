-- ============================================================
-- tests/singular/assert_scd2_no_overlap_customer.sql
-- SCD Type 2 dim_customer tidak boleh memiliki dua baris
-- dengan rentang valid_from / valid_to yang overlap
-- untuk customer_id yang sama.
-- Test LULUS jika query mengembalikan 0 baris.
-- ============================================================

WITH ranked AS (
    SELECT
        customer_id,
        customer_key,
        valid_from,
        valid_to,
        LEAD(valid_from) OVER (
            PARTITION BY customer_id
            ORDER BY valid_from
        ) AS next_valid_from
    FROM {{ ref('dim_customer') }}
)

SELECT
    customer_id,
    customer_key,
    valid_from,
    valid_to,
    next_valid_from,
    'scd2_overlap' AS violation_type
FROM ranked
WHERE
    valid_to IS NOT NULL
    AND next_valid_from IS NOT NULL
    AND valid_to > next_valid_from
