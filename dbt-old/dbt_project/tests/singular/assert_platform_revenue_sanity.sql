-- ============================================================
-- tests/singular/assert_platform_revenue_sanity.sql
-- Platform revenue (net_amount - driver_payout) tidak boleh
-- negatif secara signifikan (lebih dari -Rp 1.000.000 per hari per kota).
-- Nilai sedikit negatif bisa terjadi karena timing payout vs payment,
-- tetapi nilai sangat negatif indikasi bug transformasi.
-- Test LULUS jika query mengembalikan 0 baris.
-- ============================================================

SELECT
    date_key,
    city,
    total_net_amount,
    total_driver_payout,
    estimated_platform_revenue
FROM {{ ref('dm_finance_daily_city') }}
WHERE estimated_platform_revenue < -1000000   -- threshold: -Rp 1 juta
