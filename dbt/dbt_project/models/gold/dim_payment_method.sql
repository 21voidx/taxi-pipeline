{{
  config(
    materialized = 'table',
    tags         = ['gold', 'dim']
  )
}}

/*
  GOLD DIM: dim_payment_method
  ══════════════════════════════════════════════════════════════
  Dimensi channel pembayaran: CASH, EWALLET, CARD, BANK (SCD-1).
  Grain: satu baris per metode pembayaran aktif.
*/

SELECT
    {{ dbt_utils.generate_surrogate_key(['method_id']) }}   AS payment_method_sk,
    method_id,
    method_code,
    method_name,
    method_type,
    provider,
    is_active,

    -- Display grouping
    CASE method_type
      WHEN 'EWALLET' THEN 'Digital Wallet'
      WHEN 'CASH'    THEN 'Tunai'
      WHEN 'CARD'    THEN 'Kartu Kredit/Debit'
      WHEN 'BANK'    THEN 'Virtual Account'
      ELSE 'Lainnya'
    END                                                     AS method_category,

    CURRENT_TIMESTAMP()                                     AS _loaded_at

FROM {{ source('mysql_raw', 'payment_methods') }}

WHERE is_active = 1
