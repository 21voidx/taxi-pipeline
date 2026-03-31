{#
  ============================================================
  macros/helpers.sql
  Macro-macro utilitas umum untuk project ride_hailing
  ============================================================
#}


{# ──────────────────────────────────────────────────────────
   generate_surrogate_key
   Wrapper dbt_utils.generate_surrogate_key dengan nama
   yang lebih pendek dan konsisten di seluruh project.
   Contoh: {{ surrogate_key(['customer_id', 'valid_from']) }}
────────────────────────────────────────────────────────── #}
{% macro surrogate_key(field_list) %}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}


{# ──────────────────────────────────────────────────────────
   date_to_key
   Mengubah kolom DATE / TIMESTAMP menjadi integer date key
   format YYYYMMDD yang dipakai di dimension dan fact.
   Contoh: {{ date_to_key('request_ts') }}  → 20260115
────────────────────────────────────────────────────────── #}
{% macro date_to_key(col) %}
    CAST(FORMAT_DATE('%Y%m%d', DATE({{ col }}, 'Asia/Jakarta')) AS INT64)
{% endmacro %}


{# ──────────────────────────────────────────────────────────
   hour_key
   Mengambil jam (0–23) dari TIMESTAMP, disesuaikan WIB.
   Contoh: {{ hour_key('request_ts') }}  → 7
────────────────────────────────────────────────────────── #}
{% macro hour_key(col) %}
    EXTRACT(HOUR FROM DATETIME({{ col }}, 'Asia/Jakarta'))
{% endmacro %}


{# ──────────────────────────────────────────────────────────
   safe_div
   Pembagian aman yang mengembalikan NULL (bukan error)
   ketika penyebut = 0.
   Contoh: {{ safe_div('completed', 'total') }}
────────────────────────────────────────────────────────── #}
{% macro safe_div(numerator, denominator) %}
    SAFE_DIVIDE({{ numerator }}, {{ denominator }})
{% endmacro %}


{# ──────────────────────────────────────────────────────────
   round_idr
   Pembulatan ke satuan terkecil IDR yang dipakai di laporan.
   Contoh: {{ round_idr('net_amount') }}
────────────────────────────────────────────────────────── #}
{% macro round_idr(col, nearest=500) %}
    ROUND({{ col }} / {{ nearest }}) * {{ nearest }}
{% endmacro %}


{# ──────────────────────────────────────────────────────────
   incremental_filter
   Menambahkan filter WHERE pada incremental run agar
   hanya memproses baris baru / berubah.
   Mengacu pada var 'incremental_lookback_days'.
   Contoh: {{ incremental_filter('updated_at') }}
────────────────────────────────────────────────────────── #}
{% macro incremental_filter(ts_col, lookback_var='incremental_lookback_days') %}
    {% if is_incremental() %}
        WHERE {{ ts_col }} >= TIMESTAMP_SUB(
            (SELECT MAX({{ ts_col }}) FROM {{ this }}),
            INTERVAL {{ var(lookback_var, 3) }} DAY
        )
    {% endif %}
{% endmacro %}


{# ──────────────────────────────────────────────────────────
   get_current_timestamp_wib
   Timestamp sekarang dalam zona WIB (UTC+7) untuk
   kolom audit seperti _dbt_loaded_at.
────────────────────────────────────────────────────────── #}
{% macro current_ts_wib() %}
    DATETIME(CURRENT_TIMESTAMP(), 'Asia/Jakarta')
{% endmacro %}


{# ──────────────────────────────────────────────────────────
   classify_age_group
   Mengelompokkan umur ke dalam bucket analitik.
   Contoh: {{ classify_age_group('birth_date') }}
────────────────────────────────────────────────────────── #}
{% macro classify_age_group(birth_date_col) %}
    CASE
        WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), {{ birth_date_col }}, YEAR) < 18
            THEN 'under_18'
        WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), {{ birth_date_col }}, YEAR) BETWEEN 18 AND 24
            THEN '18_24'
        WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), {{ birth_date_col }}, YEAR) BETWEEN 25 AND 34
            THEN '25_34'
        WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), {{ birth_date_col }}, YEAR) BETWEEN 35 AND 44
            THEN '35_44'
        WHEN DATE_DIFF(CURRENT_DATE('Asia/Jakarta'), {{ birth_date_col }}, YEAR) BETWEEN 45 AND 54
            THEN '45_54'
        ELSE '55_plus'
    END
{% endmacro %}


{# ──────────────────────────────────────────────────────────
   classify_time_bucket
   Mengklasifikasikan jam ke bucket operasional taxi.
   Contoh: {{ classify_time_bucket('hour_num') }}
────────────────────────────────────────────────────────── #}
{% macro classify_time_bucket(hour_col) %}
    CASE
        WHEN {{ hour_col }} BETWEEN 5 AND 8   THEN 'early_morning'
        WHEN {{ hour_col }} BETWEEN 9 AND 11  THEN 'morning'
        WHEN {{ hour_col }} BETWEEN 12 AND 14 THEN 'midday'
        WHEN {{ hour_col }} BETWEEN 15 AND 17 THEN 'afternoon'
        WHEN {{ hour_col }} BETWEEN 18 AND 21 THEN 'evening_peak'
        WHEN {{ hour_col }} BETWEEN 22 AND 23 THEN 'night'
        ELSE 'midnight'
    END
{% endmacro %}


{# ──────────────────────────────────────────────────────────
   generate_date_spine_cte
   CTE helper: menghasilkan deretan tanggal dari
   start_date hingga end_date (dipakai di dim_date).
────────────────────────────────────────────────────────── #}
{% macro generate_date_spine_cte(start_date, end_date) %}
    {{ dbt_utils.date_spine(
        datepart  = "day",
        start_date= "cast('" ~ start_date ~ "' as date)",
        end_date  = "cast('" ~ end_date   ~ "' as date)"
    ) }}
{% endmacro %}
