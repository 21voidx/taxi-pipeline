{#
  ============================================================
  macros/custom_tests.sql
  Generic tests tambahan yang belum tersedia di dbt core
  atau dbt_utils, disesuaikan dengan kebutuhan ride_hailing.
  ============================================================
#}


{# ──────────────────────────────────────────────────────────
   test_not_negative
   Gagal jika ada nilai negatif di kolom numerik.
   Cocok untuk gross_amount, actual_distance_km, dll.

   Pemakaian di schema.yml:
     - name: gross_amount
       tests:
         - ride_hailing.not_negative
────────────────────────────────────────────────────────── #}
{% test not_negative(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < 0
{% endtest %}


{# ──────────────────────────────────────────────────────────
   test_timestamp_order
   Gagal jika kolom 'after_col' lebih awal dari 'before_col'.
   Contoh: pickup_ts harus >= request_ts.

   Pemakaian:
     - name: pickup_ts
       tests:
         - ride_hailing.timestamp_order:
             before_col: request_ts
────────────────────────────────────────────────────────── #}
{% test timestamp_order(model, column_name, before_col) %}
    SELECT *
    FROM {{ model }}
    WHERE
        {{ column_name }} IS NOT NULL
        AND {{ before_col }} IS NOT NULL
        AND {{ column_name }} < {{ before_col }}
{% endtest %}


{# ──────────────────────────────────────────────────────────
   test_completed_trip_has_timestamps
   Trip berstatus 'completed' wajib punya pickup_ts & dropoff_ts.

   Pemakaian:
     - name: trip_status
       tests:
         - ride_hailing.completed_trip_has_timestamps
────────────────────────────────────────────────────────── #}
{% test completed_trip_has_timestamps(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE
        {{ column_name }} = 'completed'
        AND (pickup_ts IS NULL OR dropoff_ts IS NULL)
{% endtest %}


{# ──────────────────────────────────────────────────────────
   test_date_key_format
   Pastikan date_key bertipe INT64 dalam format YYYYMMDD
   (8 digit, antara 20000101 – 20991231).

   Pemakaian:
     - name: date_key
       tests:
         - ride_hailing.date_key_format
────────────────────────────────────────────────────────── #}
{% test date_key_format(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE
        {{ column_name }} IS NOT NULL
        AND (
            CAST({{ column_name }} AS STRING) NOT REGEXP r'^\d{8}$'
            OR {{ column_name }} < 20000101
            OR {{ column_name }} > 20991231
        )
{% endtest %}


{# ──────────────────────────────────────────────────────────
   test_net_amount_consistency
   net_amount = gross_amount - discount_amount + tax_amount
                + toll_amount + tip_amount  (toleransi Rp 500)

   Pemakaian:
     - name: net_amount
       tests:
         - ride_hailing.net_amount_consistency
────────────────────────────────────────────────────────── #}
{% test net_amount_consistency(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE
        ABS(
            {{ column_name }}
            - (gross_amount - discount_amount + tax_amount + toll_amount + tip_amount)
        ) > 500
{% endtest %}


{# ──────────────────────────────────────────────────────────
   test_is_current_unique_per_id
   Hanya boleh ada 1 record dengan is_current = TRUE
   per natural key (misal customer_id di dim_customer).

   Pemakaian:
     - name: is_current
       tests:
         - ride_hailing.is_current_unique_per_id:
             id_col: customer_id
────────────────────────────────────────────────────────── #}
{% test is_current_unique_per_id(model, column_name, id_col) %}
    SELECT {{ id_col }}, COUNT(*) AS cnt
    FROM {{ model }}
    WHERE {{ column_name }} = TRUE
    GROUP BY {{ id_col }}
    HAVING cnt > 1
{% endtest %}


{# ──────────────────────────────────────────────────────────
   test_rating_score_range
   Skor rating harus antara 1–5.

   Pemakaian:
     - name: rating_score
       tests:
         - ride_hailing.rating_score_range
────────────────────────────────────────────────────────── #}
{% test rating_score_range(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} NOT BETWEEN 1 AND 5
{% endtest %}


{# ──────────────────────────────────────────────────────────
   test_referential_integrity
   Memastikan setiap nilai di FK column ada di parent table.
   Pengganti not_null + relationships untuk query yang lebih
   informatif.

   Pemakaian:
     - name: driver_id
       tests:
         - ride_hailing.referential_integrity:
             to: ref('dim_driver')
             field: driver_id
────────────────────────────────────────────────────────── #}
{% test referential_integrity(model, column_name, to, field) %}
    SELECT child.{{ column_name }}
    FROM {{ model }} child
    LEFT JOIN {{ to }} parent
        ON child.{{ column_name }} = parent.{{ field }}
    WHERE
        child.{{ column_name }} IS NOT NULL
        AND parent.{{ field }} IS NULL
{% endtest %}
