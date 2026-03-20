-- ============================================================
-- silver_core.dim_trip_status
-- Dimensi status trip (static lookup)
-- Grain: 1 baris = 1 jenis status
-- Materialization: table (full refresh)
-- ============================================================

SELECT
    ROW_NUMBER() OVER (ORDER BY trip_status_name) AS trip_status_key,
    trip_status_name,
    is_terminal_status,
    is_successful_completion,
    status_description

FROM (
    SELECT
        'completed'  AS trip_status_name,
        TRUE         AS is_terminal_status,
        TRUE         AS is_successful_completion,
        'Trip selesai dan penumpang sudah diantar ke tujuan' AS status_description
    UNION ALL
    SELECT 'cancelled', TRUE,  FALSE, 'Trip dibatalkan sebelum pickup'
    UNION ALL
    SELECT 'no_show',   TRUE,  FALSE, 'Driver sudah tiba tapi penumpang tidak muncul'
    UNION ALL
    SELECT 'requested', FALSE, FALSE, 'Order baru dibuat, belum ada driver yang menerima'
    UNION ALL
    SELECT 'ongoing',   FALSE, FALSE, 'Trip sedang berlangsung'
) AS t
