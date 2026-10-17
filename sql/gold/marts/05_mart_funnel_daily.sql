-- =============================================================================
-- mart_funnel_daily: embudo de conversión web/app por día, plataforma y fuente
-- =============================================================================
CREATE OR REPLACE TABLE ${gold}.mart_funnel_daily
USING DELTA
COMMENT 'Embudo sesiones -> carrito -> checkout -> compra'
AS
SELECT
    session_date,
    platform,
    device_type,
    utm_source,
    utm_medium,
    COALESCE(experiment_id, 'sin experimento')           AS experiment_id,
    COUNT(*)                                             AS sessions,
    COUNT_IF(customer_key <> -1)                         AS identified_sessions,
    COUNT_IF(is_bounce)                                  AS bounces,
    COUNT_IF(reached_cart)                               AS cart_sessions,
    COUNT_IF(reached_checkout)                           AS checkout_sessions,
    COUNT_IF(converted)                                  AS converted_sessions,
    ROUND(COUNT_IF(converted) / COUNT(*), 5)             AS conversion_rate,
    ROUND(TRY_DIVIDE(COUNT_IF(converted), COUNT_IF(reached_cart)), 4) AS cart_to_purchase_rate,
    ROUND(AVG(duration_s), 1)                            AS avg_duration_s,
    PERCENTILE_APPROX(n_events, 0.5)                     AS median_events
FROM ${gold}.fact_web_sessions
GROUP BY ALL;
