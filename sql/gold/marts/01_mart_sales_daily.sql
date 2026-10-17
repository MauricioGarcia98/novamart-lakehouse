-- =============================================================================
-- mart_sales_daily: agregado diario por tienda x departamento x canal
-- -----------------------------------------------------------------------------
-- Por qué existe: los dashboards consultan ventas por día cientos de veces.
-- Pre-agregar 36M de líneas a ~1M de filas hace que cada consulta lea 30x menos.
-- Es una tabla DERIVADA: se puede reconstruir siempre desde fact_sales.
-- =============================================================================
CREATE OR REPLACE TABLE ${gold}.mart_sales_daily
USING DELTA
COMMENT 'Ventas diarias agregadas por tienda, departamento y canal'
AS
SELECT
    f.order_date,
    f.order_date_key,
    d.year,
    d.month,
    d.year_month,
    d.day_name,
    d.commercial_event,
    s.store_key,
    s.store_name,
    s.store_format,
    s.country                                AS store_country,
    p.department,
    ch.channel_code,
    ch.channel_group,
    COUNT(DISTINCT f.order_id)               AS orders,
    COUNT(*)                                 AS order_lines,
    SUM(f.quantity)                          AS units,
    SUM(f.gross_amount)                      AS gross_sales,
    SUM(f.discount_amount)                   AS discounts,
    SUM(f.net_amount)                        AS net_sales,
    SUM(f.cost_amount)                       AS cost,
    SUM(f.margin_amount)                     AS margin,
    TRY_DIVIDE(SUM(f.margin_amount), SUM(f.net_amount)) AS margin_pct,
    TRY_DIVIDE(SUM(f.net_amount), COUNT(DISTINCT f.order_id)) AS avg_ticket
FROM ${gold}.fact_sales f
JOIN ${gold}.dim_date     d  ON f.order_date_key = d.date_key
JOIN ${gold}.dim_store    s  ON f.store_key      = s.store_key
JOIN ${gold}.dim_product  p  ON f.product_key    = p.product_key
JOIN ${gold}.dim_channel  ch ON f.channel_key    = ch.channel_key
GROUP BY ALL;
