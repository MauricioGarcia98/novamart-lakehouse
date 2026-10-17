-- =============================================================================
-- v_sales_enriched: "One Big Table" lógica sobre el modelo estrella
-- -----------------------------------------------------------------------------
-- Vista (no materializada) que desnormaliza el fact con sus dimensiones.
-- Útil para analistas y para Genie / AI/BI: una sola tabla con nombres claros.
-- Comparación estrella vs OBT en docs/03_modelado_de_datos.md.
-- =============================================================================
CREATE OR REPLACE VIEW ${gold}.v_sales_enriched
COMMENT 'Ventas desnormalizadas con atributos de cliente, producto, tienda, canal y fecha'
AS
SELECT
    f.order_id,
    f.line_id,
    f.order_ts,
    d.date              AS order_date,
    d.year_month,
    d.day_name,
    d.commercial_event,
    c.customer_id,
    c.full_name         AS customer_name,
    c.segment           AS customer_segment,
    c.loyalty_tier,
    c.city              AS customer_city_at_purchase,
    c.country           AS customer_country_at_purchase,
    p.sku,
    p.product_name,
    p.brand,
    p.department,
    p.category,
    p.subcategory,
    p.price_band,
    s.store_name,
    s.store_format,
    s.region            AS store_region,
    ch.channel_code     AS channel,
    ch.channel_group,
    pm.payment_method,
    pm.installments_band,
    f.promo_code,
    f.quantity,
    f.unit_price,
    f.discount_pct,
    f.gross_amount,
    f.discount_amount,
    f.net_amount,
    f.cost_amount,
    f.margin_amount
FROM ${gold}.fact_sales f
JOIN ${gold}.dim_date      d  ON f.order_date_key = d.date_key
JOIN ${gold}.dim_customer  c  ON f.customer_key   = c.customer_key
JOIN ${gold}.dim_product   p  ON f.product_key    = p.product_key
JOIN ${gold}.dim_store     s  ON f.store_key      = s.store_key
JOIN ${gold}.dim_channel   ch ON f.channel_key    = ch.channel_key
LEFT JOIN ${gold}.dim_payment pm ON f.payment_key = pm.payment_key;
