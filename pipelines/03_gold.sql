-- =============================================================================
-- Lakeflow Declarative Pipelines · GOLD (vistas materializadas)
-- -----------------------------------------------------------------------------
-- MATERIALIZED VIEW: el motor decide si puede refrescar INCREMENTALMENTE
-- (sólo lo que cambió) o si necesita recomputar. Con serverless lo hace solo.
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_customer
COMMENT 'Clientes SCD2 con clave subrogada determinística'
AS SELECT
    XXHASH64(customer_id, __START_AT)                   AS customer_key,
    customer_id,
    CONCAT_WS(' ', first_name, last_name)               AS full_name,
    email, gender, birth_date, city, state, country, segment,
    COALESCE(loyalty_tier, 'SIN PROGRAMA')              AS loyalty_tier,
    __START_AT                                          AS valid_from,
    __END_AT                                            AS valid_to,
    __END_AT IS NULL                                    AS is_current
FROM silver_customers_history;

CREATE OR REFRESH MATERIALIZED VIEW dim_product
AS SELECT
    XXHASH64(product_id, __START_AT) AS product_key,
    product_id, sku, product_name, brand, department, category, subcategory, color,
    list_price, unit_cost, is_active,
    __START_AT AS valid_from, __END_AT AS valid_to, __END_AT IS NULL AS is_current
FROM silver_products_history;

CREATE OR REFRESH MATERIALIZED VIEW fact_sales (
    CONSTRAINT product_resolved EXPECT (product_key IS NOT NULL)
)
COMMENT 'Ventas a nivel línea con FKs resueltas point-in-time'
CLUSTER BY (order_date)
AS
WITH c AS (
    SELECT customer_id, customer_key, valid_from,
           COALESCE(valid_to, TIMESTAMP'9999-12-31') AS valid_to,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY valid_from) AS rn
    FROM dim_customer
),
p AS (
    SELECT product_id, product_key, unit_cost, valid_from,
           COALESCE(valid_to, TIMESTAMP'9999-12-31') AS valid_to,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY valid_from) AS rn
    FROM dim_product
)
SELECT
    CAST(DATE_FORMAT(l.order_date, 'yyyyMMdd') AS INT)  AS order_date_key,
    l.order_date, l.order_ts, l.order_id, l.line_id,
    COALESCE(c.customer_key, -1)                        AS customer_key,
    p.product_key,
    COALESCE(o.store_id, -1)                            AS store_key,
    o.channel, o.promo_code,
    l.quantity, l.unit_price, l.gross_amount, l.discount_amount, l.net_amount,
    CAST(l.quantity * p.unit_cost AS DECIMAL(14,2))     AS cost_amount,
    CAST(l.net_amount - l.quantity * p.unit_cost AS DECIMAL(14,2)) AS margin_amount
FROM silver_order_lines l
JOIN silver_orders o ON o.order_id = l.order_id
LEFT JOIN c ON c.customer_id = o.customer_id
           AND l.order_ts >= CASE WHEN c.rn = 1 THEN TIMESTAMP'1900-01-01' ELSE c.valid_from END
           AND l.order_ts <  c.valid_to
LEFT JOIN p ON p.product_id = l.product_id
           AND l.order_ts >= CASE WHEN p.rn = 1 THEN TIMESTAMP'1900-01-01' ELSE p.valid_from END
           AND l.order_ts <  p.valid_to;

CREATE OR REFRESH MATERIALIZED VIEW mart_sales_daily_ldp
AS SELECT
    f.order_date, f.channel, p.department,
    COUNT(DISTINCT f.order_id) AS orders,
    SUM(f.quantity)            AS units,
    SUM(f.net_amount)          AS net_sales,
    SUM(f.margin_amount)       AS margin
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY ALL;
