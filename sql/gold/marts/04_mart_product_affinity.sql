-- =============================================================================
-- mart_product_affinity: análisis de canasta (qué subcategorías se compran juntas)
-- -----------------------------------------------------------------------------
-- Un self-join de líneas de pedido es O(n²) por pedido: con 36M de líneas
-- explota. Estrategia: (1) bajar el grano a subcategoría, (2) usar sólo los
-- últimos 180 días, (3) a.subcategory < b.subcategory para no duplicar pares.
-- Métricas: support, confidence y lift (lift > 1 => se compran juntos más de
-- lo esperable por azar).
-- =============================================================================
CREATE OR REPLACE TABLE ${gold}.mart_product_affinity
USING DELTA
COMMENT 'Pares de subcategorías con soporte, confianza y lift (market basket)'
AS
WITH params AS (
    SELECT DATE_SUB(MAX(order_date), 180) AS since FROM ${gold}.fact_sales
),
baskets AS (
    SELECT DISTINCT f.order_id, p.subcategory
    FROM ${gold}.fact_sales f
    JOIN ${gold}.dim_product p ON f.product_key = p.product_key
    CROSS JOIN params
    WHERE f.order_date >= params.since
),
totals AS (SELECT COUNT(DISTINCT order_id) AS n_orders FROM baskets),
item_freq AS (
    SELECT subcategory, COUNT(*) AS orders_with_item FROM baskets GROUP BY subcategory
),
pairs AS (
    SELECT a.subcategory AS item_a, b.subcategory AS item_b, COUNT(*) AS orders_together
    FROM baskets a
    JOIN baskets b ON a.order_id = b.order_id AND a.subcategory < b.subcategory
    GROUP BY a.subcategory, b.subcategory
)
SELECT
    p.item_a,
    p.item_b,
    p.orders_together,
    ROUND(p.orders_together / t.n_orders, 6)                                   AS support,
    ROUND(p.orders_together / fa.orders_with_item, 4)                          AS confidence_a_to_b,
    ROUND((p.orders_together / t.n_orders)
          / ((fa.orders_with_item / t.n_orders) * (fb.orders_with_item / t.n_orders)), 4) AS lift
FROM pairs p
CROSS JOIN totals t
JOIN item_freq fa ON fa.subcategory = p.item_a
JOIN item_freq fb ON fb.subcategory = p.item_b
WHERE p.orders_together >= 5;
