-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 04 · Subconsultas, CTEs y vistas
-- MAGIC
-- MAGIC Una **CTE** (`WITH nombre AS (...)`) es una subconsulta con nombre. Hace que consultas complejas se
-- MAGIC lean de arriba hacia abajo como una receta. No materializa datos: el optimizador la "inlinea".

-- COMMAND ----------

USE CATALOG novamart;

-- COMMAND ----------

-- MAGIC %md ## 1. Subconsulta escalar y en FROM

-- COMMAND ----------

-- Pedidos con ticket mayor al doble del ticket promedio
SELECT order_id, order_net_amount
FROM gold.fact_order_fulfillment
WHERE order_net_amount > 2 * (SELECT AVG(order_net_amount) FROM gold.fact_order_fulfillment)
ORDER BY order_net_amount DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md ## 2. Subconsulta correlacionada (y por qué conviene reescribirla)

-- COMMAND ----------

-- Productos cuyo precio supera el promedio de SU subcategoría
SELECT p.product_name, p.subcategory, p.list_price
FROM gold.dim_product p
WHERE p.is_current
  AND p.list_price > (SELECT AVG(x.list_price) FROM gold.dim_product x
                      WHERE x.subcategory = p.subcategory AND x.is_current);

-- COMMAND ----------

-- Lo mismo con CTE + JOIN (o con una función de ventana, módulo 05): una sola pasada
WITH avg_sub AS (
  SELECT subcategory, AVG(list_price) AS avg_price
  FROM gold.dim_product WHERE is_current GROUP BY subcategory
)
SELECT p.product_name, p.subcategory, p.list_price, ROUND(a.avg_price, 2) AS avg_price
FROM gold.dim_product p JOIN avg_sub a USING (subcategory)
WHERE p.is_current AND p.list_price > a.avg_price;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. CTEs encadenadas: análisis de "primera compra → segunda compra"
-- MAGIC ¿Cuántos días tarda un cliente nuevo en volver a comprar, según el canal de su primera compra?

-- COMMAND ----------

WITH orders_by_customer AS (
  SELECT
    c.customer_id,
    f.order_id,
    MIN(f.order_ts)      AS order_ts,
    MIN(ch.channel_code) AS channel
  FROM gold.fact_sales f
  JOIN gold.dim_customer c ON f.customer_key = c.customer_key
  JOIN gold.dim_channel ch ON f.channel_key = ch.channel_key
  WHERE f.customer_key <> -1
  GROUP BY c.customer_id, f.order_id
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts) AS n
  FROM orders_by_customer
),
first_two AS (
  SELECT
    customer_id,
    MAX(CASE WHEN n = 1 THEN channel END)  AS first_channel,
    MAX(CASE WHEN n = 1 THEN order_ts END) AS first_ts,
    MAX(CASE WHEN n = 2 THEN order_ts END) AS second_ts
  FROM ranked WHERE n <= 2
  GROUP BY customer_id
)
SELECT
  first_channel,
  COUNT(*)                                                     AS new_customers,
  ROUND(AVG(CAST(second_ts IS NOT NULL AS INT)), 3)            AS repurchase_rate,
  PERCENTILE_APPROX(DATEDIFF(second_ts, first_ts), 0.5)        AS median_days_to_2nd
FROM first_two
GROUP BY first_channel
ORDER BY new_customers DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Vistas: reutilizar lógica
-- MAGIC * **Vista temporal**: vive en la sesión.
-- MAGIC * **Vista permanente** (`CREATE VIEW gold.x`): queda en Unity Catalog, con permisos y linaje.
-- MAGIC * **Vista materializada**: guarda el resultado y se refresca (incrementalmente si es posible).

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW high_value_customers AS
SELECT customer_id, monetary, rfm_segment
FROM gold.mart_customer_rfm
WHERE rfm_segment IN ('Campeones', 'Leales');

SELECT rfm_segment, COUNT(*), ROUND(AVG(monetary), 2) FROM high_value_customers GROUP BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. EXISTS
-- MAGIC Clientes Campeones que además navegaron la app en el último mes de datos.

-- COMMAND ----------

SELECT COUNT(*)
FROM gold.mart_customer_rfm r
WHERE r.rfm_segment = 'Campeones'
  AND EXISTS (
    SELECT 1 FROM gold.fact_web_sessions w
    JOIN gold.dim_customer c ON w.customer_key = c.customer_key
    WHERE c.customer_id = r.customer_id
      AND w.platform = 'APP'
      AND w.session_date >= (SELECT DATE_SUB(MAX(session_date), 30) FROM gold.fact_web_sessions)
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🏋️ Ejercicios
-- MAGIC 1. Con CTEs: top 3 subcategorías por venta en cada país de tienda (pista: necesitarás ROW_NUMBER, módulo 05).
-- MAGIC 2. Tiendas cuya venta 2025 creció más que el promedio de crecimiento de su región.
-- MAGIC 3. Creá una vista permanente `gold.v_kpi_mensual` con ventas, margen, pedidos y ticket por mes.
-- MAGIC
-- MAGIC <details><summary>💡 Solución 2</summary>
-- MAGIC
-- MAGIC ```sql
-- MAGIC WITH by_store AS (
-- MAGIC   SELECT s.store_key, s.region,
-- MAGIC          SUM(f.net_amount) FILTER (WHERE year(f.order_date) = 2024) s24,
-- MAGIC          SUM(f.net_amount) FILTER (WHERE year(f.order_date) = 2025) s25
-- MAGIC   FROM gold.fact_sales f JOIN gold.dim_store s USING (store_key) WHERE s.is_physical GROUP BY ALL),
-- MAGIC growth AS (SELECT *, s25 / s24 - 1 g FROM by_store WHERE s24 > 0),
-- MAGIC region_avg AS (SELECT region, AVG(g) avg_g FROM growth GROUP BY region)
-- MAGIC SELECT g.* FROM growth g JOIN region_avg r USING (region) WHERE g.g > r.avg_g;
-- MAGIC ```
-- MAGIC </details>
