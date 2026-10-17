-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 05 · Funciones de ventana (window functions)
-- MAGIC
-- MAGIC Una función de ventana calcula un valor **para cada fila** mirando un conjunto de filas relacionadas,
-- MAGIC **sin colapsarlas** (a diferencia de GROUP BY).
-- MAGIC ```sql
-- MAGIC funcion(...) OVER (PARTITION BY grupo ORDER BY orden ROWS BETWEEN ... AND ...)
-- MAGIC ```
-- MAGIC | familia | funciones |
-- MAGIC |---|---|
-- MAGIC | ranking | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `PERCENT_RANK` |
-- MAGIC | desplazamiento | `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE` |
-- MAGIC | agregados | `SUM`, `AVG`, `COUNT`, `MIN`, `MAX` ... `OVER (...)` |

-- COMMAND ----------

USE CATALOG novamart;

-- COMMAND ----------

-- MAGIC %md ## 1. Ranking: top N por grupo (el clásico de entrevistas)

-- COMMAND ----------

WITH sales_by_product AS (
  SELECT p.department, p.product_name, SUM(f.net_amount) AS net_sales
  FROM gold.fact_sales f JOIN gold.dim_product p USING (product_key)
  WHERE f.order_date >= '2025-01-01'
  GROUP BY ALL
)
SELECT *
FROM sales_by_product
QUALIFY ROW_NUMBER() OVER (PARTITION BY department ORDER BY net_sales DESC) <= 3   -- QUALIFY: filtra sobre ventanas
ORDER BY department, net_sales DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ROW_NUMBER vs RANK vs DENSE_RANK
-- MAGIC Con empates: `ROW_NUMBER` 1,2,3 · `RANK` 1,1,3 · `DENSE_RANK` 1,1,2.

-- COMMAND ----------

SELECT department, price_band, COUNT(*) AS products,
  ROW_NUMBER() OVER (PARTITION BY department ORDER BY COUNT(*) DESC) AS rn,
  RANK()       OVER (PARTITION BY department ORDER BY COUNT(*) DESC) AS rk,
  DENSE_RANK() OVER (PARTITION BY department ORDER BY COUNT(*) DESC) AS drk
FROM gold.dim_product WHERE is_current
GROUP BY ALL
ORDER BY department, rn;

-- COMMAND ----------

-- MAGIC %md ## 2. Acumulados y medias móviles

-- COMMAND ----------

WITH daily AS (
  SELECT order_date, SUM(net_sales) AS net_sales
  FROM gold.mart_sales_daily GROUP BY order_date
)
SELECT
  order_date,
  net_sales,
  SUM(net_sales) OVER (PARTITION BY YEAR(order_date) ORDER BY order_date)            AS ytd_sales,
  AVG(net_sales) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7d,
  net_sales / SUM(net_sales) OVER (PARTITION BY DATE_TRUNC('MONTH', order_date))      AS share_of_month
FROM daily
ORDER BY order_date DESC
LIMIT 60;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ⚠️ ROWS vs RANGE
-- MAGIC * `ROWS BETWEEN 6 PRECEDING` = las 6 **filas** anteriores (si falta un día, toma uno de más atrás).
-- MAGIC * `RANGE BETWEEN INTERVAL 6 DAYS PRECEDING` = por **valor** de la columna de orden (respeta huecos).
-- MAGIC * Sin frame explícito y con `ORDER BY`, el default es `RANGE UNBOUNDED PRECEDING` → acumulado.

-- COMMAND ----------

-- MAGIC %md ## 3. LAG / LEAD: comparar con la fila anterior

-- COMMAND ----------

-- Crecimiento mes contra mes y año contra año por canal
WITH monthly AS (
  SELECT DATE_TRUNC('MONTH', order_date) AS month, channel_code, SUM(net_sales) AS net_sales
  FROM gold.mart_sales_daily GROUP BY ALL
)
SELECT
  month, channel_code, net_sales,
  ROUND(net_sales / LAG(net_sales, 1)  OVER w - 1, 3) AS mom_growth,
  ROUND(net_sales / LAG(net_sales, 12) OVER w - 1, 3) AS yoy_growth
FROM monthly
WINDOW w AS (PARTITION BY channel_code ORDER BY month)    -- ventana con nombre, reutilizable
ORDER BY channel_code, month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Sessionización con SQL puro (gaps & islands)
-- MAGIC El clickstream trae `session_id`, pero en la vida real muchas veces hay que **inventarlo**:
-- MAGIC una sesión nueva empieza cuando pasan más de 30 minutos sin actividad.
-- MAGIC 1. `LAG` para ver el evento anterior del mismo visitante.
-- MAGIC 2. Marcar `1` si el hueco > 30 min.
-- MAGIC 3. `SUM` acumulado de esas marcas = número de sesión.

-- COMMAND ----------

WITH ev AS (
  SELECT anonymous_id, event_ts, event_type,
         LAG(event_ts) OVER (PARTITION BY anonymous_id ORDER BY event_ts) AS prev_ts
  FROM silver.web_events
  WHERE event_date = (SELECT MAX(event_date) FROM silver.web_events)
),
flagged AS (
  SELECT *,
    CASE WHEN prev_ts IS NULL OR event_ts > prev_ts + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session
  FROM ev
),
sessions AS (
  SELECT *, SUM(new_session) OVER (PARTITION BY anonymous_id ORDER BY event_ts) AS session_n
  FROM flagged
)
SELECT anonymous_id, session_n, MIN(event_ts) AS start_ts, MAX(event_ts) AS end_ts, COUNT(*) AS events,
       MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS converted
FROM sessions
GROUP BY anonymous_id, session_n
ORDER BY events DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Deduplicación con ROW_NUMBER
-- MAGIC Exactamente lo que hace silver con los pedidos re-enviados: quedarse con la primera llegada.

-- COMMAND ----------

SELECT order_id, COUNT(*) AS copies
FROM bronze.orders
GROUP BY order_id HAVING COUNT(*) > 1
LIMIT 10;

-- COMMAND ----------

SELECT COUNT(*) AS bronze_rows, COUNT(DISTINCT order_id) AS unique_orders
FROM bronze.orders;

-- COMMAND ----------

SELECT *
FROM bronze.orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _ingested_at, _source_file) = 1
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🏋️ Ejercicios
-- MAGIC 1. Para cada cliente, monto de su compra anterior y días entre compras (LAG). ¿Cuál es la mediana por segmento?
-- MAGIC 2. Ranking de tiendas por venta dentro de su país, por año. ¿Qué tiendas subieron más de 5 posiciones?
-- MAGIC 3. Participación acumulada de productos (curva de Pareto): ¿qué % de SKUs explica el 80% de la venta?
-- MAGIC 4. En `fact_inventory_snapshot`, detectá rachas de 3+ semanas consecutivas en quiebre de stock (gaps & islands).
-- MAGIC
-- MAGIC <details><summary>💡 Solución 3 (Pareto)</summary>
-- MAGIC
-- MAGIC ```sql
-- MAGIC WITH s AS (SELECT p.product_id, SUM(f.net_amount) v FROM gold.fact_sales f
-- MAGIC            JOIN gold.dim_product p USING (product_key) GROUP BY 1),
-- MAGIC r AS (SELECT *, SUM(v) OVER (ORDER BY v DESC ROWS UNBOUNDED PRECEDING) / SUM(v) OVER () cum_share,
-- MAGIC              ROW_NUMBER() OVER (ORDER BY v DESC) / COUNT(*) OVER () sku_share FROM s)
-- MAGIC SELECT MIN(sku_share) FROM r WHERE cum_share >= 0.8;
-- MAGIC ```
-- MAGIC </details>
-- MAGIC
-- MAGIC <details><summary>💡 Solución 4 (rachas)</summary>
-- MAGIC
-- MAGIC ```sql
-- MAGIC WITH so AS (SELECT store_key, product_key, snapshot_date,
-- MAGIC        DATEADD(WEEK, -DENSE_RANK() OVER (PARTITION BY store_key, product_key ORDER BY snapshot_date), snapshot_date) grp
-- MAGIC        FROM gold.fact_inventory_snapshot WHERE is_stockout)
-- MAGIC SELECT store_key, product_key, MIN(snapshot_date) desde, COUNT(*) semanas
-- MAGIC FROM so GROUP BY store_key, product_key, grp HAVING COUNT(*) >= 3;
-- MAGIC ```
-- MAGIC </details>
