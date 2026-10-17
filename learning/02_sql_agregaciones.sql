-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 02 · Agregaciones: GROUP BY, HAVING, funciones de agregado
-- MAGIC
-- MAGIC Agregar = pasar de muchas filas a pocas. Regla de oro: **toda columna del SELECT que no esté
-- MAGIC dentro de una función de agregado debe estar en el GROUP BY** (o usar `GROUP BY ALL`).

-- COMMAND ----------

USE CATALOG novamart_dev;  -- catálogo del target dev (en prod: novamart)

-- COMMAND ----------

-- MAGIC %md ## 1. Agregados básicos sobre 36M de filas (SF=1)

-- COMMAND ----------

SELECT
  COUNT(*)                    AS order_lines,
  COUNT(DISTINCT order_id)    AS orders,
  SUM(net_amount)             AS net_sales,
  AVG(net_amount)             AS avg_line,
  MIN(order_date)             AS first_day,
  MAX(order_date)             AS last_day
FROM gold.fact_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. GROUP BY
-- MAGIC Ventas por año y canal. `GROUP BY ALL` agrupa por todas las columnas no agregadas (atajo de Databricks).

-- COMMAND ----------

SELECT
  d.year,
  c.channel_code,
  COUNT(DISTINCT f.order_id)              AS orders,
  ROUND(SUM(f.net_amount), 0)             AS net_sales,
  ROUND(SUM(f.net_amount) / COUNT(DISTINCT f.order_id), 2) AS avg_ticket
FROM gold.fact_sales f
JOIN gold.dim_date d     ON f.order_date_key = d.date_key
JOIN gold.dim_channel c  ON f.channel_key   = c.channel_key
GROUP BY ALL
ORDER BY d.year, net_sales DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. HAVING: filtrar DESPUÉS de agregar
-- MAGIC `WHERE` filtra filas antes de agrupar; `HAVING` filtra grupos.

-- COMMAND ----------

-- Subcategorías con más de 1M USD vendidos en 2025 y margen menor al 20%
SELECT
  p.department, p.subcategory,
  ROUND(SUM(f.net_amount), 0)                          AS net_sales,
  ROUND(SUM(f.margin_amount) / SUM(f.net_amount), 3)  AS margin_pct
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_key = p.product_key
WHERE f.order_date >= '2025-01-01'
GROUP BY ALL
HAVING SUM(f.net_amount) > 1000000 AND SUM(f.margin_amount) / SUM(f.net_amount) < 0.20
ORDER BY net_sales DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Agregación condicional (pivot "a mano")
-- MAGIC `COUNT_IF`, `SUM(CASE WHEN ...)` y `FILTER (WHERE ...)` permiten varias métricas en una sola pasada.

-- COMMAND ----------

SELECT
  d.year_month,
  SUM(f.net_amount) FILTER (WHERE c.channel_group = 'Digital') AS digital_sales,
  SUM(f.net_amount) FILTER (WHERE c.channel_group = 'Físico')  AS store_sales,
  ROUND(SUM(f.net_amount) FILTER (WHERE c.channel_group = 'Digital') / SUM(f.net_amount), 3) AS digital_share
FROM gold.fact_sales f
JOIN gold.dim_date d    ON f.order_date_key = d.date_key
JOIN gold.dim_channel c ON f.channel_key   = c.channel_key
GROUP BY d.year_month
ORDER BY d.year_month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Agregados aproximados para grandes volúmenes
-- MAGIC `COUNT(DISTINCT)` exacto sobre 150M de eventos requiere un shuffle enorme.
-- MAGIC `APPROX_COUNT_DISTINCT` (HyperLogLog) da ~2% de error usando una fracción de la memoria.

-- COMMAND ----------

SELECT
  COUNT(DISTINCT anonymous_id)         AS exact_visitors,
  APPROX_COUNT_DISTINCT(anonymous_id)  AS approx_visitors,
  PERCENTILE_APPROX(n_events, ARRAY(0.5, 0.9, 0.99)) AS events_p50_p90_p99
FROM gold.fact_web_sessions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. ⚠️ Medidas semi-aditivas
-- MAGIC El stock (`on_hand_qty`) se puede sumar entre tiendas, **pero no entre semanas**: sumar el stock de 52
-- MAGIC domingos no significa nada. Para el tiempo se usa promedio o el último valor.

-- COMMAND ----------

SELECT
  snapshot_date,
  SUM(on_hand_qty)           AS units_in_stock_all_stores,     -- ✅ suma entre tiendas
  ROUND(AVG(fill_rate), 3)   AS avg_fill_rate,
  COUNT_IF(is_stockout)      AS stockouts
FROM gold.fact_inventory_snapshot
GROUP BY snapshot_date
ORDER BY snapshot_date DESC
LIMIT 12;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🏋️ Ejercicios
-- MAGIC 1. Top 10 marcas por margen total en 2024.
-- MAGIC 2. Para cada `commercial_event` (Hot Sale, Black Friday...), unidades vendidas y ticket promedio vs. un día normal.
-- MAGIC 3. Medios de pago (`dim_payment`) con más de 6 cuotas: ¿qué % de la venta online representan?
-- MAGIC 4. Stock promedio semanal por tienda en el último trimestre (¡cuidado con la semi-aditividad!).
-- MAGIC
-- MAGIC <details><summary>💡 Soluciones</summary>
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- 1
-- MAGIC SELECT p.brand, SUM(f.margin_amount) margin FROM gold.fact_sales f
-- MAGIC JOIN gold.dim_product p USING (product_key) WHERE f.order_date BETWEEN '2024-01-01' AND '2024-12-31'
-- MAGIC GROUP BY p.brand ORDER BY margin DESC LIMIT 10;
-- MAGIC -- 2
-- MAGIC SELECT COALESCE(d.commercial_event, 'Día normal') evento,
-- MAGIC        SUM(f.quantity) / COUNT(DISTINCT d.date) units_per_day,
-- MAGIC        SUM(f.net_amount) / COUNT(DISTINCT f.order_id) avg_ticket
-- MAGIC FROM gold.fact_sales f JOIN gold.dim_date d ON f.order_date_key = d.date_key GROUP BY 1;
-- MAGIC -- 3
-- MAGIC SELECT SUM(f.net_amount) FILTER (WHERE pm.installments > 6) / SUM(f.net_amount)
-- MAGIC FROM gold.fact_sales f JOIN gold.dim_payment pm USING (payment_key)
-- MAGIC JOIN gold.dim_channel c USING (channel_key) WHERE c.channel_group = 'Digital';
-- MAGIC -- 4: primero total por tienda y semana (suma), después promedio en el tiempo
-- MAGIC WITH weekly AS (SELECT store_key, snapshot_date, SUM(on_hand_qty) units
-- MAGIC                 FROM gold.fact_inventory_snapshot WHERE snapshot_date >= add_months(current_date(), -3)
-- MAGIC                 GROUP BY ALL)
-- MAGIC SELECT store_key, AVG(units) avg_weekly_stock FROM weekly GROUP BY store_key;
-- MAGIC ```
-- MAGIC </details>
