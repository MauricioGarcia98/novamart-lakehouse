-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 07 · SQL avanzado: datos anidados, PIVOT, GROUPING SETS, MERGE y Time Travel
-- MAGIC
-- MAGIC Lo que separa a un analista de un data engineer: trabajar con JSON anidado, generar varios niveles de
-- MAGIC agregación en una pasada, modificar tablas con MERGE y viajar en el tiempo con Delta.

-- COMMAND ----------

USE CATALOG novamart;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Datos anidados: STRUCT, ARRAY, MAP
-- MAGIC `bronze.orders` es JSON: `payment` y `shipping` son structs, `items` es un array de structs.

-- COMMAND ----------

DESCRIBE bronze.orders;

-- COMMAND ----------

SELECT
  order_id,
  payment.method                  AS payment_method,     -- acceder a un campo de un struct
  shipping.city,
  SIZE(items)                     AS n_items,
  items[0].product_id             AS first_product,      -- índice de array (base 0)
  items
FROM bronze.orders
LIMIT 5;

-- COMMAND ----------

-- EXPLODE: una fila por elemento del array (así se construye silver.order_lines)
SELECT o.order_id, it.line_id, it.product_id, it.quantity, it.unit_price
FROM bronze.orders o
LATERAL VIEW EXPLODE(o.items) AS it
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Funciones de orden superior (sin explotar el array)
-- MAGIC `TRANSFORM`, `FILTER`, `AGGREGATE`, `EXISTS` operan dentro del array con lambdas: evitan explotar
-- MAGIC y re-agrupar (que cuesta un shuffle).

-- COMMAND ----------

SELECT
  order_id,
  AGGREGATE(items, CAST(0 AS DOUBLE), (acc, x) -> acc + x.quantity * x.unit_price) AS order_gross,
  FILTER(items, x -> x.discount_pct > 0)                                            AS discounted_items,
  EXISTS(items, x -> x.quantity <= 0)                                               AS has_bad_qty,
  TRANSFORM(items, x -> x.product_id)                                               AS product_ids
FROM bronze.orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. PIVOT / UNPIVOT

-- COMMAND ----------

SELECT * FROM (
  SELECT year, channel_code, net_sales FROM gold.mart_sales_daily
)
PIVOT (ROUND(SUM(net_sales)) FOR channel_code IN ('WEB' AS web, 'APP' AS app, 'STORE' AS store, 'MARKETPLACE' AS mkp))
ORDER BY year;

-- COMMAND ----------

-- UNPIVOT: columnas -> filas (el camino inverso, típico al cargar planillas)
WITH wide AS (
  SELECT order_id, payment_approved_ts, shipped_ts, delivered_ts
  FROM gold.fact_order_fulfillment LIMIT 5
)
SELECT * FROM wide
UNPIVOT (ts FOR milestone IN (payment_approved_ts AS paid, shipped_ts AS shipped, delivered_ts AS delivered));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. GROUPING SETS / ROLLUP / CUBE: varios niveles de totales en una consulta

-- COMMAND ----------

SELECT
  COALESCE(store_country, '— TOTAL —')   AS country,
  COALESCE(department, '— todos —')      AS department,
  ROUND(SUM(net_sales))                  AS net_sales,
  GROUPING_ID(store_country, department) AS level   -- 0 = detalle, 1 = subtotal país, 3 = total
FROM gold.mart_sales_daily
WHERE year = 2025
GROUP BY ROLLUP (store_country, department)
ORDER BY level, country, net_sales DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. MERGE: el comando más importante del data engineering
-- MAGIC Upsert atómico: actualiza lo que existe, inserta lo nuevo, opcionalmente borra. Practicamos en una copia.

-- COMMAND ----------

CREATE OR REPLACE TABLE ops.sandbox_prices AS
SELECT product_id, list_price, current_timestamp() AS updated_at
FROM gold.dim_product WHERE is_current AND product_id BETWEEN 1 AND 1000;

-- COMMAND ----------

MERGE INTO ops.sandbox_prices AS t
USING (
  SELECT product_id, ROUND(list_price * 1.10, 2) AS list_price FROM gold.dim_product
  WHERE is_current AND product_id BETWEEN 900 AND 1100         -- 900-1000 existen, 1001-1100 son nuevos
) AS s
ON t.product_id = s.product_id
WHEN MATCHED AND t.list_price <> s.list_price THEN
  UPDATE SET t.list_price = s.list_price, t.updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (product_id, list_price, updated_at) VALUES (s.product_id, s.list_price, current_timestamp())
WHEN NOT MATCHED BY SOURCE AND t.product_id < 10 THEN
  DELETE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Time Travel y auditoría con Delta
-- MAGIC Cada escritura crea una **versión**. Se puede consultar, comparar y restaurar.

-- COMMAND ----------

DESCRIBE HISTORY ops.sandbox_prices;

-- COMMAND ----------

-- ¿Qué cambió entre la versión 0 y la actual?
SELECT 'antes' AS v, COUNT(*), ROUND(AVG(list_price), 2) FROM ops.sandbox_prices VERSION AS OF 0
UNION ALL
SELECT 'ahora', COUNT(*), ROUND(AVG(list_price), 2) FROM ops.sandbox_prices;

-- COMMAND ----------

RESTORE TABLE ops.sandbox_prices TO VERSION AS OF 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Change Data Feed: el "diff" fila a fila
-- MAGIC Las tablas del proyecto tienen `delta.enableChangeDataFeed = true`. Sirve para propagar cambios
-- MAGIC incrementalmente a sistemas downstream.

-- COMMAND ----------

SELECT _change_type, COUNT(*)
FROM table_changes('silver.customers_history', 1)
GROUP BY _change_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. CTE recursiva: jerarquía del catálogo
-- MAGIC Útil para organigramas, categorías anidadas, grafos. Requiere Databricks Runtime 17.0+ o serverless.

-- COMMAND ----------

WITH RECURSIVE hierarchy (node, parent, level, path) AS (
  SELECT DISTINCT department, CAST(NULL AS STRING), 1, department FROM gold.dim_product WHERE is_current
  UNION ALL
  SELECT c.child, h.node, h.level + 1, CONCAT(h.path, ' > ', c.child)
  FROM hierarchy h
  JOIN (SELECT DISTINCT department AS parent, category AS child FROM gold.dim_product
        UNION SELECT DISTINCT category, subcategory FROM gold.dim_product) c
    ON c.parent = h.node
  WHERE h.level < 3
)
SELECT * FROM hierarchy ORDER BY path;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🏋️ Ejercicios
-- MAGIC 1. Sin explotar `items`, calculá el % de pedidos con al menos un ítem con descuento > 20%.
-- MAGIC 2. Con `CUBE (channel_code, department)`, armá una tabla con todos los subtotales y marcá cada nivel.
-- MAGIC 3. Escribí un MERGE que actualice el `segment` en una copia de `dim_customer` sólo si cambió, y que registre
-- MAGIC    cuántas filas actualizó (pista: `DESCRIBE HISTORY` → `operationMetrics`).
-- MAGIC 4. Usá `table_changes` sobre `gold.fact_order_fulfillment` para ver qué pedidos cambiaron de estado en la última corrida.
