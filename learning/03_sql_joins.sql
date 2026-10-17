-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 03 · JOINs y el modelo estrella
-- MAGIC
-- MAGIC | JOIN | devuelve | uso típico en NovaMart |
-- MAGIC |---|---|---|
-- MAGIC | `INNER` | filas con match en ambos lados | fact ⨝ dimensión |
-- MAGIC | `LEFT` | todo lo de la izquierda + match o NULL | clientes con o sin compras |
-- MAGIC | `FULL` | todo de ambos lados | conciliar dos fuentes |
-- MAGIC | `LEFT SEMI` | filas de la izquierda que tienen match (sin duplicar) | "clientes que compraron alguna vez" |
-- MAGIC | `LEFT ANTI` | filas de la izquierda SIN match | "clientes que nunca compraron", FKs huérfanas |
-- MAGIC | `CROSS` | producto cartesiano | generar grillas (fechas × tiendas) |

-- COMMAND ----------

USE CATALOG novamart;

-- COMMAND ----------

-- MAGIC %md ## 1. Fact + dimensiones (star join)

-- COMMAND ----------

SELECT
  s.region,
  s.store_format,
  p.department,
  ROUND(SUM(f.net_amount), 0) AS net_sales
FROM gold.fact_sales f
JOIN gold.dim_store   s ON f.store_key   = s.store_key
JOIN gold.dim_product p ON f.product_key = p.product_key
WHERE s.is_physical
GROUP BY ALL
ORDER BY net_sales DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. LEFT JOIN y la trampa del filtro
-- MAGIC Poner una condición sobre la tabla derecha en el `WHERE` convierte el LEFT en INNER (las filas sin match
-- MAGIC tienen NULL y no pasan el filtro). La condición va en el `ON`.

-- COMMAND ----------

-- ❌ Parece LEFT pero se comporta como INNER
SELECT COUNT(DISTINCT c.customer_id)
FROM gold.dim_customer c
LEFT JOIN gold.fact_sales f ON f.customer_key = c.customer_key
WHERE c.is_current AND f.order_date >= '2025-01-01';

-- COMMAND ----------

-- ✅ Todos los clientes vigentes, con o sin compras en 2025
SELECT
  COUNT(DISTINCT c.customer_id)                                   AS customers,
  COUNT(DISTINCT CASE WHEN f.order_id IS NOT NULL THEN c.customer_id END) AS buyers_2025
FROM gold.dim_customer c
LEFT JOIN gold.fact_sales f
  ON f.customer_key = c.customer_key AND f.order_date >= '2025-01-01'
WHERE c.is_current;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. SEMI y ANTI joins (más eficientes que IN / NOT IN con subconsultas)
-- MAGIC ⚠️ `NOT IN (subconsulta)` devuelve **cero filas** si la subconsulta tiene un solo NULL. `LEFT ANTI` no tiene ese problema.

-- COMMAND ----------

-- Productos del catálogo vigente que NUNCA se vendieron
SELECT p.product_id, p.product_name, p.department
FROM gold.dim_product p
LEFT ANTI JOIN gold.fact_sales f ON f.product_key = p.product_key
WHERE p.is_current AND p.product_key <> -1;

-- COMMAND ----------

-- Sesiones web de clientes que además compraron en tienda física
SELECT COUNT(*)
FROM gold.fact_web_sessions w
LEFT SEMI JOIN gold.fact_sales f
  ON f.customer_key = w.customer_key AND f.store_key <> -1
WHERE w.customer_key <> -1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. JOIN point-in-time contra una dimensión SCD2
-- MAGIC La ciudad del cliente **al momento de la compra** vs. su ciudad **actual**.
-- MAGIC El fact ya guarda la clave de la versión histórica; para la actual se une por clave natural.

-- COMMAND ----------

SELECT
  hist.city                 AS city_at_purchase,
  curr.city                 AS city_today,
  COUNT(*)                  AS lines,
  ROUND(SUM(f.net_amount))  AS net_sales
FROM gold.fact_sales f
JOIN gold.dim_customer hist ON f.customer_key = hist.customer_key
JOIN gold.dim_customer curr ON curr.customer_id = hist.customer_id AND curr.is_current
WHERE hist.city <> curr.city
GROUP BY ALL
ORDER BY lines DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. CROSS JOIN para completar huecos
-- MAGIC Un reporte "ventas por tienda y día" no muestra los días sin ventas. Se genera la grilla completa y se une.

-- COMMAND ----------

WITH grid AS (
  SELECT d.date, s.store_key, s.store_name
  FROM gold.dim_date d
  CROSS JOIN gold.dim_store s
  WHERE d.date BETWEEN '2025-12-20' AND '2025-12-31' AND s.is_physical
)
SELECT g.date, g.store_name, COALESCE(SUM(f.net_amount), 0) AS net_sales
FROM grid g
LEFT JOIN gold.fact_sales f ON f.order_date = g.date AND f.store_key = g.store_key
GROUP BY ALL
ORDER BY net_sales
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Cómo ejecuta Spark un JOIN (adelanto del módulo 10)
-- MAGIC * **Broadcast hash join**: la tabla chica (dimensión) se copia a todos los nodos → sin shuffle del fact. ✅
-- MAGIC * **Sort-merge join**: ambas tablas grandes → se re-particionan (shuffle) por la clave. 💸
-- MAGIC
-- MAGIC Mirá el plan:

-- COMMAND ----------

EXPLAIN FORMATTED
SELECT p.department, SUM(f.net_amount)
FROM gold.fact_sales f JOIN gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.department;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🏋️ Ejercicios
-- MAGIC 1. Clientes vigentes que se registraron (primera versión) en 2025 y todavía no compraron.
-- MAGIC 2. Devoluciones (`fact_returns`) cuyo pedido original fue en una tienda distinta a la región del cliente.
-- MAGIC 3. Validá integridad referencial: ¿hay `store_key` en `fact_sales` que no existan en `dim_store`?
-- MAGIC 4. ¿Por qué `SELECT COUNT(*) FROM fact_sales JOIN dim_customer USING (customer_key)` devuelve lo mismo que
-- MAGIC    `COUNT(*) FROM fact_sales`, pero si uniéramos por `customer_id` se inflaría? (pista: SCD2)
-- MAGIC
-- MAGIC <details><summary>💡 Soluciones</summary>
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- 1
-- MAGIC WITH buyers AS (SELECT DISTINCT c.customer_id FROM gold.fact_sales f JOIN gold.dim_customer c USING (customer_key)),
-- MAGIC first_version AS (SELECT customer_id, MIN(valid_from) AS since FROM gold.dim_customer
-- MAGIC                   WHERE customer_key <> -1 GROUP BY customer_id)
-- MAGIC SELECT c.customer_id, c.full_name, fv.since
-- MAGIC FROM gold.dim_customer c
-- MAGIC JOIN first_version fv USING (customer_id)
-- MAGIC LEFT ANTI JOIN buyers b USING (customer_id)
-- MAGIC WHERE c.is_current AND year(fv.since) = 2025;
-- MAGIC -- 3
-- MAGIC SELECT COUNT(*) FROM gold.fact_sales f LEFT ANTI JOIN gold.dim_store s USING (store_key);
-- MAGIC -- 4: customer_key es única por versión; customer_id se repite una vez por versión histórica,
-- MAGIC --    así que cada venta se multiplicaría por la cantidad de versiones del cliente (fan-out).
-- MAGIC ```
-- MAGIC </details>
