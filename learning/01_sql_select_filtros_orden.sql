-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 01 · SQL desde cero: SELECT, WHERE, ORDER BY, LIMIT
-- MAGIC
-- MAGIC **Prerrequisito:** módulo 00. **Tablas:** `gold.dim_product`, `gold.dim_store`, `silver.orders`.
-- MAGIC
-- MAGIC Una consulta SQL se **escribe** en este orden:
-- MAGIC ```sql
-- MAGIC SELECT columnas FROM tabla WHERE filtro GROUP BY ... HAVING ... ORDER BY ... LIMIT n
-- MAGIC ```
-- MAGIC pero el motor la **ejecuta** en este otro: `FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT`.
-- MAGIC Entender ese orden explica el 80% de los errores de principiante (ej.: no poder usar un alias del SELECT en el WHERE).

-- COMMAND ----------

USE CATALOG novamart;

-- COMMAND ----------

-- MAGIC %md ## 1. Seleccionar columnas

-- COMMAND ----------

SELECT product_id, product_name, brand, list_price
FROM gold.dim_product
LIMIT 10;

-- COMMAND ----------

-- Columnas calculadas y alias
SELECT
  product_name,
  list_price,
  unit_cost,
  list_price - unit_cost                         AS unit_margin,
  ROUND((list_price - unit_cost) / list_price, 3) AS margin_pct
FROM gold.dim_product
WHERE is_current
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md ## 2. Filtrar filas con WHERE
-- MAGIC Operadores: `=  <>  <  >  <=  >=`, `AND / OR / NOT`, `IN (...)`, `BETWEEN a AND b`, `LIKE 'pat%'`, `IS NULL`.

-- COMMAND ----------

SELECT store_name, store_format, city, country, sales_area_sqm
FROM gold.dim_store
WHERE country IN ('AR', 'UY')
  AND store_format = 'Hipermercado'
  AND sales_area_sqm BETWEEN 7000 AND 12000;

-- COMMAND ----------

-- LIKE / ILIKE (sin distinguir mayúsculas)
SELECT DISTINCT subcategory, department
FROM gold.dim_product
WHERE subcategory ILIKE '%smart%' OR department = 'Gaming';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. NULL: el valor que no es igual a nada
-- MAGIC `NULL = NULL` **no** es verdadero: es `NULL`. Por eso se usa `IS NULL`. Las ventas en caja sin
-- MAGIC tarjeta de fidelidad tienen `customer_id` nulo.

-- COMMAND ----------

SELECT
  COUNT(*)                                        AS total_orders,
  COUNT(customer_id)                              AS with_customer,      -- COUNT(col) ignora NULLs
  COUNT_IF(customer_id IS NULL)                   AS anonymous,
  COUNT_IF(customer_id = NULL)                    AS trap_always_zero    -- ¡trampa!
FROM silver.orders
WHERE channel = 'STORE';

-- COMMAND ----------

-- MAGIC %md ## 4. Ordenar y limitar

-- COMMAND ----------

SELECT order_id, order_ts, channel, shipping_cost
FROM silver.orders
WHERE order_date = '2025-11-28'           -- Black Friday 2025
ORDER BY shipping_cost DESC NULLS LAST, order_ts
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Pensar en volumen desde el día 1
-- MAGIC `silver.orders` tiene millones de filas. Filtrar por la columna de *clustering* (`order_date`)
-- MAGIC permite **saltear archivos** (data skipping). Compará el plan de estas dos consultas
-- MAGIC (pestaña *Query profile* → "files pruned"):

-- COMMAND ----------

SELECT COUNT(*) FROM silver.orders WHERE order_date = '2025-06-15';   -- lee pocos archivos

-- COMMAND ----------

SELECT COUNT(*) FROM silver.orders WHERE DATE_FORMAT(order_ts, 'yyyy-MM-dd') = '2025-06-15';  -- función sobre la columna: lee más

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🏋️ Ejercicios
-- MAGIC 1. Listá los 15 productos vigentes (`is_current`) más caros del departamento *Electrónica*.
-- MAGIC 2. ¿Cuántos pedidos de canal `MARKETPLACE` usaron código promocional en diciembre de 2024?
-- MAGIC 3. Tiendas de formato *Express* abiertas después de 2020, ordenadas por superficie.
-- MAGIC 4. ¿Por qué `WHERE unit_margin > 10` falla en la segunda consulta del punto 1? Reescribila para que funcione.
-- MAGIC
-- MAGIC <details><summary>💡 Soluciones (intentá antes de mirar)</summary>
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- 1
-- MAGIC SELECT product_name, brand, list_price FROM gold.dim_product
-- MAGIC WHERE is_current AND department = 'Electrónica' ORDER BY list_price DESC LIMIT 15;
-- MAGIC -- 2
-- MAGIC SELECT COUNT(*) FROM silver.orders
-- MAGIC WHERE channel = 'MARKETPLACE' AND promo_code IS NOT NULL
-- MAGIC   AND order_date BETWEEN '2024-12-01' AND '2024-12-31';
-- MAGIC -- 3
-- MAGIC SELECT store_name, opened_date, sales_area_sqm FROM gold.dim_store
-- MAGIC WHERE store_format = 'Express' AND opened_date > '2020-12-31' ORDER BY sales_area_sqm DESC;
-- MAGIC -- 4: WHERE se evalúa ANTES que SELECT, el alias todavía no existe.
-- MAGIC SELECT * FROM (SELECT product_name, list_price - unit_cost AS unit_margin FROM gold.dim_product)
-- MAGIC WHERE unit_margin > 10;
-- MAGIC ```
-- MAGIC </details>
