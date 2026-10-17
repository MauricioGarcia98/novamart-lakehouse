-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 13 · Modelado de datos: de las preguntas del negocio al modelo estrella
-- MAGIC
-- MAGIC Teoría completa y diagramas en `docs/03_modelado_de_datos.md` y `docs/diagrams/`. Acá lo aplicamos.
-- MAGIC
-- MAGIC ## Los 4 pasos de Kimball
-- MAGIC 1. **Elegir el proceso de negocio** → "vender", "despachar", "devolver", "reponer stock", "navegar".
-- MAGIC 2. **Declarar el grano** → *¿qué representa UNA fila?* "una línea de pedido". Es la decisión más importante.
-- MAGIC 3. **Identificar las dimensiones** → quién, qué, dónde, cuándo, cómo: cliente, producto, tienda, fecha, canal, pago.
-- MAGIC 4. **Identificar los hechos** → medidas numéricas del evento: cantidad, importe bruto, descuento, neto, costo.
-- MAGIC
-- MAGIC ## Matriz de bus (dimensiones conformadas)
-- MAGIC | proceso \ dimensión | fecha | cliente | producto | tienda | canal | pago |
-- MAGIC |---|:-:|:-:|:-:|:-:|:-:|:-:|
-- MAGIC | Ventas (`fact_sales`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
-- MAGIC | Fulfillment (`fact_order_fulfillment`) | ✅ ×4 | ✅ | | ✅ | ✅ | |
-- MAGIC | Devoluciones (`fact_returns`) | ✅ ×2 | ✅ | ✅ | ✅ | ✅ | |
-- MAGIC | Inventario (`fact_inventory_snapshot`) | ✅ | | ✅ | ✅ | | |
-- MAGIC | Navegación (`fact_web_sessions`) | ✅ | ✅ | | | | |
-- MAGIC
-- MAGIC Como todas comparten **las mismas** dimensiones, se pueden cruzar procesos ("drill-across").

-- COMMAND ----------

USE CATALOG novamart;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Verificar el grano (siempre, en cada fact)
-- MAGIC Si la clave del grano no es única, el modelo está roto y todas las sumas van a estar infladas.

-- COMMAND ----------

SELECT 'fact_sales' AS fact, COUNT(*) AS rows, COUNT(DISTINCT order_id, line_id) AS grain_keys FROM gold.fact_sales
UNION ALL
SELECT 'fact_order_fulfillment', COUNT(*), COUNT(DISTINCT order_id) FROM gold.fact_order_fulfillment
UNION ALL
SELECT 'fact_inventory_snapshot', COUNT(*), COUNT(DISTINCT snapshot_date, store_key, product_key) FROM gold.fact_inventory_snapshot
UNION ALL
SELECT 'fact_web_sessions', COUNT(*), COUNT(DISTINCT session_id) FROM gold.fact_web_sessions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Los 3 tipos de fact (+1)
-- MAGIC ### Transaccional — `fact_sales`
-- MAGIC Un evento puntual. Se inserta y no cambia. Todas las medidas son **aditivas**.

-- COMMAND ----------

SELECT order_id, line_id, order_ts, quantity, unit_price, discount_pct, gross_amount, discount_amount, net_amount, margin_amount
FROM gold.fact_sales LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Accumulating snapshot — `fact_order_fulfillment`
-- MAGIC Una fila por pedido que **se actualiza** a medida que avanza (pago → despacho → entrega).
-- MAGIC Tiene varias fechas (role-playing de `dim_date`) y medidas de *lag* entre hitos.

-- COMMAND ----------

SELECT
  ch.channel_code,
  shipping_method,
  COUNT(*)                                      AS orders,
  ROUND(AVG(hours_to_pay), 2)                   AS avg_h_to_pay,
  ROUND(AVG(hours_to_ship), 1)                  AS avg_h_to_ship,
  ROUND(PERCENTILE_APPROX(hours_to_deliver, 0.9), 1) AS p90_h_to_deliver,
  ROUND(AVG(CAST(is_on_time AS INT)), 3)        AS on_time_rate
FROM gold.fact_order_fulfillment f
JOIN gold.dim_channel ch USING (channel_key)
WHERE current_status = 'DELIVERED'
GROUP BY ALL ORDER BY orders DESC;

-- COMMAND ----------

-- Role-playing: la MISMA dim_date usada como fecha de pedido y fecha de entrega
SELECT od.day_name AS order_day, dd.day_name AS delivery_day, COUNT(*) AS orders
FROM gold.fact_order_fulfillment f
JOIN gold.dim_date od ON f.order_date_key     = od.date_key
JOIN gold.dim_date dd ON f.delivered_date_key = dd.date_key
GROUP BY ALL ORDER BY orders DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Periodic snapshot — `fact_inventory_snapshot`
-- MAGIC Foto del estado cada X tiempo. `on_hand_qty` es **semi-aditiva** (ver módulo 02).
-- MAGIC
-- MAGIC ### Factless / agregado — `fact_web_sessions`
-- MAGIC Registra que algo ocurrió (una sesión) con flags; las "medidas" son conteos.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. SCD: cómo cambian las dimensiones
-- MAGIC | tipo | qué hace | en NovaMart |
-- MAGIC |---|---|---|
-- MAGIC | 0 | nunca cambia | `birth_date` |
-- MAGIC | 1 | sobrescribe (sin historia) | `dim_store` |
-- MAGIC | 2 | nueva fila por cambio (historia completa) | `dim_customer`, `dim_product` |
-- MAGIC | 3 | columna "valor anterior" | (ejercicio) |
-- MAGIC
-- MAGIC ¿Por qué importa? Si un cliente se muda de Rosario a Córdoba, ¿las ventas de 2023 son de Rosario o de Córdoba?
-- MAGIC Con SCD2 la respuesta es correcta en ambos sentidos:

-- COMMAND ----------

-- "As was": ciudad al momento de la venta (la versión que apunta el fact)
SELECT c.city, ROUND(SUM(f.net_amount)) AS sales_as_was
FROM gold.fact_sales f JOIN gold.dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_key <> -1
GROUP BY c.city ORDER BY sales_as_was DESC LIMIT 8;

-- COMMAND ----------

-- "As is": todo re-expresado con la ciudad ACTUAL del cliente
SELECT cur.city, ROUND(SUM(f.net_amount)) AS sales_as_is
FROM gold.fact_sales f
JOIN gold.dim_customer h   ON f.customer_key = h.customer_key
JOIN gold.dim_customer cur ON cur.customer_id = h.customer_id AND cur.is_current
GROUP BY cur.city ORDER BY sales_as_is DESC LIMIT 8;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Claves subrogadas y miembros especiales
-- MAGIC * `customer_key = xxhash64(customer_id, valid_from)`: única por **versión**, estable si se reconstruye la dimensión.
-- MAGIC * `-1` = "Cliente no identificado" (compras en caja sin tarjeta). Así el fact **nunca** tiene FKs nulas
-- MAGIC   y los INNER JOIN no pierden ventas.

-- COMMAND ----------

SELECT CASE WHEN customer_key = -1 THEN 'anónimo' ELSE 'identificado' END AS tipo,
       COUNT(*) AS lines, ROUND(SUM(net_amount)) AS sales
FROM gold.fact_sales GROUP BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Estrella vs copo de nieve vs One Big Table (OBT)
-- MAGIC | | estrella | copo de nieve | OBT (`v_sales_enriched`) |
-- MAGIC |---|---|---|---|
-- MAGIC | joins por consulta | pocos (fact + dims) | muchos (dims normalizadas) | ninguno |
-- MAGIC | almacenamiento | medio | menor | mayor (repite atributos) |
-- MAGIC | cambios de atributos | 1 lugar | 1 lugar | reprocesar toda la tabla |
-- MAGIC | ideal para | BI, semántica clara | dims enormes y jerárquicas | ML features, herramientas sin joins |
-- MAGIC
-- MAGIC En Databricks, con Photon y formatos columnares, la estrella + vistas OBT suele ser el mejor equilibrio.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🏋️ Ejercicios de diseño (en papel / Excalidraw primero, después SQL)
-- MAGIC 1. **Nuevo proceso:** el área de marketing quiere medir campañas de email (envío, apertura, click, compra atribuida).
-- MAGIC    Declará el grano, las dimensiones (¿reutilizás `dim_customer` y `dim_date`?) y los hechos. Dibujalo.
-- MAGIC 2. **SCD3:** agregá a una copia de `dim_store` la columna `previous_manager_name`. Escribí el MERGE que la mantiene.
-- MAGIC 3. **Bridge table:** un pedido puede tener varios códigos promocionales. ¿Cómo lo modelarías sin romper el grano de `fact_sales`?
-- MAGIC 4. **Mini-dimensión:** `age_band`, `segment` y `loyalty_tier` cambian seguido y hacen crecer `dim_customer`. Diseñá una
-- MAGIC    mini-dimensión `dim_customer_profile` y explicá qué FK nueva tendría `fact_sales`.
-- MAGIC 5. **Data Vault:** ¿cómo quedarían `customers` y `orders` como hubs, links y satellites? ¿Cuándo convendría sobre Kimball?
-- MAGIC
-- MAGIC <details><summary>💡 Pista del ejercicio 1</summary>
-- MAGIC
-- MAGIC Grano: "un envío de email a un cliente". Dimensiones: fecha (role-playing: envío, apertura, click), cliente,
-- MAGIC campaña (nueva), plantilla. Hechos: flags `opened`, `clicked`, `converted` (factless + accumulating) y
-- MAGIC `attributed_revenue`. La atribución a `fact_sales` se hace por `customer_key` + ventana de 7 días.
-- MAGIC </details>
