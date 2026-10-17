-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 06 · Tipos de datos, fechas, textos, NULLs y modo ANSI
-- MAGIC
-- MAGIC Los datos reales están sucios. NovaMart trae: fechas en 3 formatos, epoch en milisegundos, emails con
-- MAGIC mayúsculas y espacios, teléfonos en 4 formatos, montos que necesitan precisión exacta.

-- COMMAND ----------

USE CATALOG novamart_dev;  -- catálogo del target dev (en prod: novamart)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. DECIMAL vs DOUBLE: nunca uses DOUBLE para plata

-- COMMAND ----------

SELECT
  0.1 + 0.2                                         AS decimal_literal,
  CAST(0.1 AS DOUBLE) + CAST(0.2 AS DOUBLE)         AS double_sum,       -- 0.30000000000000004
  CAST(0.1 AS DOUBLE) + CAST(0.2 AS DOUBLE) = 0.3   AS double_equals;    -- false

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Modo ANSI: errores en vez de NULLs silenciosos
-- MAGIC Databricks (y Spark 4) trabajan con `ansi_mode = true`: un cast inválido **falla**. Para datos sucios
-- MAGIC se usan las funciones `try_*`, que devuelven NULL.

-- COMMAND ----------

SELECT
  TRY_CAST('12a' AS INT)                                    AS try_cast_bad,
  TRY_TO_TIMESTAMP('2025-03-01 10:00:00', 'yyyy-MM-dd HH:mm:ss') AS ok,
  TRY_TO_TIMESTAMP('01/03/2025', 'yyyy-MM-dd HH:mm:ss')     AS wrong_format,
  TRY_DIVIDE(10, 0)                                         AS safe_division;

-- COMMAND ----------

-- La realidad de bronze: dos formatos de fecha en la misma columna
SELECT
  order_ts AS raw,
  COALESCE(
    TRY_TO_TIMESTAMP(order_ts, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
    TRY_TO_TIMESTAMP(order_ts, 'yyyy-MM-dd HH:mm:ss')
  ) AS parsed
FROM bronze.orders
WHERE order_ts NOT LIKE '%T%'
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md ## 3. Fechas y horas

-- COMMAND ----------

SELECT
  event_ts                                          AS epoch_millis,
  TIMESTAMP_MILLIS(event_ts)                        AS ts_utc,
  FROM_UTC_TIMESTAMP(TIMESTAMP_MILLIS(event_ts), 'America/Argentina/Buenos_Aires') AS ts_ba,
  DATE_TRUNC('HOUR', TIMESTAMP_MILLIS(event_ts))    AS hour_bucket,
  DAYOFWEEK(TIMESTAMP_MILLIS(event_ts))             AS dow,
  DATE_ADD(TO_DATE(TIMESTAMP_MILLIS(event_ts)), 7)  AS plus_7d
FROM bronze.web_events
LIMIT 5;

-- COMMAND ----------

-- Distribución horaria de pedidos por canal (¿a qué hora compra la gente online?)
SELECT channel, HOUR(order_ts) AS hour, COUNT(*) AS orders
FROM silver.orders
GROUP BY ALL
ORDER BY channel, hour;

-- COMMAND ----------

-- MAGIC %md ## 4. Textos y expresiones regulares

-- COMMAND ----------

SELECT
  email                                                 AS raw_email,
  LOWER(TRIM(email))                                    AS clean_email,
  SPLIT_PART(LOWER(TRIM(email)), '@', 2)                AS domain,
  phone                                                 AS raw_phone,
  REGEXP_REPLACE(phone, '[^0-9]', '')                   AS digits,
  REGEXP_EXTRACT(email, '([0-9]+)@', 1)                 AS id_in_email,
  INITCAP(first_name)                                   AS name
FROM bronze.customers
WHERE email LIKE ' %' OR phone LIKE '(%'
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. NULLs: COALESCE, NULLIF, NVL2 y lógica de tres valores
-- MAGIC | expresión | resultado |
-- MAGIC |---|---|
-- MAGIC | `NULL AND FALSE` | `FALSE` |
-- MAGIC | `NULL AND TRUE` | `NULL` |
-- MAGIC | `NULL OR TRUE` | `TRUE` |
-- MAGIC | `NULL = NULL` | `NULL` (usar `<=>`, el operador *null-safe*) |

-- COMMAND ----------

SELECT
  NULL = NULL                       AS eq,
  NULL <=> NULL                     AS null_safe_eq,
  COALESCE(NULL, NULL, 'tercero')   AS coalesce_,
  NULLIF(0, 0)                      AS nullif_,
  NVL2(NULL, 'no nulo', 'es nulo')  AS nvl2_;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🏋️ Ejercicios
-- MAGIC 1. ¿Qué % de emails de `bronze.customers` necesitaban limpieza (espacios o mayúsculas)?
-- MAGIC 2. Normalizá teléfonos a formato E.164 de Argentina (`+54` + 10 dígitos) cuando sea posible.
-- MAGIC 3. Calculá la edad de cada cliente al momento de su primera compra.
-- MAGIC 4. ¿Cuántos pedidos se hicieron en la madrugada (0 a 6 hs) **hora de Ciudad de México** desde México?
