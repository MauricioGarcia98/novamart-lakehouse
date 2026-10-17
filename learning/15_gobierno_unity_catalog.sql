-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 15 · Gobierno con Unity Catalog
-- MAGIC
-- MAGIC Un lakehouse profesional no es sólo tablas: es **quién puede ver qué**, de dónde viene cada dato y qué
-- MAGIC contiene información sensible.
-- MAGIC
-- MAGIC | capacidad | comando / lugar |
-- MAGIC |---|---|
-- MAGIC | Permisos | `GRANT / REVOKE` sobre catálogo, esquema, tabla, volumen, función |
-- MAGIC | Enmascarar columnas | `ALTER TABLE ... ALTER COLUMN ... SET MASK fn` |
-- MAGIC | Filtrar filas | `ALTER TABLE ... SET ROW FILTER fn ON (col)` |
-- MAGIC | Clasificación | `SET TAGS ('pii' = 'email')` |
-- MAGIC | Linaje | automático, a nivel tabla y columna (pestaña *Lineage*) |
-- MAGIC | Auditoría | tablas de sistema `system.access.audit` |

-- COMMAND ----------

USE CATALOG novamart;

-- COMMAND ----------

-- MAGIC %md ## 1. Inspeccionar permisos y metadatos

-- COMMAND ----------

SHOW GRANTS ON SCHEMA gold;

-- COMMAND ----------

SELECT table_name, comment
FROM information_schema.tables
WHERE table_schema = 'gold'
ORDER BY table_name;

-- COMMAND ----------

-- Tags aplicados por sql/ddl/physical_design.sql
SELECT * FROM information_schema.column_tags WHERE schema_name = 'gold';

-- COMMAND ----------

-- Constraints PK/FK del modelo estrella
SELECT * FROM information_schema.table_constraints WHERE table_schema = 'gold';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Enmascarar PII
-- MAGIC Ejecutá `sql/ddl/governance_masking.sql` (reemplazando `${gold}` por `novamart.gold`) y luego:

-- COMMAND ----------

SELECT customer_id, full_name, email FROM gold.dim_customer WHERE is_current LIMIT 5;
-- Si no pertenecés al grupo `pii_readers`, ves 'ma***@gmail.com'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Linaje
-- MAGIC Abrí `gold.fact_sales` en Catalog Explorer → *Lineage* → *See lineage graph*. Vas a ver el recorrido
-- MAGIC `landing → bronze.orders → silver.orders / order_lines → gold.fact_sales → mart_*`. También por SQL:

-- COMMAND ----------

SELECT source_table_full_name, target_table_full_name, MAX(event_time) AS last_seen
FROM system.access.table_lineage
WHERE target_table_full_name LIKE 'novamart.gold.%'
GROUP BY ALL
ORDER BY target_table_full_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Observabilidad del costo: tablas de sistema
-- MAGIC ¿Cuánto cuesta el pipeline? `system.billing.usage` registra el consumo de DBUs por job.

-- COMMAND ----------

SELECT
  usage_metadata.job_id,
  usage_date,
  SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 30 DAYS
  AND usage_metadata.job_id IS NOT NULL
GROUP BY ALL
ORDER BY usage_date DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 🏋️ Ejercicios
-- MAGIC 1. Diseñá el esquema de permisos: analistas (sólo gold), data engineers (todo), gerentes regionales (sólo su región).
-- MAGIC 2. Activá el row filter de `governance_masking.sql` y verificá con un usuario de prueba.
-- MAGIC 3. Escribí una consulta sobre `system.access.audit` que liste quién leyó `gold.dim_customer` en la última semana.
