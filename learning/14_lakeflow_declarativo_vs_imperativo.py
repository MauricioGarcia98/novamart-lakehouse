# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 14 · Pipelines declarativos (Lakeflow) vs imperativos (notebooks + jobs)
# MAGIC
# MAGIC El proyecto implementa el mismo flujo **dos veces**, a propósito:
# MAGIC
# MAGIC | | Imperativo (`src/` + `notebooks/` + Job) | Declarativo (`pipelines/*.sql`) |
# MAGIC |---|---|---|
# MAGIC | Ingesta | `readStream.format("cloudFiles")` + checkpoint | `STREAM read_files(...)` |
# MAGIC | Calidad | `quality/expectations.py` + tabla de cuarentena | `CONSTRAINT ... EXPECT ... ON VIOLATION` |
# MAGIC | SCD2 | `silver/scd2.py` (~100 líneas, MERGE a mano) | `AUTO CDC ... STORED AS SCD TYPE 2` |
# MAGIC | Orden de ejecución | `depends_on` en el Job | inferido del grafo de dependencias |
# MAGIC | Gold | `replaceWhere` + watermarks manuales | `MATERIALIZED VIEW` con refresh incremental |
# MAGIC | Testing local | ✅ pytest con Spark OSS | ⚠️ sólo en Databricks |
# MAGIC | Control fino | ✅ total | limitado a lo que ofrece el framework |
# MAGIC
# MAGIC **¿Cuál usar?** Declarativo para la mayoría de pipelines estándar (menos código, menos bugs, observabilidad
# MAGIC incluida). Imperativo cuando necesitás lógica muy específica, testing exhaustivo o control del costo.
# MAGIC Saber ambos es lo que piden las búsquedas laborales.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Práctica
# MAGIC 1. Desplegá el bundle (`databricks bundle deploy`) y ejecutá el pipeline `novamart-declarative-pipeline`.
# MAGIC 2. Abrí el grafo del pipeline: identificá streaming tables, vistas temporales y materialized views.
# MAGIC 3. Consultá el **event log** para ver las métricas de calidad:

# COMMAND ----------

dbutils.widgets.text("catalog", "novamart_dev")  # en prod: novamart
catalog = dbutils.widgets.get("catalog")

display(spark.sql(f"""
  SELECT
    timestamp,
    details:flow_progress.data_quality.expectations AS expectations
  FROM event_log(TABLE({catalog}.ldp.silver_orders))
  WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
  ORDER BY timestamp DESC
  LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Compará el SCD2 de ambos enfoques para el mismo cliente:

# COMMAND ----------

display(spark.sql(f"""
  WITH imp AS (SELECT customer_id, city, valid_from, valid_to FROM {catalog}.silver.customers_history),
       dec AS (SELECT customer_id, city, __START_AT AS valid_from, __END_AT AS valid_to
               FROM {catalog}.ldp.silver_customers_history)
  SELECT 'imperativo' AS impl, * FROM imp WHERE customer_id = 116
  UNION ALL
  SELECT 'declarativo', * FROM dec WHERE customer_id = 116
  ORDER BY impl, valid_from
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Ejercicios
# MAGIC 1. ¿Coinciden las cantidades de filas de `silver.orders` (imperativo) y `ldp.silver_orders`? Si no, explicá la diferencia.
# MAGIC 2. Agregá `fact_order_fulfillment` al pipeline declarativo como materialized view.
# MAGIC 3. Cambiá una expectation de `DROP ROW` a `FAIL UPDATE` y forzá un error. ¿Qué pasa con el resto del grafo?
