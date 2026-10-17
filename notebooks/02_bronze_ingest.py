# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: ingesta incremental con Auto Loader
# MAGIC
# MAGIC > **¿Para qué existe este paso?** Tener una copia fiel, incremental y con linaje de todo lo que llegó, para poder reprocesar sin pedirle nada a nadie.
# MAGIC >
# MAGIC > **¿Qué pasaría si no existiera?** Un error en la limpieza obligaría a releer todos los archivos (si todavía existen); y cada día se releerían años de datos.
# MAGIC >
# MAGIC > Explicación completa: [`docs/11_por_que_asi.md`](../docs/11_por_que_asi.md#capas) · glosario: [`docs/12_glosario.md`](../docs/12_glosario.md)
# MAGIC
# MAGIC ```
# MAGIC landing/<fuente>/ingest_date=.../*.json  ──Auto Loader──▶  bronze.<fuente>  (Delta)
# MAGIC ```
# MAGIC * `cloudFiles` descubre **sólo archivos nuevos** (estado en el checkpoint).
# MAGIC * `schemaEvolutionMode=addNewColumns`: si aparece una columna nueva (ej. `experiment_id` en web_events),
# MAGIC   el stream se detiene una vez y actualiza el esquema; `bronze.py` lo reinicia solo (ver `docs/07_bitacora_primera_ejecucion.md`, sección 7).
# MAGIC * Lo que no encaja en el esquema va a `_rescued_data`: **nunca se pierde un dato**.
# MAGIC * `trigger(availableNow=True)`: procesa todo lo pendiente y termina (costo de batch, garantías de streaming).

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from novamart.ingestion.bronze import ingest_all

new_rows = ingest_all(spark, cfg)
display(spark.createDataFrame(list(new_rows.items()), "source string, new_rows long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Linaje a nivel archivo
# MAGIC Cada fila sabe de qué archivo vino (`_source_file`) y cuándo se cargó (`_ingested_at`).

# COMMAND ----------

display(spark.sql(f"""
  SELECT ingest_date, COUNT(*) AS rows, COUNT(DISTINCT _source_file) AS files
  FROM {cfg.table('bronze', 'orders')}
  GROUP BY ingest_date ORDER BY ingest_date DESC LIMIT 15
"""))
