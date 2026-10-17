# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 12 · Structured Streaming y Auto Loader
# MAGIC
# MAGIC Streaming en Spark = **la misma API de DataFrames** sobre una tabla que crece. El motor procesa
# MAGIC micro-lotes y guarda en un **checkpoint** qué ya procesó (exactly-once).
# MAGIC
# MAGIC | trigger | comportamiento | uso |
# MAGIC |---|---|---|
# MAGIC | `availableNow=True` | procesa todo lo pendiente y termina | pipelines batch incrementales (este proyecto) |
# MAGIC | `processingTime='1 minute'` | micro-lote cada minuto, no termina | baja latencia (clusters clásicos) |
# MAGIC | `realTime` (nuevo) | latencia de milisegundos | casos operacionales |

# COMMAND ----------

dbutils.widgets.text("catalog", "novamart")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")
from pyspark.sql import functions as F

base = f"/Volumes/{catalog}/landing/raw"
lab = f"{base}/_lab_streaming"
dbutils.fs.rm(lab, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Auto Loader paso a paso
# MAGIC Leemos el clickstream crudo con inferencia y evolución de esquema.

# COMMAND ----------

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{lab}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.maxFilesPerTrigger", 50)          # controla el tamaño de cada micro-lote
    .load(f"{base}/landing/web_events")
)
stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Agregación por ventanas de tiempo con watermark
# MAGIC Compras por ventana de 1 hora. El **watermark** le dice a Spark cuánto esperar eventos tardíos
# MAGIC (acá 2 horas) antes de cerrar una ventana y liberar su estado de memoria.

# COMMAND ----------

purchases = (
    stream.where("event_type = 'purchase'")
    .withColumn("ts", F.timestamp_millis("event_ts"))
    .withWatermark("ts", "2 hours")
    .groupBy(F.window("ts", "1 hour"), F.col("device.platform").alias("platform"))
    .agg(F.count("*").alias("purchases"), F.approx_count_distinct("session_id").alias("sessions"))
)
q = (
    purchases.writeStream.format("delta")
    .outputMode("append")                                  # con watermark: emite cada ventana una vez cerrada
    .option("checkpointLocation", f"{lab}/chk_hourly")
    .trigger(availableNow=True)
    .toTable("ops.lab_hourly_purchases")
)
q.awaitTermination()
display(spark.table("ops.lab_hourly_purchases").orderBy(F.desc("window")).limit(24))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Volver a correr = sólo lo nuevo
# MAGIC Si se ejecuta de nuevo la celda anterior sin archivos nuevos, procesa 0 filas: el checkpoint recuerda.
# MAGIC Generá un día nuevo con `notebooks/10_generate_next_day` y volvé a correrla.

# COMMAND ----------

# lastProgress muestra cuántas filas leyó el último micro-lote y cuánto tardó cada fase
print(q.lastProgress)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deduplicación en streaming
# MAGIC `dropDuplicatesWithinWatermark` descarta duplicados que llegan dentro del margen del watermark, manteniendo
# MAGIC un estado acotado (a diferencia de `dropDuplicates`, cuyo estado crece para siempre).

# COMMAND ----------

dedup = (
    stream.withColumn("ts", F.timestamp_millis("event_ts"))
    .withWatermark("ts", "1 hour")
    .dropDuplicatesWithinWatermark(["event_id"])
)
q2 = (dedup.writeStream.format("delta").option("checkpointLocation", f"{lab}/chk_dedup")
      .trigger(availableNow=True).toTable("ops.lab_events_dedup"))
q2.awaitTermination()
print(spark.table("ops.lab_events_dedup").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `foreachBatch`: MERGE dentro de un stream
# MAGIC Es el patrón que usa toda la capa silver del proyecto (`src/novamart/silver/load.py`).
# MAGIC Dentro de la función recibís un DataFrame **batch** normal: podés hacer MERGE, escribir a varias tablas, etc.

# COMMAND ----------

def upsert_last_seen(batch_df, batch_id):
    last_seen = (batch_df.where("customer_id IS NOT NULL")
                 .groupBy("customer_id").agg(F.max(F.timestamp_millis("event_ts")).alias("last_seen")))
    if not spark.catalog.tableExists("ops.lab_last_seen"):
        last_seen.limit(0).write.saveAsTable("ops.lab_last_seen")
    last_seen.createOrReplaceTempView("updates")
    batch_df.sparkSession.sql("""
      MERGE INTO ops.lab_last_seen t USING updates s ON t.customer_id = s.customer_id
      WHEN MATCHED AND s.last_seen > t.last_seen THEN UPDATE SET t.last_seen = s.last_seen
      WHEN NOT MATCHED THEN INSERT *
    """)


(stream.writeStream.foreachBatch(upsert_last_seen)
 .option("checkpointLocation", f"{lab}/chk_last_seen").trigger(availableNow=True).start().awaitTermination())
display(spark.table("ops.lab_last_seen").orderBy(F.desc("last_seen")).limit(10))

# COMMAND ----------

for t in ["ops.lab_hourly_purchases", "ops.lab_events_dedup", "ops.lab_last_seen"]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
dbutils.fs.rm(lab, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Ejercicios
# MAGIC 1. Calculá en streaming el embudo (vistas → carrito → compra) por ventana de 15 minutos.
# MAGIC 2. Hacé un *stream-static join* entre los eventos y `gold.dim_product` para obtener el departamento de cada vista.
# MAGIC 3. ¿Qué pasa con el estado si sacás el watermark del punto 2 de este notebook? ¿Por qué es peligroso en producción?
