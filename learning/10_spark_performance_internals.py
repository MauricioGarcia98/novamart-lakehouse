# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 10 · Spark avanzado: performance con grandes volúmenes
# MAGIC
# MAGIC > Recomendado: correr este módulo en un **cluster clásico** (ej. 1 driver + 4 workers) con `scale_factor ≥ 1`,
# MAGIC > para poder cambiar configuraciones y ver la Spark UI. En serverless algunas `spark.conf` no son modificables.
# MAGIC
# MAGIC | problema | síntoma en Spark UI | solución |
# MAGIC |---|---|---|
# MAGIC | shuffle grande | stage con GBs de *Shuffle Read/Write* | broadcast join, pre-agregar, clustering |
# MAGIC | data skew | 1 tarea tarda 50x más que la mediana | AQE skew join, salting, aislar la clave caliente |
# MAGIC | archivos chicos | miles de tareas de milisegundos | OPTIMIZE, auto compaction, `maxFilesPerTrigger` |
# MAGIC | sin data skipping | "files read" = total de archivos | filtrar por columnas de clustering, liquid clustering |
# MAGIC | spill a disco | *Spill (Memory/Disk)* > 0 | más particiones, menos datos por tarea |

# COMMAND ----------

dbutils.widgets.text("catalog", "novamart")
spark.sql(f"USE CATALOG {dbutils.widgets.get('catalog')}")

import time

from pyspark.sql import Window
from pyspark.sql import functions as F


def timed(label, df):
    t = time.time()
    df.write.format("noop").mode("overwrite").save()      # ejecuta TODO el plan sin escribir nada
    print(f"{label:45s} {time.time() - t:7.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer un plan: `explain`
# MAGIC Buscá: `Scan` (qué archivos/columnas lee y qué filtros empuja), `Exchange` (shuffle),
# MAGIC `BroadcastHashJoin` vs `SortMergeJoin`, `HashAggregate` parcial + final.

# COMMAND ----------

q = (
    spark.table("gold.fact_sales").where("order_date >= '2025-10-01'")
    .join(spark.table("gold.dim_product"), "product_key")
    .groupBy("department").agg(F.sum("net_amount").alias("v"))
)
q.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Broadcast join vs sort-merge join
# MAGIC Si una tabla entra en memoria (umbral `spark.sql.autoBroadcastJoinThreshold`, AQE lo ajusta en ejecución),
# MAGIC Spark la copia a cada executor y **el fact no se mueve**.

# COMMAND ----------

fact = spark.table("gold.fact_sales")
dim = spark.table("gold.dim_product")

timed("broadcast (hint explícito)", fact.join(F.broadcast(dim), "product_key").groupBy("department").count())
timed("sort-merge forzado (hint MERGE)", fact.join(dim.hint("merge"), "product_key").groupBy("department").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data skew: el problema #1 con datos reales
# MAGIC El generador usa una distribución de potencia: pocos clientes concentran muchas compras y la tienda
# MAGIC "online" (`store_key = -1`) concentra ~60% de las líneas. Veamos la distribución:

# COMMAND ----------

display(
    spark.table("gold.fact_sales").groupBy("store_key").count()
    .withColumn("share", F.round(F.col("count") / F.sum("count").over(Window.partitionBy()), 4))
    .orderBy(F.desc("count")).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Un join por `store_key` contra otra tabla grande pone el 60% de los datos en **una sola tarea**.
# MAGIC Simulamos el caso desactivando AQE y forzando sort-merge:

# COMMAND ----------

lines = spark.table("gold.fact_sales").select("store_key", "net_amount")
store_targets = (                                           # tabla "grande" del otro lado (metas diarias)
    spark.table("gold.fact_sales").select("store_key", "order_date").distinct()
    .withColumn("target", F.lit(1000.0))
)

spark.conf.set("spark.sql.adaptive.enabled", "false")
timed("skew SIN AQE", lines.join(store_targets.hint("merge"), "store_key").groupBy("order_date").sum("net_amount"))

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
timed("skew CON AQE (divide la partición caliente)", lines.join(store_targets.hint("merge"), "store_key").groupBy("order_date").sum("net_amount"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salting manual (cuando AQE no alcanza, ej. en agregaciones o UDFs)
# MAGIC Se agrega un número aleatorio 0..N-1 a la clave caliente en el lado grande y se replica N veces el lado chico.

# COMMAND ----------

N = 16
salted_left = lines.withColumn("salt", F.when(F.col("store_key") == -1, (F.rand() * N).cast("int")).otherwise(0))
salted_right = store_targets.withColumn(
    "salt", F.explode(F.when(F.col("store_key") == -1, F.sequence(F.lit(0), F.lit(N - 1))).otherwise(F.array(F.lit(0))))
)
spark.conf.set("spark.sql.adaptive.enabled", "false")
timed("skew con SALTING (sin AQE)", salted_left.join(salted_right.hint("merge"), ["store_key", "salt"]).groupBy("order_date").sum("net_amount"))
spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Particiones de shuffle
# MAGIC `spark.sql.shuffle.partitions` (default 200) define cuántas tareas tiene un stage después de un shuffle.
# MAGIC Muy pocas → tareas enormes con *spill*; demasiadas → overhead. AQE las **coalesce** automáticamente.

# COMMAND ----------

ev = spark.table("silver.web_events")
original = spark.conf.get("spark.sql.shuffle.partitions")
for parts in ["8", "200", "2000"]:
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", parts)
    timed(f"groupBy session_id con {parts} particiones", ev.groupBy("session_id").count())
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", original)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. El problema de los archivos chicos y OPTIMIZE
# MAGIC Bronze recibe muchos archivos chicos por día. Miremos cuántos archivos y de qué tamaño hay:

# COMMAND ----------

for t in ["bronze.web_events", "silver.web_events", "gold.fact_sales"]:
    d = spark.sql(f"DESCRIBE DETAIL {t}").collect()[0]
    print(f"{t:22s} archivos={d.numFiles:>7,}  promedio={d.sizeInBytes / max(d.numFiles, 1) / 1e6:8.1f} MB")

# COMMAND ----------

timed("count bronze.web_events ANTES de OPTIMIZE", spark.table("bronze.web_events").where("event_type = 'purchase'"))
spark.sql("OPTIMIZE bronze.web_events")
timed("count bronze.web_events DESPUÉS de OPTIMIZE", spark.table("bronze.web_events").where("event_type = 'purchase'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data skipping y Liquid Clustering
# MAGIC Delta guarda min/max de cada columna por archivo. Si los datos están **agrupados** por la columna del filtro,
# MAGIC Spark descarta archivos enteros sin abrirlos. `CLUSTER BY` mantiene ese agrupamiento de forma incremental.

# COMMAND ----------

spark.sql("""
  CREATE OR REPLACE TABLE ops.lab_events_unclustered AS
  SELECT * FROM silver.web_events ORDER BY rand()             -- datos desordenados a propósito
""")
spark.sql("""
  CREATE OR REPLACE TABLE ops.lab_events_clustered CLUSTER BY (event_date, event_type) AS
  SELECT * FROM silver.web_events
""")
spark.sql("OPTIMIZE ops.lab_events_clustered")

flt = "event_date = (SELECT MAX(event_date) FROM silver.web_events) AND event_type = 'purchase'"
timed("filtro sobre tabla SIN clustering", spark.sql(f"SELECT * FROM ops.lab_events_unclustered WHERE {flt}"))
timed("filtro sobre tabla CON clustering", spark.sql(f"SELECT * FROM ops.lab_events_clustered WHERE {flt}"))
# En el Query Profile compará "files read" vs "files pruned"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Caché: cuándo sí y cuándo no
# MAGIC * **Disk cache** de Databricks: automática en clusters con SSD, cachea archivos Parquet leídos. No hacés nada.
# MAGIC * `df.cache()`: guarda el DataFrame en memoria de los executors. Útil si se **reutiliza varias veces** en la
# MAGIC   misma sesión (ML iterativo). No disponible en serverless. Siempre `unpersist()` al terminar.
# MAGIC * Anti-patrón: `cache()` + una sola acción → más lento que no cachear.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Photon
# MAGIC Motor vectorizado en C++ que ejecuta operadores de Spark SQL. En el plan aparecen como `Photon...`.
# MAGIC Acelera scans, joins, agregaciones y escrituras Delta; **no** acelera UDFs de Python (otra razón para evitarlas).

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS ops.lab_events_unclustered")
spark.sql("DROP TABLE IF EXISTS ops.lab_events_clustered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Ejercicios (con Spark UI abierta)
# MAGIC 1. Encontrá en la Spark UI el stage con más *shuffle write* del job `gold_facts`. ¿Qué operación lo genera?
# MAGIC 2. Hacé un join `fact_sales ⨝ fact_web_sessions` por `customer_key`. ¿Qué tipo de join elige Spark? ¿Hay skew?
# MAGIC    (pista: `customer_key = -1`). Proponé dos soluciones.
# MAGIC 3. Medí la diferencia de tiempo de `mart_customer_rfm` con `scale_factor` 0.1 vs 1. ¿Escala linealmente?
# MAGIC 4. ¿Qué columnas elegirías para `CLUSTER BY` en `fact_order_fulfillment`? Justificá con las consultas del dashboard.
