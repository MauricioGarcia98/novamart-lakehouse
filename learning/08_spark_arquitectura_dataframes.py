# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 08 · Spark desde cero: arquitectura y DataFrames
# MAGIC
# MAGIC ## 1. ¿Cómo funciona Spark?
# MAGIC ```
# MAGIC            ┌──────────── DRIVER ────────────┐
# MAGIC  tu código │ SparkSession → plan lógico →   │
# MAGIC  ────────▶ │ optimizador (Catalyst) → plan  │
# MAGIC            │ físico → JOBS → STAGES → TASKS │
# MAGIC            └───────┬──────────┬──────────┬──┘
# MAGIC                    ▼          ▼          ▼
# MAGIC               EXECUTOR 1  EXECUTOR 2  EXECUTOR N     (cada uno procesa PARTICIONES en paralelo)
# MAGIC ```
# MAGIC * **Partición**: un pedazo de los datos (≈ un archivo o 128 MB). 1 tarea = 1 partición = 1 core.
# MAGIC * **Transformación** (`select`, `filter`, `join`...): *lazy*, sólo agrega pasos al plan.
# MAGIC * **Acción** (`count`, `collect`, `write`, `display`): dispara la ejecución.
# MAGIC * **Narrow** (filter, select): cada partición se procesa sola. **Wide** (groupBy, join): requiere
# MAGIC   **shuffle** = mover datos entre máquinas por la red. El shuffle es lo más caro de Spark.
# MAGIC
# MAGIC En serverless no ves los executors, pero existen y escalan solos.

# COMMAND ----------

dbutils.widgets.text("catalog", "novamart")
spark.sql(f"USE CATALOG {dbutils.widgets.get('catalog')}")

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md ## 2. Lazy evaluation: nada se ejecuta hasta la acción

# COMMAND ----------

import time

t = time.time()
orders = spark.table("silver.orders")                       # no lee nada todavía
online = orders.where(F.col("channel") != "STORE")          # tampoco
by_day = online.groupBy("order_date").count()               # tampoco
print(f"Definir el plan tardó {time.time() - t:.3f}s")

t = time.time()
n = by_day.count()                                          # ACCIÓN → ahora sí se ejecuta
print(f"Ejecutar tardó {time.time() - t:.1f}s → {n} días")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. La API de DataFrames = SQL con Python
# MAGIC Cada consulta de los módulos 01-07 se puede escribir con la API. Ambas generan **el mismo plan**.

# COMMAND ----------

fact = spark.table("gold.fact_sales")
prod = spark.table("gold.dim_product")

top_departments = (
    fact.join(prod, "product_key")                                   # JOIN
    .where(F.col("order_date") >= "2025-01-01")                      # WHERE
    .groupBy("department")                                           # GROUP BY
    .agg(
        F.sum("net_amount").alias("net_sales"),
        F.countDistinct("order_id").alias("orders"),
        (F.sum("margin_amount") / F.sum("net_amount")).alias("margin_pct"),
    )
    .where(F.col("net_sales") > 0)                                   # HAVING
    .orderBy(F.desc("net_sales"))                                    # ORDER BY
)
display(top_departments)

# COMMAND ----------

top_departments_sql = spark.sql("""
  SELECT department, SUM(net_amount) net_sales, COUNT(DISTINCT order_id) orders,
         SUM(margin_amount)/SUM(net_amount) margin_pct
  FROM gold.fact_sales JOIN gold.dim_product USING (product_key)
  WHERE order_date >= '2025-01-01'
  GROUP BY department HAVING net_sales > 0 ORDER BY net_sales DESC
""")
# Comparar planes optimizados: son equivalentes
top_departments.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Columnas: `withColumn`, `select`, `when`, expresiones SQL
# MAGIC Regla: preferí **funciones nativas** (`pyspark.sql.functions`) antes que UDFs de Python (módulo 09).

# COMMAND ----------

enriched = (
    spark.table("silver.orders")
    .select(
        "order_id", "order_ts", "channel", "installments", "shipping_cost",
        F.hour("order_ts").alias("hour"),
        F.when(F.col("installments") >= 6, "financiado").otherwise("contado").alias("payment_type"),
        F.expr("CASE WHEN shipping_cost = 0 THEN 'gratis' ELSE 'pago' END").alias("shipping_type"),
    )
    .withColumn("is_night", F.col("hour").between(0, 5))
)
display(enriched.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Leer y escribir archivos
# MAGIC Los mismos datos se pueden leer en distintos formatos. Comparemos tamaño y velocidad.

# COMMAND ----------

landing = f"/Volumes/{dbutils.widgets.get('catalog')}/landing/raw/landing"
raw_orders_json = spark.read.json(f"{landing}/orders")
raw_orders_json.printSchema()

# COMMAND ----------

tmp = f"/Volumes/{dbutils.widgets.get('catalog')}/landing/raw/_tmp_formats"
sample = spark.table("silver.order_lines").limit(2_000_000)
for fmt in ["csv", "json", "parquet", "delta"]:
    t = time.time()
    (sample.write.mode("overwrite").format(fmt).option("header", True).save(f"{tmp}/{fmt}"))
    size = sum(f.size for f in dbutils.fs.ls(f"{tmp}/{fmt}") if f.name.startswith("part"))
    print(f"{fmt:8s} escritura {time.time()-t:5.1f}s   tamaño {size/1e6:8.1f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC **Conclusión:** los formatos **columnares** (Parquet/Delta) ocupan 5-10x menos y se leen más rápido porque
# MAGIC (a) comprimen por columna y (b) sólo leen las columnas pedidas (*column pruning*). Delta = Parquet + log de
# MAGIC transacciones (ACID, time travel, MERGE).

# COMMAND ----------

dbutils.fs.rm(tmp, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Ejercicios
# MAGIC 1. Reescribí con la API de DataFrames la consulta de *Top 3 productos por departamento* del módulo 05
# MAGIC    (pista: `Window.partitionBy(...).orderBy(...)` + `F.row_number()`).
# MAGIC 2. ¿Cuántas particiones tiene `spark.table("gold.fact_sales")` al leerla? (`df.rdd.getNumPartitions()` en clusters clásicos;
# MAGIC    en serverless mirá el *Query profile*). ¿Y después de un `groupBy`?
# MAGIC 3. Con `explain()`, identificá en el plan dónde hay un `Exchange` (= shuffle) en la consulta del punto 3.
