# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 11 · Delta Lake a fondo
# MAGIC
# MAGIC Delta = archivos Parquet + un **log de transacciones** (`_delta_log/*.json`). Ese log es lo que da:
# MAGIC ACID, time travel, MERGE/UPDATE/DELETE, schema enforcement/evolution, Change Data Feed y concurrencia optimista.

# COMMAND ----------

dbutils.widgets.text("catalog", "novamart")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## 1. Anatomía de una tabla Delta

# COMMAND ----------

spark.sql("""
  CREATE OR REPLACE TABLE ops.lab_delta (id BIGINT, city STRING, amount DECIMAL(10,2))
  TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.sql("INSERT INTO ops.lab_delta VALUES (1, 'Rosario', 10.00), (2, 'Lima', 20.00)")
spark.sql("UPDATE ops.lab_delta SET amount = amount * 2 WHERE id = 1")
spark.sql("DELETE FROM ops.lab_delta WHERE id = 2")
display(spark.sql("DESCRIBE HISTORY ops.lab_delta").select("version", "operation", "operationMetrics"))

# COMMAND ----------

location = spark.sql("DESCRIBE DETAIL ops.lab_delta").collect()[0].location
print(location)
# En tablas gestionadas por UC no se accede al storage directamente; el log se inspecciona con DESCRIBE HISTORY.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema enforcement vs schema evolution
# MAGIC Delta **rechaza** escrituras con columnas inesperadas (protege la tabla). Se habilita la evolución explícitamente.

# COMMAND ----------

new_rows = spark.createDataFrame([(3, "Quito", 5.0, "EC")], "id long, city string, amount double, country string")
try:
    new_rows.write.mode("append").saveAsTable("ops.lab_delta")
except Exception as e:
    print("❌ Rechazado:", str(e)[:200])

new_rows.write.mode("append").option("mergeSchema", "true").saveAsTable("ops.lab_delta")
display(spark.table("ops.lab_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Change Data Feed
# MAGIC Fila a fila: qué se insertó, actualizó (pre/post imagen) o borró, y en qué versión.

# COMMAND ----------

display(spark.read.option("readChangeFeed", "true").option("startingVersion", 1).table("ops.lab_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Concurrencia optimista
# MAGIC Dos escritores a la vez: si tocan **archivos distintos**, ambos confirman; si modifican los mismos, uno recibe
# MAGIC `ConcurrentModificationException` y debe reintentar. Por eso en el proyecto:
# MAGIC * las tasks paralelas de silver escriben **tablas distintas**;
# MAGIC * las tablas de `ops` sólo reciben **appends** (que nunca conflictúan entre sí);
# MAGIC * los MERGE incluyen la columna de clustering en la condición para tocar menos archivos.
# MAGIC
# MAGIC Con *row-level concurrency* (deletion vectors + liquid clustering) Databricks resuelve conflictos a nivel fila.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Deletion vectors
# MAGIC Sin DV, borrar 1 fila reescribe el archivo entero (128 MB). Con DV se marca la fila como borrada en un bitmap
# MAGIC y la reescritura se difiere hasta el próximo OPTIMIZE.

# COMMAND ----------

spark.sql("ALTER TABLE ops.lab_delta SET TBLPROPERTIES (delta.enableDeletionVectors = true)")
spark.sql("DELETE FROM ops.lab_delta WHERE id = 3")
display(spark.sql("DESCRIBE HISTORY ops.lab_delta LIMIT 1").select("operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. SCD2 del proyecto, por dentro
# MAGIC Elegí un cliente con historia y seguí su evolución en el tiempo combinando la tabla con su historial de versiones.

# COMMAND ----------

cid = spark.sql("""
  SELECT customer_id FROM silver.customers_history GROUP BY 1 HAVING COUNT(*) >= 3 LIMIT 1
""").collect()[0][0]
display(spark.sql(f"""
  SELECT customer_id, city, segment, loyalty_tier, valid_from, valid_to, is_current, is_deleted
  FROM silver.customers_history WHERE customer_id = {cid} ORDER BY valid_from
"""))

# COMMAND ----------

# Estado de la dimensión "como estaba" en una versión anterior de la tabla
v = spark.sql("DESCRIBE HISTORY silver.customers_history").agg(F.min("version")).collect()[0][0]
display(spark.sql(f"SELECT COUNT(*) AS rows_then FROM silver.customers_history VERSION AS OF {v}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Mantenimiento: OPTIMIZE, VACUUM, retención
# MAGIC * `VACUUM` borra archivos no referenciados con más de 7 días (default). Después de VACUUM no se puede
# MAGIC   hacer time travel a versiones cuyos archivos fueron borrados.
# MAGIC * `delta.logRetentionDuration` controla cuánto historial del log se mantiene.

# COMMAND ----------

spark.sql("OPTIMIZE ops.lab_delta")
display(spark.sql("VACUUM ops.lab_delta DRY RUN"))
spark.sql("DROP TABLE ops.lab_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Ejercicios
# MAGIC 1. Usá CDF sobre `silver.customers_history` para contar cuántos clientes cambiaron de ciudad en la última corrida.
# MAGIC 2. Simulá un error: borrá por accidente las ventas de un día en una copia de `fact_sales` y recuperalas con `RESTORE`.
# MAGIC 3. ¿Qué pasa si dos notebooks hacen `MERGE` al mismo tiempo sobre la misma fecha de `fact_sales`? Probalo.
