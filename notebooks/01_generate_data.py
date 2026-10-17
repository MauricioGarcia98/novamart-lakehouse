# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generación de datos a gran escala
# MAGIC
# MAGIC > **¿Para qué existe este paso?** Simular los 8 sistemas de origen de NovaMart (caja, web, stock…) escribiendo archivos en *landing*, como lo harían en una empresa real.
# MAGIC >
# MAGIC > **¿Qué pasaría si no existiera?** No habría datos: en la vida real este paso no existe porque los sistemas mandan sus archivos solos.
# MAGIC >
# MAGIC > Explicación completa: [`docs/11_por_que_asi.md`](../docs/11_por_que_asi.md#entrada) · glosario: [`docs/12_glosario.md`](../docs/12_glosario.md)
# MAGIC
# MAGIC El generador corre **distribuido en Spark** (no en el driver): cada fila se calcula con
# MAGIC `xxhash64(id, sal)`, así que es determinístico y escala linealmente con el cluster.
# MAGIC
# MAGIC | scale_factor | pedidos | líneas | eventos web | inventario | tamaño aprox. |
# MAGIC |---|---|---|---|---|---|
# MAGIC | 0.01 | 120 K | 360 K | 1.5 M | 78 K | ~300 MB |
# MAGIC | 0.1 | 1.2 M | 3.6 M | 15 M | 1.6 M | ~3 GB |
# MAGIC | 1 | 12 M | 36 M | 150 M | 157 M | ~30 GB |
# MAGIC | 10 | 120 M | 360 M | 1.500 M | 1.570 M | ~300 GB |
# MAGIC
# MAGIC El inventario escala por SKUs; las tiendas tienen un tope de 400.
# MAGIC
# MAGIC Problemas de calidad **inyectados a propósito**: duplicados, re-envíos, nulos, cantidades negativas,
# MAGIC formatos de fecha mezclados, emails sucios, llegadas tardías y *schema drift*.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

dbutils.widgets.dropdown("mode", "full", ["full", "incremental"], "Modo")
mode = dbutils.widgets.get("mode")

# COMMAND ----------

import time

from novamart.datagen.writer import generate_all

t0 = time.time()
result = generate_all(spark, cfg, mode=mode)
print(f"Generado en {time.time() - t0:,.0f}s")
result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspección: ¿qué llegó a la zona de aterrizaje?
# MAGIC `LIST` sobre el volumen muestra la estructura `fuente/ingest_date=YYYY-MM-DD/part-*.json`.

# COMMAND ----------

display(dbutils.fs.ls(f"{cfg.root_path}/landing/"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM json.`{cfg.landing_path('orders')}` LIMIT 10"))
