# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Simular la llegada de un día nuevo
# MAGIC
# MAGIC > **¿Para qué existe este paso?** Simular que llega un día nuevo, para practicar y verificar la carga incremental.
# MAGIC >
# MAGIC > **¿Qué pasaría si no existiera?** No habría forma de probar que el pipeline procesa sólo lo nuevo (y que no duplica nada).
# MAGIC >
# MAGIC > Explicación completa: [`docs/11_por_que_asi.md`](../docs/11_por_que_asi.md#operacion) · glosario: [`docs/12_glosario.md`](../docs/12_glosario.md)
# MAGIC Genera los archivos de **un solo día** (el siguiente al último procesado) en modo incremental.
# MAGIC Después el Job dispara el pipeline completo, que sólo procesa lo nuevo.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from datetime import date, timedelta

from novamart.datagen.writer import generate_all

dbutils.widgets.text("run_date", "", "Fecha a generar (vacío = siguiente)")
raw = dbutils.widgets.get("run_date")
if raw:
    run_date = date.fromisoformat(raw)
else:
    last = spark.sql(f"SELECT MAX(order_date) FROM {cfg.table('silver', 'orders')}").collect()[0][0]
    run_date = (last or cfg.end_date) + timedelta(days=1)

print("Generando", run_date)
generate_all(spark, cfg, start=run_date, end=run_date, mode="incremental")
dbutils.jobs.taskValues.set("run_date", run_date.isoformat())
