# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Gold: tablas de hechos
# MAGIC
# MAGIC > **¿Para qué existe este paso?** Construir las tablas de hechos (lo que pasó y se mide), procesando sólo las fechas afectadas.
# MAGIC >
# MAGIC > **¿Qué pasaría si no existiera?** Cada día se recalcularían años de ventas, y cada reintento podría duplicarlas.
# MAGIC >
# MAGIC > Explicación completa: [`docs/12_por_que_asi.md`](../docs/12_por_que_asi.md#rendimiento) · glosario: [`docs/13_glosario.md`](../docs/13_glosario.md)
# MAGIC
# MAGIC | fact | tipo Kimball | grano | carga |
# MAGIC |---|---|---|---|
# MAGIC | `fact_sales` | transaccional | línea de pedido | `replaceWhere` por fechas afectadas |
# MAGIC | `fact_order_fulfillment` | accumulating snapshot | pedido | `MERGE` (se actualiza en cada hito) |
# MAGIC | `fact_returns` | transaccional | línea devuelta | full |
# MAGIC | `fact_inventory_snapshot` | periodic snapshot | tienda × SKU × semana | `replaceWhere` |
# MAGIC | `fact_web_sessions` | agregado | sesión | `replaceWhere` (d y d-1) |
# MAGIC
# MAGIC Las FKs a dimensiones SCD2 se resuelven con un **join point-in-time**: la versión vigente
# MAGIC al momento del hecho (`order_ts BETWEEN effective_from AND effective_to`).

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"], "Full refresh")
full_refresh = dbutils.widgets.get("full_refresh") == "true"

from novamart.gold.facts import load_facts
from novamart.utils.audit import audited

with audited(cfg, spark, "gold_facts", "gold.fact_*"):
    load_facts(spark, cfg, full_refresh=full_refresh)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {cfg.table('gold', 'fact_sales')} LIMIT 5"))
