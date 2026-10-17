# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: limpieza, calidad, deduplicación e historia (por entidad)
# MAGIC
# MAGIC > **¿Para qué existe este paso?** Obtener "la verdad": datos tipados, sin duplicados, validados y con historia, reutilizables por toda la empresa.
# MAGIC >
# MAGIC > **¿Qué pasaría si no existiera?** Los re-envíos duplicarían ventas, las cantidades negativas restarían plata y un cliente que se muda "movería" su historia.
# MAGIC >
# MAGIC > Explicación completa: [`docs/11_por_que_asi.md`](../docs/11_por_que_asi.md#calidad) · glosario: [`docs/12_glosario.md`](../docs/12_glosario.md)
# MAGIC
# MAGIC Este notebook se ejecuta **una vez por entidad** gracias a un `for_each_task` del Job
# MAGIC (8 entidades, 4 en paralelo). Patrón por micro-lote:
# MAGIC
# MAGIC ```
# MAGIC readStream(bronze) ─▶ foreachBatch: limpiar ─▶ reglas de calidad ─▶ MERGE en silver
# MAGIC                                                     └─▶ ops.quarantine (filas inválidas)
# MAGIC ```
# MAGIC | entidad | estrategia de carga |
# MAGIC |---|---|
# MAGIC | customers, products | **SCD Tipo 2** con MERGE (historia completa) |
# MAGIC | stores | SCD Tipo 1 (upsert) |
# MAGIC | orders, order_lines, events, returns, inventory | insert-only idempotente (MERGE sin UPDATE) |

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

dbutils.widgets.text("entity", "orders", "Entidad")
entity = dbutils.widgets.get("entity")

from novamart.silver.load import LOADERS
from novamart.utils.audit import audited

with audited(cfg, spark, f"silver_{entity}", f"silver.{entity}"):
    LOADERS[entity](spark, cfg)

# COMMAND ----------

display(spark.sql(f"""
  SELECT table_name, rule, action, SUM(failed_rows) AS failed, SUM(total_rows) AS total
  FROM {cfg.table('ops', 'dq_results')}
  WHERE table_name LIKE '%{entity.split('_')[0]}%'
  GROUP BY ALL ORDER BY failed DESC
"""))
