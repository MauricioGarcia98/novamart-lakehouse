# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold: dimensiones
# MAGIC
# MAGIC > **¿Para qué existe este paso?** Armar el contexto de los hechos (quién, qué, dónde, cuándo) con claves estables y la fila `-1` para lo desconocido.
# MAGIC >
# MAGIC > **¿Qué pasaría si no existiera?** Las ventas sin cliente desaparecerían de los totales y reconstruir una dimensión cambiaría las claves de todas las ventas.
# MAGIC >
# MAGIC > Explicación completa: [`docs/11_por_que_asi.md`](../docs/11_por_que_asi.md#modelado) · glosario: [`docs/12_glosario.md`](../docs/12_glosario.md)
# MAGIC
# MAGIC * `dim_customer`, `dim_product`: **SCD2** con clave subrogada determinística `xxhash64(natural_key, valid_from)`.
# MAGIC * `dim_store`: SCD1 + miembro `-1` = "Canal digital (sin tienda)".
# MAGIC * `dim_date`: calendario con eventos comerciales y año fiscal (role-playing).
# MAGIC * `dim_channel`, `dim_payment` (*junk dimension*).

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from novamart.gold.dimensions import load_dimensions
from novamart.utils.audit import audited

with audited(cfg, spark, "gold_dimensions", "gold.dim_*"):
    counts = load_dimensions(spark, cfg)
counts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Un cliente con historia (SCD2)

# COMMAND ----------

display(spark.sql(f"""
  WITH changed AS (
    SELECT customer_id FROM {cfg.table('gold','dim_customer')} GROUP BY 1 HAVING COUNT(*) >= 3 LIMIT 1
  )
  SELECT customer_key, customer_id, city, segment, loyalty_tier, valid_from, valid_to, is_current
  FROM {cfg.table('gold','dim_customer')} JOIN changed USING (customer_id)
  ORDER BY valid_from
"""))
