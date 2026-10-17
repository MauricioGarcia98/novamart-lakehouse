# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Mantenimiento: OPTIMIZE, VACUUM, ANALYZE
# MAGIC * `OPTIMIZE`: compacta archivos chicos y aplica el clustering.
# MAGIC * `VACUUM`: borra archivos que ya no referencia ninguna versión (respeta 7 días de time travel).
# MAGIC * `ANALYZE TABLE ... COMPUTE STATISTICS`: estadísticas para el optimizador (mejores planes de join).
# MAGIC
# MAGIC > Con *Predictive Optimization* activado en UC, Databricks hace esto automáticamente.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from novamart.config import EXPECTED_TABLES

for layer in ("silver", "gold"):
    for name in EXPECTED_TABLES[layer]:
        t = cfg.table(layer, name)
        if not spark.catalog.tableExists(t):
            continue
        spark.sql(f"OPTIMIZE {t}")
        spark.sql(f"VACUUM {t}")
        spark.sql(f"ANALYZE TABLE {t} COMPUTE STATISTICS FOR ALL COLUMNS")
        print("✔", t)
