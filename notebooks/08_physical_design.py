# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Diseño físico y gobierno (Unity Catalog)
# MAGIC Aplica `sql/ddl/physical_design.sql`: liquid clustering, PK/FK informativas, comentarios de columnas,
# MAGIC tags de gobierno y propiedades de tabla. Es idempotente.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from novamart.gold.marts import SQL_DIR, run_sql_file

run_sql_file(spark, cfg, SQL_DIR / "ddl" / "physical_design.sql")
display(spark.sql(f"DESCRIBE DETAIL {cfg.table('gold', 'fact_sales')}"))
