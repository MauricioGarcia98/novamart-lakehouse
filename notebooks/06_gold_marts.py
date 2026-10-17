# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Gold: marts en SQL
# MAGIC Ejecuta en orden los scripts de `sql/gold/marts/` (ventas diarias, RFM, cohortes, afinidad de productos,
# MAGIC embudo web y la vista desnormalizada `v_sales_enriched`).

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from novamart.gold.marts import build_marts
from novamart.utils.audit import audited

with audited(cfg, spark, "gold_marts", "gold.mart_*"):
    built = build_marts(spark, cfg)
built

# COMMAND ----------

display(spark.sql(f"""
  SELECT year_month, channel_group, ROUND(SUM(net_sales)/1e6, 2) AS net_sales_musd, ROUND(AVG(margin_pct), 3) AS margin
  FROM {cfg.table('gold','mart_sales_daily')}
  GROUP BY ALL ORDER BY year_month, channel_group
"""))
