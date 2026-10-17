# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Quality gate
# MAGIC
# MAGIC > **¿Para qué existe este paso?** Ser la última barrera: el job falla si los números no tienen sentido, antes de que alguien los vea.
# MAGIC >
# MAGIC > **¿Qué pasaría si no existiera?** El pipeline podría terminar "verde" con ventas en cero por un join mal hecho, y nadie se enteraría.
# MAGIC >
# MAGIC > Explicación completa: [`docs/11_por_que_asi.md`](../docs/11_por_que_asi.md#calidad) · glosario: [`docs/12_glosario.md`](../docs/12_glosario.md)
# MAGIC El Job **falla** si se rompe alguna invariante del modelo. Es la última barrera antes de que
# MAGIC los dashboards muestren datos incorrectos.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

g = lambda t: cfg.table("gold", t)  # noqa: E731
s = lambda t: cfg.table("silver", t)  # noqa: E731

checks = {
    "fact_sales tiene el mismo grano que silver.order_lines":
        f"SELECT (SELECT COUNT(*) FROM {g('fact_sales')}) - (SELECT COUNT(*) FROM {s('order_lines')})",
    "sin FKs huérfanas de producto":
        f"SELECT COUNT(*) FROM {g('fact_sales')} f LEFT ANTI JOIN {g('dim_product')} d USING (product_key)",
    "sin FKs huérfanas de cliente":
        f"SELECT COUNT(*) FROM {g('fact_sales')} f LEFT ANTI JOIN {g('dim_customer')} d USING (customer_key)",
    "una sola versión vigente por cliente":
        f"SELECT COUNT(*) FROM (SELECT customer_id FROM {s('customers_history')} WHERE is_current GROUP BY 1 HAVING COUNT(*) > 1)",
    "pedidos sin duplicar en silver":
        f"SELECT COUNT(*) FROM (SELECT order_id FROM {s('orders')} GROUP BY 1 HAVING COUNT(*) > 1)",
    "margen nunca mayor que la venta neta":
        f"SELECT COUNT(*) FROM {g('fact_sales')} WHERE margin_amount > net_amount",
}

results = [(name, spark.sql(q).collect()[0][0]) for name, q in checks.items()]
display(spark.createDataFrame(results, "check string, violations long"))
failed = [n for n, v in results if v != 0]

# COMMAND ----------

quarantine_ratio = spark.sql(f"""
  SELECT SUM(failed_rows) / SUM(total_rows) FROM {cfg.table('ops','dq_results')} WHERE action = 'quarantine'
""").collect()[0][0] or 0
print(f"Ratio global de filas en cuarentena: {quarantine_ratio:.4%}")
if quarantine_ratio > 0.01:
    failed.append(f"cuarentena {quarantine_ratio:.2%} > 1%")

assert not failed, f"Quality gate FALLÓ: {failed}"
print("Quality gate OK ✅")
