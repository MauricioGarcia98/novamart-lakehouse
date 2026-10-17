# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 09 · Spark intermedio: ventanas, tipos complejos y UDFs
# MAGIC
# MAGIC Cubrimos lo que más se usa en pipelines reales: ventanas con la API, structs/arrays,
# MAGIC y cuándo (no) usar funciones definidas por el usuario.

# COMMAND ----------

dbutils.widgets.text("catalog", "novamart_dev")  # en prod: novamart
spark.sql(f"USE CATALOG {dbutils.widgets.get('catalog')}")

import time

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md ## 1. Ventanas con la API: RFM de cada cliente en 15 líneas

# COMMAND ----------

sales = spark.table("gold.fact_sales").where("customer_key <> -1")
cust = spark.table("gold.dim_customer").select("customer_key", "customer_id")
orders = (
    sales.join(cust, "customer_key")
    .groupBy("customer_id", "order_id")
    .agg(F.max("order_date").alias("order_date"), F.sum("net_amount").alias("amount"))
)
w = Window.partitionBy("customer_id").orderBy("order_date")
with_gaps = (
    orders.withColumn("prev_date", F.lag("order_date").over(w))
    .withColumn("days_between", F.datediff("order_date", "prev_date"))
    .withColumn("order_n", F.row_number().over(w))
    .withColumn("cum_amount", F.sum("amount").over(w.rowsBetween(Window.unboundedPreceding, 0)))
)
display(with_gaps.where("order_n <= 5").orderBy("customer_id", "order_n").limit(30))

# COMMAND ----------

# MAGIC %md ## 2. Structs, arrays y maps con la API

# COMMAND ----------

raw = spark.table("bronze.orders")
lines = (
    raw.select("order_id", "payment.*", F.posexplode("items").alias("pos", "item"))   # payment.* aplana el struct
    .select("order_id", "method", "installments", "pos", "item.*")
)
display(lines.limit(10))

# COMMAND ----------

# Volver a anidar: un documento por cliente con sus últimas 3 compras (útil para servir a una app/API)
profile = (
    with_gaps.withColumn("rn_desc", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.desc("order_date"))))
    .where("rn_desc <= 3")
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_list(F.struct("order_date", "order_id", "amount")), asc=False).alias("last_orders"))
)
display(profile.limit(5))
print(profile.limit(1).toJSON().first())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. UDFs: Python vs pandas (vectorizada) vs nativo
# MAGIC Una UDF de Python procesa **fila por fila** y obliga a serializar datos entre la JVM y Python.
# MAGIC Una *pandas UDF* procesa **lotes** con Apache Arrow. Una función nativa no sale nunca de la JVM/Photon.
# MAGIC
# MAGIC Caso: clasificar el ticket del pedido en rangos.

# COMMAND ----------

lines_df = spark.table("silver.order_lines").select("net_amount").limit(5_000_000)


@F.udf("string")
def band_py(x):
    if x is None:
        return None
    x = float(x)
    return "bajo" if x < 20 else "medio" if x < 200 else "alto"


@F.pandas_udf("string")
def band_pandas(s: pd.Series) -> pd.Series:
    s = s.astype(float)
    return pd.cut(s, [-float("inf"), 20, 200, float("inf")], labels=["bajo", "medio", "alto"], right=False).astype(str)


band_native = (F.when(F.col("net_amount") < 20, "bajo").when(F.col("net_amount") < 200, "medio").otherwise("alto"))

for name, col in [("UDF Python", band_py("net_amount")), ("pandas UDF", band_pandas("net_amount")),
                  ("nativo", band_native)]:
    t = time.time()
    lines_df.select(col.alias("band")).groupBy("band").count().collect()
    print(f"{name:12s} {time.time() - t:6.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC **Regla práctica:** nativo > SQL `CASE`/higher-order functions > pandas UDF > Python UDF.
# MAGIC Usá UDFs sólo para lógica que no existe en Spark (ej. una librería de Python específica).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `applyInPandas`: lógica de pandas por grupo, distribuida
# MAGIC Ej.: detección de anomalías de venta diaria **por tienda** con z-score (cada tienda en su propia tarea).

# COMMAND ----------

daily = (
    spark.table("gold.mart_sales_daily").where("store_key <> -1")
    .groupBy("store_key", "order_date").agg(F.sum("net_sales").cast("double").alias("net_sales"))
)


def zscore(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("order_date")
    roll = pdf["net_sales"].rolling(28, min_periods=7)
    pdf["z"] = (pdf["net_sales"] - roll.mean()) / roll.std()
    pdf["is_anomaly"] = pdf["z"].abs() > 3
    return pdf


anomalies = daily.groupBy("store_key").applyInPandas(
    zscore, schema="store_key long, order_date date, net_sales double, z double, is_anomaly boolean"
)
display(anomalies.where("is_anomaly").orderBy(F.desc("z")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Ejercicios
# MAGIC 1. Con la API, calculá para cada producto la venta de los últimos 7 y 28 días con `rangeBetween` sobre una
# MAGIC    columna numérica de días (pista: `F.datediff(col, F.lit('2000-01-01'))`).
# MAGIC 2. Construí con `collect_list` + `struct` el "carrito" de cada sesión web (productos vistos en orden).
# MAGIC 3. ¿Las anomalías detectadas coinciden con eventos comerciales? Uní con `dim_date.commercial_event`.
