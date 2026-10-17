"""Dimensiones del modelo estrella (Kimball).

Decisiones de modelado:
  * Claves subrogadas DETERMINÍSTICAS: `xxhash64(clave_natural, valid_from)`.
    Ventaja: se pueden reconstruir las dims completas sin que cambien las
    claves (los facts no se rompen) y se calculan en paralelo sin secuencias.
    Alternativa en Databricks: columnas IDENTITY (secuenciales, pero no
    reproducibles al reconstruir). Ver docs/03_modelado_de_datos.md.
  * Miembro desconocido (-1): ventas en caja sin cliente identificado, o de
    canal online sin tienda. Evita NULLs en las FKs del fact.
  * `effective_from`/`effective_to` se usan para el join "point-in-time":
    la primera versión arranca en 1900-01-01 para cubrir hechos que llegan
    antes que la dimensión (late arriving dimension).
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from novamart.config import ProjectConfig
from novamart.datagen.calendar import promo_name

UNKNOWN_KEY = -1
MIN_TS = "1900-01-01 00:00:00"
MAX_TS = "9999-12-31 23:59:59"

MONTHS_ES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
             "septiembre", "octubre", "noviembre", "diciembre"]
DAYS_ES = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]


def build_dim_date(spark: SparkSession, start: date = date(2020, 1, 1), end: date = date(2027, 12, 31)) -> DataFrame:
    """Dimensión fecha (role-playing: fecha de pedido, de entrega, de devolución...)."""
    rows = []
    d = start
    while d <= end:
        iso = d.isocalendar()
        rows.append((
            int(d.strftime("%Y%m%d")), d, d.year, (d.month - 1) // 3 + 1, d.month, MONTHS_ES[d.month - 1],
            f"{d.year}-{d.month:02d}", iso[1], d.isoweekday(), DAYS_ES[d.weekday()], d.weekday() >= 5,
            d.day, (d + timedelta(days=1)).month != d.month, promo_name(d),
            # año fiscal de NovaMart: arranca en febrero (común en retail)
            d.year if d.month >= 2 else d.year - 1,
        ))
        d += timedelta(days=1)
    schema = ("date_key int, date date, year int, quarter int, month int, month_name string, year_month string, "
              "iso_week int, day_of_week int, day_name string, is_weekend boolean, day_of_month int, "
              "is_month_end boolean, commercial_event string, fiscal_year int")
    return spark.createDataFrame(rows, schema)


def _effective_window(df: DataFrame, key: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy("valid_from")
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .withColumn("effective_from", F.when(F.col("_rn") == 1, F.to_timestamp(F.lit(MIN_TS))).otherwise(F.col("valid_from")))
        .withColumn("effective_to", F.coalesce(F.col("valid_to"), F.to_timestamp(F.lit(MAX_TS))))
        .drop("_rn")
    )


def build_dim_customer(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    hist = spark.table(cfg.table("silver", "customers_history"))
    age = F.floor(F.months_between(F.current_date(), F.col("birth_date")) / 12)
    dim = _effective_window(hist, "customer_id").select(
        F.xxhash64("customer_id", "valid_from").alias("customer_key"),
        "customer_id",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "email", "gender", "birth_date",
        F.when(age < 25, "18-24").when(age < 35, "25-34").when(age < 45, "35-44")
         .when(age < 55, "45-54").when(age < 65, "55-64").otherwise("65+").alias("age_band"),
        "city", "state", "country", "segment",
        F.coalesce("loyalty_tier", F.lit("SIN PROGRAMA")).alias("loyalty_tier"),
        "marketing_opt_in", "is_deleted",
        "valid_from", "valid_to", "effective_from", "effective_to", "is_current",
    )
    unknown = spark.createDataFrame(
        [(UNKNOWN_KEY, -1, "Cliente no identificado", None, "X", None, "N/D", "N/D", "N/D", "N/D", "N/D",
          "SIN PROGRAMA", False, False)],
        "customer_key bigint, customer_id bigint, full_name string, email string, gender string, birth_date date, "
        "age_band string, city string, state string, country string, segment string, loyalty_tier string, "
        "marketing_opt_in boolean, is_deleted boolean",
    ).select("*",
             F.to_timestamp(F.lit(MIN_TS)).alias("valid_from"), F.lit(None).cast("timestamp").alias("valid_to"),
             F.to_timestamp(F.lit(MIN_TS)).alias("effective_from"), F.to_timestamp(F.lit(MAX_TS)).alias("effective_to"),
             F.lit(True).alias("is_current"))
    return dim.unionByName(unknown)


def build_dim_product(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    hist = spark.table(cfg.table("silver", "products_history"))
    margin = F.round((F.col("list_price") - F.col("unit_cost")) / F.col("list_price"), 4)
    dim = _effective_window(hist, "product_id").select(
        F.xxhash64("product_id", "valid_from").alias("product_key"),
        "product_id", "sku", "product_name", "brand",
        (F.col("brand") == "NovaMart Basics").alias("is_own_brand"),
        "department", "category", "subcategory", "color", "weight_kg", "warranty_months",
        "list_price", "unit_cost", margin.alias("target_margin_pct"),
        F.when(F.col("list_price") < 20, "1. < 20").when(F.col("list_price") < 100, "2. 20-99")
         .when(F.col("list_price") < 500, "3. 100-499").otherwise("4. 500+").alias("price_band"),
        "is_active",
        "valid_from", "valid_to", "effective_from", "effective_to", "is_current",
    )
    unknown = (
        spark.range(1).select(
            F.lit(UNKNOWN_KEY).cast("bigint").alias("product_key"), F.lit(-1).cast("bigint").alias("product_id"),
            F.lit("N/D").alias("sku"), F.lit("Producto desconocido").alias("product_name"))
    )
    return dim.unionByName(unknown, allowMissingColumns=True)


def build_dim_store(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    stores = spark.table(cfg.table("silver", "stores"))
    dim = stores.select(
        F.col("store_id").alias("store_key"), "store_id", "store_name", "store_format",
        "city", "state", "country", "region", "latitude", "longitude", "sales_area_sqm",
        "opened_date", "manager_name", F.lit(True).alias("is_physical"),
    )
    online = spark.range(1).select(
        F.lit(UNKNOWN_KEY).cast("bigint").alias("store_key"), F.lit(-1).cast("bigint").alias("store_id"),
        F.lit("Canal digital (sin tienda)").alias("store_name"), F.lit("Online").alias("store_format"),
        F.lit(False).alias("is_physical"),
    )
    return dim.unionByName(online, allowMissingColumns=True)


def build_dim_channel(spark: SparkSession) -> DataFrame:
    rows = [(1, "WEB", "Sitio web", "Digital"), (2, "APP", "App móvil", "Digital"),
            (3, "STORE", "Tienda física", "Físico"), (4, "MARKETPLACE", "Marketplace de terceros", "Digital")]
    return spark.createDataFrame(rows, "channel_key int, channel_code string, channel_name string, channel_group string")


def build_dim_payment(spark: SparkSession, cfg: ProjectConfig) -> DataFrame:
    """Dimensión *junk*: combina flags/códigos de baja cardinalidad del pago."""
    orders = spark.table(cfg.table("silver", "orders"))
    band = (F.when(F.col("installments") <= 1, "1 pago").when(F.col("installments") <= 6, "2-6 cuotas")
            .otherwise("7+ cuotas"))
    return (
        orders.select(
            "payment_method", F.coalesce("installments", F.lit(1)).alias("installments"),
            F.coalesce("card_brand", F.lit("N/A")).alias("card_brand"),
        )
        .distinct()
        .select(
            F.xxhash64("payment_method", "installments", "card_brand").alias("payment_key"),
            "payment_method", "installments", band.alias("installments_band"), "card_brand",
        )
    )


DIMENSIONS = {
    "dim_date": lambda spark, cfg: build_dim_date(spark),
    "dim_customer": build_dim_customer,
    "dim_product": build_dim_product,
    "dim_store": build_dim_store,
    "dim_channel": lambda spark, cfg: build_dim_channel(spark),
    "dim_payment": build_dim_payment,
}


def load_dimensions(spark: SparkSession, cfg: ProjectConfig) -> dict[str, int]:
    """Reconstruye las dimensiones (full refresh: son chicas y las claves son estables)."""
    out = {}
    for name, builder in DIMENSIONS.items():
        df = builder(spark, cfg)
        (df.write.format("delta").mode("overwrite")
         .saveAsTable(cfg.table("gold", name)))
        out[name] = spark.table(cfg.table("gold", name)).count()
    return out
