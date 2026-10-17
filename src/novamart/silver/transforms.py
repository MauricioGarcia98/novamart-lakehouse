"""Transformaciones puras bronze -> silver (DataFrame in, DataFrame out).

Son funciones sin efectos secundarios: no leen ni escriben tablas. Por eso se
pueden testear con pytest usando DataFrames chicos armados a mano.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

MONEY = DecimalType(14, 2)
RATE = DecimalType(5, 4)

ISO_FORMATS = (
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
    "yyyy-MM-dd'T'HH:mm:ss'Z'",
    "yyyy-MM-dd HH:mm:ss",
)


def parse_timestamp(col: str | Column, formats=ISO_FORMATS) -> Column:
    """Prueba varios formatos y se queda con el primero que funcione.

    `try_to_timestamp` devuelve NULL si no puede parsear (con ANSI activado,
    `to_timestamp` lanzaría error y abortaría el job entero).
    """
    c = F.col(col) if isinstance(col, str) else col
    return F.coalesce(*[F.try_to_timestamp(c.cast("string"), F.lit(f)) for f in formats])


def _opt(df: DataFrame, name: str, dtype: str = "string") -> Column:
    """Columna opcional: existe sólo desde que ocurrió un schema drift."""
    return F.col(name).cast(dtype) if name in df.columns else F.lit(None).cast(dtype)


def clean_name(col: str) -> Column:
    return F.initcap(F.trim(F.regexp_replace(F.col(col), r"\s+", " ")))


def latest_per_key(df: DataFrame, keys: list[str], order_col: str, ascending: bool = False) -> DataFrame:
    """Deduplicación clásica con ROW_NUMBER() (qualify rn = 1)."""
    order = F.col(order_col).asc() if ascending else F.col(order_col).desc()
    w = Window.partitionBy(*keys).orderBy(order)
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


# =================================================================== clientes
def clean_customers(df: DataFrame) -> DataFrame:
    phone_digits = F.regexp_replace(F.col("phone"), r"[^0-9]", "")
    return (
        df.select(
            F.col("customer_id").cast("bigint").alias("customer_id"),
            clean_name("first_name").alias("first_name"),
            clean_name("last_name").alias("last_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.when(F.length(phone_digits) >= 8, phone_digits).alias("phone"),
            F.to_date(F.try_to_timestamp(F.col("birth_date"), F.lit("dd/MM/yyyy"))).alias("birth_date"),
            F.upper(F.trim(F.col("gender"))).alias("gender"),
            F.trim(F.col("city")).alias("city"),
            F.trim(F.col("state")).alias("state"),
            F.upper(F.trim(F.col("country"))).alias("country"),
            F.coalesce(F.trim(F.col("segment")), F.lit("Sin segmento")).alias("segment"),
            (F.lower(F.col("marketing_opt_in").cast("string")) == "true").alias("marketing_opt_in"),
            _opt(df, "loyalty_tier").alias("loyalty_tier"),
            parse_timestamp("updated_at").alias("updated_at"),
            (F.upper(F.col("op")) == "DELETE").alias("is_deleted"),
            F.col("_ingested_at"),
        )
        .dropDuplicates(["customer_id", "updated_at", "is_deleted"])
    )


CUSTOMER_TRACKED = [
    "first_name", "last_name", "email", "phone", "birth_date", "gender", "city", "state",
    "country", "segment", "marketing_opt_in", "loyalty_tier",
]


# ================================================================== productos
def clean_products(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("product_id").cast("bigint").alias("product_id"),
            F.upper(F.trim(F.col("sku"))).alias("sku"),
            F.trim(F.col("product_name")).alias("product_name"),
            F.trim(F.col("brand")).alias("brand"),
            F.col("department"),
            F.col("category"),
            F.col("subcategory"),
            F.col("attributes.color").alias("color"),
            F.col("attributes.weight_kg").cast("double").alias("weight_kg"),
            F.col("attributes.warranty_months").cast("int").alias("warranty_months"),
            F.col("list_price").cast(MONEY).alias("list_price"),
            F.col("unit_cost").cast(MONEY).alias("unit_cost"),
            F.col("is_active").cast("boolean").alias("is_active"),
            parse_timestamp("updated_at").alias("updated_at"),
            F.lit(False).alias("is_deleted"),
            F.col("_ingested_at"),
        )
        .dropDuplicates(["product_id", "updated_at"])
    )


PRODUCT_TRACKED = [
    "sku", "product_name", "brand", "department", "category", "subcategory", "color",
    "weight_kg", "warranty_months", "list_price", "unit_cost", "is_active",
]


# =================================================================== tiendas
def clean_stores(df: DataFrame) -> DataFrame:
    return latest_per_key(
        df.select(
            F.col("store_id").cast("bigint").alias("store_id"),
            F.trim(F.col("store_name")).alias("store_name"),
            F.col("store_format"),
            F.col("city"),
            F.col("state"),
            F.upper(F.col("country")).alias("country"),
            F.col("region"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("sales_area_sqm").cast("int").alias("sales_area_sqm"),
            F.to_date(F.col("opened_date")).alias("opened_date"),
            F.col("manager_name"),
            F.col("_ingested_at"),
        ),
        ["store_id"],
        "_ingested_at",
    )


# =================================================================== pedidos
def clean_orders(df: DataFrame) -> DataFrame:
    """Cabecera de pedido: aplana structs, parsea fechas y deduplica re-envíos."""
    order_ts = parse_timestamp("order_ts")
    flat = df.select(
        F.col("order_id").cast("bigint").alias("order_id"),
        order_ts.alias("order_ts"),
        F.to_date(order_ts).alias("order_date"),
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.upper(F.col("channel")).alias("channel"),
        F.col("store_id").cast("bigint").alias("store_id"),
        F.col("currency"),
        F.upper(F.col("promo_code")).alias("promo_code"),
        F.col("payment.method").alias("payment_method"),
        F.col("payment.installments").cast("int").alias("installments"),
        F.col("payment.card_brand").alias("card_brand"),
        F.col("shipping.method").alias("shipping_method"),
        F.col("shipping.cost").cast(MONEY).alias("shipping_cost"),
        F.col("shipping.city").alias("ship_city"),
        F.col("shipping.country").alias("ship_country"),
        F.size("items").alias("n_items"),
        F.col("items"),
        F.col("_ingested_at"),
        F.col("_source_file"),
    )
    # el primer arribo gana: los re-envíos posteriores se descartan
    return latest_per_key(flat, ["order_id"], "_ingested_at", ascending=True)


def explode_order_lines(orders: DataFrame) -> DataFrame:
    """Una fila por línea de pedido: es el GRANO del fact_sales."""
    lines = orders.select(
        "order_id", "order_ts", "order_date", "channel", F.explode("items").alias("it"), "_ingested_at"
    )
    qty = F.col("it.quantity").cast("int")
    price = F.col("it.unit_price").cast(MONEY)
    disc = F.col("it.discount_pct").cast(RATE)
    gross = (qty * price).cast(MONEY)
    discount = F.round(gross * disc, 2).cast(MONEY)
    return lines.select(
        "order_id",
        F.col("it.line_id").cast("int").alias("line_id"),
        "order_ts",
        "order_date",
        F.col("it.product_id").cast("bigint").alias("product_id"),
        qty.alias("quantity"),
        price.alias("unit_price"),
        disc.alias("discount_pct"),
        gross.alias("gross_amount"),
        discount.alias("discount_amount"),
        (gross - discount).cast(MONEY).alias("net_amount"),
        "_ingested_at",
    )


def clean_status_events(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("event_id"),
            F.col("order_id").cast("bigint").alias("order_id"),
            F.upper(F.col("status")).alias("status"),
            parse_timestamp("status_ts").alias("status_ts"),
            F.col("_ingested_at"),
        )
        .dropDuplicates(["event_id"])
    )


def clean_returns(df: DataFrame) -> DataFrame:
    ts = parse_timestamp("return_ts")
    return (
        df.select(
            F.col("return_id"),
            F.col("order_id").cast("bigint").alias("order_id"),
            F.col("line_id").cast("int").alias("line_id"),
            F.col("product_id").cast("bigint").alias("product_id"),
            F.col("quantity").cast("int").alias("quantity"),
            F.trim(F.col("reason")).alias("reason"),
            F.col("refund_amount").cast(MONEY).alias("refund_amount"),
            ts.alias("return_ts"),
            F.to_date(ts).alias("return_date"),
            F.col("_ingested_at"),
        )
        .dropDuplicates(["return_id"])
    )


# ================================================================ clickstream
def clean_web_events(df: DataFrame) -> DataFrame:
    ts = F.timestamp_millis(F.col("event_ts").cast("bigint"))
    return (
        df.select(
            F.col("event_id"),
            F.col("session_id"),
            F.col("anonymous_id"),
            F.col("customer_id").cast("bigint").alias("customer_id"),
            F.lower(F.col("event_type")).alias("event_type"),
            ts.alias("event_ts"),
            F.to_date(ts).alias("event_date"),
            F.col("product_id").cast("bigint").alias("product_id"),
            F.col("search_query"),
            F.col("page_path"),
            F.col("device.platform").alias("platform"),
            F.col("device.device_type").alias("device_type"),
            F.col("device.os").alias("os"),
            F.col("device.browser").alias("browser"),
            F.col("traffic.source").alias("utm_source"),
            F.col("traffic.medium").alias("utm_medium"),
            F.col("traffic.campaign").alias("utm_campaign"),
            F.col("geo.city").alias("geo_city"),
            F.col("geo.country").alias("geo_country"),
            _opt(df, "experiment_id").alias("experiment_id"),
            F.col("_ingested_at"),
        )
        .dropDuplicates(["event_id"])
    )


def clean_inventory(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("snapshot_date").cast("date").alias("snapshot_date"),
        F.col("store_id").cast("bigint").alias("store_id"),
        F.col("product_id").cast("bigint").alias("product_id"),
        F.col("on_hand_qty").cast("int").alias("on_hand_qty"),
        F.col("on_order_qty").cast("int").alias("on_order_qty"),
        F.col("max_capacity").cast("int").alias("max_capacity"),
        F.col("unit_cost").cast(MONEY).alias("unit_cost"),
        F.col("_ingested_at"),
    ).dropDuplicates(["snapshot_date", "store_id", "product_id"])
