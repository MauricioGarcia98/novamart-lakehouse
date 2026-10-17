"""Tablas de hechos: los 4 tipos clásicos de Kimball en un solo proyecto.

    fact_sales               TRANSACCIONAL       1 fila por línea de pedido
    fact_returns             TRANSACCIONAL       1 fila por línea devuelta
    fact_order_fulfillment   ACCUMULATING SNAP.  1 fila por pedido, se ACTUALIZA en cada hito
    fact_inventory_snapshot  PERIODIC SNAPSHOT   1 fila por tienda x SKU x semana (semi-aditivo)
    fact_web_sessions        AGREGADO/SESIÓN     1 fila por sesión (clickstream sessionizado)

Carga incremental: se calcula qué fechas de negocio cambiaron desde el último
watermark y se re-generan SOLO esas particiones lógicas con `replaceWhere`.
Re-ejecutar es idempotente: el resultado no depende de cuántas veces corra.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from novamart.config import ProjectConfig
from novamart.gold.dimensions import UNKNOWN_KEY
from novamart.utils.audit import get_watermark, set_watermark
from novamart.utils.delta import create_table_if_not_exists, overwrite_partitions


def _date_key(col: str) -> F.Column:
    return F.date_format(F.col(col), "yyyyMMdd").cast("int")


def _pit_join(fact: DataFrame, dim: DataFrame, natural_key: str, ts_col: str, key_col: str,
              extra: list[str] = ()) -> DataFrame:
    """Join "point-in-time": la versión de la dimensión vigente al momento del hecho.

    Es un equi-join por la clave natural + un filtro de rango temporal. Así una
    venta de 2023 queda asociada a la ciudad/segmento que el cliente tenía en 2023.
    """
    d = dim.select(F.col(natural_key).alias("_nk"), F.col(key_col), "effective_from", "effective_to", *extra)
    joined = fact.join(
        d,
        (fact[natural_key] == d["_nk"]) & (fact[ts_col] >= d["effective_from"]) & (fact[ts_col] < d["effective_to"]),
        "left",
    )
    return (joined.drop("_nk", "effective_from", "effective_to")
            .withColumn(key_col, F.coalesce(F.col(key_col), F.lit(UNKNOWN_KEY).cast("bigint"))))


def _changed_dates(spark: SparkSession, table: str, date_col: str, since: datetime | None) -> list:
    df = spark.table(table)
    if since is not None:
        df = df.where(F.col("_ingested_at") > F.lit(since))
    return [r[0] for r in df.select(date_col).distinct().collect() if r[0] is not None]


def _max_ingested(spark: SparkSession, table: str) -> datetime | None:
    return spark.table(table).agg(F.max("_ingested_at")).collect()[0][0]


def _in_dates(col: str, dates: list) -> str:
    return f"{col} IN ({', '.join(repr(d.isoformat()) for d in sorted(dates))})"


# ================================================================= fact_sales
def build_fact_sales(spark: SparkSession, cfg: ProjectConfig, dates: list | None) -> DataFrame:
    lines = spark.table(cfg.table("silver", "order_lines"))
    orders = spark.table(cfg.table("silver", "orders"))
    if dates is not None:
        lines = lines.where(F.col("order_date").isin(dates))
        orders = orders.where(F.col("order_date").isin(dates))
    header = orders.select(
        "order_id", "order_date", "customer_id", "store_id", "channel", "promo_code",
        "payment_method", F.coalesce("installments", F.lit(1)).alias("installments"),
        F.coalesce("card_brand", F.lit("N/A")).alias("card_brand"),
    )
    f = lines.drop("_ingested_at").join(header, ["order_id", "order_date"], "inner")
    dim_c = spark.table(cfg.table("gold", "dim_customer")).where("customer_key <> -1")
    dim_p = spark.table(cfg.table("gold", "dim_product")).where("product_key <> -1")
    dim_ch = spark.table(cfg.table("gold", "dim_channel"))
    f = _pit_join(f, dim_c, "customer_id", "order_ts", "customer_key")
    f = _pit_join(f, dim_p, "product_id", "order_ts", "product_key", extra=[F.col("unit_cost").alias("_unit_cost")])
    f = f.join(F.broadcast(dim_ch.select(F.col("channel_code").alias("channel"), "channel_key")), "channel", "left")
    cost = (F.col("quantity") * F.col("_unit_cost")).cast("decimal(14,2)")
    return f.select(
        _date_key("order_date").alias("order_date_key"),
        "order_date",
        "order_ts",
        "order_id",            # dimensión degenerada
        "line_id",
        "customer_key",
        "product_key",
        F.coalesce("store_id", F.lit(UNKNOWN_KEY)).cast("bigint").alias("store_key"),
        "channel_key",
        F.xxhash64("payment_method", "installments", "card_brand").alias("payment_key"),
        "promo_code",          # dimensión degenerada
        "quantity", "unit_price", "discount_pct",
        "gross_amount", "discount_amount", "net_amount",
        cost.alias("cost_amount"),
        (F.col("net_amount") - cost).cast("decimal(14,2)").alias("margin_amount"),
    )


def load_fact_sales(spark: SparkSession, cfg: ProjectConfig, full_refresh: bool = False) -> int:
    target = cfg.table("gold", "fact_sales")
    src = cfg.table("silver", "order_lines")
    wm = None if full_refresh or not spark.catalog.tableExists(target) else get_watermark(cfg, spark, "fact_sales")
    new_wm = _max_ingested(spark, src)
    dates = None if wm is None else _changed_dates(spark, src, "order_date", wm)
    if dates == []:
        return 0
    df = build_fact_sales(spark, cfg, dates)
    if dates is None:
        create_table_if_not_exists(spark, target, df, cluster_by=["order_date", "store_key"],
                                   comment="Ventas a nivel línea de pedido (grano: order_id + line_id)")
        df.write.format("delta").mode("overwrite").saveAsTable(target)
    else:
        overwrite_partitions(df, target, _in_dates("order_date", dates))
    if new_wm:
        set_watermark(cfg, spark, "fact_sales", new_wm)
    return len(dates) if dates else -1


# ===================================================== fact_order_fulfillment
MILESTONES = ["CREATED", "PAYMENT_APPROVED", "SHIPPED", "READY_FOR_PICKUP", "DELIVERED", "CANCELLED"]


def build_fact_fulfillment(spark: SparkSession, cfg: ProjectConfig, order_ids: DataFrame | None) -> DataFrame:
    ev = spark.table(cfg.table("silver", "order_status_events"))
    if order_ids is not None:
        ev = ev.join(order_ids, "order_id", "left_semi")
    # PIVOT: una columna por hito (filas -> columnas)
    piv = ev.groupBy("order_id").pivot("status", MILESTONES).agg(F.min("status_ts"))
    for m in MILESTONES:
        piv = piv.withColumnRenamed(m, f"{m.lower()}_ts")
    orders = spark.table(cfg.table("silver", "orders")).select(
        "order_id", "order_date", "order_ts", "customer_id", "store_id", "channel", "shipping_method")
    sales = spark.table(cfg.table("gold", "fact_sales"))
    if order_ids is not None:
        sales = sales.join(order_ids, "order_id", "left_semi")
    sales = sales.groupBy("order_id").agg(
        F.sum("net_amount").alias("order_net_amount"), F.sum("quantity").alias("order_units"),
        F.first("customer_key").alias("customer_key"), F.first("channel_key").alias("channel_key"))
    f = piv.join(orders, "order_id", "inner").join(sales, "order_id", "left")
    hours = lambda a, b: F.round((F.unix_timestamp(b) - F.unix_timestamp(a)) / 3600.0, 2)  # noqa: E731
    delivered = F.col("delivered_ts")
    status = (F.when(F.col("cancelled_ts").isNotNull(), "CANCELLED").when(delivered.isNotNull(), "DELIVERED")
              .when(F.col("ready_for_pickup_ts").isNotNull(), "READY_FOR_PICKUP")
              .when(F.col("shipped_ts").isNotNull(), "SHIPPED")
              .when(F.col("payment_approved_ts").isNotNull(), "PAID").otherwise("CREATED"))
    return f.select(
        "order_id",
        _date_key("order_date").alias("order_date_key"),
        F.date_format("payment_approved_ts", "yyyyMMdd").cast("int").alias("paid_date_key"),
        F.date_format("shipped_ts", "yyyyMMdd").cast("int").alias("shipped_date_key"),
        F.date_format("delivered_ts", "yyyyMMdd").cast("int").alias("delivered_date_key"),
        "order_date",
        F.coalesce("customer_key", F.lit(UNKNOWN_KEY).cast("bigint")).alias("customer_key"),
        F.coalesce("store_id", F.lit(UNKNOWN_KEY)).cast("bigint").alias("store_key"),
        "channel_key", "shipping_method",
        "order_ts", "payment_approved_ts", "shipped_ts", "ready_for_pickup_ts", "delivered_ts", "cancelled_ts",
        status.alias("current_status"),
        hours("order_ts", "payment_approved_ts").alias("hours_to_pay"),
        hours("payment_approved_ts", F.coalesce("shipped_ts", "ready_for_pickup_ts")).alias("hours_to_ship"),
        hours("order_ts", "delivered_ts").alias("hours_to_deliver"),
        F.when(delivered.isNotNull(), hours("order_ts", "delivered_ts") <= 120).alias("is_on_time"),
        "order_net_amount", "order_units",
        F.current_timestamp().alias("_updated_at"),
    )


def load_fact_fulfillment(spark: SparkSession, cfg: ProjectConfig, full_refresh: bool = False) -> None:
    """Accumulating snapshot: MERGE que ACTUALIZA la fila del pedido con cada hito nuevo."""
    target = cfg.table("gold", "fact_order_fulfillment")
    src = cfg.table("silver", "order_status_events")
    wm = None if full_refresh or not spark.catalog.tableExists(target) else get_watermark(cfg, spark, "fulfillment")
    new_wm = _max_ingested(spark, src)
    ids = None
    if wm is not None:
        ids = spark.table(src).where(F.col("_ingested_at") > F.lit(wm)).select("order_id").distinct()
    df = build_fact_fulfillment(spark, cfg, ids)
    if create_table_if_not_exists(spark, target, df, cluster_by=["order_date"]) or ids is None:
        df.write.format("delta").mode("overwrite").saveAsTable(target)
    else:
        (DeltaTable.forName(spark, target).alias("t")
         .merge(df.alias("s"), "t.order_id = s.order_id AND t.order_date = s.order_date")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    if new_wm:
        set_watermark(cfg, spark, "fulfillment", new_wm)


# ============================================================== fact_returns
def load_fact_returns(spark: SparkSession, cfg: ProjectConfig) -> None:
    r = spark.table(cfg.table("silver", "returns"))
    sales = spark.table(cfg.table("gold", "fact_sales")).select(
        "order_id", "line_id", "customer_key", "product_key", "store_key", "channel_key", "order_date_key", "unit_price")
    df = r.join(sales, ["order_id", "line_id"], "left").select(
        "return_id",
        _date_key("return_date").alias("return_date_key"),
        "return_date", "return_ts", "order_id", "line_id", "order_date_key",
        F.coalesce("customer_key", F.lit(UNKNOWN_KEY).cast("bigint")).alias("customer_key"),
        F.coalesce("product_key", F.lit(UNKNOWN_KEY).cast("bigint")).alias("product_key"),
        F.coalesce("store_key", F.lit(UNKNOWN_KEY).cast("bigint")).alias("store_key"),
        "channel_key", "reason", "quantity", "refund_amount",
        F.datediff("return_date", F.to_date(F.col("order_date_key").cast("string"), "yyyyMMdd")).alias("days_since_order"),
    )
    df.write.format("delta").mode("overwrite").saveAsTable(cfg.table("gold", "fact_returns"))


# =================================================== fact_inventory_snapshot
def load_fact_inventory(spark: SparkSession, cfg: ProjectConfig, full_refresh: bool = False) -> None:
    target = cfg.table("gold", "fact_inventory_snapshot")
    src = cfg.table("silver", "inventory_snapshots")
    wm = None if full_refresh or not spark.catalog.tableExists(target) else get_watermark(cfg, spark, "inventory")
    new_wm = _max_ingested(spark, src)
    dates = None if wm is None else _changed_dates(spark, src, "snapshot_date", wm)
    if dates == []:
        return
    inv = spark.table(src)
    if dates is not None:
        inv = inv.where(F.col("snapshot_date").isin(dates))
    inv = inv.withColumn("_snap_ts", F.to_timestamp("snapshot_date"))
    dim_p = spark.table(cfg.table("gold", "dim_product")).where("product_key <> -1")
    f = _pit_join(inv, dim_p, "product_id", "_snap_ts", "product_key")
    df = f.select(
        _date_key("snapshot_date").alias("snapshot_date_key"), "snapshot_date",
        F.col("store_id").alias("store_key"), "product_key",
        "on_hand_qty", "on_order_qty", "max_capacity",
        (F.col("on_hand_qty") * F.col("unit_cost")).cast("decimal(16,2)").alias("inventory_value"),
        (F.col("on_hand_qty") == 0).alias("is_stockout"),
        F.round(F.col("on_hand_qty") / F.col("max_capacity"), 4).alias("fill_rate"),
    )
    if dates is None:
        create_table_if_not_exists(spark, target, df, cluster_by=["snapshot_date", "store_key"])
        df.write.format("delta").mode("overwrite").saveAsTable(target)
    else:
        overwrite_partitions(df, target, _in_dates("snapshot_date", dates))
    if new_wm:
        set_watermark(cfg, spark, "inventory", new_wm)


# ========================================================= fact_web_sessions
def build_web_sessions(events: DataFrame) -> DataFrame:
    """Sessionización: agrega eventos a nivel sesión con flags del embudo."""
    flag = lambda t: F.max(F.when(F.col("event_type") == t, 1).otherwise(0)).cast("boolean")  # noqa: E731
    first = lambda c: F.min_by(c, "event_ts")  # noqa: E731
    s = events.groupBy("session_id").agg(
        F.min("event_ts").alias("session_start"),
        F.max("event_ts").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum(F.when(F.col("event_type") == "product_view", 1).otherwise(0)).alias("n_product_views"),
        F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("n_searches"),
        flag("add_to_cart").alias("reached_cart"),
        flag("begin_checkout").alias("reached_checkout"),
        flag("purchase").alias("converted"),
        F.max("customer_id").alias("customer_id"),
        first("anonymous_id").alias("anonymous_id"),
        first("platform").alias("platform"),
        first("device_type").alias("device_type"),
        first("os").alias("os"),
        first("utm_source").alias("utm_source"),
        first("utm_medium").alias("utm_medium"),
        first("utm_campaign").alias("utm_campaign"),
        first("geo_country").alias("geo_country"),
        first("page_path").alias("landing_page"),
        F.max("experiment_id").alias("experiment_id"),
    )
    return (
        s.withColumn("session_date", F.to_date("session_start"))
        .withColumn("session_date_key", _date_key("session_start"))
        .withColumn("duration_s", F.unix_timestamp("session_end") - F.unix_timestamp("session_start"))
        .withColumn("is_bounce", F.col("n_events") == 1)
    )


def load_fact_web_sessions(spark: SparkSession, cfg: ProjectConfig, full_refresh: bool = False) -> None:
    target = cfg.table("gold", "fact_web_sessions")
    src = cfg.table("silver", "web_events")
    wm = None if full_refresh or not spark.catalog.tableExists(target) else get_watermark(cfg, spark, "web_sessions")
    new_wm = _max_ingested(spark, src)
    dates = None if wm is None else _changed_dates(spark, src, "event_date", wm)
    if dates == []:
        return
    ev = spark.table(src)
    read_dates = None
    if dates is not None:
        # una sesión que cruza la medianoche toca 2 días: se reemplaza también el día anterior,
        # y se leen eventos de un día más atrás para no recalcular sesiones a medias
        dates = sorted(set(dates) | {d - timedelta(days=1) for d in dates})
        read_dates = sorted(set(dates) | {d - timedelta(days=1) for d in dates})
        ev = ev.where(F.col("event_date").isin(read_dates))
    sessions = build_web_sessions(ev)
    if dates is not None:
        sessions = sessions.where(F.col("session_date").isin(dates))
    dim_c = spark.table(cfg.table("gold", "dim_customer")).where("customer_key <> -1")
    df = _pit_join(sessions, dim_c, "customer_id", "session_start", "customer_key").drop("customer_id")
    if dates is None:
        create_table_if_not_exists(spark, target, df, cluster_by=["session_date"])
        df.write.format("delta").mode("overwrite").saveAsTable(target)
    else:
        overwrite_partitions(df, target, _in_dates("session_date", dates))
    if new_wm:
        set_watermark(cfg, spark, "web_sessions", new_wm)


def load_facts(spark: SparkSession, cfg: ProjectConfig, full_refresh: bool = False) -> None:
    load_fact_sales(spark, cfg, full_refresh)
    load_fact_fulfillment(spark, cfg, full_refresh)
    load_fact_returns(spark, cfg)
    load_fact_inventory(spark, cfg, full_refresh)
    load_fact_web_sessions(spark, cfg, full_refresh)
