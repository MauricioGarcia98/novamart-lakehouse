"""Carga incremental bronze -> silver con Structured Streaming + foreachBatch.

Patrón "streaming incremental en modo batch":
    readStream(bronze)  ->  foreachBatch(limpiar + validar + MERGE)  ->  trigger(availableNow)

* `availableNow` procesa todo lo pendiente y termina: semántica batch, costo batch,
  pero con el checkpoint del streaming (exactly-once, sin re-procesar).
* `foreachBatch` permite usar MERGE (upsert) dentro de un stream.
* Los MERGE incluyen la columna de clustering (fecha) en la condición para que
  Delta pueda descartar archivos (file pruning) y no escanear toda la tabla.
"""

from __future__ import annotations

from collections.abc import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from novamart.config import ProjectConfig
from novamart.quality.expectations import enforce
from novamart.silver import rules
from novamart.silver import transforms as T
from novamart.silver.scd2 import scd2_merge
from novamart.utils.delta import create_table_if_not_exists, merge_upsert


def _insert_only(spark: SparkSession, df: DataFrame, target: str, keys: list[str],
                 cluster_by: list[str], comment: str | None = None) -> None:
    """MERGE que sólo inserta lo que no existe: idempotente ante duplicados."""
    create_table_if_not_exists(spark, target, df, cluster_by=cluster_by, comment=comment)
    cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(df.alias("s"), cond)
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_incremental(
    spark: SparkSession,
    cfg: ProjectConfig,
    bronze_name: str,
    checkpoint_name: str,
    process_batch: Callable[[DataFrame, int], None],
) -> None:
    query = (
        spark.readStream.table(cfg.table("bronze", bronze_name))
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", cfg.checkpoint_path(f"silver_{checkpoint_name}"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


# ===================================================================== clientes
def load_customers(spark: SparkSession, cfg: ProjectConfig) -> None:
    target = cfg.table("silver", "customers_history")

    def batch(df: DataFrame, batch_id: int) -> None:
        s = df.sparkSession
        clean = enforce(s, cfg, T.clean_customers(df), rules.CUSTOMERS, target, batch_id)
        scd2_merge(s, clean.drop("_ingested_at"), target, ["customer_id"], T.CUSTOMER_TRACKED,
                   ts_col="updated_at", cluster_by=["customer_id"])

    run_incremental(spark, cfg, "customers", "customers", batch)


# ==================================================================== productos
def load_products(spark: SparkSession, cfg: ProjectConfig) -> None:
    target = cfg.table("silver", "products_history")

    def batch(df: DataFrame, batch_id: int) -> None:
        s = df.sparkSession
        clean = enforce(s, cfg, T.clean_products(df), rules.PRODUCTS, target, batch_id)
        scd2_merge(s, clean.drop("_ingested_at"), target, ["product_id"], T.PRODUCT_TRACKED,
                   ts_col="updated_at", cluster_by=["product_id"])

    run_incremental(spark, cfg, "products", "products", batch)


# ====================================================================== tiendas
def load_stores(spark: SparkSession, cfg: ProjectConfig) -> None:
    target = cfg.table("silver", "stores")

    def batch(df: DataFrame, batch_id: int) -> None:
        s = df.sparkSession
        clean = T.clean_stores(df)
        create_table_if_not_exists(s, target, clean, cluster_by=["store_id"])
        merge_upsert(s, clean, target, keys=["store_id"], sequence_col="_ingested_at")  # SCD tipo 1

    run_incremental(spark, cfg, "stores", "stores", batch)


# ====================================================================== pedidos
def load_orders(spark: SparkSession, cfg: ProjectConfig) -> None:
    orders_t = cfg.table("silver", "orders")
    lines_t = cfg.table("silver", "order_lines")

    def batch(df: DataFrame, batch_id: int) -> None:
        s = df.sparkSession
        orders = enforce(s, cfg, T.clean_orders(df), rules.ORDERS, orders_t, batch_id)
        lines = enforce(s, cfg, T.explode_order_lines(orders), rules.ORDER_LINES, lines_t, batch_id)
        _insert_only(s, orders.drop("items", "_source_file"), orders_t, ["order_date", "order_id"],
                     ["order_date", "channel"], "Cabecera de pedidos (1 fila por pedido)")
        _insert_only(s, lines, lines_t, ["order_date", "order_id", "line_id"],
                     ["order_date", "product_id"], "Líneas de pedido (1 fila por producto en el pedido)")

    run_incremental(spark, cfg, "orders", "orders", batch)


def load_order_status(spark: SparkSession, cfg: ProjectConfig) -> None:
    target = cfg.table("silver", "order_status_events")

    def batch(df: DataFrame, batch_id: int) -> None:
        s = df.sparkSession
        clean = enforce(s, cfg, T.clean_status_events(df), rules.STATUS_EVENTS, target, batch_id)
        _insert_only(s, clean, target, ["event_id"], ["order_id"])

    run_incremental(spark, cfg, "order_status_events", "order_status_events", batch)


def load_returns(spark: SparkSession, cfg: ProjectConfig) -> None:
    target = cfg.table("silver", "returns")

    def batch(df: DataFrame, batch_id: int) -> None:
        s = df.sparkSession
        clean = enforce(s, cfg, T.clean_returns(df), rules.RETURNS, target, batch_id)
        _insert_only(s, clean, target, ["return_id"], ["return_date"])

    run_incremental(spark, cfg, "returns", "returns", batch)


# ================================================================== clickstream
def load_web_events(spark: SparkSession, cfg: ProjectConfig) -> None:
    target = cfg.table("silver", "web_events")

    def batch(df: DataFrame, batch_id: int) -> None:
        s = df.sparkSession
        clean = enforce(s, cfg, T.clean_web_events(df), rules.WEB_EVENTS, target, batch_id)
        _insert_only(s, clean, target, ["event_date", "event_id"], ["event_date", "event_type"],
                     "Clickstream web/app (1 fila por evento)")

    run_incremental(spark, cfg, "web_events", "web_events", batch)


def load_inventory(spark: SparkSession, cfg: ProjectConfig) -> None:
    target = cfg.table("silver", "inventory_snapshots")

    def batch(df: DataFrame, batch_id: int) -> None:
        s = df.sparkSession
        clean = enforce(s, cfg, T.clean_inventory(df), rules.INVENTORY, target, batch_id)
        _insert_only(s, clean, target, ["snapshot_date", "store_id", "product_id"], ["snapshot_date", "store_id"])

    run_incremental(spark, cfg, "inventory_snapshots", "inventory_snapshots", batch)


LOADERS = {
    "customers": load_customers,
    "products": load_products,
    "stores": load_stores,
    "orders": load_orders,
    "order_status_events": load_order_status,
    "returns": load_returns,
    "web_events": load_web_events,
    "inventory_snapshots": load_inventory,
}


def load_all(spark: SparkSession, cfg: ProjectConfig, names: list[str] | None = None) -> None:
    for name in names or list(LOADERS):
        LOADERS[name](spark, cfg)
