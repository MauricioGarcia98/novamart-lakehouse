"""Configuración central del proyecto.

Un único objeto `ProjectConfig` resuelve:
  * nombres de tablas de 3 niveles (catalog.schema.table) de Unity Catalog,
  * rutas de la zona de aterrizaje (UC Volume) y checkpoints de streaming,
  * el "scale factor" que controla el volumen de datos generados.

En Databricks el catálogo por defecto es `novamart`. En local (tests) se usa
`spark_catalog` y un directorio temporal como base de rutas.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date

LAYERS = ("landing", "bronze", "silver", "gold", "ops")

SOURCES = ("customers", "products", "stores", "orders", "order_status_events", "returns",
           "web_events", "inventory_snapshots")

EXPECTED_TABLES = {
    "bronze": list(SOURCES),
    "silver": ["customers_history", "products_history", "stores", "orders", "order_lines",
               "order_status_events", "returns", "web_events", "inventory_snapshots"],
    "gold": ["dim_date", "dim_customer", "dim_product", "dim_store", "dim_channel", "dim_payment",
             "fact_sales", "fact_order_fulfillment", "fact_returns", "fact_inventory_snapshot",
             "fact_web_sessions", "mart_sales_daily", "mart_customer_rfm", "mart_cohort_retention",
             "mart_product_affinity", "mart_funnel_daily"],
    "ops": ["pipeline_runs", "watermarks", "dq_results", "quarantine"],
}


def is_databricks() -> bool:
    """True cuando el código corre dentro de un runtime de Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@dataclass(frozen=True)
class ScaleProfile:
    """Volúmenes base para scale_factor = 1.

    Con SF=1 se generan ~12M de pedidos, ~36M de líneas y ~150M de eventos web
    (≈ 25-30 GB en crudo). SF escala linealmente todo excepto tiendas.
    """

    customers: int = 1_000_000
    products: int = 25_000
    stores: int = 400
    orders: int = 12_000_000
    web_sessions: int = 25_000_000  # ~6 eventos por sesión => ~150M eventos
    inventory_products: int = 2_500  # SKUs con snapshot semanal de inventario
    customer_daily_change_rate: float = 0.001  # 0,1% de clientes cambia cada día
    product_daily_change_rate: float = 0.002


@dataclass(frozen=True)
class ProjectConfig:
    catalog: str = "novamart"
    schema_prefix: str = ""  # en local: "novamart_" para no chocar con otras bases
    volume: str = "raw"
    base_path: str | None = None  # override local (por defecto /Volumes/...)
    scale_factor: float = 1.0
    start_date: date = date(2023, 1, 1)
    end_date: date = date(2025, 12, 31)
    seed: int = 42
    profile: ScaleProfile = field(default_factory=ScaleProfile)

    # ------------------------------------------------------------------ nombres
    def schema(self, layer: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"Capa desconocida: {layer}. Válidas: {LAYERS}")
        return f"{self.catalog}.{self.schema_prefix}{layer}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"

    # ------------------------------------------------------------------- rutas
    @property
    def root_path(self) -> str:
        if self.base_path:
            return self.base_path.rstrip("/")
        return f"/Volumes/{self.catalog}/{self.schema_prefix}landing/{self.volume}"

    def landing_path(self, source: str) -> str:
        return f"{self.root_path}/landing/{source}"

    def checkpoint_path(self, name: str) -> str:
        return f"{self.root_path}/_checkpoints/{name}"

    def schema_location(self, source: str) -> str:
        return f"{self.root_path}/_schemas/{source}"

    # ------------------------------------------------------------------ volumen
    def scaled(self, base: int, minimum: int = 1) -> int:
        return max(minimum, int(round(base * self.scale_factor)))

    @property
    def n_customers(self) -> int:
        return self.scaled(self.profile.customers, 100)

    @property
    def n_products(self) -> int:
        return self.scaled(self.profile.products, 50)

    @property
    def n_stores(self) -> int:
        # Las tiendas físicas no escalan linealmente: una cadena real no pasa de
        # 400 a 4.000 tiendas por tener más clientes.
        return max(10, min(self.profile.stores, int(self.profile.stores * max(self.scale_factor, 0.05))))

    @property
    def n_orders(self) -> int:
        return self.scaled(self.profile.orders, 500)

    @property
    def n_web_sessions(self) -> int:
        return self.scaled(self.profile.web_sessions, 500)

    @property
    def n_inventory_products(self) -> int:
        return min(self.n_products, self.scaled(self.profile.inventory_products, 20))

    @property
    def n_days(self) -> int:
        return (self.end_date - self.start_date).days + 1


def local_config(base_path: str, scale_factor: float = 0.001, **kwargs) -> ProjectConfig:
    """Config para tests / ejecución local con Spark + Delta OSS."""
    return ProjectConfig(
        catalog="spark_catalog",
        schema_prefix="novamart_",
        base_path=base_path,
        scale_factor=scale_factor,
        **kwargs,
    )


def config_from_widgets(dbutils) -> ProjectConfig:  # pragma: no cover - sólo Databricks
    """Construye la config a partir de los widgets/parámetros del job."""

    def get(name: str, default: str) -> str:
        try:
            value = dbutils.widgets.get(name)
            return value if value != "" else default
        except Exception:
            return default

    return ProjectConfig(
        catalog=get("catalog", "novamart"),
        scale_factor=float(get("scale_factor", "1")),
        start_date=date.fromisoformat(get("start_date", "2023-01-01")),
        end_date=date.fromisoformat(get("end_date", "2025-12-31")),
    )
