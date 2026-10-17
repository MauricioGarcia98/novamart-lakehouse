"""Creación de catálogo, esquemas (capas) y volumen de aterrizaje."""

from __future__ import annotations

from pyspark.sql import SparkSession

from novamart.config import LAYERS, ProjectConfig, is_databricks

LAYER_COMMENTS = {
    "landing": "Zona de aterrizaje: archivos crudos (CSV/JSON/Parquet) en un UC Volume",
    "bronze": "Copia fiel e incremental de las fuentes + linaje. Sin limpieza.",
    "silver": "Datos limpios, tipados, deduplicados e historizados (SCD2). Fuente de verdad.",
    "gold": "Modelo dimensional (Kimball) y marts listos para BI/ML.",
    "ops": "Auditoría de ejecuciones, watermarks, métricas de calidad y cuarentena.",
}


def create_namespace(spark: SparkSession, cfg: ProjectConfig) -> None:
    if is_databricks():  # pragma: no cover
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog} COMMENT 'NovaMart Lakehouse'")
        except Exception as exc:  # sin permisos de metastore admin: usar un catálogo existente
            print(f"No se pudo crear el catálogo {cfg.catalog} ({exc}). Se asume que ya existe.")
    for layer in LAYERS:
        if is_databricks():  # pragma: no cover
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)} COMMENT '{LAYER_COMMENTS[layer]}'")
        else:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.schema(layer)}")
    ensure_ops_tables(spark, cfg)
    if is_databricks():  # pragma: no cover
        spark.sql(
            f"CREATE VOLUME IF NOT EXISTS {cfg.schema('landing')}.{cfg.volume} "
            "COMMENT 'Archivos crudos que llegan de los sistemas origen'"
        )


def ensure_ops_tables(spark: SparkSession, cfg: ProjectConfig) -> None:
    """Crea las tablas de `ops` antes de que las tasks paralelas escriban en ellas.

    Si dos tasks del for_each intentan crear la misma tabla a la vez, una falla
    con TABLE_ALREADY_EXISTS. Crearlas acá evita esa carrera.
    """
    from novamart.quality.expectations import DQ_RESULTS_SCHEMA
    from novamart.utils.audit import RUNS_SCHEMA, WATERMARK_SCHEMA

    quarantine = "table_name STRING, quarantined_at TIMESTAMP, failed_rules ARRAY<STRING>, record STRING"
    for name, schema in [("pipeline_runs", RUNS_SCHEMA), ("watermarks", WATERMARK_SCHEMA),
                         ("dq_results", DQ_RESULTS_SCHEMA), ("quarantine", quarantine)]:
        full = cfg.table("ops", name)
        if not spark.catalog.tableExists(full):
            spark.createDataFrame([], schema).write.format("delta").saveAsTable(full)
