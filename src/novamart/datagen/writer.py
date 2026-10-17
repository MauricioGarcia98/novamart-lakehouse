"""Escritura de las fuentes generadas en la zona de aterrizaje (landing).

Estructura en el UC Volume:

    /Volumes/novamart/landing/raw/landing/<fuente>/ingest_date=YYYY-MM-DD/part-*.{csv,json,json.gz,parquet}

Cada carpeta `ingest_date` representa "los archivos que llegaron ese día".
Auto Loader los descubre incrementalmente en la capa bronze.
"""

from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from novamart.config import ProjectConfig
from novamart.datagen.generators import SCHEMA_DRIFT, SOURCES

log = logging.getLogger(__name__)


def _write(df: DataFrame, path: str, fmt: str, options: dict, mode: str) -> None:
    writer = (
        df.write.format(fmt)
        .mode("overwrite" if mode in ("full", "incremental") else "append")
        .partitionBy("ingest_date")
        .option("maxRecordsPerFile", 1_000_000)
    )
    if mode == "incremental":
        # sólo reemplaza las carpetas ingest_date presentes en df: re-ejecutar un día es idempotente
        writer = writer.option("partitionOverwriteMode", "dynamic")
    for k, v in options.items():
        writer = writer.option(k, v)
    writer.save(path)


def write_source(
    spark: SparkSession, cfg: ProjectConfig, name: str, start: date, end: date, mode: str = "full"
) -> str:
    gen, fmt, options = SOURCES[name]
    df = gen(spark, cfg, start, end)
    if "ingest_date" not in df.columns:  # fuente sin datos en el rango (p.ej. stores incremental)
        return "sin datos"
    path = cfg.landing_path(name)
    drift = SCHEMA_DRIFT.get(name)
    if drift and fmt == "csv":
        # CSV no tiene esquema: la columna nueva aparece en el header recién desde la fecha de drift
        column, since = drift
        before = df.where(F.col("ingest_date") < F.lit(since)).drop(column)
        after = df.where(F.col("ingest_date") >= F.lit(since))
        _write(before, path, fmt, options, mode)
        _write(after, path, fmt, options, "incremental" if mode == "full" else mode)
    else:
        _write(df, path, fmt, options, mode)
    log.info("Fuente %s escrita en %s", name, path)
    return path


def generate_all(
    spark: SparkSession,
    cfg: ProjectConfig,
    start: date | None = None,
    end: date | None = None,
    mode: str = "full",
    sources: list[str] | None = None,
) -> dict[str, str]:
    """Genera todas las fuentes para el rango indicado.

    mode="full"        borra y regenera la historia completa.
    mode="incremental" agrega/reemplaza sólo los días del rango (simula "llegó un día nuevo").
    """
    start = start or cfg.start_date
    end = end or cfg.end_date
    result = {}
    for name in sources or list(SOURCES):
        result[name] = write_source(spark, cfg, name, start, end, mode)
    return result
