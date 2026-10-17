"""Orquestación end-to-end (lo mismo que hace el Job de Databricks, en un solo proceso).

Útil para correr todo localmente o desde un único notebook. En producción cada
paso es una *task* separada del Job (ver resources/novamart_job.yml) para poder
paralelizar, reintentar y monitorear por separado.
"""

from __future__ import annotations

import logging
import time
from datetime import date

from pyspark.sql import SparkSession

from novamart.config import ProjectConfig
from novamart.datagen.writer import generate_all
from novamart.gold.dimensions import load_dimensions
from novamart.gold.facts import load_facts
from novamart.gold.marts import build_marts
from novamart.ingestion.bronze import ingest_all
from novamart.setup import create_namespace
from novamart.silver.load import load_all
from novamart.utils.audit import audited

log = logging.getLogger("novamart")


def run_e2e(
    spark: SparkSession,
    cfg: ProjectConfig,
    generate: bool = True,
    gen_start: date | None = None,
    gen_end: date | None = None,
    gen_mode: str = "full",
) -> dict[str, float]:
    timings: dict[str, float] = {}

    def step(name: str, target: str, fn):
        t0 = time.time()
        with audited(cfg, spark, name, target) as res:
            res["rows_written"] = None
            fn()
        timings[name] = round(time.time() - t0, 1)
        log.info("%-10s %6.1fs", name, timings[name])

    create_namespace(spark, cfg)
    if generate:
        step("generate", "landing", lambda: generate_all(spark, cfg, gen_start, gen_end, gen_mode))
    step("bronze", "bronze.*", lambda: ingest_all(spark, cfg))
    step("silver", "silver.*", lambda: load_all(spark, cfg))
    step("gold_dims", "gold.dim_*", lambda: load_dimensions(spark, cfg))
    step("gold_facts", "gold.fact_*", lambda: load_facts(spark, cfg))
    step("gold_marts", "gold.mart_*", lambda: build_marts(spark, cfg))
    return timings
