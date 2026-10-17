"""Ejecuta los scripts SQL de `sql/` sustituyendo `${catalogo.esquema}`.

Los marts están escritos en SQL puro a propósito: es el lenguaje que usan los
analistas y Databricks SQL. Python sólo orquesta.
"""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import SparkSession

from novamart.config import LAYERS, ProjectConfig

SQL_DIR = Path(__file__).resolve().parents[3] / "sql"


def render(sql: str, cfg: ProjectConfig) -> str:
    for layer in LAYERS:
        sql = sql.replace("${" + layer + "}", cfg.schema(layer))
    return sql.replace("${catalog}", cfg.catalog)


def split_statements(sql: str) -> list[str]:
    """Separa por ';' de fin de sentencia ignorando comentarios de línea."""
    no_comments = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def run_sql_file(spark: SparkSession, cfg: ProjectConfig, path: Path) -> None:
    for stmt in split_statements(render(path.read_text(encoding="utf-8"), cfg)):
        spark.sql(stmt)


def build_marts(spark: SparkSession, cfg: ProjectConfig, sql_dir: Path | None = None) -> list[str]:
    folder = (sql_dir or SQL_DIR) / "gold" / "marts"
    done = []
    for path in sorted(folder.glob("*.sql")):
        run_sql_file(spark, cfg, path)
        done.append(path.stem)
    return done
