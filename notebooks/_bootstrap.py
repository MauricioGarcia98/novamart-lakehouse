# Databricks notebook source
# MAGIC %md
# MAGIC ### Bootstrap común
# MAGIC Se ejecuta con `%run ./_bootstrap` desde cada notebook. Hace dos cosas:
# MAGIC 1. Agrega `../src` al `sys.path` para importar el paquete `novamart` (código versionado y testeado).
# MAGIC 2. Declara los widgets/parámetros comunes y construye `cfg` (la configuración del proyecto).
# MAGIC
# MAGIC Cuando el notebook corre dentro de un Job, los *job parameters* llenan estos widgets automáticamente.

# COMMAND ----------

import os
import sys

_src = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if _src not in sys.path:
    sys.path.insert(0, _src)

dbutils.widgets.text("catalog", "novamart_dev", "Catálogo UC")
dbutils.widgets.text("scale_factor", "0.1", "Scale factor")
dbutils.widgets.text("start_date", "2023-01-01", "Fecha inicio")
dbutils.widgets.text("end_date", "2025-12-31", "Fecha fin")

from novamart.config import config_from_widgets  # noqa: E402

cfg = config_from_widgets(dbutils)
print(f"catalog={cfg.catalog} | scale_factor={cfg.scale_factor} | rango={cfg.start_date}..{cfg.end_date}")
print(f"landing={cfg.root_path}")
