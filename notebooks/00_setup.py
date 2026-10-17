# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: catálogo, esquemas y volumen
# MAGIC
# MAGIC > **¿Para qué existe este paso?** Crear el "archivero" (catálogo, esquemas por capa, volumen de llegada y tablas de auditoría) antes de que llegue cualquier dato.
# MAGIC >
# MAGIC > **¿Qué pasaría si no existiera?** Las tareas paralelas de silver intentarían crear las mismas tablas de `ops` a la vez y una fallaría (condición de carrera).
# MAGIC >
# MAGIC > Explicación completa: [`docs/12_por_que_asi.md`](../docs/12_por_que_asi.md#codigo) · glosario: [`docs/13_glosario.md`](../docs/13_glosario.md)
# MAGIC
# MAGIC Crea la estructura de **Unity Catalog** del proyecto:
# MAGIC
# MAGIC | Objeto | Nombre | Para qué |
# MAGIC |---|---|---|
# MAGIC | Catálogo | `novamart` | Aísla el proyecto (en dev: `novamart_dev`) |
# MAGIC | Esquema | `landing` + volumen `raw` | Archivos crudos que llegan de los sistemas origen |
# MAGIC | Esquema | `bronze` | Copia fiel e incremental de las fuentes |
# MAGIC | Esquema | `silver` | Datos limpios, tipados e historizados |
# MAGIC | Esquema | `gold` | Modelo estrella + marts |
# MAGIC | Esquema | `ops` | Auditoría, watermarks, calidad y cuarentena |
# MAGIC
# MAGIC > **Free Edition:** si no tenés permiso para crear catálogos, usá `catalog = workspace`.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from novamart.setup import create_namespace

create_namespace(spark, cfg)
display(spark.sql(f"SHOW SCHEMAS IN {cfg.catalog}"))
