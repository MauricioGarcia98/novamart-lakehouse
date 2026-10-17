# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 00 · Bienvenida: Databricks, Unity Catalog y el proyecto NovaMart
# MAGIC
# MAGIC **Objetivo:** entender dónde estás parado antes de escribir una sola consulta.
# MAGIC
# MAGIC ## 1. ¿Qué es Databricks?
# MAGIC Una plataforma *lakehouse*: los datos viven en archivos abiertos (Delta/Parquet) en almacenamiento
# MAGIC de objetos (S3/ADLS/GCS) y se procesan con **Apache Spark** y **Databricks SQL**. Combina lo mejor de:
# MAGIC
# MAGIC | Data Lake | Data Warehouse | **Lakehouse** |
# MAGIC |---|---|---|
# MAGIC | barato, cualquier formato | ACID, SQL rápido, gobierno | ambos: archivos abiertos + ACID + SQL + ML |
# MAGIC
# MAGIC ## 2. Jerarquía de Unity Catalog (3 niveles)
# MAGIC ```
# MAGIC metastore
# MAGIC  └── catálogo   (novamart)
# MAGIC       └── esquema    (bronze / silver / gold / ops / landing)
# MAGIC            ├── tabla      (gold.fact_sales)
# MAGIC            ├── vista      (gold.v_sales_enriched)
# MAGIC            ├── volumen    (landing.raw → archivos)
# MAGIC            └── función    (gold.mask_email)
# MAGIC ```
# MAGIC Toda tabla se nombra `catalogo.esquema.tabla`. Los permisos se heredan hacia abajo.
# MAGIC
# MAGIC ## 3. Cómputo
# MAGIC * **Serverless** (notebooks, jobs, SQL warehouses): arranca en segundos, no administrás clusters.
# MAGIC * **Clusters clásicos**: control total (tipo de máquina, cantidad de workers, configuraciones de Spark).
# MAGIC * **SQL Warehouse**: optimizado para SQL/BI (Photon).
# MAGIC
# MAGIC ## 4. El proyecto
# MAGIC NovaMart es un retailer omnicanal ficticio (6 países LatAm, ~400 tiendas, web, app y marketplace).
# MAGIC Antes de seguir, corré el Job `novamart-e2e-pipeline` (o los notebooks `notebooks/00..07`) con
# MAGIC `scale_factor = 0.1` para tener datos. Todos los módulos consultan esas tablas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Primeros comandos
# MAGIC Las celdas pueden ser Python (por defecto en este notebook), SQL (`%sql`), Markdown (`%md`) o shell (`%sh`).

# COMMAND ----------

dbutils.widgets.text("catalog", "novamart", "Catálogo")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG {catalog}")
display(spark.sql("SHOW SCHEMAS"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿Qué tablas hay en gold y de qué tamaño?
# MAGIC SHOW TABLES IN gold;

# COMMAND ----------

for t in ["fact_sales", "fact_web_sessions", "dim_customer", "dim_product"]:
    d = spark.sql(f"DESCRIBE DETAIL gold.{t}").collect()[0]
    print(f"{t:22s} archivos={d.numFiles:>6,}  tamaño={d.sizeInBytes/1e9:8.2f} GB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Archivos vs tablas
# MAGIC Los archivos crudos están en un **volumen**. Se pueden consultar con SQL directamente
# MAGIC (`SELECT * FROM json.\`ruta\``) o con `read_files()`.

# COMMAND ----------

display(dbutils.fs.ls(f"/Volumes/{catalog}/landing/raw/landing/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files('/Volumes/novamart/landing/raw/landing/stores', format => 'csv', header => true) LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Checklist del módulo
# MAGIC - [ ] Sé nombrar una tabla con sus 3 niveles.
# MAGIC - [ ] Sé la diferencia entre volumen (archivos) y tabla (Delta gestionada).
# MAGIC - [ ] Ejecuté el pipeline y veo las tablas de bronze, silver y gold.
# MAGIC
# MAGIC **Ejercicio:** usando `DESCRIBE DETAIL`, ¿cuál es la tabla más grande del proyecto? ¿Cuántos archivos tiene?
