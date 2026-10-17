"""Capa BRONZE: ingesta incremental de archivos crudos a tablas Delta.

Principios de bronze:
  * "Lo que llegó, tal cual llegó": no se limpia ni se deduplica.
  * Se agregan columnas técnicas de linaje: archivo origen, fecha de ingesta.
  * Nunca se pierde un registro: lo que no encaja en el esquema va a `_rescued_data`.
  * Es incremental: cada ejecución procesa sólo archivos nuevos (checkpoint).

En Databricks se usa **Auto Loader** (`cloudFiles`), que descubre archivos
nuevos, infiere y evoluciona el esquema y escala a millones de archivos.
En local se usa el *file streaming source* de Spark OSS, que tiene la misma
semántica incremental (checkpoint + trigger availableNow) para poder testear.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from novamart.config import ProjectConfig, is_databricks

_S = T.StringType()

# Reinicios por evolución de esquema, uno por cada tanda de columnas nuevas.
_SCHEMA_EVOLUTION_RESTARTS = 3

# Contratos explícitos para CSV: en CSV el esquema es posicional y todo es texto.
# Los tipos reales se aplican en silver, donde se valida cada columna.
CSV_CONTRACTS = {
    "customers": [
        "customer_id", "first_name", "last_name", "email", "phone", "birth_date", "gender",
        "city", "state", "country", "segment", "marketing_opt_in", "updated_at", "op", "loyalty_tier",
    ],
    "stores": [
        "store_id", "store_name", "store_format", "city", "state", "country", "region",
        "latitude", "longitude", "sales_area_sqm", "opened_date", "manager_name",
    ],
    "returns": [
        "return_id", "order_id", "line_id", "product_id", "quantity", "reason", "refund_amount", "return_ts",
    ],
}

SOURCE_FORMATS = {
    "customers": "csv",
    "products": "json",
    "stores": "csv",
    "orders": "json",
    "order_status_events": "json",
    "returns": "csv",
    "web_events": "json",
    "inventory_snapshots": "parquet",
}


def csv_schema(name: str) -> T.StructType:
    fields = [T.StructField(c, _S) for c in CSV_CONTRACTS[name]]
    fields.append(T.StructField("ingest_date", T.DateType()))  # columna de partición (carpeta)
    return T.StructType(fields)


def csv_schema_hints(name: str) -> str:
    """El mismo contrato que `csv_schema`, en formato DDL para `cloudFiles.schemaHints`."""
    return ", ".join([f"`{c}` STRING" for c in CSV_CONTRACTS[name]] + ["ingest_date DATE"])


def read_landing_stream(spark: SparkSession, cfg: ProjectConfig, name: str) -> DataFrame:
    fmt = SOURCE_FORMATS[name]
    path = cfg.landing_path(name)
    if is_databricks():  # pragma: no cover - Auto Loader sólo existe en Databricks
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.schemaLocation", cfg.schema_location(name))
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.partitionColumns", "ingest_date")
            .option("cloudFiles.maxFilesPerTrigger", 2000)
            .option("rescuedDataColumn", "_rescued_data")
        )
        if fmt == "csv":
            # Auto Loader no admite .schema() con addNewColumns: el contrato va como schemaHints
            reader = (
                reader.option("cloudFiles.inferColumnTypes", "false")
                .option("cloudFiles.schemaHints", csv_schema_hints(name))
            )
    else:
        reader = spark.readStream.format(fmt).option("maxFilesPerTrigger", 2000)
        if fmt == "json":
            reader = reader.option("columnNameOfCorruptRecord", "_rescued_data")
        if fmt == "csv":
            reader = reader.schema(csv_schema(name))
    if fmt == "csv":
        reader = reader.option("header", "true").option("mode", "PERMISSIVE")
    return reader.load(path)


def add_lineage(df: DataFrame) -> DataFrame:
    """Columnas técnicas de linaje: ¿de qué archivo vino y cuándo lo cargamos?"""
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modification_time", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def ingest_source(spark: SparkSession, cfg: ProjectConfig, name: str) -> int:
    """Ingesta incremental landing/<name> -> bronze.<name>. Devuelve filas nuevas."""
    target = cfg.table("bronze", name)
    before = spark.table(target).count() if spark.catalog.tableExists(target) else 0
    for attempt in range(_SCHEMA_EVOLUTION_RESTARTS + 1):
        query = (
            add_lineage(read_landing_stream(spark, cfg, name))
            .writeStream.format("delta")
            .option("checkpointLocation", cfg.checkpoint_path(f"bronze_{name}"))
            .option("mergeSchema", "true")  # acepta columnas nuevas (schema evolution)
            .trigger(availableNow=True)
            .toTable(target)
        )
        try:
            query.awaitTermination()
            break
        except Exception as exc:
            # Auto Loader (addNewColumns) detiene el stream al detectar columnas nuevas,
            # después de guardar el esquema ampliado: reiniciarlo retoma con ese esquema.
            if "UNKNOWN_FIELD_EXCEPTION" not in str(exc) or attempt == _SCHEMA_EVOLUTION_RESTARTS:
                raise
            print(f"{name}: columnas nuevas detectadas, reiniciando el stream ({attempt + 1})")
    return spark.table(target).count() - before


def ingest_all(spark: SparkSession, cfg: ProjectConfig, sources: list[str] | None = None) -> dict[str, int]:
    return {name: ingest_source(spark, cfg, name) for name in (sources or list(SOURCE_FORMATS))}
