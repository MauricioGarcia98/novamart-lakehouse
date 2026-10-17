-- =============================================================================
-- Lakeflow Declarative Pipelines · BRONZE
-- -----------------------------------------------------------------------------
-- Mismo resultado que src/novamart/ingestion/bronze.py, pero DECLARATIVO:
-- describimos QUÉ tabla queremos y el motor resuelve checkpoints, reintentos,
-- evolución de esquema, orden de ejecución y paralelismo.
--   STREAMING TABLE + STREAM read_files(...)  ==  Auto Loader
-- `${landing_path}` viene de la `configuration` del pipeline (ver resources/).
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Pedidos crudos (JSON anidado) tal como llegan del e-commerce y los POS'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
    *,
    _metadata.file_path         AS _source_file,
    current_timestamp()         AS _ingested_at
FROM STREAM read_files(
    '${landing_path}/orders',
    format => 'json',
    inferColumnTypes => true,
    partitionColumns => 'ingest_date'
);

CREATE OR REFRESH STREAMING TABLE bronze_order_status_events
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/order_status_events', format => 'json', partitionColumns => 'ingest_date');

CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT 'CDC de clientes (CSV). El contrato es explícito porque CSV no trae tipos.'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '${landing_path}/customers',
    format => 'csv',
    header => true,
    schema => 'customer_id STRING, first_name STRING, last_name STRING, email STRING, phone STRING,
               birth_date STRING, gender STRING, city STRING, state STRING, country STRING, segment STRING,
               marketing_opt_in STRING, updated_at STRING, op STRING, loyalty_tier STRING',
    partitionColumns => 'ingest_date',
    rescuedDataColumn => '_rescued_data'
);

CREATE OR REFRESH STREAMING TABLE bronze_products
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/products', format => 'json', inferColumnTypes => true,
                       partitionColumns => 'ingest_date');

CREATE OR REFRESH STREAMING TABLE bronze_stores
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/stores', format => 'csv', header => true, inferColumnTypes => true,
                       partitionColumns => 'ingest_date');

CREATE OR REFRESH STREAMING TABLE bronze_web_events
COMMENT 'Clickstream JSON.gz (~150M filas con SF=1). gzip no es splittable: 1 archivo = 1 tarea.'
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing_path}/web_events', format => 'json', inferColumnTypes => true,
                       partitionColumns => 'ingest_date');
