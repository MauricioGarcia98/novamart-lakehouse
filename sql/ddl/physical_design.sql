-- =============================================================================
-- Diseño físico + gobierno (Databricks / Unity Catalog). Idempotente.
-- -----------------------------------------------------------------------------
-- 1. Liquid clustering   : organiza los archivos por las columnas más filtradas.
--                          Reemplaza PARTITIONED BY + ZORDER y se puede cambiar
--                          sin reescribir la tabla.
-- 2. PK / FK informativas: documentan el modelo estrella; con RELY el optimizador
--                          puede usarlas para eliminar joins innecesarios.
-- 3. CHECK constraints   : SÍ se validan en cada escritura (fallan si se violan).
-- 4. Comentarios y tags  : catálogo autodocumentado + clasificación de PII.
-- =============================================================================

-- ---------------------------------------------------------------- clustering
ALTER TABLE ${gold}.fact_sales               CLUSTER BY (order_date, store_key);
ALTER TABLE ${gold}.fact_order_fulfillment   CLUSTER BY (order_date);
ALTER TABLE ${gold}.fact_inventory_snapshot  CLUSTER BY (snapshot_date, store_key);
ALTER TABLE ${gold}.fact_web_sessions        CLUSTER BY (session_date);
ALTER TABLE ${gold}.fact_returns             CLUSTER BY (return_date);
ALTER TABLE ${gold}.dim_customer             CLUSTER BY (customer_id);
ALTER TABLE ${gold}.dim_product              CLUSTER BY (product_id);
ALTER TABLE ${silver}.web_events             CLUSTER BY (event_date, event_type);
ALTER TABLE ${silver}.order_lines            CLUSTER BY (order_date, product_id);

-- ------------------------------------------------- limpiar constraints previas
ALTER TABLE ${gold}.fact_sales DROP CONSTRAINT IF EXISTS fk_sales_customer;
ALTER TABLE ${gold}.fact_sales DROP CONSTRAINT IF EXISTS fk_sales_product;
ALTER TABLE ${gold}.fact_sales DROP CONSTRAINT IF EXISTS fk_sales_store;
ALTER TABLE ${gold}.fact_sales DROP CONSTRAINT IF EXISTS fk_sales_date;
ALTER TABLE ${gold}.fact_sales DROP CONSTRAINT IF EXISTS fk_sales_channel;
ALTER TABLE ${gold}.fact_sales DROP CONSTRAINT IF EXISTS pk_fact_sales;
ALTER TABLE ${gold}.dim_customer DROP CONSTRAINT IF EXISTS pk_dim_customer;
ALTER TABLE ${gold}.dim_product  DROP CONSTRAINT IF EXISTS pk_dim_product;
ALTER TABLE ${gold}.dim_store    DROP CONSTRAINT IF EXISTS pk_dim_store;
ALTER TABLE ${gold}.dim_date     DROP CONSTRAINT IF EXISTS pk_dim_date;
ALTER TABLE ${gold}.dim_channel  DROP CONSTRAINT IF EXISTS pk_dim_channel;

-- ------------------------------------------------------- claves primarias (PK)
ALTER TABLE ${gold}.dim_customer ALTER COLUMN customer_key SET NOT NULL;
ALTER TABLE ${gold}.dim_customer ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key) RELY;
ALTER TABLE ${gold}.dim_product  ALTER COLUMN product_key SET NOT NULL;
ALTER TABLE ${gold}.dim_product  ADD CONSTRAINT pk_dim_product PRIMARY KEY (product_key) RELY;
ALTER TABLE ${gold}.dim_store    ALTER COLUMN store_key SET NOT NULL;
ALTER TABLE ${gold}.dim_store    ADD CONSTRAINT pk_dim_store PRIMARY KEY (store_key) RELY;
ALTER TABLE ${gold}.dim_date     ALTER COLUMN date_key SET NOT NULL;
ALTER TABLE ${gold}.dim_date     ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_key) RELY;
ALTER TABLE ${gold}.dim_channel  ALTER COLUMN channel_key SET NOT NULL;
ALTER TABLE ${gold}.dim_channel  ADD CONSTRAINT pk_dim_channel PRIMARY KEY (channel_key) RELY;
ALTER TABLE ${gold}.fact_sales   ALTER COLUMN order_id SET NOT NULL;
ALTER TABLE ${gold}.fact_sales   ALTER COLUMN line_id SET NOT NULL;
ALTER TABLE ${gold}.fact_sales   ADD CONSTRAINT pk_fact_sales PRIMARY KEY (order_id, line_id);

-- ---------------------------------------------------- claves foráneas (FK)
ALTER TABLE ${gold}.fact_sales ADD CONSTRAINT fk_sales_customer FOREIGN KEY (customer_key) REFERENCES ${gold}.dim_customer (customer_key);
ALTER TABLE ${gold}.fact_sales ADD CONSTRAINT fk_sales_product  FOREIGN KEY (product_key)  REFERENCES ${gold}.dim_product (product_key);
ALTER TABLE ${gold}.fact_sales ADD CONSTRAINT fk_sales_store    FOREIGN KEY (store_key)    REFERENCES ${gold}.dim_store (store_key);
ALTER TABLE ${gold}.fact_sales ADD CONSTRAINT fk_sales_date     FOREIGN KEY (order_date_key) REFERENCES ${gold}.dim_date (date_key);
ALTER TABLE ${gold}.fact_sales ADD CONSTRAINT fk_sales_channel  FOREIGN KEY (channel_key)  REFERENCES ${gold}.dim_channel (channel_key);

-- ------------------------------------------------ CHECK constraints (enforced)
ALTER TABLE ${silver}.order_lines DROP CONSTRAINT IF EXISTS chk_quantity_positive;
ALTER TABLE ${silver}.order_lines ADD CONSTRAINT chk_quantity_positive CHECK (quantity > 0);
ALTER TABLE ${gold}.fact_sales DROP CONSTRAINT IF EXISTS chk_net_le_gross;
ALTER TABLE ${gold}.fact_sales ADD CONSTRAINT chk_net_le_gross CHECK (net_amount <= gross_amount);

-- ------------------------------------------------------------ documentación
COMMENT ON TABLE ${gold}.fact_sales IS 'Ventas. Grano: una fila por línea de pedido (order_id, line_id). Aditiva en todas las medidas.';
COMMENT ON TABLE ${gold}.fact_order_fulfillment IS 'Accumulating snapshot: una fila por pedido, se actualiza al llegar cada hito logístico.';
COMMENT ON TABLE ${gold}.fact_inventory_snapshot IS 'Periodic snapshot semanal. on_hand_qty es SEMI-aditiva: no sumar a través del tiempo.';
COMMENT ON TABLE ${gold}.dim_customer IS 'Clientes SCD Tipo 2. customer_key = xxhash64(customer_id, valid_from). -1 = no identificado.';
ALTER TABLE ${gold}.fact_sales ALTER COLUMN net_amount COMMENT 'gross_amount - discount_amount, en USD';
ALTER TABLE ${gold}.fact_sales ALTER COLUMN margin_amount COMMENT 'net_amount - cost_amount (costo de la versión del producto vigente al vender)';
ALTER TABLE ${gold}.fact_sales ALTER COLUMN order_id COMMENT 'Dimensión degenerada: número de pedido';
ALTER TABLE ${gold}.fact_inventory_snapshot ALTER COLUMN on_hand_qty COMMENT 'Semi-aditiva: sumar por tienda/producto, promediar o tomar último valor en el tiempo';

-- ---------------------------------------------------- gobierno: tags de PII
ALTER TABLE ${gold}.dim_customer SET TAGS ('domain' = 'customer', 'contains_pii' = 'true');
ALTER TABLE ${gold}.dim_customer ALTER COLUMN email SET TAGS ('pii' = 'email');
ALTER TABLE ${gold}.dim_customer ALTER COLUMN full_name SET TAGS ('pii' = 'name');
ALTER TABLE ${gold}.dim_customer ALTER COLUMN birth_date SET TAGS ('pii' = 'birth_date');
ALTER TABLE ${gold}.fact_sales SET TAGS ('domain' = 'sales', 'certified' = 'true');
