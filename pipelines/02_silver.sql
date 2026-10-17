-- =============================================================================
-- Lakeflow Declarative Pipelines · SILVER
-- -----------------------------------------------------------------------------
-- * EXPECTATIONS: reglas de calidad declarativas (WARN / DROP ROW / FAIL UPDATE).
--   Las métricas quedan en el event log del pipeline, sin código extra.
-- * AUTO CDC ... STORED AS SCD TYPE 2: el MERGE de 80 líneas de
--   src/novamart/silver/scd2.py en 10 líneas declarativas. Maneja eventos fuera
--   de orden con SEQUENCE BY y genera __START_AT / __END_AT.
-- =============================================================================

-- ------------------------------------------------------------------- clientes
CREATE TEMPORARY VIEW customers_clean (
    CONSTRAINT customer_id_not_null EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT updated_at_parseable EXPECT (updated_at IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT email_format EXPECT (op = 'DELETE' OR email RLIKE '^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$')
)
AS SELECT
    CAST(customer_id AS BIGINT)                                   AS customer_id,
    INITCAP(TRIM(REGEXP_REPLACE(first_name, '\\s+', ' ')))         AS first_name,
    INITCAP(TRIM(last_name))                                      AS last_name,
    LOWER(TRIM(email))                                            AS email,
    NULLIF(REGEXP_REPLACE(phone, '[^0-9]', ''), '')               AS phone,
    TO_DATE(TRY_TO_TIMESTAMP(birth_date, 'dd/MM/yyyy'))           AS birth_date,
    UPPER(gender)                                                 AS gender,
    city, state, UPPER(country)                                   AS country,
    COALESCE(segment, 'Sin segmento')                             AS segment,
    LOWER(marketing_opt_in) = 'true'                              AS marketing_opt_in,
    loyalty_tier,
    TRY_TO_TIMESTAMP(updated_at, 'yyyy-MM-dd HH:mm:ss')           AS updated_at,
    UPPER(op)                                                     AS op
FROM STREAM(bronze_customers);

CREATE OR REFRESH STREAMING TABLE silver_customers_history
COMMENT 'Clientes SCD Tipo 2 generada con AUTO CDC';

CREATE FLOW customers_scd2 AS AUTO CDC INTO silver_customers_history
FROM STREAM(customers_clean)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY updated_at
COLUMNS * EXCEPT (op)
STORED AS SCD TYPE 2;

-- ------------------------------------------------------------------ productos
CREATE OR REFRESH STREAMING TABLE silver_products_history;

CREATE FLOW products_scd2 AS AUTO CDC INTO silver_products_history
FROM (
    SELECT
        CAST(product_id AS BIGINT) AS product_id, sku, product_name, brand, department, category, subcategory,
        attributes.color AS color, CAST(list_price AS DECIMAL(14,2)) AS list_price,
        CAST(unit_cost AS DECIMAL(14,2)) AS unit_cost, is_active,
        TRY_TO_TIMESTAMP(updated_at, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS updated_at
    FROM STREAM(bronze_products)
)
KEYS (product_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 2
TRACK HISTORY ON list_price, unit_cost, is_active, category, subcategory, brand;

-- -------------------------------------------------------------------- pedidos
CREATE TEMPORARY VIEW orders_clean (
    CONSTRAINT order_ts_parseable  EXPECT (order_ts IS NOT NULL)                         ON VIOLATION DROP ROW,
    CONSTRAINT channel_valid       EXPECT (channel IN ('WEB','APP','STORE','MARKETPLACE')) ON VIOLATION DROP ROW,
    CONSTRAINT online_has_customer EXPECT (channel = 'STORE' OR customer_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT has_items           EXPECT (SIZE(items) > 0)                              ON VIOLATION DROP ROW,
    CONSTRAINT not_in_future       EXPECT (order_date <= current_date())
)
AS SELECT
    CAST(order_id AS BIGINT)                                               AS order_id,
    COALESCE(TRY_TO_TIMESTAMP(order_ts, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
             TRY_TO_TIMESTAMP(order_ts, 'yyyy-MM-dd HH:mm:ss'))             AS order_ts,
    TO_DATE(COALESCE(TRY_TO_TIMESTAMP(order_ts, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                     TRY_TO_TIMESTAMP(order_ts, 'yyyy-MM-dd HH:mm:ss')))    AS order_date,
    CAST(customer_id AS BIGINT) AS customer_id,
    UPPER(channel)              AS channel,
    CAST(store_id AS BIGINT)    AS store_id,
    promo_code,
    payment.method              AS payment_method,
    payment.installments        AS installments,
    shipping.method             AS shipping_method,
    CAST(shipping.cost AS DECIMAL(14,2)) AS shipping_cost,
    items,
    _ingested_at
FROM STREAM(bronze_orders);

-- AUTO CDC tipo 1 = deduplicación por clave: los re-envíos no generan filas nuevas
CREATE OR REFRESH STREAMING TABLE silver_orders;

CREATE FLOW orders_dedup AS AUTO CDC INTO silver_orders
FROM STREAM(orders_clean)
KEYS (order_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (items)
STORED AS SCD TYPE 1;

CREATE TEMPORARY VIEW order_lines_clean (
    CONSTRAINT quantity_positive EXPECT (quantity > 0) ON VIOLATION DROP ROW
)
AS SELECT
    o.order_id, it.line_id, o.order_ts, o.order_date, it.product_id,
    CAST(it.quantity AS INT)                                       AS quantity,
    CAST(it.unit_price AS DECIMAL(14,2))                           AS unit_price,
    CAST(it.discount_pct AS DECIMAL(5,4))                          AS discount_pct,
    CAST(it.quantity * it.unit_price AS DECIMAL(14,2))             AS gross_amount,
    CAST(ROUND(it.quantity * it.unit_price * it.discount_pct, 2) AS DECIMAL(14,2)) AS discount_amount,
    CAST(it.quantity * it.unit_price - ROUND(it.quantity * it.unit_price * it.discount_pct, 2) AS DECIMAL(14,2)) AS net_amount,
    o._ingested_at
FROM STREAM(orders_clean) o
LATERAL VIEW EXPLODE(o.items) AS it;

CREATE OR REFRESH STREAMING TABLE silver_order_lines;

CREATE FLOW order_lines_dedup AS AUTO CDC INTO silver_order_lines
FROM STREAM(order_lines_clean)
KEYS (order_id, line_id)
SEQUENCE BY _ingested_at
STORED AS SCD TYPE 1;

-- ---------------------------------------------------------------- clickstream
CREATE OR REFRESH STREAMING TABLE silver_web_events (
    CONSTRAINT valid_event_type EXPECT (event_type IN
        ('page_view','search','product_view','add_to_cart','begin_checkout','purchase')) ON VIOLATION DROP ROW
)
CLUSTER BY (event_date, event_type)
AS SELECT
    event_id, session_id, anonymous_id, CAST(customer_id AS BIGINT) AS customer_id, event_type,
    TIMESTAMP_MILLIS(CAST(event_ts AS BIGINT))            AS event_ts,
    TO_DATE(TIMESTAMP_MILLIS(CAST(event_ts AS BIGINT)))   AS event_date,
    CAST(product_id AS BIGINT) AS product_id, search_query,
    device.platform, device.device_type, traffic.source AS utm_source, traffic.medium AS utm_medium,
    geo.country AS geo_country, experiment_id
FROM STREAM(bronze_web_events);
