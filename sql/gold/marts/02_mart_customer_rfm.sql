-- =============================================================================
-- mart_customer_rfm: segmentación RFM (Recency, Frequency, Monetary)
-- -----------------------------------------------------------------------------
-- Técnicas SQL: CTEs encadenadas, NTILE() como función de ventana, CASE para
-- reglas de negocio y agregación sobre la versión ACTUAL del cliente (is_current).
-- Ojo: fact_sales apunta a la VERSIÓN del cliente vigente en cada compra, por
-- eso se agrupa por customer_id (clave natural) y no por customer_key.
-- =============================================================================
CREATE OR REPLACE TABLE ${gold}.mart_customer_rfm
USING DELTA
COMMENT 'Scoring RFM y segmento de valor por cliente'
AS
WITH ref AS (
    SELECT MAX(order_date) AS ref_date FROM ${gold}.fact_sales
),
customer_orders AS (
    SELECT
        c.customer_id,
        f.order_id,
        MAX(f.order_date)          AS order_date,
        SUM(f.net_amount)          AS order_amount
    FROM ${gold}.fact_sales f
    JOIN ${gold}.dim_customer c ON f.customer_key = c.customer_key
    WHERE f.customer_key <> -1
    GROUP BY c.customer_id, f.order_id
),
rfm_base AS (
    SELECT
        o.customer_id,
        DATEDIFF(r.ref_date, MAX(o.order_date)) AS recency_days,
        COUNT(*)                                AS frequency,
        SUM(o.order_amount)                     AS monetary,
        MIN(o.order_date)                       AS first_order_date,
        MAX(o.order_date)                       AS last_order_date
    FROM customer_orders o
    CROSS JOIN ref r
    GROUP BY o.customer_id, r.ref_date
),
scored AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,  -- más reciente = 5
        NTILE(5) OVER (ORDER BY frequency ASC)     AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)      AS m_score
    FROM rfm_base
)
SELECT
    s.*,
    CONCAT(r_score, f_score, m_score) AS rfm_code,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Campeones'
        WHEN r_score >= 3 AND f_score >= 3                  THEN 'Leales'
        WHEN r_score >= 4 AND f_score <= 2                  THEN 'Nuevos prometedores'
        WHEN r_score = 2 AND f_score >= 3                   THEN 'En riesgo'
        WHEN r_score = 1 AND f_score >= 4                   THEN 'No se pueden perder'
        WHEN r_score <= 2 AND f_score <= 2                  THEN 'Hibernando'
        ELSE 'Necesitan atención'
    END AS rfm_segment,
    c.segment       AS business_segment,
    c.loyalty_tier,
    c.country,
    c.age_band
FROM scored s
JOIN ${gold}.dim_customer c
  ON c.customer_id = s.customer_id AND c.is_current;
