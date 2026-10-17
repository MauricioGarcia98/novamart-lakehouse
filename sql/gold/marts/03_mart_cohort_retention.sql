-- =============================================================================
-- mart_cohort_retention: retención mensual por cohorte de primera compra
-- -----------------------------------------------------------------------------
-- Cohorte = mes de la primera compra. Para cada cohorte, qué % de clientes
-- vuelve a comprar 1, 2, 3... meses después. Técnicas: MIN() OVER(PARTITION BY),
-- aritmética de fechas con MONTHS_BETWEEN y FIRST_VALUE para el tamaño base.
-- =============================================================================
CREATE OR REPLACE TABLE ${gold}.mart_cohort_retention
USING DELTA
COMMENT 'Matriz de retención mensual por cohorte de adquisición'
AS
WITH activity AS (
    SELECT DISTINCT
        c.customer_id,
        DATE_TRUNC('MONTH', f.order_date) AS activity_month
    FROM ${gold}.fact_sales f
    JOIN ${gold}.dim_customer c ON f.customer_key = c.customer_key
    WHERE f.customer_key <> -1
),
with_cohort AS (
    SELECT
        customer_id,
        activity_month,
        MIN(activity_month) OVER (PARTITION BY customer_id) AS cohort_month
    FROM activity
),
cohort_activity AS (
    SELECT
        cohort_month,
        CAST(MONTHS_BETWEEN(activity_month, cohort_month) AS INT) AS month_offset,
        COUNT(DISTINCT customer_id)                               AS active_customers
    FROM with_cohort
    GROUP BY cohort_month, CAST(MONTHS_BETWEEN(activity_month, cohort_month) AS INT)
)
SELECT
    CAST(cohort_month AS DATE) AS cohort_month,
    month_offset,
    active_customers,
    FIRST_VALUE(active_customers) OVER (PARTITION BY cohort_month ORDER BY month_offset) AS cohort_size,
    ROUND(active_customers / FIRST_VALUE(active_customers) OVER (PARTITION BY cohort_month ORDER BY month_offset), 4) AS retention_rate
FROM cohort_activity;
