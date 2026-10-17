"""Test de integración: corre el pipeline entero (landing -> gold) y valida invariantes."""

from dataclasses import replace
from datetime import date, timedelta

import pytest

from novamart.pipeline import run_e2e


@pytest.mark.integration
def test_pipeline_end_to_end_and_incremental(spark, cfg):
    cfg = replace(cfg, start_date=date(2025, 1, 1), end_date=date(2025, 1, 21))
    run_e2e(spark, cfg)

    t = cfg.table
    lines = spark.table(t("silver", "order_lines")).count()
    facts = spark.table(t("gold", "fact_sales")).count()
    assert lines > 0 and facts == lines, "fact_sales debe tener exactamente el grano de order_lines"

    # integridad referencial: toda FK del fact existe en su dimensión
    for fk, dim, pk in [("customer_key", "dim_customer", "customer_key"), ("product_key", "dim_product", "product_key"),
                        ("store_key", "dim_store", "store_key"), ("order_date_key", "dim_date", "date_key")]:
        orphans = spark.sql(
            f"SELECT COUNT(*) c FROM {t('gold', 'fact_sales')} f LEFT ANTI JOIN {t('gold', dim)} d ON f.{fk} = d.{pk}"
        ).collect()[0].c
        assert orphans == 0, f"{fk} huérfanas"

    # los duplicados inyectados no llegan a silver
    dup = spark.sql(f"SELECT order_id FROM {t('silver', 'orders')} GROUP BY order_id HAVING COUNT(*) > 1").count()
    assert dup == 0
    # los errores inyectados terminan en cuarentena
    assert spark.table(t("ops", "quarantine")).count() > 0

    # SCD2: una sola versión vigente por cliente
    multi = spark.sql(
        f"SELECT customer_id FROM {t('silver', 'customers_history')} WHERE is_current GROUP BY 1 HAVING COUNT(*) > 1"
    ).count()
    assert multi == 0

    # incremental: llega un día nuevo -> crece, y re-procesar es idempotente
    new_day = cfg.end_date + timedelta(days=1)
    run_e2e(spark, cfg, gen_start=new_day, gen_end=new_day, gen_mode="incremental")
    facts_2 = spark.table(t("gold", "fact_sales")).count()
    assert facts_2 > facts
    assert facts_2 == spark.table(t("silver", "order_lines")).count()
    run_e2e(spark, cfg, generate=False)
    assert spark.table(t("gold", "fact_sales")).count() == facts_2

    for mart in ("mart_sales_daily", "mart_customer_rfm", "mart_cohort_retention", "mart_funnel_daily"):
        assert spark.table(t("gold", mart)).count() > 0
