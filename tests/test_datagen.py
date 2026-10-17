from datetime import date

from pyspark.sql import functions as F

from novamart.datagen.calendar import black_friday, daily_plan, promo_name
from novamart.datagen.generators import generate_customers, generate_orders, generate_products
from novamart.datagen.rand import uniform, weighted_pick


def test_black_friday_is_day_after_thanksgiving():
    assert black_friday(2024) == date(2024, 11, 29)
    assert black_friday(2025) == date(2025, 11, 28)
    assert promo_name(date(2025, 12, 1)) == "CYBERMONDAY"


def test_daily_plan_distributes_total():
    plan = daily_plan(100_000, date(2024, 1, 1), date(2024, 12, 31), date(2024, 1, 1), date(2024, 12, 31))
    total = sum(r[2] for r in plan)
    assert abs(total - 100_000) / 100_000 < 0.01
    by_day = {r[0]: r[2] for r in plan}
    assert by_day[black_friday(2024)] > 2 * by_day[date(2024, 3, 12)]  # pico de Black Friday


def test_uniform_is_deterministic_and_in_range(spark):
    df = spark.range(10_000).select(uniform("id", salt="x").alias("u"))
    a = [r.u for r in df.collect()]
    b = [r.u for r in df.repartition(7).orderBy(F.monotonically_increasing_id()).collect()]
    assert sorted(a) == sorted(b)
    assert min(a) >= 0 and max(a) < 1
    assert 0.45 < sum(a) / len(a) < 0.55


def test_weighted_pick_respects_weights(spark):
    df = spark.range(20_000).select(weighted_pick(uniform("id", salt="w"), ["A", "B"], [9, 1]).alias("v"))
    share_a = df.where("v = 'A'").count() / 20_000
    assert 0.87 < share_a < 0.93


def test_orders_reference_existing_customers_and_products(spark, cfg):
    start, end = cfg.start_date, date(cfg.start_date.year, 1, 20)
    orders = generate_orders(spark, cfg, start, end)
    customers = generate_customers(spark, cfg, cfg.start_date, end).where("op = 'INSERT'")
    products = generate_products(spark, cfg, cfg.start_date, end).where("op = 'INSERT'")
    lines = orders.select(F.explode("items").alias("it")).select("it.product_id")
    orphan_products = lines.join(products, "product_id", "left_anti").count()
    orphan_customers = (
        orders.where("customer_id IS NOT NULL").select("customer_id")
        .join(customers.select(F.col("customer_id").cast("long")), "customer_id", "left_anti").count()
    )
    assert orphan_products == 0
    assert orphan_customers == 0


def test_orders_contain_injected_quality_issues(spark, cfg):
    orders = generate_orders(spark, cfg, cfg.start_date, cfg.end_date)
    dup_ids = orders.groupBy("order_id").count().where("count > 1").count()
    alt_format = orders.where(~F.col("order_ts").contains("T")).count()
    assert dup_ids > 0, "debería haber re-envíos duplicados"
    assert alt_format > 0, "debería haber fechas en formato alternativo"
