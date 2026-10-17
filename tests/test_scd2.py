from datetime import datetime

from novamart.silver.scd2 import scd2_merge

SCHEMA = "customer_id long, city string, updated_at timestamp, is_deleted boolean"


def _hist(spark, table):
    return [
        (r.customer_id, r.city, r.is_current, r.is_deleted)
        for r in spark.table(table).orderBy("customer_id", "valid_from").collect()
    ]


def test_scd2_full_lifecycle(spark, cfg):
    t = cfg.table("silver", "scd2_test")
    d = lambda day: datetime(2025, 1, day)  # noqa: E731

    # lote 1: dos cambios del mismo cliente en el mismo lote + un re-envío idéntico
    b1 = spark.createDataFrame(
        [(1, "Rosario", d(1), False), (1, "Rosario", d(2), False), (1, "Córdoba", d(5), False),
         (2, "Lima", d(1), False)], SCHEMA)
    scd2_merge(spark, b1, t, ["customer_id"], ["city"])
    assert _hist(spark, t) == [(1, "Rosario", False, False), (1, "Córdoba", True, False), (2, "Lima", True, False)]

    # lote 2: cambio real, cambio viejo (llega tarde, se ignora) y una baja
    b2 = spark.createDataFrame(
        [(1, "Mendoza", d(10), False), (1, "Salta", d(3), False), (2, "Lima", d(11), True)], SCHEMA)
    scd2_merge(spark, b2, t, ["customer_id"], ["city"])
    assert _hist(spark, t) == [
        (1, "Rosario", False, False), (1, "Córdoba", False, False), (1, "Mendoza", True, False),
        (2, "Lima", False, False), (2, "Lima", True, True),
    ]

    # idempotencia: el mismo lote otra vez no crea versiones
    scd2_merge(spark, b2, t, ["customer_id"], ["city"])
    assert spark.table(t).count() == 5
    # exactamente una versión vigente por cliente y rangos contiguos
    assert spark.table(t).where("is_current").groupBy("customer_id").count().where("count > 1").count() == 0
    closed = spark.table(t).where("customer_id = 1").orderBy("valid_from").collect()
    assert closed[0].valid_to == closed[1].valid_from and closed[1].valid_to == closed[2].valid_from
