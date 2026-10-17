from datetime import datetime
from decimal import Decimal

from novamart.quality.expectations import Rule, split_valid_invalid
from novamart.silver import transforms as T


def test_parse_timestamp_accepts_mixed_formats(spark):
    df = spark.createDataFrame(
        [("2025-03-01T10:00:00.000Z",), ("2025-03-01 10:00:00",), ("01/03/2025",)], "ts string"
    )
    out = [r.parsed for r in df.select(T.parse_timestamp("ts").alias("parsed")).collect()]
    assert out[0] == out[1] == datetime(2025, 3, 1, 10, 0, 0)
    assert out[2] is None  # formato desconocido -> NULL (no rompe el job)


def test_clean_customers_normalizes_fields(spark):
    row = {
        "customer_id": "7", "first_name": "  maría   josé ", "last_name": "pérez", "email": "  MARIA.P7@GMAIL.COM ",
        "phone": "(011) 4567-89", "birth_date": "31/12/1990", "gender": "f", "city": "Rosario", "state": "Santa Fe",
        "country": "ar", "segment": None, "marketing_opt_in": "true", "updated_at": "2025-01-02 10:00:00",
        "op": "UPDATE", "_ingested_at": datetime(2025, 1, 2),
    }
    schema = ", ".join(f"{k} {'timestamp' if k == '_ingested_at' else 'string'}" for k in row)
    out = T.clean_customers(spark.createDataFrame([row], schema)).collect()[0]
    assert out.first_name == "María José"
    assert out.email == "maria.p7@gmail.com"
    assert out.phone == "011456789"
    assert str(out.birth_date) == "1990-12-31"
    assert out.country == "AR" and out.gender == "F"
    assert out.segment == "Sin segmento"
    assert out.loyalty_tier is None  # columna que todavía no existía (schema drift)
    assert out.is_deleted is False


def test_latest_per_key_keeps_first_arrival(spark):
    df = spark.createDataFrame([(1, 2), (1, 1), (2, 5)], "k int, ts int")
    out = {r.k: r.ts for r in T.latest_per_key(df, ["k"], "ts", ascending=True).collect()}
    assert out == {1: 1, 2: 5}


def test_explode_order_lines_uses_decimal_math(spark):
    orders = spark.createDataFrame(
        [(1, datetime(2025, 1, 1), datetime(2025, 1, 1).date(), "WEB",
          [(1, 10, 3, 19.99, 0.1)], datetime(2025, 1, 1))],
        "order_id long, order_ts timestamp, order_date date, channel string, "
        "items array<struct<line_id:int,product_id:long,quantity:int,unit_price:double,discount_pct:double>>, "
        "_ingested_at timestamp",
    )
    line = T.explode_order_lines(orders).collect()[0]
    assert line.gross_amount == Decimal("59.97")
    assert line.discount_amount == Decimal("6.00")
    assert line.net_amount == Decimal("53.97")


def test_quality_rules_treat_null_as_failure(spark):
    df = spark.createDataFrame([(1, 5), (2, None), (3, -1)], "id int, qty int")
    rules = [Rule("qty_positive", "qty > 0", "quarantine")]
    valid, invalid = split_valid_invalid(df, rules)
    assert [r.id for r in valid.collect()] == [1]
    assert sorted(r.id for r in invalid.collect()) == [2, 3]
