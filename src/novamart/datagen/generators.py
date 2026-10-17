"""Generadores distribuidos de las 8 fuentes de NovaMart.

Todas las funciones devuelven DataFrames *lazy*: nada se calcula hasta que se
escriben. La cantidad de filas depende de `cfg.scale_factor`:

    fuente                 formato   SF=1 (aprox)    modelo de llegada
    ---------------------  --------  --------------  -----------------------------
    customers              CSV       1M + CDC diario snapshot inicial + cambios
    products               JSON      25k + CDC       snapshot inicial + cambios
    stores                 CSV       400             snapshot
    orders                 JSON      12M (36M líneas) diario, con duplicados/tardíos
    order_status_events    JSON      ~40M            diario (ciclo de vida)
    returns                CSV       ~1,8M           diario
    web_events             JSON.gz   ~150M           diario (clickstream)
    inventory_snapshots    Parquet   ~157M           semanal (snapshot periódico)

Problemas de calidad inyectados a propósito (para resolverlos en silver):
duplicados exactos, re-envíos, nulos, cantidades negativas, formatos de fecha
mezclados, emails con mayúsculas/espacios, llegadas tardías y cambios de esquema.
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from novamart.config import ProjectConfig
from novamart.datagen import reference as R
from novamart.datagen.calendar import SCHEMA_DRIFT_CUSTOMERS, SCHEMA_DRIFT_EVENTS, daily_plan
from novamart.datagen.rand import int_between, pick, uniform, uniform_sql, weighted_pick, zipf_index

ORDER_ID_MULTIPLIER = 100_000_000  # order_id = day_idx * 1e8 + secuencia del día

_CITY_NAMES = [c[0] for c in R.CITIES]
_CITY_WEIGHTS = [c[6] for c in R.CITIES]
_N_SUB = len(R.SUBCATEGORIES)


# ============================================================================ util
def _arr(values) -> Column:
    return F.array(*[F.lit(v) for v in values])


def _city_attr(city_idx: Column, pos: int) -> Column:
    return F.element_at(_arr([c[pos] for c in R.CITIES]), city_idx + 1)


def _weighted_index(u: Column, weights) -> Column:
    return weighted_pick(u, list(range(len(weights))), weights).cast("int")


def _days_df(spark: SparkSession, cfg: ProjectConfig, total: int, start: date, end: date) -> DataFrame:
    plan = daily_plan(total, cfg.start_date, cfg.end_date, max(start, cfg.start_date), end)
    return spark.createDataFrame(plan, "day date, day_idx int, n int, promo string")


def _active_upto(day_idx: Column, n: int, initial_share: float, n_days: int) -> Column:
    """Cuántas entidades existen al día `day_idx` (crecen linealmente en el rango).

    Los ids son crecientes en el tiempo: id <= initial_share*n existen desde el
    inicio y el resto se van dando de alta día a día. Así un pedido nunca
    referencia a un cliente/producto que todavía no existe.
    """
    frac = F.least(F.greatest(day_idx.cast("double") / F.lit(float(n_days)), F.lit(0.0)), F.lit(1.0))
    return F.floor(F.lit(initial_share * n) + F.lit((1 - initial_share) * n) * frac).cast("long")


def _partitions(n_rows: int, rows_per_partition: int = 400_000) -> int:
    return max(4, min(4000, n_rows // rows_per_partition + 1))


def _ts(day_col: Column, seconds: Column) -> Column:
    return F.timestamp_seconds(F.unix_timestamp(day_col.cast("timestamp")) + seconds.cast("long"))


# ======================================================================= productos
def product_base_price_sql(pid: str) -> str:
    """Precio de lista base (USD) determinístico por producto, en SQL.

    Se comparte entre el catálogo y los pedidos para que los precios sean
    coherentes sin necesidad de un join durante la generación.
    """
    mins = ", ".join(str(float(s[3])) for s in R.SUBCATEGORIES)
    maxs = ", ".join(str(float(s[4])) for s in R.SUBCATEGORIES)
    sub = f"(pmod(xxhash64({pid}, 'sub'), {_N_SUB}) + 1)"
    u = uniform_sql(pid, "price")
    return (
        f"round(element_at(array({mins}), cast({sub} as int)) + "
        f"(element_at(array({maxs}), cast({sub} as int)) - element_at(array({mins}), cast({sub} as int)))"
        f" * pow({u}, 2), 2)"
    )


def _inflation(day_idx: Column) -> Column:
    # ~0,6% mensual: los precios en USD suben con el tiempo
    return F.lit(1.0) + F.lit(0.006) * (day_idx.cast("double") / F.lit(30.0))


def _product_rows(pid: Column, version: Column, day_idx: Column) -> list[Column]:
    sub_idx = (F.pmod(F.xxhash64(pid, F.lit("sub")), F.lit(_N_SUB)) + 1).cast("int")
    dept = F.element_at(_arr([s[0] for s in R.SUBCATEGORIES]), sub_idx)
    cat = F.element_at(_arr([s[1] for s in R.SUBCATEGORIES]), sub_idx)
    sub = F.element_at(_arr([s[2] for s in R.SUBCATEGORIES]), sub_idx)
    margin = F.element_at(_arr([float(s[5]) for s in R.SUBCATEGORIES]), sub_idx)
    brand = pick(uniform(pid, salt="brand"), R.BRANDS)
    base_price = F.expr(product_base_price_sql("product_id"))
    # cada versión (cambio de precio) mueve el precio entre -5% y +12%
    drift = F.when(version == 0, F.lit(1.0)).otherwise(F.lit(0.95) + F.lit(0.17) * uniform(pid, version, salt="pd"))
    list_price = F.round(base_price * _inflation(day_idx) * drift, 2)
    own_brand_bonus = F.when(brand == "NovaMart Basics", F.lit(0.10)).otherwise(F.lit(0.0))
    cost = F.round(list_price * (F.lit(1.0) - margin - own_brand_bonus) * (F.lit(0.95) + F.lit(0.1) * uniform(pid, salt="cost")), 2)
    model = F.concat(F.upper(F.substring(F.sha2(pid.cast("string"), 256), 1, 3)), F.lit("-"),
                     (F.pmod(F.xxhash64(pid, F.lit("model")), F.lit(900)) + 100).cast("string"))
    return [
        pid.alias("product_id"),
        F.concat(F.lit("SKU-"), F.lpad(pid.cast("string"), 8, "0")).alias("sku"),
        F.concat_ws(" ", sub, brand, model).alias("product_name"),
        brand.alias("brand"),
        dept.alias("department"),
        cat.alias("category"),
        sub.alias("subcategory"),
        F.struct(
            pick(uniform(pid, salt="color"), R.COLORS).alias("color"),
            F.round(F.lit(0.1) + uniform(pid, salt="w") * F.lit(25.0), 2).alias("weight_kg"),
            F.element_at(_arr([0, 3, 6, 12, 24]), int_between(uniform(pid, salt="war"), 1, 5)).alias("warranty_months"),
        ).alias("attributes"),
        list_price.alias("list_price"),
        cost.alias("unit_cost"),
        F.lit("USD").alias("currency"),
    ]


def generate_products(spark: SparkSession, cfg: ProjectConfig, start: date, end: date) -> DataFrame:
    """Catálogo inicial + altas + cambios de precio/discontinuaciones (CDC)."""
    n = cfg.n_products
    frames = []
    if start <= cfg.start_date <= end:
        initial = int(n * 0.85)
        init = (
            spark.range(1, initial + 1).withColumnRenamed("id", "product_id")
            .withColumn("version", F.lit(0)).withColumn("day_idx", F.lit(0))
            .withColumn("day", F.lit(cfg.start_date))
            .withColumn("op", F.lit("INSERT"))
            .withColumn("secs", int_between(uniform("product_id", salt="pts"), 0, 3600))
        )
        frames.append(init)
    days = _days_df(spark, cfg, 1, start, end).select("day", "day_idx")
    active = _active_upto(F.col("day_idx"), n, 0.85, cfg.n_days)
    prev_active = _active_upto(F.col("day_idx") - 1, n, 0.85, cfg.n_days)
    launches = (
        days.where("day_idx > 0")
        .withColumn("product_id", F.explode(F.sequence(prev_active + 1, active)))
        .where(F.col("product_id") <= active)
        .withColumn("version", F.lit(0)).withColumn("op", F.lit("INSERT"))
        .withColumn("secs", int_between(uniform("product_id", salt="pts"), 28_800, 64_800))
    )
    changes_per_day = max(1, int(n * cfg.profile.product_daily_change_rate))
    changes = (
        days.where("day_idx > 0")
        .withColumn("seq", F.explode(F.sequence(F.lit(1), F.lit(changes_per_day))))
        .withColumn("product_id", F.floor(uniform("day_idx", "seq", salt="pc") * active).cast("long") + 1)
        .withColumn("version", F.col("day_idx"))
        .withColumn("op", F.lit("UPDATE"))
        .withColumn("secs", int_between(uniform("day_idx", "seq", salt="pcs"), 0, 86_399))
        .drop("seq")
        .dropDuplicates(["day", "product_id"])
    )
    frames += [launches, changes]
    base = frames[0].select("product_id", "version", "day_idx", "day", "op", "secs")
    for f in frames[1:]:
        base = base.unionByName(f.select("product_id", "version", "day_idx", "day", "op", "secs"))

    pid, version, day_idx = F.col("product_id"), F.col("version"), F.col("day_idx")
    discontinued = (F.col("op") == "UPDATE") & (uniform(pid, version, salt="disc") < 0.03)
    return (
        base.select(
            *_product_rows(pid, version, day_idx),
            (~discontinued).alias("is_active"),
            F.date_format(_ts(F.col("day"), F.col("secs")), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("updated_at"),
            F.col("op"),
            F.col("day").alias("ingest_date"),
        )
    )


# ======================================================================== clientes
def _customer_rows(cid: Column, version: Column) -> list[Column]:
    fn = pick(uniform(cid, salt="fn"), R.FIRST_NAMES)
    ln = pick(uniform(cid, salt="ln"), R.LAST_NAMES)
    city_idx = _weighted_index(uniform(cid, version, salt="city"), _CITY_WEIGHTS)
    domain = pick(uniform(cid, version, salt="dom"), R.EMAIL_DOMAINS)
    ascii_fn = F.translate(F.lower(fn), "áéíóúñü", "aeiounu")
    ascii_ln = F.translate(F.lower(ln), "áéíóúñü", "aeiounu")
    email = F.concat(ascii_fn, F.lit("."), ascii_ln, cid.cast("string"), F.lit("@"), domain)
    noise = uniform(cid, version, salt="noise")
    # Ruido: 2% de emails con mayúsculas y espacios, 1% de nombres con espacios extra
    email = F.when(noise < 0.02, F.concat(F.lit("  "), F.upper(email), F.lit(" "))).otherwise(email)
    fn = F.when((noise >= 0.02) & (noise < 0.03), F.concat(fn, F.lit("  "))).otherwise(fn)
    digits = F.lpad((F.pmod(F.xxhash64(cid, F.lit("ph")), F.lit(90_000_000)) + 10_000_000).cast("string"), 8, "0")
    phone_fmt = uniform(cid, salt="phf")
    phone = (
        F.when(phone_fmt < 0.4, F.concat(F.lit("+54 9 11 "), F.substring(digits, 1, 4), F.lit("-"), F.substring(digits, 5, 4)))
        .when(phone_fmt < 0.7, F.concat(F.lit("011"), digits))
        .when(phone_fmt < 0.95, F.concat(F.lit("("), F.substring(digits, 1, 3), F.lit(") "), F.substring(digits, 4, 5)))
        .otherwise(F.lit(None).cast("string"))
    )
    segment = F.when(uniform(cid, version, salt="segnull") < 0.01, F.lit(None).cast("string")).otherwise(
        weighted_pick(uniform(cid, version, salt="seg"), R.SEGMENTS, [40, 25, 20, 10, 5])
    )
    birth = F.date_add(F.lit(date(1950, 1, 1)), int_between(uniform(cid, salt="bd"), 0, 20_500))
    return [
        cid.alias("customer_id"),
        fn.alias("first_name"),
        ln.alias("last_name"),
        email.alias("email"),
        phone.alias("phone"),
        F.date_format(birth, "dd/MM/yyyy").alias("birth_date"),  # formato local, a propósito
        weighted_pick(uniform(cid, salt="gen"), ["F", "M", "X"], [51, 47, 2]).alias("gender"),
        _city_attr(city_idx, 0).alias("city"),
        _city_attr(city_idx, 1).alias("state"),
        _city_attr(city_idx, 2).alias("country"),
        segment.alias("segment"),
        (uniform(cid, version, salt="opt") < 0.62).alias("marketing_opt_in"),
    ]


def generate_customers(spark: SparkSession, cfg: ProjectConfig, start: date, end: date) -> DataFrame:
    """Snapshot inicial + CDC diario (INSERT altas, UPDATE cambios, DELETE bajas GDPR)."""
    n = cfg.n_customers
    cols = ["customer_id", "version", "day", "op", "secs"]
    frames = []
    if start <= cfg.start_date <= end:
        initial = int(n * 0.6)
        frames.append(
            spark.range(1, initial + 1).withColumnRenamed("id", "customer_id")
            .withColumn("version", F.lit(0)).withColumn("day", F.lit(cfg.start_date))
            .withColumn("op", F.lit("INSERT"))
            .withColumn("secs", int_between(uniform("customer_id", salt="cts"), 0, 3600))
            .select(*cols)
        )
    days = _days_df(spark, cfg, 1, start, end).select("day", "day_idx")
    active = _active_upto(F.col("day_idx"), n, 0.6, cfg.n_days)
    prev_active = _active_upto(F.col("day_idx") - 1, n, 0.6, cfg.n_days)
    frames.append(
        days.where("day_idx > 0")
        .withColumn("customer_id", F.explode(F.sequence(prev_active + 1, active)))
        .where(F.col("customer_id") <= active)
        .withColumn("version", F.lit(0)).withColumn("op", F.lit("INSERT"))
        .withColumn("secs", int_between(uniform("customer_id", salt="cts"), 0, 86_399))
        .select(*cols)
    )
    per_day = max(1, int(n * cfg.profile.customer_daily_change_rate))
    frames.append(
        days.where("day_idx > 0")
        .withColumn("seq", F.explode(F.sequence(F.lit(1), F.lit(per_day))))
        .withColumn("customer_id", F.floor(uniform("day_idx", "seq", salt="cc") * active).cast("long") + 1)
        .withColumn("version", F.col("day_idx"))
        .withColumn("op", F.when(uniform("day_idx", "seq", salt="del") < 0.02, F.lit("DELETE")).otherwise(F.lit("UPDATE")))
        .withColumn("secs", int_between(uniform("day_idx", "seq", salt="ccs"), 0, 86_399))
        .dropDuplicates(["day", "customer_id"])
        .select(*cols)
    )
    base = frames[0]
    for f in frames[1:]:
        base = base.unionByName(f)

    cid, version = F.col("customer_id"), F.col("version")
    df = base.select(
        *_customer_rows(cid, version),
        F.date_format(_ts(F.col("day"), F.col("secs")), "yyyy-MM-dd HH:mm:ss").alias("updated_at"),
        F.col("op"),
        # columna nueva (schema drift): va al final, como pasa en los CSV reales
        weighted_pick(uniform(cid, version, salt="tier"), ["BRONCE", "PLATA", "ORO", "PLATINO"], [55, 28, 13, 4]).alias("loyalty_tier"),
        F.col("day").alias("ingest_date"),
    )
    # 0,2% de filas exactamente duplicadas (re-envío del sistema origen)
    dups = df.where(uniform(F.col("customer_id"), F.col("updated_at"), salt="dup") < 0.002)
    return df.unionByName(dups)


# ========================================================================= tiendas
def generate_stores(spark: SparkSession, cfg: ProjectConfig, start: date, end: date) -> DataFrame:
    if not (start <= cfg.start_date <= end):
        return spark.createDataFrame([], "store_id long")  # sólo viaja en el snapshot inicial
    s = cfg.n_stores
    sid = F.col("store_id")
    city_idx = _weighted_index(uniform(sid, salt="scity"), _CITY_WEIGHTS)
    fmt_idx = _weighted_index(uniform(sid, salt="fmt"), [f[2] for f in R.STORE_FORMATS])
    fmt = F.element_at(_arr([f[0] for f in R.STORE_FORMATS]), fmt_idx + 1)
    sqm = F.element_at(_arr([f[1] for f in R.STORE_FORMATS]), fmt_idx + 1)
    opened = F.when(
        sid <= F.lit(int(s * 0.9)),
        F.date_sub(F.lit(cfg.start_date), int_between(uniform(sid, salt="open"), 30, 7000)),
    ).otherwise(
        F.date_add(F.lit(cfg.start_date), ((sid - F.lit(int(s * 0.9))) * F.lit(cfg.n_days) / F.lit(max(1, s - int(s * 0.9)))).cast("int"))
    )
    return (
        spark.range(1, s + 1).withColumnRenamed("id", "store_id")
        .select(
            sid,
            F.concat(F.lit("NovaMart "), fmt, F.lit(" "), _city_attr(city_idx, 0), F.lit(" #"), sid.cast("string")).alias("store_name"),
            fmt.alias("store_format"),
            _city_attr(city_idx, 0).alias("city"),
            _city_attr(city_idx, 1).alias("state"),
            _city_attr(city_idx, 2).alias("country"),
            _city_attr(city_idx, 3).alias("region"),
            F.round(_city_attr(city_idx, 4) + (uniform(sid, salt="lat") - 0.5) * 0.2, 5).alias("latitude"),
            F.round(_city_attr(city_idx, 5) + (uniform(sid, salt="lon") - 0.5) * 0.2, 5).alias("longitude"),
            (sqm * (F.lit(0.7) + F.lit(0.6) * uniform(sid, salt="sqm"))).cast("int").alias("sales_area_sqm"),
            opened.alias("opened_date"),
            F.concat_ws(" ", pick(uniform(sid, salt="mfn"), R.FIRST_NAMES), pick(uniform(sid, salt="mln"), R.LAST_NAMES)).alias("manager_name"),
            F.lit(cfg.start_date).alias("ingest_date"),
        )
    )


# ========================================================================= pedidos
def _orders_core(spark: SparkSession, cfg: ProjectConfig, start: date, end: date) -> DataFrame:
    """Pedidos "verdaderos" (sin ruido) + sus hitos de ciclo de vida.

    Es la base para orders, order_status_events y returns: como todo es
    determinístico, las tres fuentes son coherentes entre sí.
    """
    days = _days_df(spark, cfg, cfg.n_orders, start, end)
    total = sum(r[2] for r in daily_plan(cfg.n_orders, cfg.start_date, cfg.end_date, max(start, cfg.start_date), end))
    df = (
        days.withColumn("seq", F.explode(F.sequence(F.lit(1), F.col("n"))))
        .repartition(_partitions(total))
        .withColumn("order_id", F.col("day_idx").cast("long") * F.lit(ORDER_ID_MULTIPLIER) + F.col("seq"))
    )
    oid = F.col("order_id")
    channel = weighted_pick(uniform(oid, salt="ch"), [c[0] for c in R.CHANNELS], [c[1] for c in R.CHANNELS])
    df = df.withColumn("channel", channel)
    is_store = F.col("channel") == "STORE"
    # hora del día: tiendas 9-21h, online con pico nocturno
    store_hour = int_between(uniform(oid, salt="h"), 9, 21)
    online_hour = weighted_pick(uniform(oid, salt="h"), list(range(24)),
                                [1, .6, .4, .3, .3, .5, 1, 2, 3, 4, 4.5, 5, 5, 4.5, 4.5, 4.5, 5, 5.5, 6, 6.5, 7.5, 8, 7, 3])
    hour = F.when(is_store, store_hour).otherwise(online_hour)
    df = df.withColumn("order_ts", _ts(F.col("day"), hour * 3600 + int_between(uniform(oid, salt="sec"), 0, 3599)))

    n_cust, n_prod, n_stores = cfg.n_customers, cfg.n_products, cfg.n_stores
    max_cust = _active_upto(F.col("day_idx"), n_cust, 0.6, cfg.n_days)
    anonymous = is_store & (uniform(oid, salt="anon") < 0.35)  # compra en caja sin tarjeta de fidelidad
    df = df.withColumn(
        "customer_id",
        F.when(anonymous, F.lit(None).cast("long")).otherwise(zipf_index(uniform(oid, salt="cust"), max_cust, 2.2)),
    )
    max_store = _active_upto(F.col("day_idx"), n_stores, 0.9, cfg.n_days)
    df = df.withColumn("store_id", F.when(is_store, zipf_index(uniform(oid, salt="store"), max_store, 1.6)))
    # ciudad del cliente (versión 0) para envíos
    city_idx = _weighted_index(uniform(F.col("customer_id"), F.lit(0), salt="city"), _CITY_WEIGHTS)
    df = df.withColumn("ship_city_idx", F.when(F.col("customer_id").isNotNull(), city_idx).otherwise(F.lit(0)))

    # ---- líneas del pedido (array de structs) -------------------------------
    max_prod = _active_upto(F.col("day_idx"), n_prod, 0.85, cfg.n_days)
    df = df.withColumn("max_prod", max_prod)
    n_items = F.least(F.lit(12), F.lit(1) + F.floor(-F.log(F.lit(1.0) - uniform(oid, salt="ni") * F.lit(0.999)) / F.lit(0.55))).cast("int")
    df = df.withColumn(
        "product_ids",
        F.transform(F.sequence(F.lit(1), n_items),
                    lambda i: (F.floor(F.pow(uniform(oid, i, salt="prod"), F.lit(1.8)) * F.col("max_prod")) + 1).cast("long")),
    )
    promo = F.col("promo")
    has_code = F.when(promo.isNotNull(), uniform(oid, salt="pc") < 0.6).otherwise(uniform(oid, salt="pc") < 0.05)
    df = df.withColumn("promo_code", F.when(has_code, F.coalesce(promo, pick(uniform(oid, salt="pcn"), ["BIENVENIDA10", "APP15"]))))
    infl = _inflation(F.col("day_idx"))
    df = df.withColumn("infl", infl)
    price_sql = product_base_price_sql("p")
    u_q = uniform_sql("order_id, i, 'q'", "qty")
    u_d = uniform_sql("order_id, i, 'd'", "disc")
    u_bad = uniform_sql("order_id, i, 'b'", "bad")
    df = df.withColumn(
        "items",
        F.expr(f"""
            transform(product_ids, (p, i) -> named_struct(
                'line_id', i + 1,
                'product_id', p,
                'quantity', CASE WHEN {u_bad} < 0.0005 THEN -1
                                 ELSE cast(1 + floor(pow({u_q}, 3) * 6) as int) END,
                'unit_price', round({price_sql} * infl, 2),
                'discount_pct', CASE WHEN promo_code IS NOT NULL THEN round(0.10 + {u_d} * 0.25, 2)
                                     WHEN {u_d} < 0.15 THEN round(0.05 + {u_d}, 2)
                                     ELSE 0.0 END
            ))
        """),
    )
    # ---- pago y envío -------------------------------------------------------
    pay = weighted_pick(uniform(oid, salt="pay"), [p[0] for p in R.PAYMENT_METHODS], [p[1] for p in R.PAYMENT_METHODS])
    pay = F.when(~is_store & (pay == "CASH"), F.lit("WALLET")).otherwise(pay)
    df = df.withColumn("payment_method", pay)
    df = df.withColumn(
        "installments",
        F.when(pay == "CREDIT_CARD", F.element_at(_arr([1, 1, 3, 6, 12, 18]), int_between(uniform(oid, salt="inst"), 1, 6))).otherwise(F.lit(1)),
    )
    df = df.withColumn("card_brand", F.when(pay.isin("CREDIT_CARD", "DEBIT_CARD"), pick(uniform(oid, salt="cb"), R.CARD_BRANDS)))
    ship = weighted_pick(uniform(oid, salt="ship"), [s[0] for s in R.SHIPPING_METHODS], [s[1] for s in R.SHIPPING_METHODS])
    df = df.withColumn("shipping_method", F.when(is_store, F.lit(None).cast("string")).otherwise(ship))
    df = df.withColumn(
        "shipping_cost",
        F.when(is_store | (F.col("shipping_method") == "PICKUP_IN_STORE"), F.lit(0.0))
        .when(F.col("shipping_method") == "EXPRESS", F.round(F.lit(8.0) + uniform(oid, salt="sc") * 10, 2))
        .otherwise(F.round(F.lit(3.0) + uniform(oid, salt="sc") * 6, 2)),
    )
    # ---- ciclo de vida (hitos) ---------------------------------------------
    failed = ~is_store & (uniform(oid, salt="fail") < 0.04)
    paid = F.col("order_ts") + F.make_interval(mins=F.when(is_store, F.lit(1)).otherwise(int_between(uniform(oid, salt="pm"), 1, 45)))
    ship_h = F.when(F.col("shipping_method") == "EXPRESS", int_between(uniform(oid, salt="sh"), 2, 20)).otherwise(int_between(uniform(oid, salt="sh"), 12, 96))
    shipped = F.col("paid_ts") + F.make_interval(hours=ship_h)
    deliver_h = F.when(F.col("shipping_method") == "EXPRESS", int_between(uniform(oid, salt="dh"), 4, 30)).otherwise(int_between(uniform(oid, salt="dh"), 24, 168))
    df = (
        df.withColumn("is_failed", failed)
        .withColumn("paid_ts", F.when(~failed, paid))
        .withColumn("cancelled_ts", F.when(failed, F.col("order_ts") + F.make_interval(mins=F.lit(30))))
        .withColumn("shipped_ts", F.when(~failed & ~is_store, shipped))
        .withColumn(
            "delivered_ts",
            F.when(is_store & ~failed, F.col("paid_ts") + F.make_interval(mins=F.lit(1)))
            .when(~failed, F.col("shipped_ts") + F.make_interval(hours=deliver_h)),
        )
    )
    return df


def generate_orders(spark: SparkSession, cfg: ProjectConfig, start: date, end: date) -> DataFrame:
    """Pedidos en JSON anidado, tal como los publica el sistema transaccional."""
    core = _orders_core(spark, cfg, start, end)
    oid = F.col("order_id")
    # 3% con formato de fecha alternativo -> obliga a parsear con coalesce en silver
    alt_fmt = uniform(oid, salt="fmt") < 0.03
    order_ts_str = F.when(alt_fmt, F.date_format("order_ts", "yyyy-MM-dd HH:mm:ss")).otherwise(
        F.date_format("order_ts", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    # 0,1% de pedidos online sin cliente (error de origen)
    bad_cust = (F.col("channel") != "STORE") & (uniform(oid, salt="bc") < 0.001)
    # 3% llega con 1-3 días de atraso (late arriving data)
    lag = F.when(uniform(oid, salt="late") < 0.03, int_between(uniform(oid, salt="lag"), 1, 3)).otherwise(F.lit(0))
    df = core.select(
        oid,
        order_ts_str.alias("order_ts"),
        F.when(bad_cust, F.lit(None).cast("long")).otherwise(F.col("customer_id")).alias("customer_id"),
        F.col("channel"),
        F.col("store_id"),
        F.lit("USD").alias("currency"),
        F.col("promo_code"),
        F.struct(
            F.col("payment_method").alias("method"),
            F.col("installments"),
            F.col("card_brand"),
        ).alias("payment"),
        F.struct(
            F.col("shipping_method").alias("method"),
            F.col("shipping_cost").alias("cost"),
            _city_attr(F.col("ship_city_idx"), 0).alias("city"),
            _city_attr(F.col("ship_city_idx"), 2).alias("country"),
        ).alias("shipping"),
        F.col("items"),
        F.date_add(F.col("day"), lag).alias("ingest_date"),
    )
    # 0,3% re-enviados al día siguiente (duplicado lógico con el mismo order_id)
    resent = df.where(uniform(oid, salt="resend") < 0.003).withColumn("ingest_date", F.date_add("ingest_date", 1))
    return df.unionByName(resent)


def generate_order_status_events(spark: SparkSession, cfg: ProjectConfig, start: date, end: date) -> DataFrame:
    """Un evento por cada cambio de estado del pedido (fuente para el accumulating snapshot)."""
    # incluye pedidos de hasta 12 días antes para capturar entregas que caen en el rango
    core = _orders_core(spark, cfg, start - timedelta(days=12), end)
    pickup = F.col("shipping_method") == "PICKUP_IN_STORE"

    def ev(status: str, ts_col: str, cond: Column | None = None) -> Column:
        c = F.col(ts_col).isNotNull() if cond is None else (F.col(ts_col).isNotNull() & cond)
        return F.when(c, F.struct(F.lit(status).alias("status"), F.col(ts_col).alias("status_ts")))

    events = F.array_compact(F.array(
        ev("CREATED", "order_ts"),
        ev("PAYMENT_APPROVED", "paid_ts"),
        ev("CANCELLED", "cancelled_ts"),
        ev("SHIPPED", "shipped_ts", ~pickup),
        ev("READY_FOR_PICKUP", "shipped_ts", pickup),
        ev("DELIVERED", "delivered_ts"),
    ))
    df = (
        core.select("order_id", F.explode(events).alias("e"))
        .select(
            F.sha2(F.concat_ws("|", F.col("order_id").cast("string"), F.col("e.status")), 256).substr(1, 32).alias("event_id"),
            "order_id",
            F.col("e.status").alias("status"),
            F.date_format("e.status_ts", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("status_ts"),
            F.to_date("e.status_ts").alias("ingest_date"),
        )
        .where(F.col("ingest_date").between(F.lit(start), F.lit(end)))
    )
    return df


def generate_returns(spark: SparkSession, cfg: ProjectConfig, start: date, end: date) -> DataFrame:
    """Devoluciones a nivel de línea: 5% general, 12% en Moda (talles!)."""
    core = _orders_core(spark, cfg, start - timedelta(days=45), end).where("delivered_ts IS NOT NULL")
    lines = core.select("order_id", "delivered_ts", F.posexplode("items").alias("pos", "it"))
    sub_idx = (F.pmod(F.xxhash64(F.col("it.product_id"), F.lit("sub")), F.lit(_N_SUB)) + 1).cast("int")
    dept = F.element_at(_arr([s[0] for s in R.SUBCATEGORIES]), sub_idx)
    rate = F.when(dept == "Moda", F.lit(0.12)).when(dept == "Supermercado", F.lit(0.01)).otherwise(F.lit(0.05))
    u = uniform(F.col("order_id"), F.col("pos"), salt="ret")
    returned = lines.where((u < rate) & (F.col("it.quantity") > 0))
    rts = F.col("delivered_ts") + F.make_interval(days=int_between(uniform("order_id", "pos", salt="rd"), 1, 30),
                                                    hours=int_between(uniform("order_id", "pos", salt="rh"), 0, 23))
    qty = F.least(F.col("it.quantity"), int_between(uniform("order_id", "pos", salt="rq"), 1, 3))
    df = returned.select(
        F.concat(F.lit("RET-"), F.col("order_id").cast("string"), F.lit("-"), (F.col("pos") + 1).cast("string")).alias("return_id"),
        F.col("order_id"),
        (F.col("pos") + 1).alias("line_id"),
        F.col("it.product_id").alias("product_id"),
        qty.alias("quantity"),
        pick(uniform("order_id", "pos", salt="rr"), R.RETURN_REASONS).alias("reason"),
        F.round(qty * F.col("it.unit_price") * (F.lit(1) - F.col("it.discount_pct")), 2).alias("refund_amount"),
        F.date_format(rts, "yyyy-MM-dd HH:mm:ss").alias("return_ts"),
        F.to_date(rts).alias("ingest_date"),
    )
    return df.where(F.col("ingest_date").between(F.lit(start), F.lit(end)))


# ===================================================================== clickstream
def generate_web_events(spark: SparkSession, cfg: ProjectConfig, start: date, end: date) -> DataFrame:
    """Clickstream de web/app: sesiones con embudo home -> producto -> carrito -> compra."""
    days = _days_df(spark, cfg, cfg.n_web_sessions, start, end)
    total = sum(r[2] for r in daily_plan(cfg.n_web_sessions, cfg.start_date, cfg.end_date, max(start, cfg.start_date), end))
    s = (
        days.withColumn("seq", F.explode(F.sequence(F.lit(1), F.col("n"))))
        .repartition(_partitions(total, 200_000))
        .withColumn("sid", F.col("day_idx").cast("long") * F.lit(ORDER_ID_MULTIPLIER) + F.col("seq"))
    )
    sid = F.col("sid")
    max_cust = _active_upto(F.col("day_idx"), cfg.n_customers, 0.6, cfg.n_days)
    max_prod = _active_upto(F.col("day_idx"), cfg.n_products, 0.85, cfg.n_days)
    logged = uniform(sid, salt="log") < 0.55
    dev_idx = _weighted_index(uniform(sid, salt="dev"), [d[3] for d in R.DEVICES])
    src_idx = _weighted_index(uniform(sid, salt="src"), [t[2] for t in R.TRAFFIC_SOURCES])
    hour = weighted_pick(uniform(sid, salt="sh"), list(range(24)),
                         [1, .6, .4, .3, .3, .5, 1, 2, 3, 4, 4.5, 5, 5, 4.5, 4.5, 4.5, 5, 5.5, 6, 6.5, 7.5, 8, 7, 3])
    u_stage = uniform(sid, salt="stage")
    s = s.select(
        "day", "day_idx", "sid",
        F.when(logged, zipf_index(uniform(sid, salt="sc"), max_cust, 2.2)).alias("customer_id"),
        max_prod.alias("max_prod"),
        _ts(F.col("day"), hour * 3600 + int_between(uniform(sid, salt="ss"), 0, 3599)).alias("session_start"),
        # profundidad del embudo: 0=navega, 1=carrito, 2=checkout, 3=compra
        F.when(u_stage < 0.035, 3).when(u_stage < 0.09, 2).when(u_stage < 0.24, 1).otherwise(0).alias("stage"),
        F.least(F.lit(40), F.lit(2) + F.floor(-F.log(F.lit(1.0) - uniform(sid, salt="ne") * F.lit(0.999)) / F.lit(0.25))).cast("int").alias("n_ev"),
        F.element_at(_arr([d[0] for d in R.DEVICES]), dev_idx + 1).alias("device_type"),
        F.element_at(_arr([d[1] for d in R.DEVICES]), dev_idx + 1).alias("os"),
        F.element_at(_arr([d[2] for d in R.DEVICES]), dev_idx + 1).alias("browser"),
        F.element_at(_arr([t[0] for t in R.TRAFFIC_SOURCES]), src_idx + 1).alias("utm_source"),
        F.element_at(_arr([t[1] for t in R.TRAFFIC_SOURCES]), src_idx + 1).alias("utm_medium"),
        F.when(uniform(sid, salt="camp") < 0.3, F.coalesce(F.col("promo"), F.lit("always_on"))).alias("utm_campaign"),
        _city_attr(_weighted_index(uniform(sid, salt="gc"), _CITY_WEIGHTS), 0).alias("geo_city"),
        _city_attr(_weighted_index(uniform(sid, salt="gc"), _CITY_WEIGHTS), 2).alias("geo_country"),
        F.when(uniform(sid, salt="app") < 0.4, F.lit("APP")).otherwise(F.lit("WEB")).alias("platform"),
    )
    # n_ev mínimo según la etapa alcanzada
    s = s.withColumn("n_ev", F.greatest(F.col("n_ev"), F.col("stage") + F.lit(2)))
    u_t = uniform_sql("sid, i", "evt")
    u_g = uniform_sql("sid, i", "gap")
    u_pp = uniform_sql("sid, i", "pp")
    terms = ", ".join(f"'{t}'" for t in R.SEARCH_TERMS)
    events = F.expr(f"""
        transform(sequence(0, n_ev - 1), i -> named_struct(
            'i', i,
            'event_type', CASE
                WHEN i = 0 THEN 'page_view'
                WHEN stage >= 3 AND i = n_ev - 1 THEN 'purchase'
                WHEN stage >= 2 AND i = n_ev - 1 - (CASE WHEN stage >= 3 THEN 1 ELSE 0 END) THEN 'begin_checkout'
                WHEN stage >= 1 AND i = n_ev - 1 - (stage - 1) THEN 'add_to_cart'
                WHEN {u_t} < 0.18 THEN 'search'
                WHEN {u_t} < 0.24 THEN 'page_view'
                ELSE 'product_view' END,
            'offset_ms', cast(i * 35000 + floor({u_g} * 30000) as bigint),
            'product_id', cast(floor(pow({u_pp}, 1.8) * max_prod) + 1 as bigint),
            'search_query', element_at(array({terms}), cast(floor({u_t} * {len(R.SEARCH_TERMS)}) + 1 as int))
        ))
    """)
    e = s.select("*", F.explode(events).alias("ev"))
    et = F.col("ev.event_type")
    ts_ms = F.unix_millis(F.col("session_start")) + F.col("ev.offset_ms")
    event_date = F.to_date(F.timestamp_millis(ts_ms))
    df = e.select(
        F.concat(F.lit("ev-"), F.col("sid").cast("string"), F.lit("-"), F.col("ev.i").cast("string")).alias("event_id"),
        F.concat(F.lit("s-"), F.col("sid").cast("string")).alias("session_id"),
        F.concat(F.lit("anon-"), F.substring(F.sha2(F.coalesce(F.col("customer_id").cast("string"), F.col("sid").cast("string")), 256), 1, 16)).alias("anonymous_id"),
        F.col("customer_id"),
        et.alias("event_type"),
        ts_ms.alias("event_ts"),  # epoch millis: OTRO formato de fecha distinto al de orders
        F.when(et.isin("product_view", "add_to_cart"), F.col("ev.product_id")).alias("product_id"),
        F.when(et == "search", F.col("ev.search_query")).alias("search_query"),
        F.when(et == "page_view", F.lit("/")).when(et == "product_view", F.concat(F.lit("/p/"), F.col("ev.product_id").cast("string")))
         .when(et == "search", F.lit("/search")).when(et == "add_to_cart", F.lit("/cart"))
         .otherwise(F.lit("/checkout")).alias("page_path"),
        F.struct("platform", "device_type", "os", "browser").alias("device"),
        F.struct(F.col("utm_source").alias("source"), F.col("utm_medium").alias("medium"), F.col("utm_campaign").alias("campaign")).alias("traffic"),
        F.struct(F.col("geo_city").alias("city"), F.col("geo_country").alias("country")).alias("geo"),
        # schema drift: desde 2025-03-01 se lanza un framework de A/B testing
        F.when(event_date >= F.lit(SCHEMA_DRIFT_EVENTS),
               F.concat(F.lit("exp-checkout-v"), (F.pmod(F.col("sid"), F.lit(2)) + 1).cast("string"))).alias("experiment_id"),
        event_date.alias("ingest_date"),
    )
    return df.where(F.col("ingest_date").between(F.lit(start), F.lit(end)))


# ====================================================================== inventario
def generate_inventory(spark: SparkSession, cfg: ProjectConfig, start: date, end: date) -> DataFrame:
    """Snapshot semanal (domingos) de stock por tienda x SKU: hecho periódico semi-aditivo."""
    first_sunday = start + timedelta(days=(6 - start.weekday()) % 7)
    sundays = []
    d = first_sunday
    while d <= end:
        sundays.append((d, (d - cfg.start_date).days))
        d += timedelta(days=7)
    if not sundays:
        return spark.createDataFrame([], "store_id long")
    weeks = spark.createDataFrame(sundays, "snapshot_date date, day_idx int")
    stores = spark.range(1, cfg.n_stores + 1).withColumnRenamed("id", "store_id")
    prods = spark.range(1, cfg.n_inventory_products + 1).withColumnRenamed("id", "product_id")
    n_rows = len(sundays) * cfg.n_stores * cfg.n_inventory_products
    grid = weeks.crossJoin(F.broadcast(stores)).crossJoin(prods).repartition(_partitions(n_rows, 1_000_000))
    pid, sid = F.col("product_id"), F.col("store_id")
    u = uniform(pid, sid, F.col("day_idx"), salt="inv")
    capacity = (F.lit(5) + F.floor(uniform(pid, sid, salt="cap") * 120)).cast("int")
    on_hand = F.when(u < 0.03, F.lit(0)).otherwise(F.floor(capacity * (F.lit(0.2) + u)).cast("int"))
    price = F.expr(product_base_price_sql("product_id"))
    return grid.select(
        F.col("snapshot_date"), sid, pid,
        on_hand.alias("on_hand_qty"),
        F.when(on_hand < capacity * 0.3, (capacity - on_hand)).otherwise(F.lit(0)).cast("int").alias("on_order_qty"),
        capacity.alias("max_capacity"),
        F.round(price * _inflation(F.col("day_idx")) * F.lit(0.7), 2).alias("unit_cost"),
        F.date_add(F.col("snapshot_date"), 1).alias("ingest_date"),
    )


# ============================================================================ API
SOURCES = {
    # nombre: (función, formato, opciones de escritura)
    "customers": (generate_customers, "csv", {"header": "true"}),
    "products": (generate_products, "json", {}),
    "stores": (generate_stores, "csv", {"header": "true"}),
    "orders": (generate_orders, "json", {}),
    "order_status_events": (generate_order_status_events, "json", {}),
    "returns": (generate_returns, "csv", {"header": "true"}),
    "web_events": (generate_web_events, "json", {"compression": "gzip"}),
    "inventory_snapshots": (generate_inventory, "parquet", {}),
}

SCHEMA_DRIFT = {"customers": ("loyalty_tier", SCHEMA_DRIFT_CUSTOMERS)}
