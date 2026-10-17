"""Estacionalidad del negocio: cuánto se vende cada día.

El volumen diario depende de: tendencia de crecimiento, día de la semana, mes
y eventos comerciales (Hot Sale, Black Friday, Cyber Monday, Navidad). Esto
genera picos reales que después se ven en los dashboards y que obligan a
pensar en skew temporal y en particionado/clustering por fecha.
"""

from __future__ import annotations

from datetime import date, timedelta

WEEKDAY_FACTOR = {0: 0.92, 1: 0.95, 2: 0.97, 3: 1.0, 4: 1.08, 5: 1.22, 6: 1.05}
MONTH_FACTOR = {1: 0.85, 2: 0.85, 3: 0.95, 4: 0.95, 5: 1.05, 6: 1.0,
                7: 1.05, 8: 0.97, 9: 0.97, 10: 1.0, 11: 1.2, 12: 1.45}

SCHEMA_DRIFT_CUSTOMERS = date(2024, 7, 1)   # aparece la columna loyalty_tier
SCHEMA_DRIFT_EVENTS = date(2025, 3, 1)      # aparece el campo experiment_id


def black_friday(year: int) -> date:
    nov1 = date(year, 11, 1)
    first_thursday = nov1 + timedelta(days=(3 - nov1.weekday()) % 7)
    return first_thursday + timedelta(days=22)


def hot_sale_days(year: int) -> set[date]:
    may1 = date(year, 5, 1)
    second_monday = may1 + timedelta(days=(0 - may1.weekday()) % 7 + 7)
    return {second_monday + timedelta(days=i) for i in range(3)}


def promo_name(d: date) -> str | None:
    bf = black_friday(d.year)
    if d == bf:
        return "BLACKFRIDAY"
    if d == bf + timedelta(days=3):
        return "CYBERMONDAY"
    if d in hot_sale_days(d.year):
        return "HOTSALE"
    if date(d.year, 12, 15) <= d <= date(d.year, 12, 24):
        return "NAVIDAD"
    return None


def day_weight(d: date, start: date, n_days: int) -> float:
    idx = (d - start).days
    trend = 1.0 + 0.35 * min(max(idx, 0), n_days) / max(n_days, 1)
    weight = trend * WEEKDAY_FACTOR[d.weekday()] * MONTH_FACTOR[d.month]
    promo = promo_name(d)
    if promo in ("BLACKFRIDAY", "CYBERMONDAY"):
        weight *= 2.6
    elif promo == "HOTSALE":
        weight *= 1.8
    elif promo == "NAVIDAD":
        weight *= 1.25
    return weight


def daily_plan(total: int, start: date, end: date, range_start: date, range_end: date) -> list[tuple]:
    """Reparte `total` eventos entre los días de [start, end] según su peso.

    Devuelve sólo los días dentro de [range_start, range_end] (para generar un
    único día en modo incremental sin cambiar la cantidad de ese día).
    Si range_end supera `end` (simulación a futuro) se usa el peso medio.
    """
    n_days = (end - start).days + 1
    weights = {start + timedelta(days=i): day_weight(start + timedelta(days=i), start, n_days)
               for i in range(n_days)}
    total_w = sum(weights.values())
    plan = []
    d = range_start
    while d <= range_end:
        w = weights.get(d) or day_weight(d, start, n_days)
        idx = (d - start).days
        plan.append((d, idx, max(1, int(round(total * w / total_w))), promo_name(d)))
        d += timedelta(days=1)
    return plan
