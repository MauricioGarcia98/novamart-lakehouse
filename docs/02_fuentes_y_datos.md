# 02 · Fuentes y diccionario de datos

![Linaje](diagrams/02_linaje_tablas.png)

## Volúmenes por scale factor

| fuente | formato | SF=0.1 | SF=1 | SF=10 | llegada |
|---|---|---:|---:|---:|---|
| customers | CSV (CDC) | 100 K + 100/día | 1 M + 1 K/día | 10 M | snapshot inicial + cambios diarios |
| products | JSON (CDC) | 2,5 K | 25 K | 250 K | snapshot + cambios de precio |
| stores | CSV | 40 | 400 | 400 | snapshot |
| orders | JSON anidado | 1,2 M | 12 M | 120 M | diario, 3% tardío, 0,3% re-enviado |
| order lines (en orders) | — | 3,6 M | 36 M | 360 M | — |
| order_status_events | JSON | 4 M | 40 M | 400 M | diario |
| returns | CSV | 180 K | 1,8 M | 18 M | diario |
| web_events | JSON.gz | 15 M | 150 M | 1.500 M | diario |
| inventory_snapshots | Parquet | 1,6 M | 157 M | 1.570 M | semanal (domingos) |

Rango por defecto: 2023-01-01 → 2025-12-31 (3 años). Todo es determinístico: misma config = mismos datos.

## Comportamiento del negocio simulado

* **Estacionalidad**: tendencia +35% en 3 años, sábados fuertes, diciembre +45%, picos de
  **Black Friday / Cyber Monday (x2,6)**, **Hot Sale (x1,8)** y Navidad.
* **Distribuciones de potencia (skew real)**: pocos clientes y productos concentran gran parte de la venta;
  ~60% de las líneas son online (`store_key = -1`).
* **Crecimiento**: los ids de clientes, productos y tiendas crecen con el tiempo; ningún pedido referencia
  algo que todavía no existe.
* **Ciclo de vida del pedido**: CREATED → PAYMENT_APPROVED → SHIPPED / READY_FOR_PICKUP → DELIVERED,
  4% de pagos online fallidos (CANCELLED). Express entrega más rápido.
* **Devoluciones**: 5% general, 12% en Moda, 1% en Supermercado, 1 a 30 días después de la entrega.
* **Clickstream**: sesiones con embudo (24% carrito, 9% checkout, 3,5% compra), dispositivos, UTM, horario.

## Problemas de calidad inyectados

| problema | dónde | cómo se resuelve |
|---|---|---|
| duplicado exacto | customers (0,2%) | `dropDuplicates` + hash en SCD2 |
| re-envío con el mismo id | orders (0,3%, al día siguiente) | primera llegada gana (`ROW_NUMBER`) + MERGE insert-only |
| llegada tardía | orders (3%, 1-3 días) | watermark por `_ingested_at` + `replaceWhere` de las fechas afectadas |
| 3 formatos de fecha | orders, products, status | `coalesce(try_to_timestamp(...))` |
| epoch en milisegundos | web_events | `timestamp_millis` |
| fecha `dd/MM/yyyy` | customers.birth_date | `try_to_timestamp(..., 'dd/MM/yyyy')` |
| email con mayúsculas/espacios | customers (2%) | `lower(trim())` + regla `warn` |
| teléfonos en 4 formatos | customers | `regexp_replace('[^0-9]', '')` |
| pedido online sin cliente | orders (0,1%) | cuarentena |
| cantidad negativa | líneas (0,05%) | cuarentena + CHECK constraint |
| columna nueva en CSV | customers.loyalty_tier (desde 2024-07-01) | contrato con columna final + `_opt()` |
| campo nuevo en JSON | web_events.experiment_id (desde 2025-03-01) | Auto Loader `addNewColumns` + `mergeSchema` |
| cambios fuera de orden | customers CDC | SCD2 ignora cambios más viejos que la versión vigente |

## Diccionario (gold)

### `fact_sales` — grano: línea de pedido
| columna | tipo | descripción |
|---|---|---|
| order_date_key | int | FK → dim_date (yyyyMMdd) |
| order_ts | timestamp | momento del pedido (UTC) |
| order_id, line_id | bigint, int | dimensión degenerada; PK compuesta |
| customer_key | bigint | FK → versión del cliente vigente al comprar; -1 = anónimo |
| product_key | bigint | FK → versión del producto vigente al vender |
| store_key | bigint | FK → dim_store; -1 = canal digital |
| channel_key / payment_key | int / bigint | FK → dim_channel / dim_payment (junk) |
| promo_code | string | dimensión degenerada |
| quantity, unit_price, discount_pct | int, decimal | |
| gross_amount, discount_amount, net_amount | decimal(14,2) | `net = gross − discount` |
| cost_amount, margin_amount | decimal(14,2) | costo de la versión del producto; `margin = net − cost` |

### Otras tablas
* `fact_order_fulfillment`: 1 fila por pedido; timestamps de cada hito, `current_status`,
  `hours_to_pay/ship/deliver`, `is_on_time` (≤ 120 h), monto y unidades.
* `fact_returns`: 1 fila por línea devuelta; motivo, cantidad, reintegro, días desde el pedido.
* `fact_inventory_snapshot`: tienda × SKU × domingo; `on_hand_qty` (semi-aditiva), `inventory_value`, `is_stockout`, `fill_rate`.
* `fact_web_sessions`: 1 fila por sesión; flags del embudo, duración, rebote, UTM, experimento.
* `dim_customer` / `dim_product`: SCD2 con `valid_from`, `valid_to`, `effective_from/to`, `is_current`.
* Marts: `mart_sales_daily`, `mart_customer_rfm`, `mart_cohort_retention`, `mart_product_affinity`,
  `mart_funnel_daily`; vista `v_sales_enriched`.
