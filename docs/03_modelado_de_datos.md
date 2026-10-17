# 03 · Modelado de datos

> Práctica en [`learning/13_modelado_dimensional.sql`](../learning/13_modelado_dimensional.sql).

## 1. Los tres niveles de un modelo

| nivel | pregunta | artefacto en NovaMart |
|---|---|---|
| **Conceptual** | ¿qué entidades y procesos existen? | cliente, producto, tienda, pedido, envío, devolución, sesión, stock |
| **Lógico** | ¿qué atributos, claves y relaciones? | [modelo estrella](diagrams/03_modelo_estrella.png), [matriz de bus](diagrams/04_matriz_bus.png) |
| **Físico** | ¿cómo se guarda para que sea rápido? | Delta, tipos `DECIMAL`, liquid clustering, PK/FK, `sql/ddl/physical_design.sql` |

## 2. Normalizado (3FN) vs dimensional

Los sistemas origen (OLTP) están **normalizados**: evitan redundancia para escribir rápido y sin
anomalías. El análisis (OLAP) necesita lo contrario: **leer** millones de filas agrupando por atributos
descriptivos con pocos joins. Por eso gold usa un **modelo dimensional** (Kimball).

## 3. Proceso de diseño (Kimball, 4 pasos)

1. **Proceso de negocio**: ventas.
2. **Grano**: una fila por **línea de pedido**. Es la decisión más importante; un grano más
   grueso (pedido) pierde el producto; uno más fino no existe en el origen.
3. **Dimensiones**: fecha, cliente, producto, tienda, canal, pago.
4. **Hechos**: cantidad, precio, bruto, descuento, neto, costo, margen (todos aditivos).

![Modelo estrella](diagrams/03_modelo_estrella.png)

## 4. Tipos de tablas de hechos

| tipo | ejemplo | se actualiza | medidas |
|---|---|---|---|
| transaccional | `fact_sales`, `fact_returns` | no (insert) | aditivas |
| accumulating snapshot | `fact_order_fulfillment` | sí, en cada hito | lags entre fechas |
| periodic snapshot | `fact_inventory_snapshot` | no (una foto por período) | **semi-aditivas** |
| factless / agregado | `fact_web_sessions` | no | conteos y flags |

## 5. Dimensiones

* **Conformadas**: la misma `dim_customer` sirve a ventas, devoluciones, sesiones → *drill-across*.
* **Role-playing**: `dim_date` es fecha de pedido, pago, despacho y entrega en fulfillment.
* **Junk**: `dim_payment` agrupa método, cuotas y marca de tarjeta (baja cardinalidad).
* **Degeneradas**: `order_id`, `promo_code` viven en el fact sin tabla propia.
* **Miembro desconocido**: `-1` evita FKs nulas (cliente anónimo, canal sin tienda).

## 6. Slowly Changing Dimensions

![SCD2](diagrams/05_scd2.png)

| tipo | comportamiento | uso |
|---|---|---|
| 0 | nunca cambia | fecha de nacimiento |
| 1 | sobrescribe | `dim_store` (correcciones, sin valor histórico) |
| 2 | nueva versión con vigencia | `dim_customer` (ciudad, segmento, tier), `dim_product` (precio, costo, categoría) |
| 3 | columna "anterior" | ejercicio del módulo 13 |

Implementación: [`src/novamart/silver/scd2.py`](../src/novamart/silver/scd2.py) (MERGE manual con
"staged updates", manejo de cambios dentro del mismo lote, re-envíos y llegadas tardías) y
[`pipelines/02_silver.sql`](../pipelines/02_silver.sql) (`AUTO CDC ... STORED AS SCD TYPE 2`).

**Join point-in-time**: el fact se une con `order_ts >= effective_from AND order_ts < effective_to`.
La primera versión arranca en 1900-01-01 para cubrir hechos que llegan "antes" que su dimensión.

## 7. Claves subrogadas

`customer_key = xxhash64(customer_id, valid_from)`

* ✅ determinística: un full refresh de la dimensión no rompe los facts;
* ✅ se calcula en paralelo, sin secuencias ni coordinación;
* ⚠️ colisiones posibles en teoría (64 bits: despreciable para millones de filas; se valida con la PK);
* Alternativa: `GENERATED ALWAYS AS IDENTITY` (secuencial, pero cambia si se reconstruye la tabla).

## 8. Estrella vs copo de nieve vs OBT vs Data Vault

| enfoque | cuándo |
|---|---|
| **Estrella** (este proyecto) | BI, semántica clara, la opción por defecto |
| Copo de nieve | dimensiones enormes con jerarquías que cambian independientemente |
| **OBT** (`v_sales_enriched`) | herramientas sin joins, features de ML, Genie; como vista sobre la estrella |
| Data Vault | muchas fuentes que cambian seguido, auditoría estricta; capa intermedia antes de la estrella |

## 9. Checklist de un buen modelo

- [ ] el grano de cada fact está escrito y **testeado** (`COUNT(*) = COUNT(DISTINCT clave_grano)`)
- [ ] ninguna FK nula (miembros `-1`)
- [ ] todas las FKs existen en su dimensión (test de integridad referencial)
- [ ] medidas en `DECIMAL`, nunca `DOUBLE`, para dinero
- [ ] semi-aditivas documentadas
- [ ] una sola versión vigente por clave natural en cada SCD2
