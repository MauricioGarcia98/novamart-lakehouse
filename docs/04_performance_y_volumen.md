# 04 · Performance y grandes volúmenes

> Laboratorio práctico: [`learning/10_spark_performance_internals.py`](../learning/10_spark_performance_internals.py).

![Spark](diagrams/06_spark_ejecucion.png)

## Técnicas aplicadas en el pipeline

| técnica | dónde | efecto |
|---|---|---|
| Generación con `xxhash64` en vez de `rand()` | `datagen/rand.py` | reproducible y 100% distribuida (sin driver) |
| `repartition` según volumen | generadores | tareas de ~400K filas, sin particiones gigantes |
| `maxRecordsPerFile` | escritura landing | archivos de ≤ 1M filas |
| Auto Loader `maxFilesPerTrigger` | bronze | micro-lotes acotados en memoria |
| MERGE con columna de clustering en la condición | silver | file pruning: no escanea toda la tabla destino |
| `replaceWhere` por fechas afectadas | gold facts | reescribe días, no la tabla |
| `broadcast` de dimensiones | facts, joins | el fact no se mueve por la red |
| Agregados en una pasada (`summarize`) | calidad | 1 scan para N reglas |
| `F.transform` / funciones de orden superior | generador, SQL | evita `explode` + `groupBy` |
| Pre-agregación (`mart_sales_daily`) | gold | dashboards leen 30x menos |
| Liquid clustering + OPTIMIZE | `physical_design.sql`, mantenimiento | data skipping |
| `APPROX_COUNT_DISTINCT`, `PERCENTILE_APPROX` | marts | estado acotado en memoria |

## Skew conocido (a propósito)

* `store_key = -1` (online) ≈ 60% de `fact_sales`.
* Clientes y productos con ley de potencias (`zipf_index`, skew 1,8–2,2).
* `customer_key = -1` en ventas anónimas.

Soluciones vistas en el módulo 10: AQE skew join, salting manual, aislar la clave caliente, broadcast.

## Tiempos de referencia

Ejecución local (laptop/CI, 4 cores) con SF=0.002 y 90 días:

| paso | segundos |
|---|---:|
| generate | 65 |
| bronze | 82 |
| silver | 98 |
| gold dims | 22 |
| gold facts | 52 |
| gold marts | 24 |

En Databricks el costo dominante con SF ≥ 1 es `web_events` (gzip no es *splittable*: 1 archivo = 1 tarea)
y el self-join de `mart_product_affinity`. Medilo con el *Query Profile* y documentá tus mejoras
(desafío 1 de [`learning/17_desafios_finales.md`](../learning/17_desafios_finales.md)).

## Elegir el scale factor

| entorno | SF sugerido |
|---|---|
| tests / CI | 0.0005 – 0.002 |
| Databricks Free Edition | 0.05 – 0.1 |
| workspace de prueba (serverless) | 1 |
| cluster de varios nodos | 10+ |
