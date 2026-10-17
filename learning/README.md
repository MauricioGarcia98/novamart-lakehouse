# Ruta de aprendizaje: Spark + SQL + modelado en Databricks

![Ruta](../docs/diagrams/09_ruta_aprendizaje.png)

> **Antes de empezar:** si sos nuevo, leé [`docs/00_empeza_aca.md`](../docs/00_empeza_aca.md) y tené a mano el
> [glosario](../docs/12_glosario.md). Los módulos usan el catálogo `novamart_dev` (el que crea la
> [guía](../docs/06_guia_desde_la_web.md)); si desplegaste prod, cambialo por `novamart`.
> Las 68 consultas de los módulos SQL fueron verificadas contra los datos reales (2026-10-17).

Todos los módulos trabajan sobre las tablas que genera el pipeline de NovaMart: nada de datasets de juguete.
Importalos al workspace junto con el repo (Git folder) y corré antes el job `novamart-e2e-pipeline`.

| # | módulo | formato | conceptos clave | SF sugerido |
|---|---|---|---|---|
| 00 | [Bienvenida y entorno](00_bienvenida_y_entorno.py) | Python | lakehouse, Unity Catalog, volúmenes, serverless | 0.1 |
| 01 | [SELECT, WHERE, ORDER BY](01_sql_select_filtros_orden.sql) | SQL | orden lógico de ejecución, NULL, data skipping | 0.1 |
| 02 | [Agregaciones](02_sql_agregaciones.sql) | SQL | GROUP BY ALL, HAVING, FILTER, aproximados, semi-aditivos | 0.1 |
| 03 | [JOINs y modelo estrella](03_sql_joins.sql) | SQL | LEFT/SEMI/ANTI, trampa del WHERE, point-in-time, CROSS | 0.1 |
| 04 | [Subconsultas, CTEs, vistas](04_sql_subconsultas_ctes.sql) | SQL | correlacionadas, CTEs encadenadas, EXISTS, vistas | 0.1 |
| 05 | [Funciones de ventana](05_sql_funciones_ventana.sql) | SQL | ranking, QUALIFY, acumulados, LAG/LEAD, gaps & islands, sessionización | 0.1 |
| 06 | [Tipos, fechas, textos, NULLs](06_sql_tipos_fechas_textos_nulos.sql) | SQL | DECIMAL vs DOUBLE, ANSI, `try_*`, regex, zonas horarias | 0.1 |
| 07 | [SQL avanzado](07_sql_avanzado.sql) | SQL | structs/arrays, higher-order functions, PIVOT, ROLLUP, MERGE, time travel, CDF, CTE recursiva | 0.1 |
| 08 | [Arquitectura de Spark y DataFrames](08_spark_arquitectura_dataframes.py) | PySpark | driver/executors, lazy, narrow/wide, formatos | 1 |
| 09 | [Transformaciones avanzadas](09_spark_transformaciones_avanzadas.py) | PySpark | ventanas API, tipos complejos, UDF vs pandas UDF vs nativo, applyInPandas | 1 |
| 10 | [Performance con volumen](10_spark_performance_internals.py) | PySpark | explain, broadcast, **skew + salting**, AQE, shuffle partitions, OPTIMIZE, liquid clustering | 1–10 |
| 11 | [Delta Lake a fondo](11_delta_lake_a_fondo.py) | PySpark | log de transacciones, schema evolution, CDF, concurrencia, deletion vectors | 1 |
| 12 | [Structured Streaming](12_structured_streaming.py) | PySpark | Auto Loader, watermarks, ventanas, dedup, foreachBatch | 1 |
| 13 | [Modelado dimensional](13_modelado_dimensional.sql) | SQL | grano, facts, SCD, role-playing, junk, OBT, Data Vault | 1 |
| 14 | [Declarativo vs imperativo](14_lakeflow_declarativo_vs_imperativo.py) | Python | Lakeflow Declarative Pipelines, expectations, AUTO CDC, event log | 0.1 |
| 15 | [Gobierno con Unity Catalog](15_gobierno_unity_catalog.sql) | SQL | grants, máscaras, row filters, tags, linaje, system tables | 0.1 |
| 16 | [Orquestación y CI/CD](16_orquestacion_cicd.md) | Markdown | Jobs, DAB, GitHub Actions, runbooks | — |
| 17 | [Desafíos finales](17_desafios_finales.md) | Markdown | 8 mini-proyectos de nivel senior para portfolio | 1–10 |

## Cómo estudiar cada módulo

1. Leé la teoría (celdas de Markdown) y ejecutá los ejemplos **mirando el resultado y el plan**.
2. Hacé los ejercicios **sin mirar** las soluciones (están en `<details>` al final).
3. Volvé a ejecutar con un `scale_factor` mayor y observá qué cambia (tiempo, archivos leídos, shuffle).
4. Anotá en tu propio repo qué aprendiste: es material para entrevistas.

## Además del código de los módulos

El propio pipeline es material de estudio. Leelo en este orden:

1. `src/novamart/datagen/` — cómo generar datos realistas y distribuidos.
2. `src/novamart/ingestion/bronze.py` — Auto Loader.
3. `src/novamart/silver/transforms.py` y `scd2.py` — limpieza y SCD2 con MERGE.
4. `src/novamart/gold/facts.py` — joins point-in-time, `replaceWhere`, accumulating snapshot.
5. `sql/gold/marts/` — SQL analítico de producción.
6. `pipelines/` — lo mismo, declarativo.
7. `tests/` — cómo se testea un pipeline de Spark.
