# 01 · Arquitectura

![Arquitectura](diagrams/01_arquitectura.png)

## Capas (medallion)

| capa | esquema UC | qué contiene | reglas |
|---|---|---|---|
| Landing | `landing` + volumen `raw` | archivos tal cual llegan (`fuente/ingest_date=YYYY-MM-DD/`) | inmutable; sólo se agrega |
| Bronze | `bronze` | 1 tabla Delta por fuente + `_source_file`, `_ingested_at`, `_rescued_data` | sin limpieza, sin dedup, incremental |
| Silver | `silver` | datos tipados, limpios, deduplicados; clientes y productos con historia SCD2 | contratos de calidad, MERGE idempotentes |
| Gold | `gold` | dimensiones, hechos, marts y vistas para consumo | modelo Kimball, claves subrogadas |
| Ops | `ops` | `pipeline_runs`, `watermarks`, `dq_results`, `quarantine` | sólo appends / upserts chicos |

## Decisiones de diseño (ADR resumidos)

| # | decisión | alternativas | por qué |
|---|---|---|---|
| 1 | Lógica en un **paquete Python** (`src/novamart`) y notebooks delgados | todo en notebooks | testeable con pytest, reutilizable, revisable en PRs |
| 2 | **Auto Loader + `availableNow`** para bronze | `COPY INTO`, batch completo | descubre sólo archivos nuevos, schema evolution, costo de batch |
| 3 | Silver con **streaming + foreachBatch + MERGE** | overwrite diario | exactly-once vía checkpoint; MERGE idempotente ante re-envíos |
| 4 | **SCD2 a mano** + versión **AUTO CDC** declarativa | sólo una | aprender el mecanismo y la herramienta que lo abstrae |
| 5 | Claves subrogadas **xxhash64(nk, valid_from)** | IDENTITY | reconstruibles, paralelas, estables entre full refresh |
| 6 | Facts con **`replaceWhere` por fechas afectadas** | MERGE fila a fila | más barato a gran escala; idempotente |
| 7 | Accumulating snapshot con **MERGE** | recalcular todo | sólo cambian los pedidos con eventos nuevos |
| 8 | **Liquid clustering** en vez de particiones | `PARTITIONED BY` + ZORDER | sin problema de particiones chicas; cambiable sin reescribir |
| 9 | Marts en **SQL puro** | PySpark | los mantiene un analista; corre igual en un SQL Warehouse |
| 10 | Tablas `ops` creadas en `setup` | crearlas on-demand | evita carreras entre las tasks paralelas del `for_each` |

## Flujo incremental de un día

1. `10_generate_next_day` escribe `landing/*/ingest_date=D/` (dinámico: re-ejecutar el día lo reemplaza).
2. `bronze_ingest`: Auto Loader toma sólo esos archivos (el checkpoint recuerda el resto).
3. `silver` (8 entidades en paralelo): `readStream(bronze)` procesa sólo filas nuevas; MERGE en silver.
4. `gold_dimensions`: reconstrucción completa (son chicas y las claves son estables).
5. `gold_facts`: lee el watermark de `ops.watermarks`, calcula las fechas afectadas
   (incluye llegadas tardías de días anteriores) y las reemplaza con `replaceWhere`.
6. `gold_marts`: CTAS de los marts.
7. `data_quality_gate`: invariantes; si falla, el job falla.

## Seguridad y gobierno

* Todo vive en Unity Catalog: permisos por catálogo/esquema, linaje automático.
* `sql/ddl/physical_design.sql`: PK/FK informativas (`RELY`), CHECK constraints, comentarios, tags PII.
* `sql/ddl/governance_masking.sql`: máscara de email y filtro de filas por región.
* CI/CD sin credenciales en el código: `DATABRICKS_HOST`/`DATABRICKS_TOKEN` en GitHub Environments.
