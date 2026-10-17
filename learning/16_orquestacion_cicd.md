# Módulo 16 · Orquestación, CI/CD y operación

> Este módulo es práctico sobre los archivos del repositorio: `databricks.yml`, `resources/*.yml`,
> `.github/workflows/ci.yml` y `tests/`.

## 1. Lakeflow Jobs (ex Workflows)

El job `novamart-e2e-pipeline` muestra los patrones de orquestación más usados:

| patrón | dónde | para qué |
|---|---|---|
| `job parameters` | `parameters:` | un mismo job para dev/prod y distintos volúmenes |
| `condition_task` | `should_generate` | ramas condicionales (generar datos o no) |
| `run_if: NONE_FAILED` | `bronze_ingest` | continuar aunque una rama se haya salteado |
| `for_each_task` | `silver` | la misma lógica en paralelo para 8 entidades |
| `run_job_task` | job diario | componer jobs (generar día → pipeline completo) |
| quality gate | `data_quality_gate` | fallar el job si se rompe una invariante |
| `max_retries` | `bronze_ingest` | reintento tras *schema evolution* de Auto Loader |

**Práctica:** abrí un run del job, mirá el *Gantt* de tareas y encontrá el cuello de botella.

## 2. Databricks Asset Bundles (DAB)

Infraestructura como código. Todo (jobs, pipelines, permisos, notebooks) se versiona en Git y se despliega igual en cada entorno.

```bash
databricks auth login --host https://<tu-workspace>.cloud.databricks.com
databricks bundle validate
databricks bundle deploy -t dev          # recursos con prefijo "[dev tu_usuario]"
databricks bundle run novamart_e2e -t dev --params scale_factor=0.1
databricks bundle summary
databricks bundle destroy -t dev         # limpia todo
```

`mode: development` pausa schedules y prefija nombres; `mode: production` exige que el código venga de CI.

## 3. CI/CD con GitHub Actions

```
push / PR ──▶ lint (ruff) ──▶ unit tests (pytest + Spark local) ──▶ integration test (pipeline completo local)
                    └──────▶ bundle validate (si hay credenciales)
workflow_dispatch(target=prod) ──▶ todo lo anterior ──▶ bundle deploy -t prod
```

* Los tests corren **sin Databricks**: Spark + Delta OSS en el runner de GitHub.
* Las credenciales van en *GitHub Environments* (`DATABRICKS_HOST` como variable, `DATABRICKS_TOKEN` como secreto).
  Mejor aún: OAuth con *service principal* + *workload identity federation* (sin tokens de larga duración).

## 4. Operación del día a día

| pregunta | dónde mirar |
|---|---|
| ¿Corrió bien anoche? | `ops.pipeline_runs`, historial del job |
| ¿Cuántos datos malos llegaron? | `ops.dq_results`, `ops.quarantine` |
| ¿Hasta dónde se procesó? | `ops.watermarks`, checkpoints de streaming |
| ¿Cuánto costó? | `system.billing.usage` |
| ¿Quién usa esta tabla? | `system.access.table_lineage`, `system.access.audit` |

### Runbook: "el job falló en `bronze_ingest` con `UnknownFieldException`"
1. Es **schema evolution** de Auto Loader: apareció una columna nueva (ej. `experiment_id`).
2. El esquema ya se actualizó en `schemaLocation`. **Reintentar** la tarea (el job tiene reintentos).
3. Verificar en silver que la columna nueva se maneja (`_opt()` en `transforms.py`).

### Runbook: "reprocesar un día"
```sql
-- 1. regenerar archivos del día (idempotente)
-- notebooks/10_generate_next_day con run_date = 2025-06-15
-- 2. correr el pipeline con generate=false: bronze/silver toman sólo lo nuevo,
--    gold re-genera las fechas afectadas con replaceWhere.
```

### Runbook: "backfill completo de gold"
Correr el job con `full_refresh=true`: los facts se reconstruyen desde silver (bronze y silver no se tocan).

## 🏋️ Ejercicios
1. Agregá una tarea `notify_slack` que se ejecute sólo si `data_quality_gate` falla (`run_if: AT_LEAST_ONE_FAILED`).
2. Creá un target `staging` en `databricks.yml` con su propio catálogo.
3. Agregá a CI un paso que falle si la cobertura de tests de `src/novamart/silver` baja del 70%.
