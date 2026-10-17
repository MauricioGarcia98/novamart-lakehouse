# 05 · Despliegue y operación

![Job](diagrams/07_job_dag.png)

## Requisitos

* Workspace de Databricks con **Unity Catalog** (incluye Free Edition) y compute serverless.
* Permiso para crear catálogo (o usar uno existente: parámetro `catalog`).
* Databricks CLI ≥ 0.2xx para usar el bundle.

## Con Asset Bundles

```bash
cd novamart-lakehouse
databricks auth login --host https://<workspace>
# una sola vez (Default Storage no permite crear catálogos por API):
#   CREATE CATALOG IF NOT EXISTS novamart_dev   -- desde el SQL Editor
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run novamart_e2e -t dev --params scale_factor=0.1
```

| recurso | nombre | qué hace |
|---|---|---|
| job | `novamart-e2e-pipeline` | landing → gold + quality gate |
| job | `novamart-daily-increment` | genera el día siguiente y dispara el e2e (schedule pausado) |
| job | `novamart-weekly-maintenance` | diseño físico + OPTIMIZE/VACUUM/ANALYZE |
| pipeline | `novamart-declarative-pipeline` | versión Lakeflow Declarative (SQL) en el esquema `ldp` (en `dev`: `dev_<usuario>_ldp`) |

`-t dev` crea todo con prefijo `[dev <usuario>]` en el catálogo `novamart_dev`.
`-t prod` usa `novamart` y `scale_factor=1`.

## Sin CLI (Git folder)

> Guía detallada con sub-pasos: [06_guia_desde_la_web.md](06_guia_desde_la_web.md).

1. *Workspace → Create → Git folder* con la URL del repositorio.
2. Abrir `novamart-lakehouse/notebooks/00_setup` y ejecutar en orden hasta `07_data_quality_report`
   (los notebooks `03_silver_entity` se corren una vez por entidad, widget `entity`).

## Free Edition: tips

* Usá `scale_factor = 0.1` (≈ 3 GB). Para practicar performance podés subir a 0.3.
* Si no podés crear catálogos: `catalog = workspace`.
* Serverless no permite `df.cache()` ni algunas `spark.conf`; los módulos que lo necesitan lo indican.

## CI/CD

![CI/CD](diagrams/08_cicd.png)

`.github/workflows/ci.yml`:

1. `lint` — ruff.
2. `unit-tests` — pytest con Spark + Delta locales.
3. `integration-tests` — pipeline completo + incremental + idempotencia.
4. `bundle-validate` — sólo si existe la variable `DATABRICKS_HOST`.
5. `deploy` — manual (`workflow_dispatch` con `deploy_target=dev|prod`).

## Operación

* **Re-procesar un día**: correr `novamart-daily-increment` con `run_date=YYYY-MM-DD`.
* **Backfill de gold**: job e2e con `generate=false` y `full_refresh=true`.
* **Empezar de cero**: `DROP CATALOG novamart_dev CASCADE` y volver a correr el e2e.
* **Monitoreo**: `ops.pipeline_runs`, `ops.dq_results`, `ops.quarantine`, `system.billing.usage`.
