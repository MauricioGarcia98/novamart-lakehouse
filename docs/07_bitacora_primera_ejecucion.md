# 07 · Bitácora de la primera ejecución real (2026-10-17)

> Qué pasó, en orden, cuando desplegamos y corrimos el proyecto por primera vez en un workspace
> **Free Edition**. Cada error está explicado en tres niveles: **qué vimos**, **qué significa
> (en criollo)** y **cómo se arregló**. Es la parte más valiosa para aprender: en el trabajo real
> nada funciona a la primera, y lo importante es saber leer un error.

---

## 1 · Verificar la conexión

**Qué hicimos:** llamar a la API `GET /api/2.0/preview/scim/v2/Me` con `DATABRICKS_HOST` y
`DATABRICKS_TOKEN`.

**En criollo:** antes de mandar nada, preguntamos "¿quién soy?". Si Databricks responde con tu
usuario, el **token** (una contraseña para programas) es válido y la URL es correcta.

**Resultado:** `200 OK`, usuario `nahu98garcia@gmail.com`, grupos `users` y `admins`.

> **Buena práctica:** el token nunca se imprime ni se sube a GitHub. Se lee de una variable de
> entorno. Si se filtra, se revoca en *Settings → Developer → Access tokens*.

---

## 2 · Instalar el Databricks CLI

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks --version     # Databricks CLI v1.21.0
```

**En criollo:** el CLI es un programa de terminal para manejar Databricks sin clics. Lo usamos para
desplegar el *bundle*.

---

## 3 · `databricks bundle validate` → OK

Revisa que el YAML esté bien escrito (sintaxis, variables, referencias). **Validar no garantiza
que el deploy funcione**: muchas reglas sólo las chequea el servidor al crear los recursos. Lo
vimos enseguida.

---

## 4 · Error 1 en el deploy: dependencia de un if/else sin `outcome`

**Qué vimos:**
```
Error: cannot create resources.jobs.novamart_e2e: The dependency on an if/else condition
"should_generate" does not specify an outcome. (400 INVALID_PARAMETER_VALUE)
```

**En criollo:** el job tiene una tarea "pregunta" (`should_generate`: ¿genero datos nuevos?) con
dos salidas, `true` y `false`. La tarea `bronze_ingest` decía "dependo de la pregunta", pero no de
**cuál respuesta**. La API de Jobs exige decirlo.

**Cómo se arregló** (`resources/novamart_e2e.job.yml`):
```yaml
- task_key: bronze_ingest
  depends_on: [{ task_key: generate_data }, { task_key: should_generate, outcome: "false" }]
  run_if: NONE_FAILED
```
Se lee así: *"corré bronze cuando terminó la generación **o** cuando la pregunta dijo `false`
(no generar)"*. `run_if: NONE_FAILED` permite correr aunque una de las dos ramas se haya salteado.

**Lección:** cada rama de un if/else es un camino distinto, y las dependencias tienen que
nombrar el camino.

---

## 5 · Error 2 en el deploy: el catálogo no existe

**Qué vimos:**
```
Error: cannot create resources.pipelines.novamart_ldp: Catalog 'novamart_dev' does not exist.
```

**En criollo:** el pipeline declarativo escribe en un esquema de `novamart_dev` (en dev se llama `dev_<usuario>_ldp`), y Databricks valida eso
**al crearlo**. El catálogo lo creaba el notebook `00_setup`, pero ese notebook corre **después**
del deploy. Es el problema del huevo y la gallina.

**Intento 1 (falló):** declarar el catálogo dentro del bundle. Resultado:
```
Metastore storage root URL does not exist. Default Storage is enabled in your account.
You can use the UI to create a new catalog using Default Storage...
```
En Free Edition (*Default Storage*), **la API REST no puede crear catálogos**. Sólo la UI o SQL.

**Intento 2 (funcionó):**
1. Crear el catálogo **una vez** por SQL: `CREATE CATALOG IF NOT EXISTS novamart_dev`.
2. Declarar en el bundle sólo el **esquema** `ldp` (en modo `development` se crea como `dev_nahu98garcia_ldp`) (`resources/novamart_catalog.yml`). El
   pipeline lo referencia (`${resources.schemas.ldp.name}`), y así el bundle sabe que primero
   tiene que crear el esquema y después el pipeline.
3. Documentarlo en el README y en la [guía web](06_guia_desde_la_web.md) (Paso 3).

**Lección:** el orden de creación importa. Cuando un recurso referencia a otro con `${resources...}`,
el bundle respeta ese orden. Si no hay referencia, puede crear en cualquier orden.

**Deploy final:** 3 jobs + 1 pipeline + 1 esquema creados.

---

## 6 · Error 3 en el job: Auto Loader no acepta esquema fijo + evolución

`setup` (63 s) y `generate_data` (25 min) terminaron OK. Falló `bronze_ingest`:
```
[CF_ADD_NEW_NOT_SUPPORTED] Schema evolution mode addNewColumns is not supported when the
schema is specified. To use this mode, you can provide the schema through
`cloudFiles.schemaHints` instead.
```

**En criollo:** Auto Loader lee archivos nuevos de una carpeta. Le habíamos pedido dos cosas
incompatibles para los CSV:
* *"usá exactamente este esquema"* (`.schema(...)`), y
* *"si aparecen columnas nuevas, agregalas"* (`addNewColumns`).

Si el esquema es fijo, no puede crecer. La solución que sugiere el propio error es pasar el
esquema como **pista** (`schemaHints`): "estas columnas son así; si aparecen otras, sumalas".

**Cómo se arregló** (`src/novamart/ingestion/bronze.py`):
```python
if fmt == "csv":
    reader = reader.option("cloudFiles.inferColumnTypes", "false").option(
        "cloudFiles.schemaHints", csv_schema_hints(name)   # "`customer_id` STRING, ..."
    )
```
En local (sin Auto Loader) se sigue usando `.schema(...)`, porque ahí no hay conflicto.

**Lección:** leé el error entero. Muchas veces Databricks te dice la solución ("*you can provide
the schema through `cloudFiles.schemaHints` instead*").

> ¿Por qué los tests no lo detectaron? Porque Auto Loader sólo existe en Databricks: en local se
> usa el *file source* de Spark OSS, que no tiene esa regla. Por eso hace falta probar también en
> el entorno real (un test de integración en Databricks en el CI).

---

## 7 · Error 4 en el job: llegó una columna nueva

Con el arreglo anterior, bronze cargó 7 de 8 fuentes y frenó en `web_events`:
```
[UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_RECORD_WITH_FILE_PATH] Encountered unknown fields during
parsing: {"experiment_id":"exp-checkout-v2"}, which can be fixed by an automatic retry: true
```

**En criollo:** a partir de cierta fecha los eventos web traen un campo nuevo, `experiment_id`
(un A/B test). El generador lo mete **a propósito** para simular la realidad. Auto Loader
funciona así: guarda el esquema nuevo y **frena el stream una vez**, para que al reiniciar se lea
todo con el esquema ampliado. Es comportamiento esperado, no un bug.

El job confiaba en `max_retries: 2` para ese reinicio, pero el Error 3 ya había gastado los
reintentos.

**Cómo se arregló:** ahora el código reinicia el stream solo cuando ve ese error puntual (hasta
3 veces) y ya no depende de los reintentos del job:
```python
for attempt in range(_SCHEMA_EVOLUTION_RESTARTS + 1):
    query = ...writeStream...toTable(target)
    try:
        query.awaitTermination(); break
    except Exception as exc:
        if "UNKNOWN_FIELD_EXCEPTION" not in str(exc) or attempt == _SCHEMA_EVOLUTION_RESTARTS:
            raise
```
Los reintentos del job quedan para fallas transitorias (red, máquinas).

**Lección:** no mezcles "fallas esperadas del negocio" con "fallas de infraestructura". A las
primeras manejalas en el código. A las segundas, con reintentos.

---

## 8 · Repair run: re-ejecutar sólo lo que falló

![Grafo del run reparado: bronze_ingest con 4 attempts](img/ui/09b_run_grafo.png)

En vez de correr todo de nuevo (25 min más de generación), usamos **Repair run**:
```bash
databricks jobs repair-run <run_id> --rerun-all-failed-tasks --rerun-dependent-tasks
```
(En la UI: abrir el run → **Repair run**.) Se re-ejecutaron bronze y todo lo que depende de bronze, con el
código ya corregido.

---

## 9 · Resultado final ✅

![Línea de tiempo real del job: bronze falla 3 veces, el repair run lo re-ejecuta y todo termina OK a los 50 min](img/charts/01_linea_de_tiempo_run.svg)

> **Cómo leerla:** cada fila es una tarea y cada barra, un intento. Las rojas rayadas son los
> fallos de bronze (secciones 6 y 7). Los rombos grises son tareas **omitidas** porque falló una
> anterior. La línea punteada marca el *repair run*: desde ahí, todo en verde.

| Tarea | Estado | Duración |
|---|---|---|
| setup | ✅ SUCCESS | 63 s |
| should_generate | ✅ SUCCESS (`true`) | 0 s |
| generate_data | ✅ SUCCESS | 1.516 s (~25 min) |
| bronze_ingest | ✅ SUCCESS (en el repair, con los 2 fixes) | 198 s |
| silver (for_each × 8) | ✅ SUCCESS | 146 s |
| gold_dimensions | ✅ SUCCESS | 32 s |
| gold_facts | ✅ SUCCESS | 79 s |
| gold_marts | ✅ SUCCESS | 40 s |
| data_quality_gate | ✅ SUCCESS | 12 s |

### Filas por capa (`scale_factor = 0.1`)

![Filas por tabla en silver](img/charts/02_filas_silver.svg)

| Entidad | Bronze | Silver | Gold |
|---|---:|---:|---:|
| Clientes | 209.836 | 209.285 (historia SCD2) | `dim_customer` 209.286 |
| Productos | 8.691 | 8.465 (historia SCD2) | `dim_product` 8.466 |
| Tiendas | 40 | 40 | `dim_store` 41 |
| Pedidos | 1.203.610 | 1.199.310 | `fact_order_fulfillment` 1.199.310 |
| Líneas de pedido | — | 2.816.655 | `fact_sales` 2.816.655 |
| Eventos de estado | 4.254.845 | 4.254.845 | — |
| Devoluciones | 142.802 | 142.802 | `fact_returns` 142.802 |
| Eventos web | 14.009.762 | 14.009.762 | `fact_web_sessions` 2.499.992 |
| Stock (snapshots) | 1.570.000 | 1.570.000 | `fact_inventory_snapshot` 1.570.000 |

![Pedidos de bronze a silver: 1.203.610 − 3.599 duplicados − 701 en cuarentena = 1.199.310](img/charts/03_pedidos_bronze_a_silver.svg)

**¿Por qué bronze tiene más filas que silver?** Porque silver **deduplica** (el generador mete
pedidos repetidos a propósito) y manda los registros inválidos a **cuarentena**. Las dimensiones
tienen +1 fila: es el miembro "desconocido" (`-1`), al que apuntan los hechos sin cliente/tienda.

**Marts:** `mart_sales_daily` 304.076 · `mart_customer_rfm` 93.441 · `mart_funnel_daily` 57.535 ·
`mart_cohort_retention` 666 · `mart_product_affinity` 630.

![Ventas netas por año](img/charts/04_ventas_por_anio.svg)

**Ventas netas** (`fact_sales.net_amount`): 2023 = 496,3 M · 2024 = 589,2 M · 2025 = 690,4 M
(total 1.775,9 M sobre 1.199.054 pedidos).

### Calidad de datos (`ops.dq_results`)

23 reglas evaluadas, 2 con hallazgos (ambas con acción *quarantine*):

| Tabla | Regla | Filas que fallaron | Total |
|---|---|---:|---:|
| `silver.order_lines` | `quantity_positive` | 1.420 | 2.818.075 |
| `silver.orders` | `online_order_has_customer` | 701 | 1.200.011 |

Total en `ops.quarantine`: **2.121** registros. Ejemplo: una línea con `"quantity": -1`. **Esto
es lo esperado:** el generador inyecta datos sucios y el pipeline los detecta y los aparta sin
frenar todo.

---

## Glosario rápido de lo que apareció

| Término | Qué es |
|---|---|
| **Token** | Contraseña para que un programa hable con Databricks |
| **CLI** | Programa de terminal para manejar Databricks |
| **Bundle (DAB)** | Proyecto descrito en YAML: jobs, pipelines, esquemas. Se despliega con un comando |
| **Deploy** | Subir el código y crear/actualizar los recursos en el workspace |
| **Target `dev`** | Ambiente de desarrollo: prefija con tu usuario y pausa schedules |
| **Job / Task** | Receta de ejecución / cada paso de la receta |
| **Condition task (if/else)** | Tarea que decide qué rama sigue |
| **for_each** | Correr la misma tarea N veces con distintos valores |
| **Auto Loader** | Lector incremental de archivos: procesa sólo lo nuevo |
| **Checkpoint** | "Señalador" que recuerda hasta dónde se leyó |
| **Schema evolution** | Que la tabla acepte columnas nuevas sin romperse |
| **Cuarentena** | Tabla donde van los registros inválidos, para revisarlos después |
| **Repair run** | Re-ejecutar sólo las tareas que fallaron |
