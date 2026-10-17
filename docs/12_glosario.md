# 12 · Glosario (en criollo)

> Cada término tiene una definición simple y, cuando aplica, **dónde aparece en NovaMart**.
> Ordenado por tema, no alfabéticamente: leelo de arriba hacia abajo y los conceptos se apoyan entre sí.

## 1 · Lo básico

| Término | Qué es | En NovaMart |
|---|---|---|
| **Dato** | El registro de algo que pasó o de algo que existe | Una venta, un cliente, un clic |
| **Tabla** | Datos organizados en filas (cada registro) y columnas (cada atributo) | `gold.fact_sales` |
| **SQL** | El lenguaje para preguntarle cosas a las tablas | `SELECT SUM(net_amount) FROM fact_sales` |
| **Python** | Lenguaje de programación de uso general | Todo `src/novamart/` |
| **Pipeline** | Una cadena de pasos que mueve y transforma datos | landing → bronze → silver → gold |
| **Data Engineer** | Quien construye y mantiene los pipelines | Vos, en este proyecto |
| **Data Analyst** | Quien usa los datos para responder preguntas del negocio | Quien consulta gold o usa Genie |
| **Sistema de origen (fuente)** | El sistema donde nacen los datos | La caja, la web, el sistema de stock (simulados) |

## 2 · Plataforma

| Término | Qué es | En NovaMart |
|---|---|---|
| **Databricks** | Plataforma en la nube para procesar y analizar datos | Donde corre todo |
| **Workspace** | Tu "escritorio" dentro de Databricks | `dbc-862efc79-9e80…` |
| **Free Edition** | Versión gratuita, con límites de cómputo | La que usamos |
| **Spark** | Motor que reparte el procesamiento entre muchas máquinas | Lo usan bronze, silver y gold |
| **Cluster** | Un grupo de máquinas que trabajan juntas | No usamos clusters propios |
| **Serverless** | Databricks pone y saca las máquinas por vos | Todo el proyecto |
| **SQL Warehouse** | Cómputo optimizado para consultas SQL | *Serverless Starter Warehouse* |
| **Notebook** | Documento con celdas de código que se ejecutan de a una | `notebooks/00_setup.py` |
| **DBU** | La unidad con la que Databricks mide (y cobra) el cómputo | Consumo de cada job |

## 3 · Almacenamiento y gobierno

| Término | Qué es | En NovaMart |
|---|---|---|
| **Unity Catalog (UC)** | El "archivero" con permisos de todos los datos | `catálogo.esquema.tabla` |
| **Catálogo** | Primer nivel: agrupa todo un proyecto o ambiente | `novamart_dev` |
| **Esquema** | Segundo nivel: agrupa tablas relacionadas | `bronze`, `silver`, `gold`, `ops` |
| **Volume (volumen)** | Una carpeta de archivos dentro de UC | `landing.raw` |
| **Delta Lake** | Formato de tabla sobre archivos Parquet con transacciones e historia | Todas las tablas |
| **Parquet** | Formato de archivo columnar y comprimido | Lo que hay "debajo" de Delta |
| **Transacción (ACID)** | Garantía de que una escritura se hace completa o no se hace | Dos procesos no corrompen una tabla |
| **Time travel** | Consultar una tabla como estaba antes (`VERSION AS OF`) | Pestaña *History* de una tabla |
| **VACUUM** | Borrar archivos viejos que ninguna versión usa | Liberar espacio |
| **Linaje (lineage)** | El "árbol genealógico" de una tabla: de dónde viene y quién la usa | Pestaña *Lineage* |
| **PII** | Datos personales identificables (email, teléfono, DNI) | `dim_customer.email` |
| **Máscara** | Regla que oculta un dato a quien no tiene permiso | `mask_email` |
| **Row filter** | Regla que muestra sólo algunas filas según quién consulta | Filtro por región (en `sql/ddl/`) |

## 4 · Arquitectura

| Término | Qué es | En NovaMart |
|---|---|---|
| **Lakehouse** | Data lake (barato, abierto) + data warehouse (ACID, SQL, permisos) | La arquitectura del proyecto |
| **Medallion** | Organizar los datos en capas bronze → silver → gold | Las capas del proyecto |
| **Landing** | La zona donde llegan los archivos crudos | `landing/<fuente>/ingest_date=…` |
| **Bronze** | Copia fiel e incremental de lo que llegó | `bronze.orders` |
| **Silver** | Datos limpios, tipados, sin duplicados, con historia | `silver.orders` |
| **Gold** | Datos modelados para el negocio | `gold.fact_sales`, marts |
| **Ops** | Tablas de operación: auditoría, calidad, cuarentena | `ops.dq_results` |
| **Mart** | Tabla precalculada para una pregunta frecuente | `mart_sales_daily` |

## 5 · Ingesta y procesamiento

| Término | Qué es | En NovaMart |
|---|---|---|
| **Ingesta** | Traer datos desde la fuente a la plataforma | Paso bronze |
| **Batch** | Procesar un lote de datos de una vez | El job diario |
| **Streaming** | Procesar datos continuamente, a medida que llegan | Bronze y silver usan su mecánica |
| **Auto Loader** | Herramienta que detecta y lee sólo archivos nuevos | `cloudFiles` en `bronze.py` |
| **Checkpoint** | "Señalador" donde se guarda hasta dónde se procesó | `_checkpoints/` en el volumen |
| **Incremental** | Procesar sólo lo nuevo, no todo de nuevo | El job diario tarda 7 min, no 50 |
| **Idempotente** | Ejecutar una o diez veces da el mismo resultado | MERGE y replaceWhere |
| **MERGE** | Inserta lo nuevo y actualiza lo existente en una sola operación | Silver |
| **replaceWhere** | Reemplazar sólo un rango (por ejemplo, unas fechas) de una tabla | Facts de gold |
| **Watermark** | La marca de "hasta dónde procesé" de un paso | `ops.watermarks` |
| **CDC** | *Change Data Capture*: recibir sólo los cambios (altas, bajas, modificaciones) | Clientes y productos |
| **Schema** (de datos) | Las columnas y sus tipos | `customer_id STRING, …` |
| **Schema evolution** | Que una tabla acepte columnas nuevas | `experiment_id` en web_events |
| **schemaHints** | Pistas de tipo para Auto Loader sin fijar el esquema completo | CSV en bronze |
| **Llegada tardía** | Un dato que llega días después de cuando ocurrió | 3 % de los pedidos |
| **Deduplicar** | Eliminar registros repetidos | Silver |

## 6 · Calidad

| Término | Qué es | En NovaMart |
|---|---|---|
| **Regla de calidad / expectation** | Condición que un dato debe cumplir | `quantity > 0` |
| **Cuarentena** | Lugar donde se apartan los datos que no cumplen las reglas | `ops.quarantine` |
| **Quality gate** | Control final que hace fallar el job si los datos no tienen sentido | Tarea `data_quality_gate` |
| **Invariante** | Algo que siempre tiene que ser verdad | "Toda venta tiene producto" |

## 7 · Modelado

| Término | Qué es | En NovaMart |
|---|---|---|
| **Modelo dimensional (Kimball)** | Organizar los datos en hechos y dimensiones | Gold |
| **Fact (hecho)** | Algo que pasó y se mide | `fact_sales` |
| **Dimensión** | El contexto del hecho: quién, qué, dónde, cuándo | `dim_customer`, `dim_date` |
| **Modelo estrella** | Un hecho en el centro y sus dimensiones alrededor | Diagrama `03_modelo_estrella` |
| **Grano** | Qué representa exactamente una fila de una fact | `fact_sales`: una línea de pedido |
| **Clave natural** | El id que viene de la fuente | `customer_id` |
| **Clave subrogada** | Un id propio del modelo, por versión | `customer_key` |
| **SCD Tipo 1** | Pisar el valor viejo | `stores` |
| **SCD Tipo 2** | Guardar una versión nueva con fechas de vigencia | `dim_customer` |
| **Miembro desconocido** | Fila `-1` para hechos sin dimensión conocida | "Cliente no identificado" |
| **Join point-in-time** | Unir con la versión vigente en la fecha del hecho | Ventas con la versión del cliente de ese día |
| **OBT** | *One Big Table*: todo en una tabla ancha | Vista `v_sales_enriched` |
| **Métrica aditiva / semi-aditiva** | Se puede sumar en todo / sólo en algunas dimensiones | Ventas / stock (no se suma en el tiempo) |
| **RFM** | Segmentar clientes por Recencia, Frecuencia y Monto | `mart_customer_rfm` |
| **Cohorte** | Grupo de clientes que empezaron en el mismo período | `mart_cohort_retention` |

## 8 · Orquestación y despliegue

| Término | Qué es | En NovaMart |
|---|---|---|
| **Job** | Receta que ejecuta tareas en orden | `novamart-e2e-pipeline` |
| **Task (tarea)** | Cada paso de un job | `bronze_ingest` |
| **DAG** | El dibujo de tareas y sus dependencias | Pestaña *Tasks* del job |
| **Condition task** | Tarea if/else que decide qué rama sigue | `should_generate` |
| **for_each** | Correr la misma tarea con varios valores | Silver × 8 entidades |
| **Repair run** | Re-ejecutar sólo lo que falló | Primera ejecución |
| **Schedule** | Horario de ejecución automática | Diario 05:30 (pausado en dev) |
| **Bundle** (*Declarative Automation Bundle*, ex *Asset Bundle*) | Proyecto descrito en YAML que se despliega con un comando | `databricks.yml` |
| **Deploy** | Crear o actualizar los recursos en el workspace | `databricks bundle deploy -t dev` |
| **Target / ambiente** | Destino del deploy: dev o prod | `-t dev` |
| **CLI** | Programa de terminal para manejar Databricks | `databricks …` |
| **Token** | Contraseña para programas | `DATABRICKS_TOKEN` |
| **Git / GitHub** | Control de versiones / sitio donde se guarda el repo | Este repositorio |
| **Commit / Branch / PR** | Un cambio guardado / una línea de trabajo paralela / pedido para unir cambios | PR #1 |
| **CI/CD** | Probar y desplegar automáticamente cada cambio | GitHub Actions |
| **Test** | Código que verifica que otro código funcione | `tests/` |

## 9 · Consumo e IA

| Término | Qué es | En NovaMart |
|---|---|---|
| **BI / Dashboard** | Tableros visuales para el negocio | AI/BI Dashboards |
| **Genie Agent** (ex *Genie space*) | Espacio donde se pregunta a los datos en lenguaje natural | Paso 9 de la guía |
| **Genie Code** (ex *Databricks Assistant*) | Asistente que escribe y arregla código | Dentro de notebooks |
| **Genie One** | Interfaz simple para usuarios de negocio | Para gerentes y vendedores |
| **Metric view** | Definición única y oficial de una métrica | Propuesta en `09_genie_a_fondo.md` |
| **Benchmark (Genie)** | Preguntas con respuesta correcta para medir la precisión | Propuesta en `09_genie_a_fondo.md` |
