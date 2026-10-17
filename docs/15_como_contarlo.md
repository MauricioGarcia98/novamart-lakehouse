# 15 · Cómo contar este proyecto (para entrevistas y recruiters)

> Para dos públicos: **quien hizo el proyecto** y tiene que contarlo en una entrevista, y **quien
> evalúa** (recruiter o entrevistador técnico) y quiere saber qué demuestra en 15 minutos.

---

## 1 · Qué demuestra (para el que evalúa)

| Habilidad | Evidencia concreta en el repo |
|---|---|
| Ingesta incremental de varias fuentes y formatos | `src/novamart/ingestion/bronze.py`: Auto Loader con CSV, JSON y Parquet |
| Calidad de datos con criterio | `src/novamart/silver/rules.py`: reglas `warn` / `quarantine`, cuarentena con motivo, quality gate |
| Modelado dimensional profesional | Gold: estrella Kimball, SCD2, claves subrogadas, 4 tipos de facts |
| SQL analítico | `sql/gold/marts/`: RFM, cohortes, market basket, embudo |
| Ingeniería de software aplicada a datos | Paquete testeable, 13 tests, lint, CI/CD |
| Infraestructura como código | `databricks.yml` + `resources/`: jobs, pipeline, ambientes dev/prod |
| **Resolver problemas reales** | [Bitácora](07_bitacora_primera_ejecucion.md): 4 errores reales diagnosticados y corregidos |
| Comunicación | Documentación para principiantes, diagramas, guía con capturas |

**Lo que lo distingue de un proyecto típico de portfolio:** no usa un dataset limpio de Kaggle. Los
datos llegan sucios a propósito, el pipeline es incremental e idempotente (probado con números
reales) y hay un registro honesto de lo que falló y de cómo se arregló.

---

## 2 · El pitch de 2 minutos (para quien lo cuenta)

Usá esta estructura (**contexto → problema → qué hice → resultado → qué aprendí**) y adaptala con tus palabras:

> "Construí un lakehouse de punta a punta en Databricks para un retailer ficticio de Latinoamérica
> con tiendas, web y app.
>
> **El problema:** los datos llegan de 8 sistemas distintos, en CSV, JSON y Parquet, con
> duplicados, fechas en varios formatos, llegadas tardías y columnas que aparecen de golpe.
>
> **Lo que hice:** un pipeline en capas. Bronze ingiere sólo los archivos nuevos con Auto Loader,
> silver limpia, deduplica y guarda historia con SCD2, y gold arma un modelo estrella con marts
> para el negocio. Las reglas de calidad apartan lo inválido a una cuarentena en vez de frenar todo,
> y un quality gate final hace fallar el job si los números no cierran. Todo se despliega como código
> con un bundle, en dos ambientes, con CI/CD en GitHub Actions.
>
> **El resultado:** procesa 24 millones de filas en Free Edition. La carga completa tarda unos 50
> minutos y la diaria incremental 7, agregando exactamente los datos nuevos, sin duplicar nada.
>
> **Lo que aprendí:** en la primera ejecución real fallaron 4 cosas que los tests locales no
> detectaban, como una incompatibilidad de Auto Loader entre esquema fijo y evolución de esquema.
> Las diagnostiqué, las arreglé y las documenté. Eso me enseñó por qué hace falta probar también en
> el entorno real."

---

## 3 · Historias para preguntas de comportamiento (formato STAR)

**"Contame de un problema técnico difícil que resolviste."**
* **Situación:** primera ejecución real del pipeline en Databricks.
* **Tarea:** que la ingesta bronze funcionara con 8 fuentes.
* **Acción:** leí el error completo (`CF_ADD_NEW_NOT_SUPPORTED`), entendí que Auto Loader no permite
  un esquema fijo junto con la evolución de esquema, cambié el contrato a `schemaHints` y usé
  *repair run* para no regenerar 25 minutos de datos.
* **Resultado:** bronze cargó las 8 fuentes en 3 minutos.

**"¿Alguna vez algo funcionó en tu máquina y falló en producción?"**
* Los 13 tests pasaban y el job falló, porque Auto Loader no existe en Spark local. Lo resolví y
  propuse sumar al CI una ejecución en Databricks con un volumen mínimo.

**"¿Cómo manejás datos de mala calidad?"**
* Por niveles: lo que se puede corregir se corrige en silver (formatos de fecha, emails); lo inválido
  va a cuarentena con su motivo; lo que rompe invariantes del modelo hace fallar el quality gate. En
  la primera ejecución: 23 reglas, 2.121 registros en cuarentena, el pipeline completo terminó OK.

**"¿Cómo garantizás que un reproceso no duplique datos?"**
* Idempotencia: checkpoints en bronze, MERGE en silver, replaceWhere por fechas en gold. Lo
  verifiqué: el job diario agregó exactamente las 2.395 líneas del día nuevo y quedaron 0 pedidos
  repetidos.

---

## 4 · Preguntas técnicas que te pueden hacer (y dónde está la respuesta)

| Pregunta | Dónde repasarla |
|---|---|
| ¿Por qué arquitectura medallion? | [Por qué así, C1](12_por_que_asi.md#capas) |
| ¿Diferencia entre SCD1 y SCD2? ¿Cuándo usarías cada uno? | [Por qué así, E2](12_por_que_asi.md#modelado) |
| ¿Cómo funciona Auto Loader? ¿Qué es un checkpoint? | [Glosario](13_glosario.md) + `learning/12_structured_streaming.py` |
| ¿MERGE o replaceWhere? ¿Cuándo cada uno? | [Por qué así, C5 y F1](12_por_que_asi.md#rendimiento) |
| ¿Qué es el skew y cómo lo resolvés? | `learning/10_spark_performance_internals.py` |
| ¿Particiones o liquid clustering? | [Por qué así, F2](12_por_que_asi.md#rendimiento) |
| ¿Cómo testeás un pipeline de Spark? | `tests/` + [Por qué así, G2](12_por_que_asi.md#codigo) |
| ¿Cómo desplegás a producción? | [Despliegue](05_despliegue.md) + `.github/workflows/ci.yml` |
| ¿Cómo prepararías los datos para Genie? | [Genie a fondo, sección 2](09_genie_a_fondo.md) |

---

## 5 · Sé honesto sobre los límites (suma puntos)

Un buen entrevistador valora que conozcas los límites de tu proyecto. Decilos vos antes de que te pregunten:

* **Los datos son sintéticos.** Las fuentes son simuladas; en un trabajo real habría conectores (CDC, Kafka, APIs).
* **Corre en Free Edition** con `scale_factor` 0.1 (24 M de filas). El diseño apunta a escalar, pero
  no lo probé a 300 GB.
* **Prod está definido, pero no desplegado.**
* **El CI no prueba en Databricks:** es la mejora pendiente que surgió de la bitácora.

Y cerrá con lo que harías después (de [Hacerlo más real](08_hacerlo_mas_real.md)): fuentes reales,
monitoreo de costos con `system.billing.usage`, alertas y una capa Genie con benchmarks.
