# 12 · Por qué así y no de otra manera

> En data engineering **casi nunca hay una única forma correcta**: hay decisiones con ventajas y
> costos. Este documento explica cada decisión importante del proyecto con el mismo formato:
>
> * **Qué hicimos**
> * **La alternativa** (lo que otro equipo podría haber hecho)
> * **Por qué elegimos esto**
> * **Qué pasaría si no lo hiciéramos** (la forma más rápida de entender para qué sirve algo)
> * **Cuándo elegirías la otra opción** (porque ninguna decisión es universal)
>
> La versión técnica y resumida de estas decisiones está en [Arquitectura](01_arquitectura.md#decisiones-de-diseño-adr-resumidos).
> Si una palabra no la conocés, está en el [Glosario](13_glosario.md).

**Índice:** [Plataforma](#plataforma) · [Datos de entrada](#entrada) · [Capas](#capas) ·
[Calidad](#calidad) · [Modelado](#modelado) · [Rendimiento](#rendimiento) · [Código](#codigo) ·
[Operación](#operacion)

---

<a id="plataforma"></a>
## A · Plataforma

### A1 · Un *lakehouse* en Databricks
* **Qué hicimos:** guardar todo en archivos abiertos (Delta Lake) y procesarlos con Spark y SQL en Databricks.
* **Alternativa:** un *data warehouse* clásico (BigQuery, Snowflake, Redshift) o una base de datos (PostgreSQL).
* **Por qué:** el lakehouse junta lo bueno de los dos mundos: almacenamiento barato y en formato
  abierto (como un *data lake*) con transacciones, SQL rápido y permisos (como un *warehouse*). Y en
  la misma plataforma tenés ingeniería, BI, Genie y ML.
* **Si no:** con PostgreSQL, analizar 14 millones de eventos sería lento y caro de escalar; con
  archivos sueltos sin Delta, dos procesos escribiendo a la vez podrían corromper una tabla.
* **Cuándo elegirías otra:** si la empresa ya tiene todo en Snowflake o BigQuery y sólo hace SQL, cambiar no se justifica.

### A2 · Unity Catalog con un catálogo por ambiente (`novamart_dev` / `novamart`)
* **Qué hicimos:** todo vive en `catálogo.esquema.tabla`, con un catálogo para dev y otro para prod.
* **Alternativa:** un solo catálogo con prefijos en los nombres de tabla (`dev_orders`, `prod_orders`).
* **Por qué:** los permisos se dan por catálogo ("los analistas leen prod, no dev") y es imposible
  mezclar por error datos de prueba con datos reales.
* **Si no:** un `DROP TABLE` en pruebas podría borrar una tabla de producción con nombre parecido.
* **Cuándo otra:** en un workspace personal y chiquito, un solo catálogo (`workspace`) alcanza.

### A3 · Cómputo *serverless*
* **Qué hicimos:** ningún job declara clusters; Databricks pone las máquinas.
* **Alternativa:** definir clusters propios (tipo de máquina, cantidad de nodos).
* **Por qué:** arranca en segundos, no hay que administrar nada y es lo único disponible en Free Edition.
* **Si no:** habría que elegir y mantener máquinas, y pagar mientras están encendidas sin usar.
* **Cuándo otra:** cargas enormes y constantes donde un cluster bien dimensionado sale más barato,
  o cuando se necesita una configuración especial.

---

<a id="entrada"></a>
## B · Datos de entrada

### B1 · Datos sintéticos generados por el propio proyecto
* **Qué hicimos:** un generador en Spark (`src/novamart/datagen/`) que simula 8 sistemas de origen.
* **Alternativa:** un dataset público (Kaggle, NYC Taxi, TPC-H).
* **Por qué:** los datasets públicos vienen **limpios y estáticos**. El generador produce volumen
  controlable, errores realistas, días nuevos (para probar lo incremental) y un negocio con historia
  (estacionalidad, Black Friday, crecimiento).
* **Si no:** no podrías practicar duplicados, llegadas tardías, columnas nuevas ni carga diaria,
  que son el 70 % del trabajo real.
* **Cuándo otra:** para aprender sólo SQL, un dataset público alcanza.

### B2 · Archivos en un *volume* (landing) en vez de leer directo de las fuentes
* **Qué hicimos:** las fuentes dejan archivos en `landing/<fuente>/ingest_date=AAAA-MM-DD/`.
* **Alternativa:** que el pipeline consulte las bases de datos de origen directamente.
* **Por qué:** los archivos **desacoplan**. Si el pipeline se cae, los archivos esperan; si la
  fuente se cae, el pipeline procesa lo que ya llegó. Además, tenés el registro exacto de qué llegó y cuándo.
* **Si no:** consultar la base de datos de la web en horario comercial la puede poner lenta para los clientes.
* **Cuándo otra:** con conectores de CDC (Lakeflow Connect) que leen el log de cambios sin cargar la base de origen.

---

<a id="capas"></a>
## C · Capas (arquitectura *medallion*)

### C1 · Cuatro capas: landing → bronze → silver → gold
* **Qué hicimos:** cada capa tiene una sola responsabilidad.
* **Alternativa:** un solo paso que lea los archivos y escriba las tablas finales.
* **Por qué:** si un paso falla o tiene un error, **se rehace sólo ese paso** desde la capa
  anterior. Cada capa es un punto de control.
* **Si no:** un error en la limpieza obliga a re-leer y re-procesar todo desde los archivos, y si los
  archivos ya no están, **el dato se perdió para siempre**.
* **Cuándo otra:** para un análisis de una sola vez, sobre un archivo chico, las capas son burocracia.

### C2 · Bronze guarda todo, incluso lo que está mal
* **Qué hicimos:** bronze copia los archivos tal cual y agrega linaje (`_source_file`, `_ingested_at`).
  Lo que no encaja en el esquema va a `_rescued_data`.
* **Alternativa:** limpiar al ingerir.
* **Por qué:** "mal" depende de las reglas de hoy. Bronze es el registro histórico: si mañana cambia
  una regla, se reprocesa desde ahí.
* **Si no:** un error en una regla de limpieza borraría datos que no se pueden recuperar.
* **Cuándo otra:** casi nunca. Es una de las prácticas más universales.

### C3 · Auto Loader con `trigger(availableNow=True)` para bronze
* **Qué hicimos:** Auto Loader descubre **sólo los archivos nuevos** y procesa todo lo pendiente; después termina.
* **Alternativa A:** leer toda la carpeta cada vez (batch completo).
* **Alternativa B:** un stream corriendo las 24 horas.
* **Por qué:** con A, el día 1.000 releerías 1.000 días de archivos. Con B pagás máquinas encendidas
  todo el día para datos que llegan una vez por día. `availableNow` tiene lo mejor de los dos: la
  memoria del streaming (el *checkpoint* recuerda qué leyó) con el costo de un batch.
* **Si no:** el job diario no tardaría 7 minutos, sino lo mismo que la carga completa, y cada vez más.
* **Cuándo otra:** si necesitás datos con segundos de demora (fraude, alertas), un stream continuo.

### C4 · *Schema evolution* (`addNewColumns`), con reinicio en código
* **Qué hicimos:** cuando aparece una columna nueva (`experiment_id`), la tabla la incorpora sola.
  El código reinicia el stream cuando Auto Loader frena para actualizar el esquema.
* **Alternativa:** esquema fijo, que rechaza o descarta lo nuevo.
* **Por qué:** las fuentes cambian sin avisar. Mejor recibir la columna y decidir después qué hacer,
  que perderla.
* **Si no:** pasó en la primera ejecución: el job falló al encontrar `experiment_id` y había gastado
  los reintentos ([bitácora, sección 7](07_bitacora_primera_ejecucion.md)).
* **Cuándo otra:** en fuentes reguladas, donde un cambio de esquema tiene que pasar por aprobación,
  se prefiere fallar y avisar.

### C5 · Silver con streaming + `foreachBatch` + `MERGE`
* **Qué hicimos:** silver lee sólo las filas nuevas de bronze y las incorpora con `MERGE`.
* **Alternativa:** borrar y reescribir silver completa cada día (*overwrite*).
* **Por qué:** `MERGE` inserta lo nuevo y no duplica lo que ya existe, así que es **idempotente**.
  Y procesar sólo lo nuevo es mucho más barato.
* **Si no:** con *overwrite*, cada día procesarías 3 años; con un `INSERT` simple, los re-envíos
  duplicarían pedidos.
* **Cuándo otra:** tablas chicas (como `stores`, 40 filas), donde reescribir todo es trivial.

---

<a id="calidad"></a>
## D · Calidad de datos

### D1 · Cuarentena en vez de borrar o frenar
* **Qué hicimos:** cada regla tiene una acción: `warn` (avisar), `quarantine` (apartar) o fallar.
  Las filas apartadas van a `ops.quarantine` con el motivo.
* **Alternativa A:** descartar en silencio. **Alternativa B:** frenar todo el pipeline ante cualquier error.
* **Por qué:** A esconde problemas; B deja al negocio sin datos por 2 filas malas entre millones. La
  cuarentena deja pasar lo bueno y **guarda la evidencia** de lo malo.
* **Si no:** en la primera ejecución, 1.420 líneas con cantidad negativa habrían restado ventas, o
  habrían frenado todo.
* **Cuándo otra:** datos críticos (pagos, salud), donde **ningún** error es tolerable y conviene frenar.

### D2 · Un *quality gate* al final del job
* **Qué hicimos:** la última tarea revisa invariantes del modelo y **hace fallar el job** si se rompen.
* **Alternativa:** confiar en que, si nada falló antes, todo está bien.
* **Por qué:** un pipeline puede terminar "verde" y producir números absurdos (por ejemplo, ventas en
  cero por un join mal hecho). El gate es la última barrera antes de que el gerente vea un número incorrecto.
* **Si no:** el error se descubre cuando alguien del negocio pregunta "¿por qué vendimos 0 ayer?".
  Peor: nadie lo descubre.
* **Cuándo otra:** nunca se saca. Se ajusta qué revisa.

---

<a id="modelado"></a>
## E · Modelado

### E1 · Modelo estrella (Kimball) en gold
* **Qué hicimos:** tablas de hechos (lo que pasó) rodeadas de dimensiones (el contexto).
* **Alternativas:** una tabla gigante (OBT) o *Data Vault* (muy normalizado y auditable).
* **Por qué:** la estrella es el formato que mejor entienden los analistas, las herramientas de BI y
  **Genie**, y es eficiente para sumar y filtrar.
* **Si no:** con una OBT, un cambio en un cliente obliga a reescribir millones de filas; con Data
  Vault, cada consulta del negocio necesita 10 joins.
* **Cuándo otra:** OBT para un tablero puntual (el proyecto la ofrece como vista); Data Vault en
  empresas con muchísimas fuentes y requisitos de auditoría fuertes.

### E2 · Historia con SCD Tipo 2 en clientes y productos
* **Qué hicimos:** cuando un cliente o producto cambia, se guarda una versión nueva con fechas de vigencia.
* **Alternativa:** SCD Tipo 1, pisar el valor viejo.
* **Por qué:** para que el pasado no cambie. Una venta de 2023 tiene que mostrar el precio y la ciudad de 2023.
* **Si no:** si un producto cambia de categoría, las ventas históricas de la categoría vieja
  "desaparecen" y los reportes del año pasado dejan de coincidir con lo que se informó en su momento.
* **Cuándo otra:** atributos donde la historia no importa (corregir un error de tipeo en un nombre) o
  tablas chicas como `stores` (Tipo 1 en este proyecto).

### E3 · SCD2 programado a mano **y** con `AUTO CDC` declarativo
* **Qué hicimos:** el mismo SCD2 en Python (`src/novamart/silver/scd2.py`) y en SQL declarativo.
* **Por qué:** escribirlo a mano enseña **cómo funciona** por dentro; la versión declarativa enseña
  **la herramienta** que lo hace por vos. En una entrevista te pueden preguntar las dos cosas.

### E4 · Claves subrogadas calculadas con *hash* (`xxhash64(id, valid_from)`)
* **Qué hicimos:** la clave de cada versión se calcula a partir del id y de la fecha de inicio.
* **Alternativa:** una columna autonumérica (`IDENTITY`: 1, 2, 3…).
* **Por qué:** el hash da **siempre la misma clave** para la misma versión, aunque se reconstruya la
  tabla. Además se calcula en paralelo, sin un contador central.
* **Si no:** con autonumérico, reconstruir `dim_customer` podría asignar otros números, y todas las
  ventas quedarían apuntando al cliente equivocado.
* **Cuándo otra:** sistemas donde la clave tiene que ser corta y secuencial por requisitos externos.

### E5 · Miembro "desconocido" (`-1`) en cada dimensión
* **Qué hicimos:** una fila especial para hechos sin cliente o sin tienda.
* **Alternativa:** dejar la clave vacía (NULL).
* **Por qué:** un `JOIN` con NULL **hace desaparecer** la fila de los totales. Con `-1`, las ventas
  anónimas siguen sumando y se pueden filtrar explícitamente.
* **Si no:** el total de ventas de un tablero no coincidiría con el total contable.

---

<a id="rendimiento"></a>
## F · Rendimiento

### F1 · `replaceWhere` por fechas afectadas en las facts
* **Qué hicimos:** gold calcula qué fechas recibieron datos nuevos (incluidas llegadas tardías) y
  reemplaza **sólo esas fechas**.
* **Alternativa:** `MERGE` fila por fila, o recalcular toda la tabla.
* **Por qué:** reemplazar un día completo es más barato que comparar millones de filas una a una, y
  es idempotente: hacerlo dos veces da lo mismo.
* **Si no:** recalcular todo cada día crecería sin límite; un `INSERT` simple duplicaría ventas en
  cada reintento.
* **Prueba real:** el job diario agregó exactamente las 2.395 líneas del día nuevo.

### F2 · *Liquid clustering* en vez de particiones
* **Qué hicimos:** `fact_sales` se agrupa físicamente por `order_date` y `store_key` (el ícono de
  llave en la captura de columnas).
* **Alternativa:** `PARTITIONED BY` fecha, la forma clásica.
* **Por qué:** particionar por día con poco volumen genera miles de archivos chiquitos (lento); el
  clustering se adapta solo y se puede cambiar sin reescribir la tabla.
* **Si no:** una consulta "ventas de marzo" leería toda la tabla en vez de sólo los archivos de marzo.

### F3 · Marts precalculados en SQL
* **Qué hicimos:** las respuestas frecuentes (ventas diarias, RFM, cohortes) se calculan una vez por
  día, en SQL puro (`sql/gold/marts/`).
* **Por qué en SQL:** los mantiene un analista, que sabe SQL pero no necesariamente Python.
* **Por qué precalcular:** que el tablero abra en 1 segundo en vez de 30, y no recalcular lo mismo
  cada vez que alguien lo abre.

---

<a id="codigo"></a>
## G · Código y organización

### G1 · La lógica en un paquete Python; los notebooks, "delgados"
* **Qué hicimos:** `src/novamart` tiene la lógica; cada notebook son 10 a 30 líneas que la llaman.
* **Alternativa:** todo dentro de notebooks.
* **Por qué:** el código del paquete se **testea** en tu PC, se **revisa** en PRs y se **reutiliza**.
  Un notebook de 2.000 líneas no se puede testear ni revisar bien.
* **Si no:** cada cambio sería "probar en producción y rezar".

### G2 · Tests locales con Spark + Delta (sin Databricks)
* **Qué hicimos:** 13 tests (unitarios e integración) que corren en tu PC o en GitHub.
* **Por qué:** detectan errores en segundos y gratis, antes de gastar cómputo.
* **Límite conocido (y documentado):** no cubren lo que sólo existe en Databricks, como Auto Loader.
  Por eso la bitácora propone sumar una prueba en Databricks al CI.

### G3 · Infraestructura como código (bundle) en vez de clics
* **Qué hicimos:** jobs, pipeline y esquema definidos en YAML (`databricks.yml`, `resources/`).
* **Por qué:** se versiona, se revisa, se replica idéntico en dev y prod, y se recrea si alguien
  borra algo por error.
* **Si no:** "¿quién cambió el horario del job y cuándo?" no tendría respuesta.

### G4 · Tablas de `ops` creadas en `setup`
* **Qué hicimos:** las tablas de auditoría y cuarentena se crean al principio, antes de las tareas paralelas.
* **Por qué:** si dos tareas del `for_each` intentan crear la misma tabla al mismo tiempo, una falla
  con "la tabla ya existe" (una *condición de carrera*).

---

<a id="operacion"></a>
## H · Operación

### H1 · Un job con dependencias, condición (if/else) y `for_each`
* **Qué hicimos:** un DAG donde cada tarea espera a las que necesita; `should_generate` decide si se
  generan datos; silver corre 8 veces en paralelo con el mismo notebook.
* **Alternativas:** un único notebook gigante, o 8 tareas de silver copiadas a mano.
* **Por qué:** si falla una tarea, se repara sólo esa (*repair run*); el `for_each` evita copiar y
  pegar 8 tareas iguales; la condición permite usar el mismo job para la carga completa y la diaria.

### H2 · Dos ambientes (dev y prod) con el mismo código
* **Por qué:** probar cambios sin riesgo para el negocio. El modo `development` prefija los nombres
  con tu usuario y pausa los horarios para que nada corra solo.

### H3 · CI/CD con GitHub Actions
* **Por qué:** cada cambio se prueba automáticamente; desplegar es un botón, no un comando desde la PC
  de alguien. Queda registrado quién desplegó qué y cuándo.

### H4 · Reparar en vez de re-ejecutar todo
* **Por qué:** *repair run* re-ejecuta sólo lo que falló. En la primera ejecución ahorró 25 minutos
  de generación de datos y cuota de Free Edition.

---

## Para practicar: elegí una decisión y "rompela" (en dev)

| Experimento | Qué cambiar | Qué deberías observar |
|---|---|---|
| Sin cuarentena | En `src/novamart/silver/rules.py`, cambiá una regla `quarantine` por `warn` | Las cantidades negativas llegan a `fact_sales` y bajan las ventas |
| Sin idempotencia | Corré el job diario dos veces seguidas | `fact_sales` **no** cambia la segunda vez (eso es idempotencia) |
| Sin incremental | Borrá el checkpoint de bronze de una fuente | Auto Loader relee todos los archivos (y tarda mucho más) |
| Sin SCD2 | Consultá ventas por ciudad de un cliente que se mudó, con `is_current = true` en vez del join por fecha | Las ventas viejas "se mudan" de ciudad |

> Hacé un experimento por vez y anotá qué predijiste y qué pasó. Esa diferencia es el aprendizaje.
