# 10 · Preguntas frecuentes (las que se hace todo el que empieza)

> Cada respuesta tiene tres partes: **la respuesta corta**, **el porqué** y **dónde verlo** en el
> repo o en Databricks. Si una respuesta usa una palabra que no conocés, buscala en el
> [Glosario](12_glosario.md).
>
> Los números de este documento son **reales**: salen de la primera ejecución del 17/10/2026 con
> `scale_factor = 0.1` en Free Edition.

**Índice:** [El proyecto](#proyecto) · [Datos](#datos) · [Capas](#capas) · [Tablas y modelo](#modelo) ·
[Ejecución](#ejecucion) · [Código](#codigo) · [Ambientes y despliegue](#ambientes) ·
[Almacenamiento, límites y costos](#limites) · [Seguridad](#seguridad) · [Consumo y Genie](#consumo) ·
[Aprender y trabajar](#carrera)

---

<a id="proyecto"></a>
## 1 · El proyecto

### ¿NovaMart existe?
**No.** Es una empresa inventada, y todos sus datos son sintéticos.
**Por qué:** así podés trabajar con un negocio "completo" (tiendas, web, logística, stock) sin
pedirle permiso a nadie ni manejar datos personales reales.

### ¿Qué problema resuelve este proyecto?
Convierte datos **desparramados, sucios y en distintos formatos** en **tablas confiables** que el
negocio puede consultar.
**Por qué importa:** sin esto, cada área calcula "las ventas" a su manera y nadie coincide. Con
esto hay **una sola definición** de cada número.
**Dónde verlo:** compará `bronze.orders` (1.203.610 filas, con duplicados) con `gold.fact_sales`
(limpia y lista para sumar).

### ¿Qué hace un Data Engineer, en una frase?
Construye y mantiene las "cañerías" que llevan los datos desde donde se generan hasta donde se usan,
asegurando que lleguen **completos, correctos, a tiempo y seguros**.

### ¿Por qué Databricks y no Excel o una base de datos común?
* **Excel** se cae con un millón de filas, y acá hay 14 millones de eventos web sólo en dev.
* Una **base de datos tradicional** (como PostgreSQL) sirve para operar un sistema (registrar una
  venta), pero sufre cuando tiene que analizar años de historia de toda la empresa a la vez.
* **Databricks** reparte el trabajo entre muchas máquinas (con Spark) y guarda los datos en
  archivos abiertos y baratos (Delta Lake). Está hecho para volumen y análisis.

**Cuándo NO usarías Databricks:** para una planilla de 5.000 filas o la base de datos de una app
chica. Sería como usar un camión para llevar una carta.

---

<a id="datos"></a>
## 2 · Datos

### ¿De dónde vienen los datos? ¿Por qué hay "fuentes" pero nada conectado?
**Respuesta corta:** no hay nada conectado porque **las fuentes son simuladas**. Un programa del
proyecto (`src/novamart/datagen/`) imita a 8 sistemas de origen y escribe sus archivos en
`/Volumes/novamart_dev/landing/raw/landing/`. Hay una carpeta por fuente y otra por día.

**Por qué así:**
* Una empresa real nunca te daría sus datos para practicar.
* El generador permite controlar el **volumen** (`scale_factor`) y los **errores** (puestos a propósito).
* Desde *landing* en adelante el pipeline **no sabe ni le importa** si los archivos son simulados o
  reales. Todo lo que aprendés del bronze en adelante es igual que en una empresa.

**En una empresa real** las fuentes se conectarían así:

| Fuente simulada | Sistema real típico | Cómo se conectaría |
|---|---|---|
| `orders`, `order_status_events` | Base de datos del e-commerce (PostgreSQL, MySQL) | Lakeflow Connect o CDC (captura de cambios) |
| `customers`, `products` | CRM / ERP (Salesforce, SAP) | Conector nativo o exportación diaria |
| `web_events` | Analítica web/app (eventos en tiempo real) | Kafka / Event Hubs → streaming |
| `stores`, `inventory_snapshots` | Sistema de tiendas / stock | Archivos que el sistema deja en un bucket |
| `returns` | Sistema de atención al cliente | API o archivos diarios |

**Dónde verlo:** Catalog → `novamart_dev` → `landing` → volumen `raw` (captura en la
[guía, Paso 7](06_guia_desde_la_web.md)). Para conectar fuentes reales, mirá
[Hacerlo más real](08_hacerlo_mas_real.md#2--data-engineer-fuentes-y-operación-reales).

### ¿Por qué los datos vienen "sucios" a propósito?
Porque **los datos reales siempre vienen sucios**: duplicados, fechas en 3 formatos, emails en
mayúsculas, cantidades negativas, columnas nuevas que aparecen sin aviso. Si practicás con datos
perfectos, no aprendés el 70 % del trabajo.
**Dónde verlo:** la tabla de "problemas inyectados" en [Fuentes y datos](02_fuentes_y_datos.md).
Cada problema dice cómo se resuelve.

### ¿Qué es el `scale_factor`?
Un número que controla **cuántos datos se generan**:

| `scale_factor` | Pedidos | Tamaño aproximado | Para qué |
|---:|---:|---:|---|
| 0.002 | 24 mil | 50 MB | Tests en tu PC |
| **0.1** | **1,2 millones** | **~3 GB** | **Free Edition (lo que usamos)** |
| 1 | 12 millones | ~30 GB | Workspace pago |
| 10 | 120 millones | ~300 GB | Practicar performance "de verdad" |

**Por qué existe:** el mismo código tiene que funcionar con 50 MB y con 300 GB. Probar con poco
y escalar es como trabaja una empresa.

### ¿Por qué hay CSV, JSON y Parquet? ¿No sería más fácil todo igual?
Sería más fácil, pero no sería real. En una empresa **cada sistema exporta como puede**: el sistema
viejo de tiendas, CSV; la web moderna, JSON; el de stock, Parquet. El trabajo del Data Engineer es
**unificarlo**. En bronze cada fuente se lee con su formato; desde silver, todo es Delta.

### ¿Qué pasa si corro el generador dos veces?
Genera **exactamente los mismos datos** (es *determinístico*: usa una semilla fija). Y como el
pipeline es **idempotente**, procesar dos veces lo mismo no duplica nada (ver
[Ejecución](#ejecucion)).

### ¿Por qué el generador tarda 25 minutos y el resto sólo 25?
Porque "inventar" 3 años de un negocio (1,2 millones de pedidos, 14 millones de eventos, con
estacionalidad y errores realistas) es mucho trabajo de cómputo. En la vida real ese paso no existe:
los datos ya llegan solos.

---

<a id="capas"></a>
## 3 · Capas (landing, bronze, silver, gold)

### ¿Por qué tantas capas? ¿No se puede ir directo de los archivos a las tablas finales?
Se puede, y es el error más común. Si limpiás y transformás todo en un solo paso y descubrís un
error en la limpieza, **perdiste el dato original** y tenés que volver a pedirlo.
Con capas:
* **Bronze** guarda la copia fiel: siempre podés reprocesar desde ahí.
* **Silver** tiene "la verdad" limpia, reutilizable por muchas áreas.
* **Gold** tiene vistas armadas para preguntas concretas del negocio.

**Analogía:** en una cocina no cocinás directo sobre el camión del proveedor. Primero recibís,
después lavás y cortás, y recién ahí armás el plato.

### ¿Por qué bronze guarda "basura"? ¿No conviene filtrar lo malo apenas llega?
No, porque **"malo" depende de las reglas de hoy**, y las reglas cambian. Una venta con cantidad
negativa hoy va a cuarentena; mañana el negocio puede decidir que es una devolución válida. Si la
hubieras borrado en bronze, no podrías recuperarla. Bronze es la caja negra del avión: registra
todo.
**Dónde verlo:** `bronze.orders` tiene 1.203.610 filas y `silver.orders`, 1.199.310.

### ¿Por qué bronze tiene más filas que silver?
Por dos motivos, y los dos son buenos:
1. **Duplicados eliminados:** 3.599 pedidos llegaron repetidos (re-envíos del sistema de origen).
2. **Cuarentena:** 701 pedidos rompían una regla ("pedido online sin cliente") y se apartaron.

`1.203.610 − 3.599 − 701 = 1.199.310` ✔️

**Dónde verlo:** gráfico en la [bitácora, sección 9](07_bitacora_primera_ejecucion.md).

### ¿Qué es la cuarentena y por qué no borrar directamente lo que está mal?
Es una tabla (`ops.quarantine`) donde van los registros que **no pasan las reglas de calidad**,
junto con el motivo.
**Por qué no borrar:**
* Alguien tiene que poder revisar **qué** falló y avisarle al sistema de origen.
* Si son muchos, es una **alarma**: algo se rompió en la fuente.
* El pipeline **no se frena** por 2.121 registros malos entre millones, pero tampoco los esconde.

**Dónde verlo:**
```sql
SELECT table_name, failed_rules, record FROM novamart_dev.ops.quarantine LIMIT 20;
```

### ¿Qué es "landing" y por qué es una carpeta y no una tabla?
Es el **muelle de descarga**: los sistemas dejan **archivos**, no tablas. Un *volume* de Unity
Catalog es una carpeta gobernada (con permisos) donde viven esos archivos. Recién en bronze se
convierten en tabla.

---

<a id="modelo"></a>
## 4 · Tablas y modelo de datos

### ¿Qué diferencia hay entre una tabla "fact" y una "dim"?
* **Fact (hecho):** algo que **pasó** y se puede medir. "El 3/5 se vendieron 2 unidades del
  producto X por $500". Tiene muchas filas y números para sumar.
* **Dim (dimensión):** el **contexto** para describir el hecho. ¿Quién compró? ¿Qué producto? ¿Qué
  tienda? ¿Qué día? Tiene pocas filas y mucho texto.

Juntas forman una **estrella**: el hecho en el centro y las dimensiones alrededor. Es la forma que
mejor entienden las herramientas de BI, los analistas y Genie.
**Dónde verlo:** `gold.fact_sales` (2,8 millones de filas) con `gold.dim_product` (8.466 filas).
Diagrama en el [README](../README.md#-modelo-de-datos).

### ¿Por qué no hacer una sola tabla gigante con todo?
Existe y tiene nombre: **OBT** (*One Big Table*); el proyecto la incluye como vista
(`v_sales_enriched`). Es cómoda para consultar, pero:
* repite el nombre del cliente en cada una de sus compras (ocupa más);
* si el cliente cambia de ciudad, hay que reescribir millones de filas;
* mezcla grados de detalle distintos.

La estrella es el punto medio: fácil de consultar y fácil de mantener.

### ¿Qué es SCD2 y para qué sirve?
*Slowly Changing Dimension tipo 2*: una forma de **guardar la historia** de algo que cambia.
Si un cliente se muda de Córdoba a Rosario, en vez de pisar la ciudad se guardan **dos versiones**,
cada una con su fecha de inicio y de fin.

**Para qué:** para que una venta de 2023 siga contando como "Córdoba" aunque hoy el cliente viva en
Rosario. Sin SCD2, al mudarse el cliente **cambiaría la historia**: las ventas de Córdoba de 2023
bajarían de golpe.
**Dónde verlo:** `silver.customers_history` y `gold.dim_customer` (columnas `valid_from`,
`valid_to`, `is_current`). Diagrama `docs/diagrams/05_scd2.png`.

### ¿Por qué las dimensiones tienen una fila más ("-1")?
Es el **"miembro desconocido"**: una fila especial con clave `-1` y el texto "Cliente no
identificado". Las ventas en caja sin cliente registrado apuntan ahí.
**Por qué:** si esas ventas no tuvieran a dónde apuntar, un `JOIN` las haría **desaparecer** de los
totales. Con `-1` siempre suman.

### ¿Qué es una "clave subrogada" (`customer_key`) y por qué no usar el `customer_id` de siempre?
El `customer_id` viene del sistema de origen y **es el mismo para todas las versiones** del cliente
(SCD2). La `customer_key` identifica **una versión específica**, así la venta apunta exactamente a
"el cliente tal como era ese día".

---

<a id="ejecucion"></a>
## 5 · Ejecución

### ¿Qué es un job y por qué no correr los notebooks a mano?
Un **job** es una receta: "corré estos notebooks en este orden, con estas dependencias, reintentá si
falla y avisame". A mano se puede (Paso 5B de la guía), pero:
* hay que acordarse del orden y hacerlo todos los días;
* si falla el paso 4 a las 3 a.m., nadie se entera;
* no queda registro de qué se corrió y con qué parámetros.

Un job hace todo eso solo, y con un horario (*schedule*) corre sin que nadie toque nada.

### ¿Por qué la carga completa tardó 50 minutos y el día siguiente sólo 7?
Porque el pipeline es **incremental**: procesa **sólo lo nuevo**. La primera vez hubo que procesar
3 años de datos; el día siguiente, sólo 1 día (988 pedidos nuevos).
**Cómo lo sabe:** bronze usa *checkpoints* (un "señalador" que recuerda qué archivos ya leyó) y gold
usa *watermarks* (`ops.watermarks`).
**Dónde verlo:** [bitácora, sección 10](07_bitacora_primera_ejecucion.md).

### ¿Qué significa "idempotente" y por qué importa tanto?
Que **ejecutar algo una vez o diez veces da el mismo resultado**. Importa porque en producción las
cosas se re-ejecutan todo el tiempo: un reintento, un *repair*, alguien que aprieta "Run" dos veces.
Si el pipeline no fuera idempotente, cada re-ejecución **duplicaría las ventas**.
**Cómo se logra acá:** `MERGE` (inserta sólo lo que no existe) y `replaceWhere` (reemplaza un rango
de fechas completo en vez de agregarle filas).
**Prueba real:** después del job diario, `silver.orders` tiene **0** `order_id` repetidos.

### ¿Qué pasa si el job falla a la mitad?
Las tareas que ya terminaron **quedan hechas**. Arreglás el problema y usás **Repair run**, que
re-ejecuta **sólo lo que falló** y lo que depende de eso. En la primera ejecución eso nos ahorró
25 minutos de generar datos de nuevo.
**Dónde verlo:** [bitácora, sección 8](07_bitacora_primera_ejecucion.md) y la captura con
"4 attempts" en la guía.

### ¿Qué es "serverless"? ¿Dónde están las máquinas?
Las máquinas existen, pero **las administra Databricks**: aparecen cuando las necesitás y se van
cuando terminás. Vos no elegís ni configurás servidores.
**Por qué en este proyecto:** Free Edition sólo ofrece serverless, y además es lo más simple para
aprender. En empresas grandes a veces se usan *clusters* propios para tener más control o bajar
costos.

### ¿Por qué el for_each de silver corre de a 4 entidades y no las 8 juntas?
Porque Free Edition permite **hasta 5 tareas simultáneas** por cuenta. Con 4 queda margen. En un
workspace pago podrías subirlo.
**Dónde verlo:** `concurrency: 4` en `resources/novamart_e2e.job.yml`; captura del for_each en la
guía.

---

<a id="codigo"></a>
## 6 · Código

### ¿Por qué el código está en `src/` y los notebooks son tan cortos?
Porque **el código en notebooks es difícil de probar y de reutilizar**. La lógica vive en un
paquete Python (`src/novamart`) que:
* se puede **testear** en tu PC sin Databricks (`pytest`);
* se puede **revisar** en un Pull Request como cualquier software;
* se reutiliza desde varios notebooks y jobs.

El notebook sólo "llama" a esa lógica: es el control remoto, no el televisor.

### Si funciona en Databricks, ¿para qué sirven los tests?
Para **enterarte antes** de que algo se rompa, en segundos y sin gastar cómputo. Pero ojo con la
lección de la bitácora: **los tests no detectaron** el error de Auto Loader, porque Auto Loader sólo
existe en Databricks. Por eso un proyecto serio tiene **las dos cosas**: tests locales rápidos **y**
una prueba en el entorno real.
**Dónde verlo:** `tests/` (13 tests, todos pasan) y la
[bitácora, sección 6](07_bitacora_primera_ejecucion.md).

### ¿Qué es el bundle (`databricks.yml`) y por qué YAML?
Es la descripción **en texto** de todo lo que el proyecto crea en Databricks: jobs, pipeline,
esquema, parámetros y ambientes. Con un comando (`databricks bundle deploy`) se crea todo igual,
siempre.
**Por qué en texto y no con clics:** porque el texto se versiona en Git (sabés quién cambió qué y
cuándo), se revisa en PRs y se puede recrear idéntico en otro workspace. Con clics, nadie recuerda
qué se configuró. YAML es simplemente un formato de texto fácil de leer para humanos.

### ¿Por qué el mismo pipeline está hecho dos veces (notebooks y SQL declarativo)?
Para **comparar dos estilos**:
* **Imperativo** (notebooks + `src/`): vos decís *cómo* hacer cada paso. Más control, más código.
* **Declarativo** (`pipelines/*.sql`): vos decís *qué* tabla querés y Databricks resuelve el cómo.
  Menos código, menos control.

En el trabajo vas a encontrar los dos. Saber cuándo conviene cada uno es parte del oficio (módulo 14).

---

<a id="ambientes"></a>
## 7 · Ambientes y despliegue

### ¿Qué diferencia hay entre dev y prod? ¿Por qué sólo desplegamos dev?
* **dev** es tu laboratorio: catálogo `novamart_dev`, pocos datos, nombres con tu usuario, nada
  corre solo.
* **prod** es lo que usa el negocio: catálogo `novamart`, todos los datos, horarios activos, y lo
  despliega el CI/CD, no una persona.

Desplegamos sólo dev porque es para aprender y porque prod, con `scale_factor 1`, probablemente
supere la cuota diaria de Free Edition.

### ¿Por qué todo dice `[dev nahu98garcia]` y el esquema se llama `dev_nahu98garcia_ldp`?
Es el **modo development** del bundle: le pone tu usuario adelante a todo. Si 5 personas despliegan
dev en el mismo workspace, cada una tiene sus propios jobs y no se pisan.

### ¿Qué es CI/CD y por qué GitHub "ejecuta cosas" solo?
* **CI** (*Integración Continua*): cada vez que subís un cambio, GitHub corre automáticamente el
  lint, los tests y la validación del bundle. Si algo falla, te enterás antes de romper nada.
* **CD** (*Despliegue Continuo*): con un botón (o al aprobar un cambio), GitHub despliega a
  Databricks.

**Por qué:** en una empresa nadie despliega a producción desde su PC. Todo pasa por un proceso
automático, revisado y registrado.
**Dónde verlo:** pestaña **Actions** de tu repo y `.github/workflows/ci.yml`.

---

<a id="limites"></a>
## 8 · Almacenamiento, límites y costos

### ¿Databricks tiene límite de almacenamiento? ¿Qué se puede hacer con lo que hay?
**Respuesta corta:** en **Free Edition**, Databricks **no publica un número fijo** de
almacenamiento. Lo que sí limita es el **cómputo**, con cuotas diarias. En un **workspace pago** el
almacenamiento es prácticamente ilimitado: los datos viven en el *bucket* de tu nube (S3, ADLS o
GCS) y se paga aparte, por GB, y es barato.

**Cuánto ocupa este proyecto** (medido en tu workspace, `scale_factor 0.1`):

| Dónde | Tamaño | Archivos |
|---|---:|---:|
| Landing (archivos crudos, CSV/JSON/Parquet) | ~1,5 GB | miles (1 carpeta por fuente y día) |
| Bronze (Delta) | 0,31 GB | 29 |
| Silver (Delta) | 0,61 GB | 31 |
| Gold (Delta) | 0,27 GB | 26 |
| **Total** | **~2,7 GB** | |

Fijate que **bronze ocupa 5 veces menos que landing** con los mismos datos. Delta guarda en
formato columnar comprimido (Parquet). Por eso se convierte todo a Delta lo antes posible.

**Qué se puede hacer con ese espacio y con esa cuota:**
* Este proyecto completo con `scale_factor` 0.1 (y probablemente hasta 0.3).
* Los 18 módulos de `learning/`, dashboards, Genie Agents y el pipeline declarativo.
* Varios proyectos chicos más, cada uno en su propio catálogo.

**Cómo liberar espacio:**
```sql
VACUUM novamart_dev.silver.orders RETAIN 168 HOURS;  -- borra archivos viejos que ya no usa ninguna versión
DROP CATALOG novamart_dev CASCADE;                   -- borra TODO el proyecto (tablas + archivos)
```
Y bajá el `scale_factor` si ves que la cuota no alcanza.

### ¿Qué otros límites tiene Free Edition?
Según la documentación oficial:
* **Sólo cómputo serverless**: no podés crear clusters propios.
* **Cuota diaria de uso:** si la superás, el cómputo se apaga hasta el día siguiente. **No te cobra
  y no borra tus datos.**
* **1 SQL warehouse**, tamaño 2X-Small.
* **Hasta 5 tareas de jobs al mismo tiempo** (por eso el for_each usa 4).
* **1 pipeline activo por tipo.**
* **Hasta 3 apps**, que se apagan 24 h después de iniciadas.
* No soporta R ni Scala.

Fuente: [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

### ¿Cuánto costaría esto en una empresa?
Se paga en **DBUs** (unidades de cómputo de Databricks) más el almacenamiento de la nube. Lo que
más consume en este proyecto es **generar los datos** (25 min) y **leer los eventos web** (lo más
grande). En una empresa no se genera nada, y el job diario incremental (7 min) costaría centavos de
dólar por día. Para medirlo, mirá la tabla `system.billing.usage` (ver
[Hacerlo más real](08_hacerlo_mas_real.md)).

### ¿Qué consume la cuota diaria y cómo la cuido?
Consume cada minuto de cómputo: notebooks, jobs, el SQL warehouse, Genie. Para cuidarla:
* no dejes el warehouse encendido (igual se apaga solo);
* usá **Repair run** en vez de correr todo de nuevo;
* no generes datos de nuevo si ya los tenés (`generate=false`).

---

<a id="seguridad"></a>
## 9 · Seguridad

### ¿Qué es el token y por qué no se muestra nunca?
Es una **contraseña para programas** (el CLI, GitHub). Quien lo tenga puede hacer en tu workspace
todo lo que podés hacer vos. Por eso:
* nunca se escribe en el código ni se sube a GitHub (va en *secrets*);
* se le pone vencimiento;
* si se filtra, se revoca en *Settings → Developer → Access tokens*.

### ¿Por qué tapamos el email en las capturas?
Porque es **dato personal** (PII). Es el mismo criterio que aplica el proyecto con los clientes:
`sql/ddl/governance_masking.sql` define una **máscara** que oculta el email a quien no tenga
permiso.

### ¿Quién puede ver mis tablas?
Por defecto, sólo vos (sos el *owner*). Unity Catalog permite dar permisos por catálogo, esquema o
tabla (`GRANT SELECT ON ...`), ocultar columnas (máscaras) y filtrar filas (cada gerente ve sólo su
país). Módulo 15 de `learning/`.

---

<a id="consumo"></a>
## 10 · Consumo y Genie

### Si silver ya está limpio, ¿para qué existe gold?
Silver está **limpio pero "técnico"**: organizado como los sistemas de origen (pedidos, líneas,
eventos). Gold está organizado **como piensa el negocio**: ventas por día, por tienda, segmentos de
clientes. Un analista o Genie responden mucho mejor y más rápido sobre gold.

### ¿Genie reemplaza al analista o al Data Engineer?
No. Genie **traduce preguntas a SQL**, pero:
* necesita tablas **bien modeladas y documentadas** (trabajo del Data Engineer);
* necesita **definiciones de negocio** claras: ¿"ventas" con o sin descuento? (trabajo del analista);
* hay que **verificar** que sus respuestas sean correctas (benchmarks).

Genie le saca al analista el trabajo repetitivo ("¿cuánto vendimos ayer?") para que se dedique a las
preguntas difíciles. Más en [Genie a fondo](09_genie_a_fondo.md).

### ¿Por qué hay "marts" si ya están las facts?
Los **marts** son respuestas **pre-calculadas** a preguntas frecuentes (ventas diarias, segmentos
RFM, retención por cohorte). Calcularlas una vez por día es más barato y rápido que hacerlo cada
vez que alguien abre el tablero.

---

<a id="carrera"></a>
## 11 · Aprender y trabajar

### ¿Necesito saber programar para seguir el proyecto?
Para **ejecutarlo y entenderlo**, no: alcanza con la [guía desde la web](06_guia_desde_la_web.md).
Para **modificarlo**, sí vas a necesitar SQL (primero) y Python (después). La ruta `learning/`
empieza por SQL desde cero.

### ¿Qué aprendo acá que sirva en un trabajo real?
Casi todo lo del día a día de un Data Engineer: ingesta incremental, limpieza y calidad, modelado
dimensional, orquestación, despliegue con CI/CD, gobierno de datos y, sobre todo, **leer errores y
resolverlos** (la bitácora es exactamente eso).

### ¿Cómo lo muestro en una entrevista?
Ver [Cómo contar este proyecto](14_como_contarlo.md): qué demuestra, cómo contarlo en 2 minutos y
las preguntas que te van a hacer.

### ¿Qué hago después de NovaMart?
1. Hacé la [autoevaluación](13_autoevaluacion.md). Si respondés el 80 %, entendiste el proyecto.
2. Elegí una mejora de [Hacerlo más real](08_hacerlo_mas_real.md) y hacela vos.
3. Armá tu propio proyecto con otro negocio, aplicando lo aprendido (y con Genie en el centro).
