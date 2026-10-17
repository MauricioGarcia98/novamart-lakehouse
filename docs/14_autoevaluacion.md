# 14 · Autoevaluación: ¿entendiste el proyecto o sólo seguiste pasos?

> **Cómo usar esta página:** respondé cada pregunta **en voz alta o por escrito antes de abrir la
> respuesta**. Intentar recordar, aunque te equivoques, fija el conocimiento mucho más que releer.
>
> * Si respondés bien **el 80 %**, entendiste NovaMart.
> * Las preguntas marcadas 🔮 son de **predicción**: pensá qué pasaría y después probalo en dev.
> * Las marcadas 🧩 **combinan** varios temas: son las que te harían en una entrevista.

---

## Nivel 1 · El panorama

**1.** ¿Qué problema de negocio resuelve NovaMart, en una oración?
<details><summary>Respuesta</summary>

Convierte datos de muchos sistemas, sucios y en formatos distintos, en tablas confiables y con una
única definición de cada número, para que el negocio pueda tomar decisiones.
</details>

**2.** ¿De dónde salen los datos, si no hay ninguna fuente conectada?
<details><summary>Respuesta</summary>

De un generador del propio proyecto (`src/novamart/datagen/`) que simula 8 sistemas de origen y
escribe archivos en el volumen *landing*. Desde landing en adelante el pipeline trabaja igual que
con datos reales.
</details>

**3.** Nombrá las 4 capas en orden y la responsabilidad de cada una, en 5 palabras o menos.
<details><summary>Respuesta</summary>

Landing: archivos tal cual llegan. Bronze: copia fiel con linaje. Silver: limpio, único, con
historia. Gold: modelado para el negocio.
</details>

**4.** ¿Por qué no se limpia directamente en bronze?
<details><summary>Respuesta</summary>

Porque "dato malo" depende de las reglas de hoy. Si se limpia en bronze y la regla estaba mal, el
dato original se pierde. Bronze es el registro que permite reprocesar.
</details>

---

## Nivel 2 · Datos y calidad

**5.** 🔮 Antes de mirar: ¿`silver.orders` tiene más, menos o igual cantidad de filas que `bronze.orders`? ¿Por qué?
<details><summary>Respuesta</summary>

Menos: 1.199.310 contra 1.203.610. Se eliminaron 3.599 duplicados (re-envíos) y 701 pedidos fueron
a cuarentena por no tener cliente siendo online.
</details>

**6.** ¿Qué diferencia hay entre una regla `warn` y una `quarantine`?
<details><summary>Respuesta</summary>

`warn` deja pasar el registro y sólo cuenta cuántos fallan (por ejemplo, un email con formato raro).
`quarantine` aparta el registro a `ops.quarantine` y no lo deja llegar a silver (por ejemplo, una
cantidad negativa).
</details>

**7.** 🧩 Llega un archivo de `web_events` con una columna nueva que nunca existió. ¿Qué pasa, paso a paso?
<details><summary>Respuesta</summary>

Auto Loader detecta la columna, guarda el esquema ampliado y frena el stream con
`UNKNOWN_FIELD_EXCEPTION`. El código de `bronze.py` detecta ese error y reinicia el stream (hasta 3
veces), que ahora lee con la columna nueva. La tabla bronze la incorpora (`mergeSchema`). Pasó de
verdad con `experiment_id` (bitácora, sección 7).
</details>

**8.** ¿Por qué el pipeline no se frena cuando encuentra 1.420 líneas con cantidad negativa?
<details><summary>Respuesta</summary>

Porque frenar dejaría al negocio sin datos por un 0,05 % de registros malos. Se apartan a
cuarentena (con la evidencia guardada) y el resto sigue. Si fueran muchísimos, el quality gate o un
monitoreo lo deberían detectar.
</details>

---

## Nivel 3 · Modelado

**9.** En `fact_sales`, ¿qué representa exactamente una fila (el grano)?
<details><summary>Respuesta</summary>

Una línea de pedido: un producto dentro de un pedido. Por eso la clave es `order_id + line_id`.
</details>

**10.** 🔮 Un cliente se muda de Córdoba a Rosario en 2024. Sin SCD2, ¿qué le pasaría al reporte "ventas por ciudad de 2023"?
<details><summary>Respuesta</summary>

Las compras de 2023 de ese cliente pasarían a contarse como Rosario. Las ventas de Córdoba de 2023
bajarían y el reporte dejaría de coincidir con lo que se informó en su momento.
</details>

**11.** ¿Para qué sirve la fila `-1` en las dimensiones?
<details><summary>Respuesta</summary>

Para que los hechos sin dimensión conocida (por ejemplo, ventas en caja sin cliente) tengan a dónde
apuntar. Con NULL, un JOIN los haría desaparecer de los totales.
</details>

**12.** ¿Por qué la clave subrogada es un hash de `(id, valid_from)` y no un autonumérico?
<details><summary>Respuesta</summary>

Porque el hash da siempre la misma clave para la misma versión, aunque se reconstruya la dimensión.
Con un autonumérico, al reconstruir podrían cambiar los números y las ventas apuntarían al cliente
equivocado.
</details>

---

## Nivel 4 · Ejecución e incremental

**13.** ¿Por qué la carga completa tardó 50 minutos y el job diario 7?
<details><summary>Respuesta</summary>

El job diario es incremental: procesa sólo el día nuevo. Los checkpoints de Auto Loader recuerdan
qué archivos ya se leyeron y gold usa watermarks para saber qué fechas reemplazar. Además, el job
diario no ejecuta el generador completo (25 min).
</details>

**14.** 🔮 Si corrés el job diario dos veces seguidas para el mismo día, ¿qué pasa con `fact_sales`?
<details><summary>Respuesta</summary>

Queda igual. El pipeline es idempotente: bronze no relee archivos ya procesados, silver usa MERGE
(no inserta lo que ya existe) y gold usa replaceWhere (reemplaza el día completo, no le agrega
filas).
</details>

**15.** Falla `gold_facts` después de 40 minutos de job. ¿Qué hacés y por qué?
<details><summary>Respuesta</summary>

Leo el error de la tarea, lo arreglo en el código, vuelvo a desplegar el bundle y uso **Repair run**,
que re-ejecuta sólo `gold_facts` y lo que depende de ella (marts y quality gate). No re-ejecuto todo,
para no repetir 40 minutos de trabajo ya hecho.
</details>

**16.** ¿Qué hace la tarea `should_generate` y por qué `bronze_ingest` necesita `outcome: "false"` y `run_if: NONE_FAILED`?
<details><summary>Respuesta</summary>

Decide si se generan datos (carga completa) o no (diario). `bronze_ingest` tiene que correr en los
dos casos: después de `generate_data` si se generó, o directamente si la condición dio `false`.
`outcome: "false"` nombra esa rama, y `NONE_FAILED` permite correr aunque una de las dos dependencias
se haya salteado.
</details>

---

## Nivel 5 · Código, despliegue y operación

**17.** ¿Por qué la lógica está en `src/` y no dentro de los notebooks?
<details><summary>Respuesta</summary>

Para poder testearla con pytest sin Databricks, revisarla en Pull Requests y reutilizarla. Los
notebooks sólo la llaman.
</details>

**18.** 🧩 Los 13 tests pasaban, pero el job falló en Databricks. ¿Cómo es posible y qué mejorarías?
<details><summary>Respuesta</summary>

Los tests corren con Spark local, que no tiene Auto Loader; el error (`CF_ADD_NEW_NOT_SUPPORTED`) era
propio de Auto Loader. Mejora: agregar al CI una ejecución del pipeline en Databricks con un
`scale_factor` mínimo antes de mergear.
</details>

**19.** ¿Qué diferencia hay entre `bundle deploy -t dev` y `-t prod` en este proyecto?
<details><summary>Respuesta</summary>

Dev usa el catálogo `novamart_dev`, `scale_factor` 0.1, prefijo `[dev usuario]` y horarios pausados.
Prod usa `novamart`, `scale_factor` 1, sin prefijos, en una carpeta compartida, y lo despliega el
CI/CD.
</details>

**20.** ¿Por qué el catálogo se creó por SQL y no desde el bundle?
<details><summary>Respuesta</summary>

En workspaces con Default Storage (Free Edition), la API REST no puede crear catálogos y el bundle
usa esa API. Por SQL (o desde la UI) sí se puede. Por eso se crea una vez antes del primer deploy.
</details>

---

## Nivel 6 · Para pensar (sin respuesta única)

Estas no tienen una respuesta "correcta". Discutilas con alguien o escribí tu argumento.

21. Si NovaMart necesitara ver las ventas **en tiempo real** (segundos), ¿qué cambiarías en bronze y en silver? ¿Qué costaría?
22. El área de marketing quiere su propia definición de "cliente activo". ¿La ponés en silver, en gold o en un metric view? ¿Por qué?
23. Te piden borrar todos los datos de un cliente (derecho al olvido). ¿En qué capas está? ¿Qué pasa con el *time travel*?
24. Si el volumen se multiplica por 100, ¿qué parte del pipeline sufre primero? (Pista: módulo 10 de `learning/`).
25. ¿Qué tres preguntas le harías a Genie para comprobar que entiende bien las ventas, y cómo sabrías si se equivoca?
