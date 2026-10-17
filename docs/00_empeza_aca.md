# 00 · Empezá acá

> **Si nunca trabajaste con datos, este es tu primer documento.** No hace falta saber programar
> para leerlo. Al terminar vas a saber qué es este proyecto, qué problema resuelve, cómo está
> armado y por dónde seguir según lo que quieras aprender.

---

## 1 · La historia en 2 minutos

**NovaMart** es una cadena de tiendas *ficticia*: 6 países de Latinoamérica, unas 400 tiendas, web,
app y marketplace. Todos los días pasan miles de cosas en ese negocio:

* un cliente compra en la app;
* un pedido sale del depósito, se atrasa y llega;
* alguien devuelve una campera porque le quedó chica;
* una tienda se queda sin stock de un producto;
* miles de personas navegan la web sin comprar nada.

Cada una de esas cosas queda registrada en **un sistema distinto**: la caja de la tienda, la base
de datos de la web, el sistema de logística, el de stock. Y cada sistema guarda los datos **a su
manera**: unos en CSV, otros en JSON, con fechas en formatos diferentes, con errores, con
duplicados.

Un día el gerente pregunta: **"¿Cuánto vendimos ayer por país, y qué clientes estamos perdiendo?"**

Nadie puede responder rápido, porque los datos están desparramados, sucios y desconectados.

**Este proyecto construye el camino completo** que lleva esos datos desordenados hasta respuestas
confiables: un tablero, una consulta SQL o una pregunta en lenguaje natural a Genie. Eso es lo que
hace un **Data Engineer**.

---

## 2 · El modelo mental: una fábrica de datos

Pensá el proyecto como una fábrica con cinto transportador. La materia prima entra cruda y sale
como producto terminado.

```text
 SISTEMAS DE ORIGEN        LANDING            BRONZE            SILVER             GOLD             CONSUMO
 (caja, web, stock…)  →   (depósito de  →    (copia fiel  →    (limpio y    →     (listo para  →   (tableros,
  generan archivos         llegada:           de todo lo        ordenado:          responder        Genie, SQL,
                           archivos           que llegó,        sin duplicados,    preguntas del    modelos de ML)
                           tal cual)          sin tocar)        tipos correctos)   negocio)
```

| Etapa | Analogía | Pregunta que responde |
|---|---|---|
| **Landing** | El muelle donde los camiones descargan cajas | ¿Llegó todo lo que mandaron? |
| **Bronze** | El inventario de lo recibido, sin abrir las cajas | ¿Qué recibimos exactamente y de dónde vino? |
| **Silver** | Abrir, revisar, descartar lo roto y ordenar en estanterías | ¿Cuál es la verdad, sin errores ni repetidos? |
| **Gold** | Armar los combos listos para vender | ¿Qué necesita saber el negocio? |
| **Consumo** | La vidriera | ¿Qué decisión tomo con esto? |

> **Por qué así:** si algo sale mal en silver, no hay que volver a pedirle los datos a nadie,
> porque bronze tiene la copia original. Cada capa protege a la siguiente. Lo explicamos a fondo
> en [12 · Por qué así y no de otra manera](12_por_que_asi.md).

---

## 3 · ¿De dónde salen los datos si no hay nada conectado?

Es la primera pregunta que se hace todo el mundo. **Los datos los genera el propio proyecto**, con
un programa (`src/novamart/datagen/`) que **simula** ser esos sistemas de origen. Ese programa
escribe archivos en la carpeta *landing*, exactamente como lo harían la caja o la web de una
empresa real.

¿Por qué simular en vez de usar datos reales?

1. **Una empresa no te presta sus datos** para practicar: tienen datos personales y secretos comerciales.
2. **Podés controlar el tamaño** con un número (`scale_factor`): 3 GB para aprender, 300 GB para sufrir.
3. **Los errores están puestos a propósito**, y cada uno tiene su solución en el pipeline. Así
   aprendés a resolver problemas reales: duplicados, fechas raras, columnas que aparecen de golpe.
4. **Es reproducible:** la misma configuración genera siempre los mismos datos, así que tus
   resultados se pueden comparar con los de la guía.

En una empresa real, en lugar del generador habría **conectores** a los sistemas de verdad. Más en
[11 · Preguntas frecuentes](11_preguntas_frecuentes.md#datos).

---

## 4 · Elegí tu camino

| Si sos… | Leé en este orden | Tiempo |
|---|---|---|
| 🧑‍🎓 **Principiante total** ("no sé nada") | Este doc → [Glosario](13_glosario.md) → [Preguntas frecuentes](11_preguntas_frecuentes.md) → [Guía desde la web](06_guia_desde_la_web.md) (hacela de verdad) → [Bitácora](07_bitacora_primera_ejecucion.md) → [Autoevaluación](14_autoevaluacion.md) | 1–2 semanas |
| 🛠️ **Futuro Data Engineer** (sabés algo de SQL o Python) | Este doc → [Por qué así](12_por_que_asi.md) → [Arquitectura](01_arquitectura.md) → [Fuentes](02_fuentes_y_datos.md) → [Modelado](03_modelado_de_datos.md) → ruta [`learning/`](../learning/README.md) → leer `src/` | 4–8 semanas |
| 👔 **Recruiter o entrevistador** | [Cómo contar este proyecto](15_como_contarlo.md) → [Bitácora](07_bitacora_primera_ejecucion.md) → [Arquitectura](01_arquitectura.md) | 15 minutos |
| 📊 **Analista / usuario de negocio** | Este doc → [Guía, Pasos 7 a 9](06_guia_desde_la_web.md#paso-7--explorar-los-datos) → [Genie a fondo](09_genie_a_fondo.md) | 1 día |

---

## 5 · El mapa del repositorio (qué es cada carpeta y para qué existe)

| Carpeta | Qué hay | Para qué existe | ¿Lo tocás como principiante? |
|---|---|---|---|
| `docs/` | Documentación, diagramas, capturas | Entender | ✅ Empezá acá |
| `learning/` | 18 módulos de ejercicios (SQL, Spark…) | Practicar sobre las tablas del proyecto | ✅ Después de correr el job |
| `notebooks/` | Un notebook por etapa (00 → 10) | Son los "pasos" que ejecuta el job | 👀 Leelos, no los cambies |
| `src/novamart/` | El código de verdad (Python) | La lógica de cada etapa, testeable | 👀 Cuando ya entiendas las capas |
| `resources/` | Archivos YAML que describen los jobs | "Recetas" que Databricks lee para crear los jobs | 👀 Mirá `novamart_e2e.job.yml` |
| `databricks.yml` | Configuración del *bundle* | Dice qué desplegar y en qué ambiente (dev/prod) | 👀 |
| `pipelines/` | El mismo pipeline escrito sólo en SQL | Comparar dos estilos de hacer lo mismo | Más adelante |
| `sql/` | Consultas SQL de los marts y de gobierno | Las tablas "de negocio" y la seguridad | Más adelante |
| `tests/` | Pruebas automáticas | Verificar que el código hace lo que dice | Más adelante |
| `.github/workflows/` | CI/CD | GitHub prueba y despliega solo | Más adelante |

> **Regla de oro para leer el código:** primero el notebook (qué se hace), después el módulo de
> `src/` que el notebook llama (cómo se hace) y por último el test (cómo sabemos que funciona).

---

## 6 · Cómo estudiar este repo (y no sólo seguir pasos)

Seguir pasos no te enseña: copiar comandos que funcionan no te deja nada. Estas 4 costumbres sí:

1. **Antes de ejecutar, predecí.** "¿Cuántas filas va a tener silver: más, menos o igual que
   bronze? ¿Por qué?" Después compará con el resultado real. Equivocarte acá es lo que más enseña.
2. **Preguntate "¿qué pasaría si no existiera esto?"** Para cada capa, tabla o regla. Si la
   respuesta es "nada", no la entendiste todavía. Las respuestas están en
   [Por qué así](12_por_que_asi.md).
3. **Rompé cosas a propósito** (en dev). Borrá una regla de calidad y mirá qué entra a gold.
   Cambiá `scale_factor` y mirá qué tarda más. La [bitácora](07_bitacora_primera_ejecucion.md)
   muestra cómo se leen los errores.
4. **Explicalo en voz alta.** Si no podés explicarle a otra persona por qué existe silver, volvé a
   leer. La [autoevaluación](14_autoevaluacion.md) tiene preguntas para eso.

---

## 7 · Lo que vas a saber hacer al final

* Explicar qué hace un Data Engineer y por qué una empresa lo necesita.
* Llevar datos crudos y sucios hasta tablas confiables, en capas.
* Escribir SQL analítico sobre un modelo estrella.
* Leer un error de Databricks, entender la causa y arreglarlo.
* Desplegar un proyecto completo con un comando (o un botón en GitHub) en dos ambientes.
* Preparar datos para que el negocio pregunte en lenguaje natural con Genie.
* **Contarlo en una entrevista**, con números reales y problemas reales resueltos.
