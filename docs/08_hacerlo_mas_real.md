# 08 · Cómo hacer NovaMart más real (la parte técnica, rol por rol)

> NovaMart ya simula bastante: datos sucios, duplicados, columnas que aparecen, CDC, SCD2 y un
> quality gate. Pero en una empresa real hay más cosas: **sistemas origen de verdad, varios
> equipos, plata, seguridad, incidentes a las 3 a.m.** Este documento lista, rol por rol, qué
> agregar para que el proyecto se parezca a un trabajo real. Cada ítem trae su **nivel**
> (🟢 básico · 🟡 intermedio · 🔴 avanzado) y **qué aprendés**.

---

## Los roles de un equipo de datos real

| Rol | Pregunta que se hace | En NovaMart |
|---|---|---|
| **Product Owner de datos** | ¿Qué problema de negocio resolvemos y cómo medimos el éxito? | Casos de negocio, KPIs, SLAs |
| **Data Engineer** | ¿Cómo llegan los datos, confiables y a tiempo? | Ingesta, bronze/silver, orquestación |
| **Analytics Engineer** | ¿Cómo modelamos para que el negocio entienda? | Gold, métricas, capa semántica |
| **Platform / DataOps Engineer** | ¿Cómo desplegamos, monitoreamos y controlamos costos? | Bundles, CI/CD, ambientes, costos |
| **Data Steward / Gobierno** | ¿Quién ve qué? ¿Qué es dato personal? | UC, permisos, máscaras, linaje |
| **Data Analyst / BI** | ¿Qué decisiones habilitan los datos? | Dashboards, Genie Agents |
| **Data Scientist / ML Engineer** | ¿Podemos predecir o recomendar? | Features, modelos, MLflow |
| **SRE / On-call** | ¿Qué pasa cuando algo se rompe? | Alertas, runbooks, postmortems |

---

## 1 · Product Owner: casos y problemas de negocio de verdad

Hoy el proyecto responde "¿cuánto vendimos?". Un negocio real pregunta cosas que **duelen**:

| Problema de negocio | Pregunta concreta | Datos que hacen falta | Entregable |
|---|---|---|---|
| **Quiebres de stock** en tiendas | ¿Cuánta venta perdimos por no tener stock? | Stock diario + demanda + ventas | `mart_lost_sales` + alerta diaria |
| **Canibalización entre canales** | ¿La app le roba ventas a la tienda física o suma clientes nuevos? | Clientes omnicanal, cohortes | Análisis de incrementalidad |
| **Fraude en devoluciones** | ¿Qué clientes devuelven de forma anómala? | Devoluciones, motivos, montos | Score de riesgo + cola de revisión |
| **Promociones que pierden plata** | ¿Qué promo vendió más pero con margen negativo? | Promos, costos, ventas | Ranking de ROI por promo |
| **Inflación y precios** | ¿Cómo afecta la inflación (AR) al ticket real? | Ventas + índice de precios (IPC) | Ventas en moneda constante |
| **Logística de última milla** | ¿Por qué se atrasan las entregas en ciertas zonas? | Eventos de estado, geografía | SLA de entrega por zona |

🟢 Escribir **3 casos de negocio** en `docs/` con el formato *problema → decisión que habilita →
métrica de éxito → datos necesarios*.
🟡 Definir **SLAs**: "gold disponible a las 7:00 a.m. con datos hasta ayer, 99 % de los días".

---

## 2 · Data Engineer: fuentes y operación reales

| Mejora | Nivel | Qué aprendés |
|---|---|---|
| Reemplazar archivos generados por una **base Postgres real** (Neon o Supabase gratis) e ingerir con **Lakeflow Connect** o JDBC + CDC | 🟡 | Cómo es la ingesta desde sistemas transaccionales |
| Eventos web en **streaming real**: un productor Kafka (Confluent free tier) o un script que escribe archivos cada minuto | 🔴 | Streaming de verdad, latencia, *watermarks* |
| **APIs externas**: cotización del dólar (BCRA), clima (SMN / Open-Meteo), feriados | 🟢 | Ingesta de APIs, reintentos, *rate limits* |
| **Datos tardíos**: pedidos que llegan 3 días después | 🟡 | `replaceWhere`, reprocesar ventanas, idempotencia |
| **Backfill**: reprocesar un mes entero sin duplicar | 🟡 | Idempotencia, parámetros de fechas |
| **Contratos de datos** (YAML por fuente: columnas, tipos, dueño, SLA) validados en bronze | 🟡 | Acuerdos entre equipos |
| **Expectations de Lakeflow** en el pipeline declarativo (`EXPECT ... ON VIOLATION DROP ROW`) | 🟢 | Calidad declarativa |
| **Chaos testing**: inyectar fallas a propósito (archivo corrupto, fuente caída, esquema roto) | 🔴 | Resiliencia (como el `experiment_id` de la bitácora, pero a propósito) |

---

## 3 · Analytics Engineer: una capa semántica que el negocio entienda

| Mejora | Nivel | Qué aprendés |
|---|---|---|
| `COMMENT` en todas las tablas y columnas de gold | 🟢 | Documentación viva (y Genie la lee) |
| Declarar **PK/FK** en el modelo estrella | 🟢 | Integridad y mejores JOIN en BI/Genie |
| **Metric views** (`ventas_netas`, `margen`, `ticket_promedio`) definidas una sola vez | 🟡 | Capa semántica: una métrica, una definición |
| **Snapshot de hechos acumulativos** (ciclo de vida del pedido) | 🟡 | Kimball avanzado |
| Tests de datos de negocio: "la suma de gold = la suma de silver ± devoluciones" | 🟡 | Reconciliación |
| **Moneda constante** (ajuste por IPC) y multi-moneda con tipo de cambio diario | 🔴 | Modelado financiero real en LatAm |

---

## 4 · Platform / DataOps: como en producción

| Mejora | Nivel | Qué aprendés |
|---|---|---|
| Tres ambientes `dev` / `staging` / `prod` en el bundle, con catálogos distintos | 🟢 | Promoción entre ambientes |
| Desplegar prod con un **service principal**, no con tu usuario | 🟡 | Identidades de máquina |
| CI/CD con **GitHub Actions + OIDC** (sin tokens guardados) | 🟡 | Seguridad en CI |
| **Test de integración en Databricks** dentro del CI: correr el e2e con `scale_factor=0.01` en cada PR | 🟡 | Habría detectado los errores 3 y 4 de la bitácora antes del merge |
| **Costos**: dashboard con `system.billing.usage` por job y tag `project=novamart` | 🟢 | FinOps |
| **Alertas**: `email_notifications` / webhook de Slack en fallas y demoras (`health rules`) | 🟢 | Operación |
| **Secrets** (`dbutils.secrets`) para claves de APIs externas | 🟢 | No hardcodear credenciales |

---

## 5 · Data Steward: gobierno y privacidad

| Mejora | Nivel | Qué aprendés |
|---|---|---|
| **Grupos** (`analistas`, `ingenieros`, `gerencia`) y `GRANT` por esquema | 🟢 | Mínimo privilegio |
| **Máscaras de columna** en email y teléfono (`MASK`) | 🟡 | Protección de datos personales (Ley 25.326 AR) |
| **Row filters**: cada gerente de país ve sólo su país | 🟡 | Seguridad a nivel fila |
| **Tags** de clasificación (`pii`, `confidencial`) y linaje revisado | 🟢 | Catálogo de datos |
| "Derecho al olvido": borrar un cliente en todas las capas (y en el *time travel*) | 🔴 | Cumplimiento real con Delta |

---

## 6 · Analyst / BI: consumo real

* 🟢 Dashboard ejecutivo con **5 KPIs**, filtros por país/canal y envío programado por mail.
* 🟡 **Genie Agent** con trusted assets y benchmarks (ver [09_genie_a_fondo](09_genie_a_fondo.md)).
* 🟡 **Alertas SQL**: "avisame si las ventas de ayer caen más de 20 % contra el mismo día de la semana pasada".
* 🔴 **Databricks App** (Streamlit/Dash) para que un gerente consulte stock por tienda.

---

## 7 · Data Scientist: inteligencia

* 🟡 **Pronóstico de demanda** por producto y tienda (para el problema de quiebres de stock).
* 🟡 **Segmentación RFM → churn**: ¿qué clientes están por irse?
* 🔴 **Recomendador** con `mart_product_affinity`, servido como endpoint.
* Todo registrado en **MLflow**: experimentos, modelos en Unity Catalog, monitoreo.

---

## 8 · SRE / On-call: cuando todo se rompe

* 🟢 **Runbooks** en `docs/`: "si falla bronze con X → hacé Y" (la [bitácora](07_bitacora_primera_ejecucion.md) es tu primer runbook).
* 🟡 **Postmortem** de cada incidente: qué pasó, impacto, causa raíz, acciones. Sin culpables.
* 🟡 **Freshness monitor**: una tabla `ops.freshness` y una alerta si gold tiene más de 26 h.
* 🔴 **Simulacro**: rompé algo a propósito y medí cuánto tardás en detectarlo y arreglarlo.

---

## Hoja de ruta sugerida (8 semanas)

| Semana | Foco | Entregable |
|---|---|---|
| 1 | Entender | Ruta `learning/` 00–07 + bitácora |
| 2 | Negocio | 3 casos de negocio escritos + SLAs |
| 3 | Fuentes reales | API del dólar + clima en bronze |
| 4 | Semántica | Comments, PK/FK, metric views |
| 5 | Genie | Genie Agent con benchmarks ≥ 80 % |
| 6 | Gobierno | Grupos, máscaras, row filters |
| 7 | Operación | Alertas, costos, CI con test en Databricks |
| 8 | ML / App | Pronóstico de demanda o Databricks App |
