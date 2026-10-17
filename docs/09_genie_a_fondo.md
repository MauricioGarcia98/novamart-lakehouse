# 09 · Genie a fondo: toda la familia Genie de Databricks (estado a octubre 2026)

> **Aviso:** Genie cambió muchísimo en 2026: renombres, productos nuevos y precios nuevos.
> Este documento resume lo investigado a **octubre de 2026**. Antes de decidir algo con plata de
> por medio, chequeá siempre las *release notes* oficiales (enlaces al final).

---

## 1 · El mapa: "Genie" ya no es un producto, es una familia

| Producto | Antes se llamaba | Para quién | Qué hace |
|---|---|---|---|
| **Genie One** | *Databricks One* → *Genie* (abr-2026) → **Genie One** (jun-2026) | Usuarios de negocio | Interfaz simplificada (web, iOS, Android) para consumir dashboards, Genie Agents y apps sin ver notebooks ni clusters. Databricks la presenta como un "compañero de trabajo agéntico". |
| **Genie Agents** | **Genie spaces** (AI/BI Genie), renombrado en jul-2026 | Analistas que lo arman, negocio que lo usa | Un "espacio" sobre un conjunto de tablas con instrucciones y ejemplos, donde se le pregunta a los datos en lenguaje natural. Genera SQL, tablas y gráficos. |
| **Agent mode** de Genie | — | Negocio y analistas | Para preguntas abiertas ("¿por qué cayeron las ventas en Chile?"): planifica varios pasos, ejecuta varias consultas, razona y entrega un **informe con citas**. |
| **Genie Code** | **Databricks Assistant** | Ingenieros, analistas técnicos, científicos de datos | Agente de desarrollo dentro de notebooks, SQL Editor, editor de Lakeflow Pipelines, dashboards y MLflow. Escribe y corrige código, arma pipelines enteros, depura jobs que fallan. |
| **Genie API** (Conversation / Agents API) | AI/BI Genie Conversation API | Desarrolladores | Llevar Genie a tu propia app: Slack, Teams, WhatsApp, una web, un bot. Hay endpoints de *chat mode* y de *agent mode* (GA en ago-2026). |

**Para recordar:**
* **Genie One** es *dónde* entra el usuario de negocio.
* **Genie Agents** son *sobre qué datos* pregunta.
* **Genie Code** es *quién te ayuda a construir* los datos.
* La **API** es *cómo lo sacás afuera* de Databricks.

---

## 2 · Genie Agents (ex Genie spaces) en profundidad

### 2.1 Componentes de un Genie Agent

1. **Data sources:** tablas, vistas o **metric views** de Unity Catalog. El límite subió de 25 a
   30 y después a **50** (sep-2026). La recomendación práctica es **5–15 tablas bien enfocadas**:
   cuantas menos, más precisión.
2. **Instructions (texto):** reglas de negocio cortas y sin contradicciones.
3. **SQL examples / expressions:** preguntas de ejemplo con su SQL correcto, y definiciones de
   métricas, filtros y dimensiones. Funcionan **mejor que párrafos largos de texto**.
4. **Trusted assets:** consultas parametrizadas o **funciones de Unity Catalog** (escalares o
   *table-valued*) que Genie ejecuta tal cual cuando la pregunta coincide. La respuesta sale
   marcada como **Trusted**: así se garantizan las métricas "oficiales".
5. **Benchmarks:** preguntas con su respuesta correcta para **medir** la precisión. Empezá con ~10
   preguntas reales de usuarios y subí a 25–50. Volvé a correrlos tras cada cambio. **No le
   enseñan nada a Genie**: sólo miden.
6. **SQL warehouse:** el cómputo que ejecuta las consultas (en Free Edition, el 2X-Small serverless).
7. **Monitoring / feedback:** los usuarios marcan 👍/👎 y el autor revisa las preguntas mal
   respondidas.

### 2.2 Lo que más mejora la precisión (en orden de impacto)

1. **Modelo de datos limpio**, como la capa gold de este repo: nombres claros, una tabla por
   concepto, claves subrogadas.
2. **Comentarios en tablas y columnas** de Unity Catalog. Genie los lee. `COMMENT ON COLUMN gold.fact_sales.net_amount IS 'Venta neta en USD, después de descuentos'`.
3. **Claves primarias y foráneas declaradas** (`PRIMARY KEY` / `FOREIGN KEY ... NOT ENFORCED`), para que Genie haga bien los JOIN.
4. **Metric views** para las métricas del negocio (ventas netas, margen, ticket promedio) definidas
   **una sola vez**. Un Genie Agent también se puede **exportar como metric view** y refinarlo con
   Genie Code.
5. **Trusted assets** para las 5–10 preguntas que no pueden fallar nunca.
6. **Pocas instrucciones de texto**, específicas y sin contradicciones.
7. **Benchmarks** + ciclo de mejora semanal.

### 2.3 Desplegar Genie Agents como código

* La API expone un campo **`serialized_space`**: un JSON portable con la configuración del
  agente. Se exporta (`GET ... ?include_serialized_space=true`) y se importa en otro workspace
  (create/update con reemplazo total).
* Los **bundles** (Declarative Automation Bundles, ex Asset Bundles) ya tienen recurso para Genie
  Agents, pero **sólo con el motor de deploy "direct"**. El soporte es nuevo: probalo antes de
  depender de él.
* Ojo: el **título y la descripción no viajan** en el JSON serializado. Y en los nombres de
  catálogo y esquema hay que reemplazar `_dev` por prod.
* Python SDK: `w.genie.*` (create/get/update space, start conversation, etc.).

---

## 3 · Genie Code (ex Databricks Assistant) en profundidad

### 3.1 Dónde vive y qué hace

| Superficie | Qué puede hacer |
|---|---|
| **Notebooks** | Explorar datos, escribir y corregir celdas, explicar código, *diagnose error*, armar análisis completos en **agent mode** |
| **SQL Editor** | Escribir y optimizar consultas, explicar planes |
| **Lakeflow Pipelines Editor** | Generar un pipeline **de punta a punta**, editar archivos SQL/Python, correr updates y arreglar errores |
| **Dashboards (AI/BI)** | Crear visualizaciones desde lenguaje natural |
| **MLflow** | Ayuda con experimentos, evaluación y trazas |
| **Jobs** | Depurar ejecuciones fallidas |

Novedades del Data + AI Summit 2026: un **command center** de página completa, mejoras para
ingeniería de datos y ML productiva, y **tareas programadas** (anunciadas como "próximamente": que
Genie Code haga trabajo solo, en horario).

### 3.2 Personalizarlo: instrucciones, skills, memoria y MCP

1. **Instrucciones de usuario:** archivo `.assistant_instructions.md` en tu carpeta personal del
   workspace. Ahí van tus preferencias: "respondé en español", "usá PySpark, no pandas", etc.
2. **Instrucciones de workspace:** `Workspace/.assistant_workspace_instructions.md`. Las define
   un admin, aplican a todo el equipo y **tienen prioridad** sobre las de usuario.
3. **Skills:** paquetes en Markdown que describen un flujo de trabajo o un estándar (siguen el
   estándar abierto *Agent Skills*). Se usan en **agent mode**. Las personales viven en
   `/Users/<tu_usuario>/.assistant/skills/`. Ejemplos útiles para este repo:
   * *skill "nueva entidad silver"*: pasos para sumar una fuente (contrato → reglas → SCD → tests).
   * *skill "convenciones NovaMart"*: nombres `dim_`/`fact_`/`mart_`, columnas de auditoría, etc.
4. **Memoria:** recuerda contexto entre conversaciones.
5. **MCP (Model Context Protocol):** le conecta herramientas o datos externos, por ejemplo leer
   estándares desde GitHub o consultar Jira.

Repo de ejemplo oficial: `databricks-solutions/genie-code-skills-demo`.

### 3.3 Precio (importante)

* Desde el **8-jul-2026**, Genie Code se cobra **pay-as-you-go**, con una **asignación gratuita
  mensual por usuario** (150 DBU/mes según las release notes) y un 25 % de descuento promocional
  sobre lo que exceda, hasta el 31-ene-2027.
* **Genie One y Genie Agents** (uso de usuarios): **gratis hasta el 31-ene-2027** (promoción).
* En **Free Edition** aplican los límites de "fair use". Usalo con criterio: pedí cosas
  concretas, no "rehacé todo el proyecto".

---

## 4 · Genie API: llevar Genie a cualquier lado

* **Chat mode:** conversaciones con estado (*start conversation → create message → poll → get
  query result*). Sirve para bots y apps.
* **Agent mode** (GA ago-2026): mandás un prompt en modo agente, recibís en *streaming* el
  razonamiento y los resultados SQL, y al final un **informe con citas**. También se puede cancelar
  una respuesta en curso.
* **List conversation items:** devuelve todo el historial de razonamiento y llamadas a
  herramientas (sirve para auditar).
* Límites de uso: terceros citan ~20 preguntas/min en la UI y ~5/min por API por workspace. **No
  está confirmado oficialmente**: diseñá con colas y reintentos.
* Integraciones típicas: **Databricks Apps** (una web propia dentro de Databricks), Slack, Teams,
  SharePoint.

---

## 5 · Cómo aplicarlo a NovaMart (ejercicios)

1. **Preparar gold para Genie:**
   1. Agregar `COMMENT` a todas las tablas y columnas de `gold`.
   2. Declarar PK/FK en dimensiones y hechos.
   3. Crear una **metric view** `gold.mv_ventas` con las métricas ventas netas, margen,
      unidades, ticket promedio y % devolución.
2. **Genie Agent "Ventas NovaMart":** `fact_sales`, dimensiones y `mv_ventas`, con 5
   instrucciones, 8 SQL examples, 3 trusted functions (ventas por período, top productos, margen
   por tienda) y 15 benchmarks.
3. **Genie Agent "Operaciones":** `fact_order_fulfillment`, `fact_returns` y
   `fact_inventory_snapshot`, para responder "¿qué tiendas tienen quiebres de stock?".
4. **Agent mode:** preguntar "¿por qué bajó el margen en marzo de 2025?" y comparar el informe
   con tu propio análisis en SQL.
5. **Genie Code:**
   1. Crear tu `.assistant_instructions.md` (idioma, estilo, convenciones del repo).
   2. Escribir una skill "agregar una fuente nueva" y usarla para sumar una fuente ficticia
      (`promotions`).
   3. Pedirle que explique `src/novamart/silver/scd2.py` línea por línea.
6. **API:** un script de Python que le pregunte al Genie Agent "ventas de ayer por país" y mande la
   respuesta a un canal de Slack (o la imprima en pantalla).
7. **Genie One:** publicar el dashboard y el Genie Agent y verlos como los vería un gerente.

---

## Fuentes

* [Genie (docs oficiales)](https://docs.databricks.com/aws/en/genie/)
* [AI/BI and Genie One release notes 2026](https://docs.databricks.com/aws/en/ai-bi/release-notes/2026) · [versión Azure](https://learn.microsoft.com/en-us/azure/databricks/ai-bi/release-notes/2026)
* [Genie Code (docs)](https://docs.databricks.com/aws/en/genie-code/) · [Genie Code para pipelines](https://docs.databricks.com/aws/en/ldp/de-agent)
* [What's new in Genie Code at Data + AI Summit 2026](https://www.databricks.com/blog/whats-new-genie-code-data-ai-summit-2026)
* [Databricks launches Genie Code (press release)](https://www.databricks.com/company/newsroom/press-releases/databricks-launches-genie-code-bringing-agentic-engineering-data)
* [Databricks launches Genie One (press release)](https://www.databricks.com/company/newsroom/press-releases/databricks-launches-genie-one-all-new-agentic-coworker-every-team)
* [Personalizing Genie Code with instructions, skills, memory, and MCP](https://www.databricks.com/blog/personalizing-genie-code-instructions-skills-memory-and-mcp)
* [genie-code-skills-demo (GitHub)](https://github.com/databricks-solutions/genie-code-skills-demo)
* [Use the Genie Agents API](https://docs.databricks.com/aws/en/genie-agents/conversation-api) · [API reference: Conversation](https://docs.databricks.com/api/genie/v1/conversation) · [Genie Space API](https://docs.databricks.com/api/genie/v1/genie-space)
* [Create and manage a Genie Agent](https://docs.databricks.com/aws/en/genie-agents/set-up)
* [How to build production-ready Genie spaces](https://www.databricks.com/blog/how-build-production-ready-genie-spaces-and-build-trust-along-way) · [Data dialogue: best practices](https://databricks.com/blog/data-dialogue-best-practices-guide-building-high-performing-genie-spaces)
* [Python SDK `w.genie`](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/dashboards/genie.html) · [Bundles resources (Azure)](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/resources)
* [Free Edition](https://www.databricks.com/learn/free-edition)
* Artículos de terceros (útiles, pero verificá): [Genie One vs Genie Code vs Genie Agents](https://www.gauraw.com/databricks-genie-one-vs-genie-code-vs-genie-agents/) · [Three products named Genie](https://colrows.com/blogs/databricks-ai-bi-tools-compared/) · [Genie pricing July 2026](https://blog.devgenius.io/databricks-genie-pricing-what-actually-changes-in-july-2026-0f3558f26d61?gi=16ab9450bf5e) · [Best practices for Genie spaces](https://medium.com/dbsql-sme-engineering/best-practices-for-ai-bi-genie-spaces-on-databricks-6f101612c792)
