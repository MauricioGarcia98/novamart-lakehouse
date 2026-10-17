# Módulo 17 · Desafíos finales (nivel senior)

Cada desafío es un mini-proyecto sobre NovaMart. Están pensados para hacerse **solo**, con `scale_factor = 1`
(o más) y para poder contarlos en una entrevista: problema → decisión → resultado medido.

## 🥇 1. Escalar a 10x
Generá `scale_factor = 10` (~1.500M de eventos web). Medí el tiempo de cada task del job.
* ¿Qué task escala peor que lineal? ¿Por qué? (Spark UI: shuffle, spill, skew).
* Proponé y aplicá 2 optimizaciones. Documentá el antes/después en una tabla.

## 🥈 2. Atribución de marketing
Uní `fact_web_sessions` con `fact_sales`: para cada venta online, ¿qué `utm_source` tuvo la última sesión del
cliente en los 7 días previos? Construí `gold.mart_attribution` (last-touch y first-touch) en SQL.
*Pista:* join por `customer_key` + rango de tiempo + `ROW_NUMBER()`.

## 🥉 3. Pronóstico de demanda
Con `mart_sales_daily`, entrená un modelo por departamento (Prophet o `applyInPandas` con regresión) y registrá el
experimento en **MLflow**. Guardá las predicciones en `gold.forecast_daily` y comparalas contra lo real.

## 4. Stock inteligente
Cruzá `fact_inventory_snapshot` con la venta semanal: calculá *weeks of supply*, detectá SKUs con quiebre recurrente
y estimá la venta perdida. Publicalo como dashboard de AI/BI.

## 5. Experimento A/B
`experiment_id` (desde 2025-03-01) divide sesiones en `exp-checkout-v1` y `v2`. ¿Hay diferencia significativa en
la conversión? Calculá intervalos de confianza en SQL (aproximación normal) y escribí la conclusión.

## 6. Dashboard ejecutivo + Genie
Con `mart_sales_daily`, `mart_customer_rfm`, `mart_funnel_daily` y `v_sales_enriched`:
* un **AI/BI Dashboard** con KPIs, tendencia, mix por canal, mapa de tiendas (lat/lon) y embudo;
* un **Genie space** que responda preguntas en lenguaje natural ("¿cuánto vendimos en Black Friday en México?").
  Agregá instrucciones y consultas de ejemplo para que Genie use bien el modelo.

## 7. CDC real
Reemplazá el CSV de clientes por un feed CDC estilo Debezium (`before`/`after`/`op`) y adaptá silver.
Implementalo en ambos enfoques (MERGE manual y `AUTO CDC`).

## 8. Costos
Con `system.billing.usage`, calculá el costo por corrida del job y el costo por millón de filas procesadas.
Probá serverless vs cluster clásico con Photon vs sin Photon.

---

### Cómo presentar el proyecto (portfolio)
1. Diagrama de arquitectura (`docs/diagrams/01_arquitectura.png`) + animación.
2. Modelo estrella (`docs/diagrams/03_modelo_estrella.png`) y matriz de bus.
3. Un problema real que resolviste (skew, SCD2, calidad) con números.
4. CI verde + tests + bundle: "esto se despliega solo".
