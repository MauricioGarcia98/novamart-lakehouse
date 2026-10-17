# 10 · Prompts para nuevos proyectos "Andes [negocio]"

> Plantillas para crear, con Claude Code, proyectos del mismo estilo que NovaMart, pero de otros
> negocios. Cada uno va en **su propio repositorio**.
>
> **Cómo usarlos:**
> 1. Creá un repo vacío en GitHub (ej. `andes-viento-austral`).
> 2. Abrí una sesión de Claude Code sobre ese repo.
> 3. Pegá el **Bloque base** (sección 1) y **debajo** el bloque del negocio que elijas.
> 4. Pedí que trabaje por etapas: primero el plan y los diagramas, después el generador, después
>    las capas, etc. Revisá cada etapa antes de seguir.
>
> Todos los negocios están pensados para salir de los proyectos típicos (e-commerce genérico,
> Titanic, Netflix, NYC Taxi): son casos **latinoamericanos**, con problemas que una empresa real
> pagaría por resolver.

---

## 1 · Bloque base (pegarlo SIEMPRE primero)

```text
Quiero que construyas un proyecto de datos end-to-end en Databricks para APRENDER, con calidad de
producción, en español rioplatense. Asumí todos los roles técnicos necesarios (Product Owner de
datos, Data Engineer, Analytics Engineer, Platform/DataOps, Data Steward, Analista BI, Data
Scientist y SRE) y dejá claro en la documentación qué decisión toma cada rol y por qué.

REQUISITOS TÉCNICOS
1. Plataforma: Databricks con Unity Catalog, compute serverless, compatible con FREE EDITION.
   - El catálogo se crea por SQL (CREATE CATALOG) porque en Default Storage la API no puede.
     Documentá ese paso antes del primer deploy.
   - Probá todo con un scale_factor chico (0.1 ≈ 3 GB) y que escale a 1 y 10.
2. Generador de datos sintéticos DISTRIBUIDO en Spark, determinístico (semilla), controlado por
   scale_factor, con "suciedad" realista inyectada a propósito: duplicados, nulos, formatos de
   fecha mezclados, valores imposibles, datos tardíos, CDC (inserts/updates/deletes), una columna
   nueva que aparece en una fecha (schema drift) y estacionalidad real del negocio y del país
   (feriados, inflación, clima, eventos).
3. Arquitectura medallion: landing (UC Volume) → bronze (Auto Loader, linaje, rescued data;
   en CSV el contrato va por cloudFiles.schemaHints, nunca .schema() con addNewColumns; reiniciar
   el stream en código ante UNKNOWN_FIELD_EXCEPTION) → silver (tipado, dedupe, reglas de calidad
   con cuarentena, SCD2) → gold (modelo dimensional Kimball + marts) → ops (auditoría, watermarks,
   resultados de calidad).
4. Código en un paquete Python (src/<proyecto>) + notebooks delgados que lo llaman. Tests con
   pytest + Spark/Delta local (unitarios e integración) y ruff.
5. Orquestación con Lakeflow Jobs definida en un Databricks Asset Bundle (databricks.yml +
   resources/*.yml): job e2e (con condition task, for_each por entidad, run_if correcto y
   "outcome" en las dependencias de if/else), job incremental diario y job de mantenimiento.
   Además, una versión DECLARATIVA del pipeline en SQL (Lakeflow Declarative Pipelines) con
   expectations, para comparar ambos estilos. Targets dev y prod.
6. CI/CD con GitHub Actions: lint, tests, bundle validate y deploy manual.
7. Gobierno: comments en tablas y columnas, PK/FK declaradas, tags de PII, máscaras de columna y
   row filters donde tenga sentido de negocio.
8. CAPA GENIE (obligatoria):
   - Metric views con las métricas oficiales del negocio.
   - Al menos 2 Genie Agents (ex Genie spaces) por dominio, con instrucciones cortas, SQL
     examples, trusted assets (funciones de UC) y 20+ benchmarks con respuesta esperada.
   - Su configuración versionada en el repo (serialized_space en JSON) y un script para
     desplegarla con la API o el SDK de Python.
   - Archivo .assistant_instructions.md y 2 skills de Genie Code para las convenciones del proyecto.
   - Preguntas de ejemplo para el Agent mode (análisis de "por qué") con la respuesta que debería
     encontrar.
   - Un ejemplo de uso de la Genie Conversation API (script o Databricks App).
9. Dashboards AI/BI (archivo .lvdash.json en el bundle) para cada caso de negocio.

REQUISITOS VISUALES (muy importantes)
- README con portada: un SVG ANIMADO del job corriendo (estilo UI oscura de Databricks, las
  tareas pasan de pending → running → succeeded), badges y un índice.
- Diagramas en Excalidraw (.excalidraw + .svg + .png): proceso completo, arquitectura, linaje de
  tablas, modelo estrella, matriz bus, SCD2, ejecución de Spark, DAG del job, CI/CD y ruta de
  aprendizaje. Al menos 2 animados (GIF/SVG).
- Tablas, emojis con moderación y secciones colapsables en el README.

REQUISITOS DE APRENDIZAJE
- Carpeta learning/ con una ruta de 15+ módulos progresivos (SQL básico → avanzado → Spark →
  performance → Delta → streaming → modelado → declarativo → gobierno → Genie → CI/CD → desafíos
  finales), todos usando los datos del proyecto, con ejercicios y soluciones.
- docs/: arquitectura con ADRs, fuentes y diccionario de datos, modelado, performance, despliegue,
  guía paso a paso desde la web de Databricks para alguien que no sabe nada, bitácora de errores
  reales explicados en criollo, "cómo hacerlo más real", y Genie.
- Cada caso de negocio documentado como: problema → quién lo sufre → decisión que habilita →
  KPI de éxito → datos necesarios → tablas gold/marts → dashboard → preguntas para Genie.

FORMA DE TRABAJO
- Trabajá por etapas, con commits claros, y mostrame el plan antes de escribir código.
- Si tenés acceso a un workspace (DATABRICKS_HOST/DATABRICKS_TOKEN), desplegá y corré el job
  real, reportá los resultados reales de cada tarea, arreglá lo que falle y anotalo en la bitácora.
- Nunca imprimas tokens ni secretos.
```

---

## 2 · Andes Viento Austral ⚡ (energía renovable en Patagonia y la Puna)

```text
NEGOCIO: "Andes Viento Austral" opera 9 parques eólicos en Chubut y Santa Cruz y 4 parques solares
en Jujuy y Catamarca. Vende energía al mercado mayorista (CAMMESA) y a grandes clientes
industriales por contratos PPA (MATER). Tiene 380 aerogeneradores y 1,2 millones de paneles.

FUENTES (con volumen a SF=1):
- Telemetría SCADA de turbinas cada 10 min: viento, rpm, potencia, temperatura de rodamientos,
  vibraciones, códigos de alarma (≈ 200M filas/año). JSON con drift: un fabricante agrega
  sensores nuevos en 2025.
- Inversores solares cada 5 min: irradiancia, temperatura de panel, potencia DC/AC.
- Órdenes de mantenimiento (CMMS): preventivo y correctivo, repuestos, cuadrillas, tiempos de
  viaje (parques a 300 km de la base).
- Despacho y restricciones de CAMMESA: curtailment (orden de bajar potencia por falta de líneas
  de transmisión) y precios spot por hora.
- Contratos PPA: precio, volumen comprometido, penalidades.
- Clima: pronóstico y real (viento, nieve, hielo en palas, polvo en paneles de la Puna).

CASOS DE NEGOCIO (todos):
1. ENERGÍA PERDIDA POR CURTAILMENT: ¿cuántos MWh y USD perdimos porque CAMMESA nos hizo bajar
   potencia? ¿En qué parque conviene invertir en baterías?
2. MANTENIMIENTO PREDICTIVO: detectar con días de anticipación las turbinas con rodamientos o
   multiplicadoras a punto de fallar (una falla en Santa Cruz en invierno = 3 semanas parada).
3. LIMPIEZA DE PANELES EN LA PUNA: el polvo baja la generación; ¿cuál es el momento óptimo para
   limpiar, sabiendo que el agua es escasa y cara?
4. CUMPLIMIENTO DE PPA: ¿vamos a llegar al volumen comprometido este mes? Si no, ¿compramos en el
   spot o pagamos la penalidad?
5. DISPONIBILIDAD CONTRACTUAL vs. GARANTÍA DEL FABRICANTE: reclamar al fabricante cuando la
   disponibilidad cae debajo de lo garantizado (97 %).

PROBLEMAS DE DATOS A RESOLVER: relojes de SCADA desfasados, huecos de comunicación por satélite,
sensores clavados (valor constante), unidades mezcladas (kW/MW), cambios de husos horarios,
turbinas renombradas.

MODELO: fact_turbine_10min, fact_curtailment_hourly, fact_maintenance_order, fact_ppa_settlement,
dim_turbine (SCD2: cambios de componentes), dim_park, dim_weather, dim_contract.

GENIE: Agent "Operación de parques" (jefes de parque), Agent "Comercial PPA" (gerente
comercial), Agent mode para "¿por qué el parque X generó 18 % menos en julio?".
```

---

## 3 · Andes Cosecha Alta 🍇 (bodega exportadora de vinos de altura)

```text
NEGOCIO: "Andes Cosecha Alta" es una bodega con viñedos propios en el Valle de Uco (Mendoza),
Cafayate (Salta) y la Patagonia. Compra uva a 140 productores, elabora 9 millones de litros por
año y exporta el 65 % a 30 países. También vende directo al consumidor (wine club y turismo).

FUENTES:
- Sensores de viñedo (humedad de suelo, temperatura, heladas, granizo) por parcela.
- Maduración: muestreos de azúcar (°Brix), acidez y pH por cuartel.
- Recepción de uva: camiones, kilos, calidad, precio pactado por productor.
- Bodega: tanques, fermentación (temperatura y densidad cada hora), barricas, cortes, botellas.
- Exportación: órdenes, contenedores, puertos, tipo de cambio, retenciones y reintegros.
- Wine club: suscriptores, cajas mensuales, bajas; turismo: reservas y degustaciones.

CASOS DE NEGOCIO:
1. HELADAS Y GRANIZO: ¿qué parcelas necesitan protección esta noche? ¿Cuánto costó cada evento
   climático en kilos y calidad?
2. FECHA ÓPTIMA DE COSECHA por cuartel, equilibrando madurez, pronóstico y capacidad de bodega.
3. PAGO JUSTO A PRODUCTORES: precio por calidad real; detectar productores que mezclan uva de
   otra zona (fraude de origen).
4. RENTABILIDAD POR MERCADO CON FX VOLÁTIL: ¿qué país deja más margen en pesos constantes
   después de retenciones, flete y tipo de cambio?
5. WINE CLUB: churn de suscriptores y qué caja los retiene.
6. TRAZABILIDAD: de la botella al cuartel, para un reclamo de calidad en Brasil.

PROBLEMAS DE DATOS: planillas manuales de productores, unidades (kg/quintales), lotes que se
mezclan (un tanque recibe uva de 5 parcelas: linaje N a N), retenciones que cambian por decreto.

GENIE: Agent "Enología" (enólogos), Agent "Exportación" (comercio exterior), Agent "Wine club"
(marketing). Agent mode: "¿por qué bajó el margen del Malbec reserva en EE. UU. este trimestre?".
```

---

## 4 · Andes Paso Cordillera 🚛 (logística de carga entre Argentina, Chile, Uruguay y Brasil)

```text
NEGOCIO: "Andes Paso Cordillera" es un operador logístico con 900 camiones propios y 2.000
terceros. Mueve carga entre Buenos Aires, Mendoza, Santiago, Valparaíso, Montevideo y São Paulo.
El cuello de botella es el Paso Internacional Cristo Redentor, que se cierra por nieve.

FUENTES:
- GPS de camiones cada 30 s (posición, velocidad, motor encendido, temperatura de carga refrigerada).
- Viajes y órdenes de transporte: origen, destino, cliente, tarifa, carga peligrosa o no.
- Aduana: documentos (MIC/DTA), tiempos en frontera, inspecciones, demoras.
- Estado del paso: abierto, cerrado, con restricciones (feed oficial + clima).
- Combustible: cargas con tarjeta, precio por país, rendimiento por camión.
- Choferes: horas de manejo, descansos obligatorios, licencias, incidentes.

CASOS DE NEGOCIO:
1. CIERRE DEL PASO: cuando se pronostica nieve, ¿qué viajes adelanto, demoro o desvío? Costo real
   de cada día de cierre (camiones parados, multas, mercadería perecedera).
2. CUMPLIMIENTO DE ENTREGA (OTIF) por cliente y ruta, y las causas raíz de los atrasos.
3. FRAUDE DE COMBUSTIBLE: cargas que no coinciden con la posición del GPS o con el consumo esperado.
4. CADENA DE FRÍO: alertas de temperatura en carga refrigerada (frutas chilenas, medicamentos).
5. RENTABILIDAD POR RUTA con tipos de cambio de 4 países y viajes de retorno vacíos.
6. FATIGA DE CHOFERES: riesgo por exceso de horas de manejo (seguridad vial).

PROBLEMAS DE DATOS: GPS con saltos imposibles, señal perdida en la cordillera, husos horarios de
4 países, monedas, choferes duplicados con distintos DNI/RUT, documentos aduaneros escaneados.

GENIE: Agent "Torre de control" (operaciones 24 h), Agent "Clientes y OTIF" (comercial),
Agent "Flota y combustible". API de Genie conectada a un bot de Slack/WhatsApp para despachantes.
```

---

## 5 · Andes Salar Vivo 🧂 (minería de litio con foco en agua y comunidades)

```text
NEGOCIO: "Andes Salar Vivo" produce carbonato de litio en 2 salares de la Puna (Catamarca y
Salta) por evaporación en pozas, y está probando extracción directa (DLE). Exporta a fabricantes
de baterías en Asia. Su licencia social depende del USO DE AGUA y de la relación con las
comunidades.

FUENTES:
- Pozos de salmuera: caudal, concentración de litio, niveles freáticos (sensores cada hora).
- Pozas de evaporación: área, nivel, concentración, evaporación diaria (clima de altura).
- Planta: lotes de producción, pureza (grado batería 99,5 %), consumo de reactivos y energía.
- Laboratorio: análisis químicos con resultados manuales y re-análisis.
- Agua dulce: extracción, reutilización, monitoreo de vegas y lagunas cercanas.
- Comunitario: acuerdos, empleo local, reclamos, inversiones sociales.
- Comercial: contratos con precio indexado al spot del litio, embarques por Antofagasta.

CASOS DE NEGOCIO:
1. RENDIMIENTO DE POZAS: litio recuperado vs. bombeado; pozas con fugas o baja evaporación.
2. HUELLA HÍDRICA: litros de agua dulce por tonelada; alerta temprana si una vega baja su nivel.
3. CALIDAD GRADO BATERÍA: predecir lotes fuera de especificación antes del final del proceso.
4. PRECIO VOLÁTIL: ¿conviene acopiar o vender cuando el precio cae 40 %?
5. REPORTE ESG automático para inversores y para la comunidad (con trazabilidad del dato).
6. DLE vs. EVAPORACIÓN: comparar costo, agua y tiempo del piloto.

PROBLEMAS DE DATOS: análisis de laboratorio re-emitidos (versionado), sensores a 4.000 m que se
congelan, unidades químicas (mg/L, g/L, % LCE), datos comunitarios cualitativos.

GENIE: Agent "Operación salar", Agent "ESG y agua" (con row filters: la comunidad ve sólo sus
indicadores). Agent mode: "¿por qué la recuperación de litio cayó en la poza 7?".
```

---

## 6 · Andes Almacén Red 🏪 (PROYECTO GENIE-FIRST: distribuidora para almacenes de barrio)

> Este es el proyecto **orientado al workspace de Genie**: el objetivo principal es que cada rol
> trabaje con un producto Genie distinto (Genie One, Genie Agents, Agent mode, Genie Code y la API).

```text
NEGOCIO: "Andes Almacén Red" es una distribuidora mayorista de consumo masivo que abastece a
45.000 almacenes, kioscos y autoservicios de barrio en AMBA, Córdoba, Rosario y Tucumán. Trabaja
con 600 preventistas (vendedores que visitan almacenes con el celular), 220 camiones de reparto
y una app de pedidos para el almacenero. Además, financia a los almacenes (crédito comercial) y les
ofrece una billetera con cobro por QR.

FUENTES:
- Visitas de preventistas: GPS, check-in, pedido tomado o no, motivo de no compra.
- Pedidos (app + preventista) con CDC: modificaciones y cancelaciones.
- Catálogo: 8.000 SKUs, precios que cambian semanalmente por inflación, listas por zona.
- Reparto: hojas de ruta, entregas, rechazos, faltantes, cobranzas en efectivo.
- Crédito: límites, saldos, atrasos, refinanciaciones.
- Billetera/QR: ventas diarias del almacén (sirve como señal de salud del negocio del cliente).
- Externos: IPC, feriados, clima, partidos de fútbol (picos de bebidas).

CASOS DE NEGOCIO:
1. RUPTURA EN GÓNDOLA DEL ALMACÉN: ¿qué almacenes están por quedarse sin productos clave?
   Sugerencia de pedido al preventista.
2. CRÉDITO A ALMACENEROS: score de riesgo usando ventas por QR; ¿a quién subirle el límite?
3. PRECIOS EN INFLACIÓN: elasticidad por zona; ¿cuándo aumentar sin perder volumen?
4. RUTAS DE REPARTO Y RECHAZOS: entregas rechazadas por falta de efectivo o por error de pedido.
5. PRODUCTIVIDAD DEL PREVENTISTA: visitas efectivas, cobertura de la cartera, "almacenes olvidados".
6. MIX DE MARCAS PROPIAS vs. LÍDERES: margen y canibalización.

CAPA GENIE (central en este proyecto):
- GENIE ONE: experiencia móvil para el gerente regional y el preventista (dashboards + agentes,
  sin ver código).
- GENIE AGENTS: "Mi cartera" (preventista: row filter por vendedor), "Crédito y cobranzas"
  (analista de riesgo), "Precios y márgenes" (pricing), "Logística" (jefe de depósito).
  Cada uno con metric views, trusted functions (ej. sugerir_pedido(almacen_id),
  riesgo_credito(almacen_id)) y 30 benchmarks.
- AGENT MODE: guiones de preguntas "por qué" (ej. "¿por qué cayó la venta de bebidas en Rosario
  en la segunda semana de junio?") con la respuesta esperada, para evaluar al agente.
- GENIE CODE: .assistant_instructions.md del equipo, skills "agregar SKU/fuente nueva" y
  "crear un benchmark", y un ejercicio de construir un pipeline completo con Genie Code en el
  Lakeflow Pipelines Editor, comparándolo con el pipeline escrito a mano.
- GENIE API: Databricks App (o bot de WhatsApp/Slack simulado) donde el preventista pregunta
  "¿qué le ofrezco a Almacén Don Pepe hoy?".
- CI de Genie: un script que corre los benchmarks por API y falla el pipeline si la precisión
  baja de 85 %.
- DOCUMENTO "GENIE PLAYBOOK": cómo diseñar, evaluar, desplegar y gobernar agentes Genie.
```

---

## 7 · Andes Latido 🏥 (red de clínicas y prepaga)

```text
NEGOCIO: "Andes Latido" es una prepaga con 650.000 afiliados y una red propia de 12 clínicas y 40
centros médicos en Argentina y Uruguay, más 9.000 prestadores externos.

FUENTES: turnos (reservas, ausentismo, sobreturnos), guardias (triage, tiempos de espera),
internaciones (camas, días, altas), autorizaciones de prácticas, facturación de prestadores,
recetas y medicamentos, afiliados (altas, bajas, planes, SCD2), encuestas NPS.
Datos SENSIBLES: aplicar máscaras, row filters y tags de PII y salud desde el diseño.

CASOS DE NEGOCIO:
1. AUSENTISMO A TURNOS (20-30 %): predecir faltas y hacer sobreturnos inteligentes.
2. SATURACIÓN DE GUARDIAS: pronóstico por hora (invierno, brotes respiratorios, dengue).
3. FRAUDE PRESTACIONAL: prestadores que facturan prácticas imposibles o duplicadas.
4. CAMAS: ocupación y altas demoradas que bloquean internaciones nuevas.
5. FUGA DE AFILIADOS: ¿quién se va a la competencia después de un rechazo de autorización?
6. COSTO MÉDICO POR PLAN: siniestralidad vs. cuota, con inflación médica.

GENIE: Agent "Dirección médica" y Agent "Auditoría de prestadores", con gobierno estricto
(ningún dato personal sale en las respuestas).
```

---

## 8 · Andes Hinchada Total ⚽ (club de fútbol y su negocio)

```text
NEGOCIO: "Andes Hinchada Total" es un club de fútbol de primera división con 180.000 socios,
estadio de 55.000 personas, tienda oficial, escuela de fútbol y venta de jugadores al exterior.

FUENTES: socios y cuotas (morosidad, categorías, SCD2), molinetes del estadio (ingresos por
puerta/minuto), venta de abonos y entradas, tienda online y física, sponsors (exposición en TV),
rendimiento deportivo (GPS de jugadores, partidos, eventos por minuto), mercado de pases (valor,
cláusulas, % de reventa), redes sociales.

CASOS DE NEGOCIO:
1. MOROSIDAD DE SOCIOS: ¿quién deja de pagar después de 3 derrotas seguidas? Campañas de retención.
2. OPERATIVO DE ESTADIO: colas en molinetes, apertura de puertas, seguridad (picos por minuto).
3. MERCHANDISING: demanda de camisetas por jugador (fichajes, goles), quiebres en la tienda.
4. VALOR DE JUGADORES: ¿cuándo conviene vender? Rendimiento vs. valor de mercado vs. edad.
5. SPONSORS: valor real de la exposición para renegociar contratos.
6. CARGA FÍSICA: riesgo de lesión con datos GPS de los entrenamientos.

GENIE: Agent "Socios y estadio" (gerencia), Agent "Scouting" (cuerpo técnico), Agent mode:
"¿por qué bajó la recaudación del último clásico?".
```

---

## 9 · Más ideas (para desarrollar con el mismo formato)

| Nombre | Negocio | Caso estrella |
|---|---|---|
| **Andes Gota Clara** 💧 | Empresa de agua con medidores inteligentes | Pérdidas no técnicas (fugas y conexiones clandestinas) por zona |
| **Andes Pampa Acopio** 🌾 | Acopio y exportación de granos | Camiones al puerto de Rosario: colas, calidad y precio en el Matba-Rofex |
| **Andes Seguro Rural** 🌦️ | Seguro agrícola paramétrico | Pago automático por sequía o granizo según datos satelitales y de clima |
| **Andes Escuela Abierta** 🎓 | Red de institutos y edtech | Deserción temprana de alumnos y becas que retienen |
| **Andes Ruedas Urbanas** 🚲 | Bicis y monopatines compartidos | Rebalanceo de estaciones y vandalismo |
| **Andes Feria Federal** 🧶 | Marketplace de artesanos y productores regionales | Envíos desde pueblos remotos y fraude de vendedores |
| **Andes Billetera Celeste** 💳 | Billetera virtual con QR y cuotas | Fraude en tiempo real y contracargos |
| **Andes Ruta del Sur** 🏔️ | Turismo en Patagonia (hoteles, excursiones, vuelos) | Overbooking, estacionalidad y precio dinámico con turismo extranjero |

---

## 10 · Consejos para que salgan bien

1. **Un repo por negocio.** No mezcles proyectos.
2. **Pedí primero el plan** y los diagramas: corregir un diagrama es más barato que corregir código.
3. **Pedí que corra todo en tu workspace** y que escriba la bitácora de errores reales: ahí se
   aprende.
4. **Hacé la ruta `learning/`** de cada proyecto vos mismo, sin mirar las soluciones.
5. **Cambiá algo por tu cuenta** en cada proyecto (un caso de negocio nuevo, una fuente nueva). Eso
   lo vuelve tuyo y te da de qué hablar en una entrevista.
