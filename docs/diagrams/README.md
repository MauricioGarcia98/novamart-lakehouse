# Diagramas y animaciones

Todos los diagramas son archivos **`.excalidraw` editables**: abrilos en
[excalidraw.com](https://excalidraw.com) (*Menú → Abrir*) o con la extensión de Excalidraw para VS Code.
Los `.png`/`.svg` son exportaciones para el README.

| archivo | contenido |
|---|---|
| `00_proceso_completo` | proceso completo de 6 etapas (hero animado del README: `../animations/proceso_completo.svg`) |
| `01_arquitectura` | arquitectura end-to-end: fuentes → landing → bronze → silver → gold → consumo + orquestación + gobierno |
| `02_linaje_tablas` | linaje tabla por tabla, marts y tablas de `ops` |
| `03_modelo_estrella` | ERD del modelo estrella con PK/FK/medidas + constelación de facts |
| `04_matriz_bus` | matriz de bus (procesos × dimensiones conformadas) |
| `05_scd2` | SCD Tipo 2 explicado con un cliente real del dataset |
| `06_spark_ejecucion` | driver, executors, particiones, shuffle y problemas de volumen |
| `07_job_dag` | DAG del Lakeflow Job + jobs diario y de mantenimiento |
| `08_cicd` | CI/CD con GitHub Actions y Asset Bundles |
| `09_ruta_aprendizaje` | los 18 módulos de la ruta de aprendizaje |

## Animaciones (Excalimate)

`../animations/*.excanim` son proyectos de [Excalimate](https://excalimate.com) (formato v2.0.0, validados
contra su esquema oficial). Cada elemento aparece en el orden del flujo de datos: fade-in de cajas e iconos
y las flechas se **dibujan** (`drawProgress`).

1. Abrí [app.excalimate.com](https://app.excalimate.com) → *Open* → elegí `01_arquitectura.excanim`.
2. Ajustá tiempos en el timeline si querés.
3. *Export* → MP4 / WebM / GIF / SVG animado.

Los `.gif` del README se generan con `src/render_frames.mjs` + `src/make_gif.py` (mismo orden de pasos).

### SVG animado (`../animations/proceso_completo.svg`)

`src/render_animated_svg.mjs` exporta cada paso con la librería oficial de Excalidraw usando el mismo encuadre
y los une en **un único SVG** con animaciones CSS: cada etapa aparece y sus trazos se dibujan a mano
(`stroke-dashoffset` con `pathLength=1`). No usa JavaScript, así que GitHub lo muestra animado dentro del README,
y respeta `prefers-reduced-motion`.

## Diagramas como código

Los diagramas se generan con [`src/build_diagrams.py`](src/build_diagrams.py) usando un mini-toolkit
([`src/excalidraw_kit.py`](src/excalidraw_kit.py)) que:

* inserta iconos de **librerías de Excalidraw** (escalados, re-identificados y agrupados);
* crea tarjetas con icono + título + subtítulo, zonas, notas, tablas ER con badges PK/FK, flechas enlazadas;
* guarda en cada elemento `customData.step` (orden de aparición para la animación).

```bash
cd docs/diagrams/src
npm install && npm run bundle        # Excalidraw oficial + Playwright
cd .. && ./build_all.sh              # .excalidraw → .svg/.png → .excanim → .gif
```

Si editás un diagrama a mano en excalidraw.com, exportalo con *Export image → PNG/SVG* y guardalo encima.

## Créditos

Iconos de librerías públicas de [libraries.excalidraw.com](https://libraries.excalidraw.com)
(copias en [`libraries/`](libraries/), licencia MIT del repositorio
[excalidraw-libraries](https://github.com/excalidraw/excalidraw-libraries)):

| librería | autor | usada para |
|---|---|---|
| Databricks Architecture Icons | lukethorp | Unity Catalog, Lakeflow, Photon, Genie, Dashboards, Databricks SQL… |
| Data Platform | chuqbach | Spark, Delta Lake, Parquet, MLflow, Databricks |
| IT icons | mateuszbaransanok | servidores, usuarios, bases de datos, gráficos |
| GitHub Actions | devdaejungyoon | GitHub, GitHub Actions |
| Data processing | erlina | recolección, limpieza y almacenamiento de datos |
| Data sources, IT Logos, System Design Icons | kvchitrapu, pclainchard, niknm | complementarios |

Las marcas pertenecen a sus dueños; se usan sólo con fines descriptivos/educativos.
