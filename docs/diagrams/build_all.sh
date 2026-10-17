#!/usr/bin/env bash
# Regenera TODOS los diagramas y animaciones.
# Requisitos: Python 3.10+, Node 20+, Chromium (o `npx playwright install chromium`).
#   cd docs/diagrams/src && npm install && npm run bundle
#   ./docs/diagrams/build_all.sh   (opcional: CHROMIUM_PATH=/ruta/a/chrome)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
python3 src/build_diagrams.py
args=()
for f in *.excalidraw; do args+=("$f" "${f%.excalidraw}"); done
node src/render.mjs "${args[@]}"
python3 src/build_animation.py 00_proceso_completo 01_arquitectura 02_linaje_tablas 09_ruta_aprendizaje
for n in 01_arquitectura 09_ruta_aprendizaje; do
  tmp="$(mktemp -d)"
  node src/render_frames.mjs "$n.excalidraw" "$tmp" 1400
  python3 src/make_gif.py "$tmp" "../animations/$n.gif"
  rm -rf "$tmp"
done
# hero del README: SVG animado (sin JavaScript, funciona en GitHub)
node src/render_animated_svg.mjs 00_proceso_completo.excalidraw ../animations/proceso_completo.svg
# hero del README estilo "run de Databricks" (CSS puro)
python3 src/build_job_svg.py
