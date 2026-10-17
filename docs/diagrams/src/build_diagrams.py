"""Genera todos los diagramas `.excalidraw` del proyecto.

    python docs/diagrams/src/build_diagrams.py        # escribe docs/diagrams/*.excalidraw
    node   docs/diagrams/src/render.mjs ...           # exporta a SVG/PNG (ver README de diagrams)

Cada elemento lleva `customData.step`: el orden en que aparece en la animación.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from excalidraw_kit import (  # noqa: E402
    BLUE,
    BRONZE,
    CODE,
    CYAN,
    GOLD,
    GRAY,
    GREEN,
    INK,
    NORMAL,
    ORANGE,
    PINK,
    RED,
    SILVER,
    TEAL,
    VIOLET,
    YELLOW,
    Scene,
)

OUT = Path(__file__).resolve().parents[1]
DB = "databricks"
DP = "data-platform"
IT = "it-icons"
GH = "github-actions"
PROC = "data-processing"



# =============================================================================
# 00 · Proceso completo (hero animado del README)
# =============================================================================
def proceso_completo() -> Scene:
    s = Scene(seed=101)
    s.text(1200, 0, "NovaMart Lakehouse · el proceso completo", size=46, anchor="n")
    s.text(1200, 62, "de datos crudos a decisiones de negocio, en Databricks, con cientos de millones de filas",
           size=21, anchor="n", font=NORMAL, color="#495057")
    stages = [
        ("1", "Generar", "datos sintéticos a escala", CYAN, PROC, "Data collection",
         "8 fuentes · scale factor\n12M pedidos · 150M eventos\nduplicados, nulos, drift", "PySpark distribuido"),
        ("2", "Aterrizar", "zona landing", GRAY, PROC, "Data storage",
         "UC Volume\nCSV · JSON.gz · Parquet\nuna carpeta por día", "Unity Catalog"),
        ("3", "Bronze", "ingesta incremental", BRONZE, DB, "Streaming",
         "Auto Loader\nschema evolution\nlinaje por archivo", "Delta Lake"),
        ("4", "Silver", "limpiar y validar", SILVER, PROC, "Data cleansing/transformation",
         "tipado + deduplicación\nreglas + cuarentena\nSCD Tipo 2 con MERGE", "Spark + MERGE"),
        ("5", "Gold", "modelar el negocio", GOLD, DB, "Lakehouse",
         "estrella Kimball\n6 dims · 5 facts\njoins point-in-time", "Liquid clustering"),
        ("6", "Consumir", "decidir con datos", GREEN, DB, "Dashboards",
         "AI/BI dashboards\nGenie · Databricks SQL\nRFM · cohortes · embudo", "Databricks SQL"),
    ]
    cards = []
    for i, (num, title, sub, color, lib, ic, bullets, tech) in enumerate(stages):
        s.step = i + 1
        x, y, w, h = 30 + i * 395, 150, 340, 460
        card = s.rect(x, y, w, h, color, fill="hachure", stroke_width=3)
        s.rect(x + 12, y + 12, w - 24, h - 24, (color[0], "#ffffff"), fill="solid", stroke_width=1)
        s.ellipse(x - 18, y - 22, 64, 64, color, fill="solid", label=num, size=30)
        ic_box = s.icon(lib, ic, 0, 0, 120)
        s.move_group(ic_box, x + (w - ic_box["width"]) / 2, y + 40)
        s.text(x + w / 2, y + 180, title, size=34, anchor="n", color=color[0])
        s.text(x + w / 2, y + 225, sub, size=17, anchor="n", font=NORMAL, color="#495057")
        s.text(x + w / 2, y + 268, bullets, size=18, anchor="n", align="center")
        b = s.badge(0, 0, tech, color, size=15)
        bw = b["width"]
        b["x"], b["y"] = x + (w - bw) / 2, y + h - 62
        for e in s.elements:
            if e.get("containerId") == b["id"]:
                e["x"], e["y"] = b["x"] + 8, b["y"] + 4
        if cards:
            s.arrow(cards[-1], card, color=color[0], width=4)
        cards.append(card)
    s.step = 7
    s.line([(60, 660), (2340, 660)], color="#adb5bd", width=2, dashed=True)
    s.text(1200, 668, "a lo largo de todo el pipeline", size=18, anchor="n", font=NORMAL, color="#868e96")
    cross = [
        (ORANGE, DB, "Lakeflow Jobs", "Orquestación", "for_each · condition · run_job\nquality gate bloqueante"),
        (RED, IT, "Firewall", "Calidad de datos", "contratos · cuarentena\nmétricas por regla y lote"),
        (VIOLET, DB, "Unity Catalog", "Gobierno", "linaje · PK/FK · tags PII\nmáscaras y row filters"),
        (BLUE, GH, "GitHub Actions", "CI/CD", "ruff · pytest · e2e local\nAsset Bundles dev/prod"),
        (TEAL, DB, "Notebooks", "Aprendizaje", "18 módulos SQL → Spark\n→ modelado → producción"),
    ]
    for k, (color, lib, ic, title, desc) in enumerate(cross):
        x = 30 + k * 475
        s.rect(x, 705, 445, 130, color, fill="solid")
        ic_box = s.icon(lib, ic, 0, 0, 70)
        s.move_group(ic_box, x + 16, 705 + (130 - ic_box["height"]) / 2)
        s.text(x + 104, 720, title, size=24, color=color[0])
        s.text(x + 104, 760, desc, size=16, font=NORMAL, color="#343a40")
    s.step = 8
    stats = [("350M+", "filas con SF=1", CYAN), ("8 → 33", "fuentes → tablas", BRONZE), ("4", "tipos de facts", GOLD),
             ("18", "módulos", TEAL), ("13", "tests", BLUE), ("1", "comando para desplegar", VIOLET)]
    for k, (big, small, color) in enumerate(stats):
        x = 30 + k * 395
        s.rect(x, 870, 340, 120, color, fill="solid", stroke_width=2)
        s.text(x + 170, 878, big, size=46, anchor="n", color=color[0])
        s.text(x + 170, 945, small, size=19, anchor="n", font=NORMAL, color="#343a40")
    return s

# =============================================================================
# 01 · Arquitectura end-to-end
# =============================================================================
def arquitectura() -> Scene:
    s = Scene(seed=11)
    s.text(1150, 10, "NovaMart Lakehouse · arquitectura end-to-end en Databricks", size=40, anchor="n")
    s.text(1150, 62, "Spark + SQL + Delta Lake + Unity Catalog · de archivos crudos a decisiones de negocio",
           size=20, anchor="n", font=NORMAL, color="#495057")

    # ---- paso 1: fuentes
    s.step = 1
    s.zone(20, 130, 330, 740, "Sistemas origen", GRAY, icon=(PROC, "Data collection", 44))
    sources = [
        (IT, "Host", "POS de tiendas", "pedidos · CSV/JSON"),
        (IT, "Device", "Web + App", "pedidos JSON anidado"),
        (IT, "Internet", "Clickstream", "150M eventos · JSON.gz"),
        (IT, "User", "CRM clientes", "CDC diario · CSV"),
        (IT, "Database", "ERP catálogo", "precios · JSON"),
        (IT, "Server", "Logística", "estados del pedido"),
        (IT, "Drive", "WMS inventario", "snapshot · Parquet"),
    ]
    src_cards = []
    for i, (lib, name, title, sub) in enumerate(sources):
        src_cards.append(s.row_card(lib, name, 45, 190 + i * 94, 280, 82, GRAY, title, sub, icon_size=48))

    # ---- paso 2: landing
    s.step = 2
    s.zone(400, 130, 270, 740, "Landing", CYAN, icon=(DB, "Unity Catalog", 40))
    vol = s.icon_card(PROC, "Data storage", 425, 230, 220, 230, CYAN, "UC Volume", "/Volumes/novamart/\nlanding/raw", icon_size=110)
    s.icon(DP, "Parquet", 440, 520, 60)
    s.icon(IT, "Document", 520, 525, 50)
    s.icon(IT, "Folder", 585, 525, 55)
    s.text(535, 600, "CSV · JSON · JSON.gz\nParquet", size=16, anchor="n", font=NORMAL, color=CYAN[0])
    s.note(420, 680, 230, "carpetas\ningest_date=AAAA-MM-DD\n= lo que llegó ese día", CYAN, size=15)
    for c in src_cards:
        s.arrow(c, vol, "right", "left", color="#868e96", width=1)

    # ---- paso 3: bronze
    s.step = 3
    s.zone(720, 130, 380, 740, "Bronze · crudo", BRONZE, icon=(DP, "Delta Lake", 40))
    al = s.row_card(DB, "Streaming", 745, 200, 330, 92, BRONZE, "Auto Loader", "cloudFiles · availableNow", icon_size=56)
    br = s.icon_card(DP, "Delta Lake", 790, 330, 240, 200, BRONZE, "8 tablas Delta", "copia fiel + _source_file\n+ _rescued_data", icon_size=90)
    s.note(745, 570, 330, "• incremental (checkpoint)\n• schema evolution\n• nunca se pierde un dato", BRONZE, size=16)
    s.arrow(vol, al, "right", "left", color=BRONZE[0], width=3, label="archivos\nnuevos")
    s.arrow(al, br, "bottom", "top", color=BRONZE[0], width=3)

    # ---- paso 4: silver
    s.step = 4
    s.zone(1150, 130, 380, 740, "Silver · confiable", SILVER, icon=(DP, "Spark", 46))
    sv = s.icon_card(PROC, "Data cleansing/transformation", 1195, 200, 290, 210, SILVER, "Limpieza + tipado", "dedup · try_to_timestamp · DECIMAL", icon_size=100)
    s.row_card(IT, "Firewall", 1175, 440, 330, 86, RED, "Calidad de datos", "reglas · cuarentena · métricas", icon_size=52)
    s.row_card(IT, "Timer", 1175, 545, 330, 86, VIOLET, "SCD Tipo 2 (MERGE)", "clientes y productos con historia", icon_size=52)
    s.row_card(IT, "Key", 1175, 650, 330, 86, SILVER, "Upserts idempotentes", "foreachBatch + MERGE", icon_size=52)
    s.arrow(br, sv, "right", "left", color=SILVER[0], width=3, label="readStream")

    # ---- paso 5: gold
    s.step = 5
    s.zone(1580, 130, 400, 740, "Gold · negocio", GOLD, icon=(DB, "Lakehouse", 42))
    gd = s.icon_card(DB, "Lakehouse", 1625, 200, 310, 200, GOLD, "Modelo estrella (Kimball)", "6 dimensiones · 5 facts", icon_size=90)
    s.row_card(IT, "Bar chart", 1605, 430, 350, 86, GOLD, "Marts en SQL", "ventas · RFM · cohortes · embudo", icon_size=52)
    s.row_card(IT, "Network", 1605, 535, 350, 86, GOLD, "Point-in-time joins", "FK a la versión SCD2 vigente", icon_size=52)
    s.row_card(DB, "Photon", 1605, 640, 350, 86, GOLD, "Liquid clustering", "data skipping + Photon", icon_size=48)
    s.arrow(sv, gd, "right", "left", color=GOLD[0], width=3, label="replaceWhere\n+ MERGE")

    # ---- paso 6: consumo
    s.step = 6
    s.zone(2030, 130, 340, 740, "Consumo", GREEN, icon=(IT, "Pie chart", 40))
    cons = [
        (DB, "Dashboards", "AI/BI Dashboards"),
        (DB, "Genie", "Genie (lenguaje natural)"),
        (DB, "Databricks SQL", "Databricks SQL"),
        (DP, "MLflow", "ML: pronóstico demanda"),
    ]
    for i, (lib, name, title) in enumerate(cons):
        c = s.row_card(lib, name, 2050, 200 + i * 130, 300, 100, GREEN, title, icon_size=60, title_size=16)
        s.arrow(gd, c, "right", "left", color=GREEN[0], width=1, elbow=True)
    s.step = 6
    s.note(1605, 750, 350, "SF=1: 12M pedidos · 36M líneas\n150M eventos · 157M filas de stock", GOLD, size=15)

    # ---- paso 7: plataforma transversal
    s.step = 7
    s.zone(400, 900, 1970, 170, "Orquestación · Lakeflow", ORANGE, fill="solid", opacity=40)
    s.row_card(DB, "Lakeflow Jobs", 430, 950, 360, 96, ORANGE, "Lakeflow Jobs", "for_each · condition · quality gate")
    s.row_card(DB, "Lakeflow Declarative Pipelines", 820, 950, 400, 96, ORANGE, "Declarative Pipelines", "misma lógica en SQL: AUTO CDC")
    s.row_card(DP, "Spark", 1250, 950, 330, 96, ORANGE, "Apache Spark", "serverless · AQE · Photon", icon_size=70)
    s.row_card(DB, "Notebooks", 1610, 950, 330, 96, ORANGE, "Notebooks + src/", "lógica testeable con pytest")
    s.row_card(IT, "Settings", 1970, 950, 330, 96, ORANGE, "ops.*", "auditoría · watermarks · DQ")

    s.step = 8
    s.zone(20, 1100, 2350, 170, "Gobierno + DevOps", VIOLET, fill="solid", opacity=40)
    s.row_card(DB, "Unity Catalog", 50, 1150, 380, 96, VIOLET, "Unity Catalog", "permisos · linaje · tags PII · máscaras")
    s.row_card(GH, "GitHub", 470, 1150, 300, 96, VIOLET, "GitHub", "código versionado")
    s.row_card(GH, "GitHub Actions", 810, 1150, 360, 96, VIOLET, "GitHub Actions", "ruff · pytest · e2e Spark local")
    s.row_card(DB, "Databricks", 1210, 1150, 420, 96, VIOLET, "Asset Bundles (DAB)", "databricks bundle deploy -t prod")
    s.row_card(DB, "Delta Sharing", 1670, 1150, 300, 96, VIOLET, "Delta Sharing", "compartir gold")
    s.row_card(IT, "Yes", 2010, 1150, 300, 96, VIOLET, "Calidad", "quality gate bloqueante")
    return s


def pill(s: Scene, x, y, name, color, lib=DP, icon="Delta Lake", w=290, h=58, sub=None):
    return s.row_card(lib, icon, x, y, w, h, color, name, sub, icon_size=34, title_size=16)


# =============================================================================
# 02 · Flujo de tablas (linaje)
# =============================================================================
def flujo_tablas() -> Scene:
    s = Scene(seed=21)
    s.text(1000, 10, "Linaje de datos: de cada archivo a cada tabla", size=38, anchor="n")
    s.text(1000, 60, "8 fuentes · 8 tablas bronze · 9 silver · 6 dims + 5 facts · 6 marts", size=19, anchor="n",
           font=NORMAL, color="#495057")
    rows = [
        ("customers", "CSV · CDC", "customers_history", "SCD2", "dim_customer"),
        ("products", "JSON · CDC", "products_history", "SCD2", "dim_product"),
        ("stores", "CSV", "stores", "SCD1", "dim_store"),
        ("orders", "JSON anidado", "orders + order_lines", "dedup", "fact_sales"),
        ("order_status_events", "JSON", "order_status_events", "dedup", "fact_order_fulfillment"),
        ("returns", "CSV", "returns", "dedup", "fact_returns"),
        ("web_events", "JSON.gz", "web_events", "dedup", "fact_web_sessions"),
        ("inventory_snapshots", "Parquet", "inventory_snapshots", "append", "fact_inventory_snapshot"),
    ]
    xs = [20, 400, 780, 1210]
    heads = [("Landing (UC Volume)", CYAN, (PROC, "Data storage")), ("Bronze", BRONZE, (DP, "Delta Lake")),
             ("Silver", SILVER, (DP, "Spark")), ("Gold", GOLD, (DB, "Lakehouse"))]
    s.step = 1
    for (title, color, ic), x in zip(heads, xs, strict=False):
        s.zone(x - 10, 110, 350, 760, title, color, icon=(*ic, 38))
    y0, dy = 180, 84
    prev_col = []
    for i, (src, fmt, silver, how, gold) in enumerate(rows):
        y = y0 + i * dy
        s.step = 2
        a = pill(s, xs[0], y, src, CYAN, PROC, "Data collection", w=330, sub=fmt)
        s.step = 3
        b = pill(s, xs[1], y, f"bronze.{src}"[:26], BRONZE, w=330, sub="Auto Loader")
        s.arrow(a, b, color=BRONZE[0])
        s.step = 4
        c = pill(s, xs[2], y, silver, SILVER, DP, "Spark", w=330, sub=how)
        s.arrow(b, c, color=SILVER[0])
        s.step = 5
        g = pill(s, xs[3], y, gold, GOLD, DB, "Lakehouse", w=330,
                 sub="dimensión" if gold.startswith("dim") else "hecho")
        s.arrow(c, g, color=GOLD[0])
        prev_col.append(g)
    s.step = 5
    s.note(1210, 860 - 10 + 30, 330, "+ generadas: dim_date · dim_channel\n  dim_payment (junk)", GOLD, size=15)
    s.step = 6
    s.zone(1590, 110, 390, 760, "Marts (SQL)", GREEN, icon=(IT, "Bar chart", 38))
    marts = [("mart_sales_daily", "ventas por día/tienda/canal"), ("mart_customer_rfm", "segmentación RFM (NTILE)"),
             ("mart_cohort_retention", "retención por cohorte"), ("mart_product_affinity", "market basket · lift"),
             ("mart_funnel_daily", "embudo web/app"), ("v_sales_enriched", "vista OBT para BI/Genie")]
    for j, (m, sub) in enumerate(marts):
        mc = pill(s, 1610, 190 + j * 108, m, GREEN, IT, "Bar chart", w=350, h=70, sub=sub)
        if j == 0:
            first_mart = mc
    s.arrow(prev_col[3], first_mart, color=GREEN[0], width=3, label="CTAS", elbow=True)
    s.step = 7
    q = s.row_card(IT, "Firewall", 780, 920, 330, 80, RED, "ops.quarantine", "filas que fallan reglas", icon_size=44)
    s.row_card(IT, "Pie chart", 400, 920, 330, 80, VIOLET, "ops.dq_results", "métricas por regla y lote", icon_size=44)
    s.row_card(IT, "Timer", 20, 920, 330, 80, GRAY, "ops.pipeline_runs", "auditoría + watermarks", icon_size=44)
    s.arrow((945, 840), q, b_side="top", color=RED[0], dashed=True, label="inválidas")
    return s


# =============================================================================
# 03 · Modelo estrella
# =============================================================================
def modelo_estrella() -> Scene:
    s = Scene(seed=31)
    s.text(1000, 0, "Modelo dimensional (Kimball) · estrella de ventas", size=38, anchor="n")
    s.text(1000, 50, "Grano de fact_sales: UNA fila por línea de pedido (order_id + line_id)", size=19, anchor="n",
           font=NORMAL, color="#495057")
    s.step = 1
    fact = s.table(800, 300, "fact_sales", [
        ("order_date_key", "int", "FK"), ("customer_key", "bigint", "FK"), ("product_key", "bigint", "FK"),
        ("store_key", "bigint", "FK"), ("channel_key", "int", "FK"), ("payment_key", "bigint", "FK"),
        ("order_id", "bigint", "DD"), ("line_id", "int", "DD"), ("promo_code", "string", "DD"),
        ("quantity", "int", "M"), ("unit_price", "decimal", "M"), ("gross_amount", "decimal", "M"),
        ("discount_amount", "decimal", "M"), ("net_amount", "decimal", "M"), ("cost_amount", "decimal", "M"),
        ("margin_amount", "decimal", "M"),
    ], ORANGE, width=330, subtitle="transaccional · 36M filas (SF=1)")
    s.icon(DP, "Delta Lake", 1080, 305, 40)
    s.step = 2
    dims = {
        "dim_date": (330, 120, BLUE, "role-playing · 2.922 filas", [
            ("date_key", "int", "PK"), ("date", "date"), ("year_month", "string"), ("day_name", "string"),
            ("is_weekend", "boolean"), ("commercial_event", "string"), ("fiscal_year", "int")]),
        "dim_customer": (1290, 110, VIOLET, "SCD Tipo 2", [
            ("customer_key", "bigint", "PK"), ("customer_id", "bigint", "NK"), ("full_name", "string", "PII"),
            ("email", "string", "PII"), ("city / country", "string", "SCD"), ("segment", "string", "SCD"),
            ("loyalty_tier", "string", "SCD"), ("valid_from / valid_to", "timestamp"), ("is_current", "boolean")]),
        "dim_product": (1290, 470, GREEN, "SCD Tipo 2", [
            ("product_key", "bigint", "PK"), ("product_id", "bigint", "NK"), ("product_name", "string"),
            ("department > category", "string"), ("subcategory", "string"), ("brand", "string"),
            ("list_price", "decimal", "SCD"), ("unit_cost", "decimal", "SCD"), ("valid_from / is_current", "")]),
        "dim_store": (330, 470, CYAN, "SCD Tipo 1 · -1 = online", [
            ("store_key", "bigint", "PK"), ("store_name", "string"), ("store_format", "string"),
            ("city / country", "string"), ("region", "string"), ("latitude / longitude", "double")]),
        "dim_channel": (560, 900, TEAL, "4 filas", [
            ("channel_key", "int", "PK"), ("channel_code", "string"), ("channel_group", "string")]),
        "dim_payment": (1010, 900, PINK, "junk dimension", [
            ("payment_key", "bigint", "PK"), ("payment_method", "string"), ("installments_band", "string"),
            ("card_brand", "string")]),
    }
    boxes = {}
    for name, (x, y, color, sub, cols) in dims.items():
        boxes[name] = s.table(x, y, name, cols, color, width=290, subtitle=sub)
    s.step = 3
    s.arrow(fact, boxes["dim_date"], "left", "right", color=BLUE[0], label="N:1", elbow=True)
    s.arrow(fact, boxes["dim_store"], "left", "right", color=CYAN[0], label="N:1", elbow=True)
    s.arrow(fact, boxes["dim_customer"], "right", "left", color=VIOLET[0], label="N:1", elbow=True)
    s.arrow(fact, boxes["dim_product"], "right", "left", color=GREEN[0], label="N:1", elbow=True)
    s.arrow(fact, boxes["dim_channel"], "bottom", "top", color=TEAL[0], label="N:1",
            via=[(965, 860), (705, 860)], sharp=True)
    s.arrow(fact, boxes["dim_payment"], "bottom", "top", color=PINK[0], via=[(965, 860), (1155, 860)], sharp=True)
    s.step = 4
    s.note(20, 120, 280, "Leyenda\nPK  clave primaria (subrogada)\nFK  clave foránea a una dim\nNK  clave natural del origen\n"
           "DD  dimensión degenerada\nM   medida (hecho numérico)\nSCD atributo historizado\nPII dato personal (enmascarar)",
           YELLOW, size=15)
    s.note(20, 900, 500, "Join point-in-time:\nfact.order_ts >= dim.effective_from\nAND fact.order_ts <  dim.effective_to\n"
           "=> cada venta apunta a la versión del cliente\n    y del producto vigente en ese momento", YELLOW, size=15)
    s.step = 5
    s.zone(1660, 110, 340, 740, "Constelación", ORANGE, icon=(IT, "Network", 36))
    others = [("fact_order_fulfillment", "accumulating snapshot", "1 fila/pedido · MERGE"),
              ("fact_returns", "transaccional", "1 fila/línea devuelta"),
              ("fact_inventory_snapshot", "periodic snapshot", "semi-aditivo (stock)"),
              ("fact_web_sessions", "agregado de eventos", "sessionización")]
    for k, (n, t, d) in enumerate(others):
        s.row_card(DP, "Delta Lake", 1680, 190 + k * 125, 300, 100, ORANGE, n, f"{t}\n{d}", icon_size=40, title_size=15)
    s.note(1680, 700, 300, "Comparten las MISMAS\ndimensiones conformadas\n=> se pueden cruzar\nprocesos (drill-across)",
           ORANGE, size=15)
    return s


# =============================================================================
# 04 · Matriz de bus
# =============================================================================
def matriz_bus() -> Scene:
    s = Scene(seed=41)
    s.text(700, 0, "Matriz de bus · dimensiones conformadas", size=36, anchor="n")
    dims = [("fecha", BLUE, IT, "Timer"), ("cliente", VIOLET, IT, "User"), ("producto", GREEN, IT, "Folder"),
            ("tienda", CYAN, IT, "Home"), ("canal", TEAL, IT, "Network"), ("pago", PINK, IT, "Key")]
    procs = [("Ventas", "fact_sales", "transaccional", [1, 1, 1, 1, 1, 1]),
             ("Fulfillment", "fact_order_fulfillment", "accumulating", [4, 1, 0, 1, 1, 0]),
             ("Devoluciones", "fact_returns", "transaccional", [2, 1, 1, 1, 1, 0]),
             ("Inventario", "fact_inventory_snapshot", "periodic snap.", [1, 0, 1, 1, 0, 0]),
             ("Navegación", "fact_web_sessions", "agregado", [1, 1, 0, 0, 0, 0])]
    cw, rh, x0, y0 = 150, 100, 420, 90
    s.step = 1
    for j, (d, color, lib, ic) in enumerate(dims):
        x = x0 + j * cw
        s.rect(x + 5, y0, cw - 10, 120, color, fill="hachure")
        b = s.icon(lib, ic, 0, 0, 46)
        s.move_group(b, x + cw / 2 - b["width"] / 2, y0 + 12)
        s.text(x + cw / 2, y0 + 78, d, size=20, anchor="n", color=color[0])
    for i, (p, f, t, marks) in enumerate(procs):
        y = y0 + 140 + i * rh
        s.step = 2 + i
        s.row_card(DP, "Delta Lake", 20, y, 380, rh - 14, ORANGE, p, f"{f}\n{t}", icon_size=40)
        for j, m in enumerate(marks):
            x = x0 + j * cw
            if m:
                c = s.ellipse(x + cw / 2 - 30, y + 13, 60, 60, dims[j][1], fill="solid")
                if m > 1:
                    s.bound_text(c, f"x{m}", size=20)
                else:
                    ok = s.icon(IT, "Yes", 0, 0, 30)
                    s.move_group(ok, x + cw / 2 - ok["width"] / 2, y + 28)
            else:
                s.text(x + cw / 2, y + 43, "—", size=24, anchor="c", color="#ced4da")
    s.step = 7
    s.note(420, y0 + 140 + 5 * rh + 10, 900, "x4 / x2 = role-playing: la misma dim_date como fecha de pedido, pago, "
           "despacho y entrega", YELLOW, size=16)
    return s


# =============================================================================
# 05 · SCD Tipo 2
# =============================================================================
def scd2() -> Scene:
    s = Scene(seed=51)
    s.text(1000, 0, "SCD Tipo 2 · cómo se guarda la historia de un cliente", size=36, anchor="n")
    s.step = 1
    s.line([(80, 300), (1560, 300)], color=INK, width=3)
    for i, lbl in enumerate(["2023-01", "2023-07", "2024-01", "2024-07", "2025-01", "2025-07", "hoy"]):
        x = 100 + i * 240
        s.line([(x, 290), (x, 310)], color=INK, width=2)
        s.text(x, 318, lbl, size=16, anchor="n", font=NORMAL)
    s.icon(IT, "User", 20, 120, 70)
    s.text(55, 200, "cliente 116", size=18, anchor="n")
    s.step = 2
    versions = [(140, 600, "v1 · Rosario · Masivo", BLUE), (600, 1080, "v2 · Córdoba · Familia", VIOLET),
                (1080, 1540, "v3 · Córdoba · Premium", GREEN)]
    for k, (xa, xb, lbl, color) in enumerate(versions):
        s.step = 2 + k
        s.rect(xa, 200 + k * 0, xb - xa - 8, 70, color, fill="cross-hatch" if k < 2 else "solid", label=lbl, size=18)
    s.step = 5
    for x, lbl in [(330, "venta A"), (850, "venta B"), (1300, "venta C")]:
        c = s.ellipse(x, 120, 90, 50, ORANGE, label=lbl, size=14)
        s.arrow(c, (x + 45, 200), "bottom", color=ORANGE[0], width=2)
    s.step = 6
    rows = [("customer_key", "city", "segment", "valid_from", "valid_to", "is_current"),
            ("8f3a…01", "Rosario", "Masivo", "2023-01-02", "2024-02-11", "false"),
            ("c71b…9e", "Córdoba", "Familia", "2024-02-11", "2025-01-20", "false"),
            ("02de…4a", "Córdoba", "Premium", "2025-01-20", "NULL", "true")]
    cols = [190, 140, 140, 170, 170, 130]
    y = 400
    for r, row in enumerate(rows):
        x = 120
        color = GRAY if r == 0 else [BLUE, VIOLET, GREEN][r - 1]
        for c, val in enumerate(row):
            s.rect(x, y, cols[c], 46, color, fill="solid" if r == 0 else "hachure", label=val, size=15,
                   font=CODE, rounded=False, roughness=0, stroke_width=1)
            x += cols[c]
        y += 46
    s.text(120, 370, "silver.customers_history / gold.dim_customer", size=18, font=CODE, color="#495057")
    s.step = 7
    s.note(1100, 390, 480, "MERGE con 'staged updates':\n1. calcular hash de atributos\n2. descartar re-envíos idénticos\n"
           "3. ignorar cambios más viejos que la versión vigente\n4. CERRAR la vigente (valid_to, is_current=false)\n"
           "5. INSERTAR la nueva versión", YELLOW, size=16)
    s.note(120, 610, 940, "fact_sales guarda customer_key de la versión vigente al vender:\n"
           "venta A -> Rosario/Masivo · venta B -> Córdoba/Familia · venta C -> Córdoba/Premium", ORANGE, size=16)
    return s


# =============================================================================
# 06 · Ejecución de Spark
# =============================================================================
def spark_ejecucion() -> Scene:
    s = Scene(seed=61)
    s.text(900, 0, "Cómo ejecuta Spark una consulta (y dónde duele con volumen)", size=36, anchor="n")
    s.step = 1
    drv = s.row_card(IT, "Host", 640, 70, 520, 110, BLUE, "DRIVER", "SparkSession · Catalyst · plan físico\njob -> stages -> tasks",
                     icon_size=70, title_size=22)
    s.icon(DP, "Spark", 1080, 80, 70)
    s.step = 2
    s.zone(40, 230, 1720, 490, "Executors (en serverless escalan solos)", GRAY, fill="hachure", title_size=22)
    colors = [BLUE, GREEN, ORANGE, VIOLET]
    for e in range(4):
        x = 80 + e * 420
        ex = s.rect(x, 290, 380, 390, GRAY, fill="solid")
        ic = s.icon(IT, "Server", x + 15, 300, 50)
        s.text(x + 80, 310, f"executor {e + 1}", size=20)
        s.step = 3
        s.text(x + 20, 370, "stage 1: scan + filter (narrow)", size=14, font=NORMAL)
        for p in range(3):
            s.rect(x + 20 + p * 115, 400, 100, 60, colors[e], fill="solid", label=f"part {e * 3 + p}", size=14,
                   font=CODE)
        s.step = 5
        s.text(x + 20, 560, "stage 2: groupBy / join", size=14, font=NORMAL)
        for p in range(3):
            s.rect(x + 20 + p * 115, 590, 100, 60, colors[(e + p) % 4], fill="cross-hatch", label=f"key {(e + p) % 4}",
                   size=14, font=CODE)
        s.step = 2
        s.arrow(drv, ex, "bottom", "top", color=BLUE[0], width=1, dashed=True)
    s.step = 4
    shuffle = s.rect(620, 480, 560, 60, RED, fill="solid", label="SHUFFLE · intercambio por red (Exchange)", size=18)
    for e in range(4):
        s.arrow((80 + e * 420 + 190, 465), shuffle, b_side="top" if e in (1, 2) else ("left" if e == 0 else "right"),
                color=RED[0], width=2)
    s.step = 6
    tips = [("Broadcast join", "la dim chica viaja a todos:\nel fact NO se mueve", GREEN, IT, "Internet"),
            ("Data skew", "1 partición enorme (online = 60%)\n=> AQE skew join o salting", RED, IT, "Error"),
            ("Archivos chicos", "miles de tasks diminutas\n=> OPTIMIZE / auto compaction", ORANGE, IT, "Document"),
            ("Data skipping", "min/max por archivo + CLUSTER BY\n=> leer 2% de los archivos", VIOLET, DB, "Photon")]
    for k, (t, d, color, lib, ic) in enumerate(tips):
        s.row_card(lib, ic, 40 + k * 435, 760, 410, 110, color, t, d, icon_size=56, title_size=19)
    return s


# =============================================================================
# 07 · DAG del Job
# =============================================================================
def job_dag() -> Scene:
    s = Scene(seed=71)
    s.text(1000, 0, "Lakeflow Job · novamart-e2e-pipeline", size=36, anchor="n")
    s.text(1000, 48, "serverless · parámetros: catalog, scale_factor, fechas, generate, full_refresh", size=18,
           anchor="n", font=NORMAL, color="#495057")
    y = 370
    s.step = 1
    t1 = s.icon_card(IT, "Settings", 20, y - 60, 170, 170, GRAY, "setup", "UC: esquemas\n+ volumen", icon_size=60)
    s.step = 2
    d = s.diamond(230, y - 40, 170, 130, YELLOW, label="generate\n== true?", size=16)
    s.arrow(t1, d)
    s.step = 3
    g = s.icon_card(PROC, "Data collection", 450, y - 230, 190, 170, CYAN, "generate_data", "8 fuentes · SF", icon_size=70)
    s.arrow(d, g, "top", "left", color=GREEN[0], label="sí", via=[(315, y - 145)])
    s.step = 4
    b = s.icon_card(DB, "Streaming", 700, y - 60, 190, 170, BRONZE, "bronze_ingest", "Auto Loader", icon_size=70)
    s.arrow(g, b, "right", "top", color=BRONZE[0], via=[(795, y - 145)])
    s.arrow(d, b, "right", "left", color="#868e96", dashed=True, label="no · run_if\nNONE_FAILED")
    s.step = 5
    s.zone(940, y - 220, 330, 500, "for_each · silver", SILVER, fill="hachure", title_size=20)
    ents = ["customers", "products", "stores", "orders", "order_status", "returns", "web_events", "inventory"]
    first = None
    for k, e in enumerate(ents):
        col, row = k % 2, k // 2
        c = s.rect(960 + col * 152, y - 165 + row * 100, 140, 80, SILVER, label=e, size=15)
        first = first or c
    s.text(1105, y + 250, "concurrency = 4", size=16, anchor="n", font=CODE, color=SILVER[0])
    s.arrow(b, (940, y + 25), color=SILVER[0], width=3)
    s.step = 6
    gd = s.icon_card(DB, "Lakehouse", 1320, y - 60, 180, 170, GOLD, "gold_dimensions", "6 dims SCD", icon_size=64)
    s.arrow((1270, y + 25), gd, color=GOLD[0], width=3)
    gf = s.icon_card(DP, "Delta Lake", 1540, y - 60, 180, 170, GOLD, "gold_facts", "replaceWhere\n+ MERGE", icon_size=60)
    s.arrow(gd, gf, color=GOLD[0])
    s.step = 7
    gm = s.icon_card(IT, "Bar chart", 1540, y + 180, 180, 170, GREEN, "gold_marts", "SQL CTAS", icon_size=60)
    s.arrow(gf, gm, "bottom", "top", color=GREEN[0])
    q = s.icon_card(IT, "Yes", 1320, y + 180, 180, 170, RED, "quality_gate", "falla si hay\nviolaciones", icon_size=56)
    s.arrow(gm, q, "left", "right", color=RED[0])
    s.step = 8
    s.zone(20, 760, 880, 230, "novamart-daily-increment (diario 05:30)", ORANGE, fill="solid", opacity=35, title_size=20)
    n1 = s.row_card(PROC, "Data collection", 50, 830, 360, 110, ORANGE, "generate_next_day", "1 día nuevo (incremental)")
    n2 = s.row_card(DB, "Lakeflow Jobs", 490, 830, 380, 110, ORANGE, "run_job_task", "dispara el e2e con generate=false")
    s.arrow(n1, n2, color=ORANGE[0])
    s.arrow(n2, b, "top", "bottom", color=ORANGE[0], dashed=True, label="reusa el pipeline")
    s.zone(960, 760, 760, 230, "novamart-weekly-maintenance (domingos)", VIOLET, fill="solid", opacity=35, title_size=20)
    m1 = s.row_card(IT, "Settings", 990, 830, 330, 110, VIOLET, "physical_design", "CLUSTER BY · PK/FK · tags")
    m2 = s.row_card(DB, "Photon", 1360, 830, 330, 110, VIOLET, "optimize_vacuum", "OPTIMIZE · VACUUM · ANALYZE")
    s.arrow(m1, m2, color=VIOLET[0])
    return s


# =============================================================================
# 08 · CI/CD
# =============================================================================
def cicd() -> Scene:
    s = Scene(seed=81)
    s.text(950, 0, "CI/CD · del commit a producción con Asset Bundles", size=36, anchor="n")
    s.step = 1
    dev = s.icon_card(IT, "User", 20, 120, 180, 190, GRAY, "data engineer", "VS Code / notebooks", icon_size=80)
    s.step = 2
    gh = s.icon_card(GH, "GitHub", 270, 120, 190, 190, GRAY, "GitHub", "push · pull request", icon_size=80)
    s.arrow(dev, gh, label="git push")
    s.step = 3
    s.zone(520, 80, 820, 300, "GitHub Actions · ci.yml", BLUE, fill="hachure", icon=(GH, "GitHub Actions", 40))
    stages = [("lint", "ruff", IT, "Yes"), ("unit tests", "pytest · Spark\n+ Delta local", DP, "Spark"),
              ("integration", "pipeline e2e\nlanding->gold", DP, "Delta Lake"), ("validate", "databricks bundle", DB, "Databricks")]
    prev = gh
    for k, (t, d, lib, ic) in enumerate(stages):
        c = s.icon_card(lib, ic, 545 + k * 195, 160, 175, 180, BLUE, t, d, icon_size=60, title_size=17)
        s.arrow(prev, c, color=BLUE[0])
        prev = c
    s.step = 4
    dep = s.icon_card(DB, "Databricks", 1400, 120, 220, 220, VIOLET, "bundle deploy", "-t dev | -t prod", icon_size=90)
    s.arrow(prev, dep, color=VIOLET[0], width=3, label="workflow\ndispatch")
    s.step = 5
    for k, (env, cat, color, desc) in enumerate([("dev", "novamart_dev", GREEN, "[dev usuario] · schedules pausados"),
                                                  ("prod", "novamart", RED, "service principal · schedules activos")]):
        w = s.zone(1080 + k * 440 - 400, 450, 400, 300, f"workspace · {env}", color, fill="solid", opacity=35)
        s.row_card(DB, "Unity Catalog", 1100 + k * 440 - 400, 510, 360, 90, color, f"catálogo {cat}", desc, icon_size=50, title_size=16)
        s.row_card(DB, "Lakeflow Jobs", 1100 + k * 440 - 400, 620, 360, 90, color, "3 jobs + 1 pipeline", "e2e · daily · maintenance", icon_size=50, title_size=16)
        s.arrow(dep, w, "bottom", "top", color=color[0], width=2)
    s.step = 6
    s.note(20, 450, 600, "Secrets: DATABRICKS_HOST (variable) + DATABRICKS_TOKEN\n(secreto) por GitHub Environment.\n"
           "Mejor: OAuth M2M con service principal.\n\nTodo es código: jobs, pipeline, notebooks, SQL,\ntests y hasta estos diagramas.",
           YELLOW, size=16)
    return s


# =============================================================================
# 09 · Ruta de aprendizaje
# =============================================================================
def ruta_aprendizaje() -> Scene:
    s = Scene(seed=91)
    s.text(1000, 0, "Ruta de aprendizaje · de cero a avanzado sobre NovaMart", size=38, anchor="n")
    levels = [
        ("Nivel 0-1 · SQL", BLUE, DB, "Databricks SQL", [("00", "Entorno y UC"), ("01", "SELECT / WHERE"), ("02", "Agregaciones"),
                                                          ("03", "JOINs y estrella"), ("04", "Subconsultas / CTEs")]),
        ("Nivel 1 · SQL analítico", CYAN, IT, "Bar chart", [("05", "Funciones de ventana"), ("06", "Tipos, fechas, NULLs"),
                                                            ("07", "Anidados, PIVOT, MERGE")]),
        ("Nivel 2 · Spark", ORANGE, DP, "Spark", [("08", "Arquitectura + DataFrames"), ("09", "Ventanas, UDFs, pandas"),
                                                   ("10", "Performance: skew, AQE"), ("11", "Delta Lake a fondo"),
                                                   ("12", "Streaming + Auto Loader")]),
        ("Nivel 3 · Modelado", VIOLET, DB, "Lakehouse", [("13", "Kimball: grano, SCD, facts")]),
        ("Nivel 4 · Producción", GREEN, DB, "Lakeflow Jobs", [("14", "Declarativo vs imperativo"), ("15", "Gobierno UC"),
                                                             ("16", "Jobs + CI/CD"), ("17", "Desafíos senior")]),
    ]
    y = 90
    prev = None
    for li, (title, color, lib, ic, mods) in enumerate(levels):
        s.step = li + 1
        h = 190
        s.zone(20, y, 1960, h, title, color, fill="hachure", icon=(lib, ic, 44), title_size=24)
        for mi, (num, name) in enumerate(mods):
            x = 330 + mi * 330
            c = s.rect(x, y + 50, 300, 110, color, fill="solid")
            s.text(x + 18, y + 56, num, size=34, color=color[0])
            s.text(x + 18, y + 128, name, size=17, anchor="w")
            if prev is not None:
                s.arrow(prev, c, "right" if mi else "bottom", "left" if mi else "top", color=color[0], width=2)
            prev = c
        extra = {1: "QUALIFY · ROLLUP/CUBE · PIVOT\nMERGE · time travel · CDF",
                 3: "estrella vs copo de nieve vs OBT · SCD 0-3\nmatriz de bus · role-playing · junk dims\n"
                    "mini-dimensiones · bridge tables · Data Vault",
                 4: "portfolio: diagrama + modelo +\nproblema real resuelto con números"}.get(li)
        if extra:
            x = 330 + len(mods) * 330
            s.note(x, y + 50, 1960 - x, extra, color, size=16)
        y += h + 30
    s.step = 6
    s.note(20, y, 1960, "Cada módulo: teoría breve -> ejemplos sobre las tablas reales del proyecto -> ejercicios -> soluciones.\n"
           "Recomendado: correr el pipeline con scale_factor=0.1 para los niveles 0-1 y scale_factor>=1 para el nivel 2.",
           YELLOW, size=17)
    return s


DIAGRAMS = {
    "00_proceso_completo": proceso_completo,
    "01_arquitectura": arquitectura,
    "02_linaje_tablas": flujo_tablas,
    "03_modelo_estrella": modelo_estrella,
    "04_matriz_bus": matriz_bus,
    "05_scd2": scd2,
    "06_spark_ejecucion": spark_ejecucion,
    "07_job_dag": job_dag,
    "08_cicd": cicd,
    "09_ruta_aprendizaje": ruta_aprendizaje,
}


def main(names: list[str] | None = None) -> None:
    for name, builder in DIAGRAMS.items():
        if names and name not in names:
            continue
        scene = builder()
        scene.save(OUT / f"{name}.excalidraw")
        print("✔", name, len(scene.elements), "elementos")


if __name__ == "__main__":
    main(sys.argv[1:])
