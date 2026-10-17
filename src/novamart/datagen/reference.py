"""Datos de referencia "realistas" para el generador sintético.

NovaMart es una cadena omnicanal ficticia que opera en 6 países de LatAm con
tiendas físicas, e-commerce web, app móvil y un marketplace.
"""

# (ciudad, provincia/estado, país, región comercial, lat, lon, peso poblacional)
CITIES = [
    ("Buenos Aires", "CABA", "AR", "Cono Sur", -34.6037, -58.3816, 14),
    ("Córdoba", "Córdoba", "AR", "Cono Sur", -31.4201, -64.1888, 4),
    ("Rosario", "Santa Fe", "AR", "Cono Sur", -32.9442, -60.6505, 3),
    ("Mendoza", "Mendoza", "AR", "Cono Sur", -32.8895, -68.8458, 2),
    ("La Plata", "Buenos Aires", "AR", "Cono Sur", -34.9215, -57.9545, 2),
    ("Mar del Plata", "Buenos Aires", "AR", "Cono Sur", -38.0055, -57.5426, 1),
    ("Santiago", "RM", "CL", "Cono Sur", -33.4489, -70.6693, 9),
    ("Valparaíso", "Valparaíso", "CL", "Cono Sur", -33.0472, -71.6127, 2),
    ("Concepción", "Biobío", "CL", "Cono Sur", -36.8201, -73.0444, 2),
    ("Montevideo", "Montevideo", "UY", "Cono Sur", -34.9011, -56.1645, 3),
    ("Punta del Este", "Maldonado", "UY", "Cono Sur", -34.9667, -54.95, 1),
    ("Ciudad de México", "CDMX", "MX", "Norte", 19.4326, -99.1332, 16),
    ("Guadalajara", "Jalisco", "MX", "Norte", 20.6597, -103.3496, 5),
    ("Monterrey", "Nuevo León", "MX", "Norte", 25.6866, -100.3161, 5),
    ("Puebla", "Puebla", "MX", "Norte", 19.0414, -98.2063, 3),
    ("Querétaro", "Querétaro", "MX", "Norte", 20.5888, -100.3899, 2),
    ("Bogotá", "Cundinamarca", "CO", "Andina", 4.711, -74.0721, 10),
    ("Medellín", "Antioquia", "CO", "Andina", 6.2442, -75.5812, 4),
    ("Cali", "Valle del Cauca", "CO", "Andina", 3.4516, -76.532, 3),
    ("Barranquilla", "Atlántico", "CO", "Andina", 10.9685, -74.7813, 2),
    ("Lima", "Lima", "PE", "Andina", -12.0464, -77.0428, 10),
    ("Arequipa", "Arequipa", "PE", "Andina", -16.409, -71.5375, 2),
    ("Trujillo", "La Libertad", "PE", "Andina", -8.1116, -79.0287, 1),
]

COUNTRY_CURRENCY = {"AR": "ARS", "CL": "CLP", "UY": "UYU", "MX": "MXN", "CO": "COP", "PE": "PEN"}

# departamento -> [(categoría, subcategoría, precio_min_usd, precio_max_usd, margen_bruto)]
CATALOG = {
    "Electrónica": [
        ("Telefonía", "Smartphones", 150, 1400, 0.18),
        ("Telefonía", "Accesorios celular", 5, 80, 0.45),
        ("Computación", "Notebooks", 400, 2500, 0.15),
        ("Computación", "Monitores", 120, 900, 0.20),
        ("TV y Video", "Smart TV", 250, 2800, 0.17),
        ("Audio", "Auriculares", 15, 450, 0.35),
        ("Gaming", "Consolas", 300, 700, 0.10),
        ("Gaming", "Videojuegos", 20, 80, 0.25),
    ],
    "Hogar": [
        ("Muebles", "Living", 120, 1800, 0.40),
        ("Muebles", "Dormitorio", 150, 2200, 0.42),
        ("Cocina", "Pequeños electrodomésticos", 25, 400, 0.30),
        ("Cocina", "Utensilios", 4, 120, 0.50),
        ("Decoración", "Iluminación", 10, 300, 0.48),
        ("Electrodomésticos", "Heladeras", 450, 2600, 0.16),
        ("Electrodomésticos", "Lavarropas", 350, 1500, 0.17),
    ],
    "Moda": [
        ("Mujer", "Remeras y blusas", 8, 70, 0.60),
        ("Mujer", "Jeans", 25, 120, 0.58),
        ("Hombre", "Camisas", 15, 90, 0.57),
        ("Hombre", "Abrigos", 40, 300, 0.55),
        ("Calzado", "Zapatillas", 35, 220, 0.50),
        ("Accesorios", "Relojes", 20, 600, 0.52),
    ],
    "Supermercado": [
        ("Almacén", "Básicos", 1, 12, 0.22),
        ("Bebidas", "Gaseosas y aguas", 1, 6, 0.25),
        ("Bebidas", "Vinos", 4, 90, 0.35),
        ("Limpieza", "Limpieza hogar", 1, 20, 0.30),
        ("Frescos", "Lácteos", 1, 10, 0.20),
        ("Mascotas", "Alimento mascotas", 5, 80, 0.28),
    ],
    "Deportes": [
        ("Fitness", "Equipamiento", 10, 900, 0.38),
        ("Ciclismo", "Bicicletas", 180, 2500, 0.25),
        ("Outdoor", "Camping", 15, 500, 0.40),
    ],
    "Belleza": [
        ("Cuidado personal", "Perfumes", 20, 220, 0.45),
        ("Cuidado personal", "Dermocosmética", 6, 90, 0.50),
    ],
    "Juguetería": [
        ("Juguetes", "Didácticos", 8, 120, 0.45),
        ("Juguetes", "Vehículos", 10, 250, 0.42),
    ],
    "Librería": [
        ("Libros", "Ficción", 8, 45, 0.35),
        ("Libros", "Técnicos", 20, 120, 0.30),
    ],
}

SUBCATEGORIES = [
    (dept, cat, sub, pmin, pmax, margin)
    for dept, rows in CATALOG.items()
    for (cat, sub, pmin, pmax, margin) in rows
]

BRANDS = [
    "Andes", "Pampa", "Austral", "Quetzal", "Cóndor", "Maya", "Inca", "Tango", "Puma Sur",
    "Lumen", "Nativa", "Volcán", "Brisa", "Selva", "Atacama", "Patagonia Pro", "Caribe",
    "NovaMart Basics",  # marca propia: margen mayor
]

COLORS = ["negro", "blanco", "gris", "azul", "rojo", "verde", "beige", "multicolor"]

FIRST_NAMES = [
    "Sofía", "Mateo", "Valentina", "Santiago", "Isabella", "Sebastián", "Camila", "Nicolás",
    "Lucía", "Martín", "Martina", "Benjamín", "Emma", "Lucas", "Mía", "Joaquín", "Julieta",
    "Diego", "Catalina", "Tomás", "Renata", "Gabriel", "Paula", "Emiliano", "Antonella",
    "Facundo", "Daniela", "Alejandro", "Florencia", "Andrés", "Mariana", "Felipe", "Carla",
    "Juan", "María", "José", "Ana", "Luis", "Laura", "Carlos", "Agustina", "Nahuel",
]

LAST_NAMES = [
    "González", "Rodríguez", "Gómez", "Fernández", "López", "Díaz", "Martínez", "Pérez",
    "García", "Sánchez", "Romero", "Sosa", "Torres", "Álvarez", "Ruiz", "Ramírez", "Flores",
    "Benítez", "Acosta", "Medina", "Herrera", "Suárez", "Aguirre", "Giménez", "Gutiérrez",
    "Pereyra", "Rojas", "Molina", "Castro", "Ortiz", "Silva", "Núñez", "Vargas", "Morales",
]

EMAIL_DOMAINS = ["gmail.com", "hotmail.com", "outlook.com", "yahoo.com", "icloud.com", "novamail.lat"]

SEGMENTS = ["Masivo", "Familia", "Joven Digital", "Premium", "Empresa"]

STORE_FORMATS = [("Hipermercado", 9000, 0.15), ("Supermercado", 2500, 0.45),
                 ("Express", 400, 0.30), ("Outlet", 1800, 0.10)]

CHANNELS = [("WEB", 0.30), ("APP", 0.22), ("STORE", 0.40), ("MARKETPLACE", 0.08)]

PAYMENT_METHODS = [("CREDIT_CARD", 0.46), ("DEBIT_CARD", 0.24), ("CASH", 0.10),
                   ("WALLET", 0.15), ("BANK_TRANSFER", 0.05)]

CARD_BRANDS = ["VISA", "MASTERCARD", "AMEX", "NARANJA", "CABAL"]

SHIPPING_METHODS = [("STANDARD", 0.55), ("EXPRESS", 0.20), ("PICKUP_IN_STORE", 0.25)]

RETURN_REASONS = ["Producto defectuoso", "No era lo esperado", "Talle incorrecto",
                  "Llegó tarde", "Arrepentimiento", "Producto dañado en envío"]

TRAFFIC_SOURCES = [("google", "cpc", 0.28), ("google", "organic", 0.22), ("direct", "none", 0.18),
                   ("instagram", "social", 0.12), ("email", "newsletter", 0.08),
                   ("tiktok", "social", 0.07), ("affiliates", "referral", 0.05)]

DEVICES = [("mobile", "Android", "Chrome", 0.42), ("mobile", "iOS", "Safari", 0.24),
           ("desktop", "Windows", "Chrome", 0.18), ("desktop", "macOS", "Safari", 0.07),
           ("desktop", "Windows", "Edge", 0.05), ("tablet", "iPadOS", "Safari", 0.04)]

SEARCH_TERMS = ["celular", "zapatillas", "notebook", "tv 55", "auriculares", "heladera",
                "colchón", "vino malbec", "bicicleta", "perfume", "lego", "freidora de aire",
                "jean mujer", "camping", "monitor 27", "play 5", "cafetera", "silla gamer"]

PROMO_CODES = ["HOTSALE", "CYBERMONDAY", "BLACKFRIDAY", "NAVIDAD", "BIENVENIDA10", "APP15"]
