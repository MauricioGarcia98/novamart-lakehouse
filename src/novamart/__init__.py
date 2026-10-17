"""NovaMart Lakehouse.

Paquete con toda la lógica del pipeline (generación de datos, ingesta, capas
silver/gold y calidad). Los notebooks de `notebooks/` son "orquestadores
delgados": leen parámetros, llaman a estas funciones y muestran resultados.
Así la lógica se puede testear con pytest fuera de Databricks.
"""

__version__ = "1.0.0"
