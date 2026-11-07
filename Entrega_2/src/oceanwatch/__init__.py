"""OceanWatch Analytics - librería compartida del pipeline de la Entrega 2.

El mismo código se ejecuta en Databricks (Unity Catalog, Auto Loader, H3 nativo)
y en el entorno local de pruebas (Delta Lake OSS por ruta, fuente de archivos de
Structured Streaming, H3 vía la librería `h3`). Las diferencias de plataforma
quedan aisladas en `lakehouse.py` y `geo.py`.
"""

__version__ = "2.0.0"
