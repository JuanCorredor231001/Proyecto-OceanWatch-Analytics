# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configuración de la Entrega 2 (capas, Volumes, tablas y referencias)
# MAGIC
# MAGIC **Objetivo.** Crear de forma idempotente el catálogo, los esquemas `bronze`, `silver`, `gold`, `ml`, `ops` (además de `landing` y `reference` de la Entrega 1), los Volumes, las tablas con DDL explícito y las tablas de referencia.
# MAGIC
# MAGIC **Entradas.** Widget `catalogo`; WPI de la Entrega 1 en `reference.world_port_index`; shapefiles Natural Earth 1:10m (`ne_10m_land`, `ne_10m_lakes`, `ne_10m_rivers_lake_centerlines`) en `reference.files/natural_earth/` (se descargan si faltan).
# MAGIC
# MAGIC **Salidas.** 12 tablas Delta, `reference.{vessel_type_catalog, ports, port_cells_h3r8, land_cells_h3r5}` y `ops.feed_calendar`.
# MAGIC
# MAGIC **Validación.** `SHOW TABLES` por esquema y conteos de referencias.

# COMMAND ----------

# MAGIC %pip install -q h3==4.5.0 shapely==2.1.2 pyshp==2.3.1 PyYAML==6.0.3

# COMMAND ----------

import json
import os
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from oceanwatch.lakehouse import get_lakehouse
from oceanwatch.notebook_params import widget
from oceanwatch.pipeline import seed_feed_calendar
from oceanwatch.setup import build_reference_tables, create_all

CATALOG = widget(dbutils, "catalogo", "oceanwatch_g07")
lh = get_lakehouse(spark, catalog=CATALOG)

# COMMAND ----------

print(json.dumps(create_all(lh), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referencias geográficas
# MAGIC
# MAGIC Puertos: WPI (NGA Pub. 150) conservado en la Entrega 1. Máscara de tierra: Natural Earth 1:10m (dominio público, naciscdn.org).

# COMMAND ----------

ne_dir = Path(lh.reference_dir()) / "natural_earth"
ne_dir.mkdir(parents=True, exist_ok=True)
for name, theme in [("ne_10m_land", "physical"), ("ne_10m_lakes", "physical"), ("ne_10m_rivers_lake_centerlines", "physical")]:
    target = ne_dir / f"{name}.zip"
    if not target.exists():
        urllib.request.urlretrieve(f"https://naciscdn.org/naturalearth/10m/{theme}/{name}.zip", target)
    print(name, target.stat().st_size)

wpi_csv = f"/Volumes/{CATALOG}/reference/world_port_index/UpdatedPub150.csv"
print(json.dumps(build_reference_tables(lh, wpi_csv, str(ne_dir)), indent=2))
print("Calendario del feed:", seed_feed_calendar(lh))

# COMMAND ----------

for schema in ["bronze", "silver", "gold", "ml", "ops", "reference"]:
    display(spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}"))
print("CONFIGURACION_E2_OK")
