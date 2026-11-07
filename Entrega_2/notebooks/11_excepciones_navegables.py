# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Excepciones navegables de la máscara de tierra
# MAGIC
# MAGIC **Job de backfill, tarea `excepciones_navegables`.** Reclasifica como agua las celdas H3 r5 de la máscara Natural Earth con >= 2 MMSI distintos en el histórico (decisión D-E2-04).

# COMMAND ----------

import os
import sys

# El código del pipeline vive en ../src (misma librería probada en local; ver README).
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from oceanwatch.lakehouse import get_lakehouse
from oceanwatch.notebook_params import fechas, widget

CATALOG = widget(dbutils, "catalogo", "oceanwatch_g07")
lh = get_lakehouse(spark, catalog=CATALOG)

# COMMAND ----------

import json

from oceanwatch.setup import build_waterway_exceptions

print(json.dumps(build_waterway_exceptions(lh, fechas(dbutils, lh)), indent=2))
