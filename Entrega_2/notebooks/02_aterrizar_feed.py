# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Llegada del feed diario a landing
# MAGIC
# MAGIC **Tarea `aterrizar_feed`.** Descarga el ZIP oficial NOAA del día (reintentos, ZIP válido, SHA-256 registrado como en D-02) y publica el CSV de forma atómica en `/Volumes/<catálogo>/landing/raw_ais/feed/<fecha>/`, directorio que observa Auto Loader.

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

from oceanwatch.feed import land_day

resultados = [land_day(lh, f) for f in fechas(dbutils, lh)]
print(json.dumps(resultados, indent=2))
