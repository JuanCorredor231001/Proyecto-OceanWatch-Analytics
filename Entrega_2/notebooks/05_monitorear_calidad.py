# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Monitoreo de calidad (compuerta operativa)
# MAGIC
# MAGIC **Tarea `monitorear_calidad`.** Reconciliación `bronce = plata + cuarentena` por fecha y umbral de cuarentena (`umbral_cuarentena_pct`, 5 % por defecto). Si falla, la tarea lanza excepción: el job queda en error, se notifica por correo y **oro no se actualiza**.

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

from oceanwatch.pipeline import monitor_quality

umbral = float(widget(dbutils, "umbral_cuarentena_pct", "5"))
for f in fechas(dbutils, lh):
    print(json.dumps(monitor_quality(lh, f, max_pct_quarantine=umbral), indent=2, ensure_ascii=False))
