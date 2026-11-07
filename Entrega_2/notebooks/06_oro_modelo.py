# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Oro: modelo de negocio
# MAGIC
# MAGIC **Tarea `oro_modelo`.** Lee exclusivamente `silver.ais_posiciones` y reescribe por fecha (`replaceWhere`) las tablas del contrato `contracts/gold_contract.yml`:
# MAGIC `trayectorias_diarias_buque`, `estadias_puerto`, `puertos_resumen_diario`, `trafico_celda_hora`.

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

from oceanwatch.gold import build_gold_for_date

for f in fechas(dbutils, lh):
    print(f, json.dumps(build_gold_for_date(lh, f)))
