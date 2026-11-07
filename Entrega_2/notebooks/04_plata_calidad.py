# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Plata: compuertas de calidad, deduplicación y cuarentena
# MAGIC
# MAGIC **Tarea `plata_calidad`.** Stream desde `bronze.ais_raw` con `foreachBatch`:
# MAGIC
# MAGIC 1. Tipificación tolerante (`try_cast`) y 12 expectativas (7 CUARENTENA, 5 ADVERTENCIA) de `oceanwatch/quality.py`.
# MAGIC 2. Deduplicación por (MMSI, BaseDateTime) dentro del lote y frente a lotes previos (D01 exacto / D02 conflictivo).
# MAGIC 3. Escrituras idempotentes (`txnAppId`, `txnVersion=batchId`) a `silver.ais_posiciones`, `silver.ais_cuarentena` y `silver.metricas_calidad`.
# MAGIC
# MAGIC Los registros inválidos se enrutan a cuarentena con su causa **sin detener el stream**.

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

from pyspark.sql import functions as F

from oceanwatch.streaming import run_silver

print(json.dumps(run_silver(lh), indent=2))
display(lh.read("silver", "metricas_calidad").orderBy(F.desc("batch_id"), "regla").limit(40))
