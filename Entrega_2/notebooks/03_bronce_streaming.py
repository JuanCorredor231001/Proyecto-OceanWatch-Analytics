# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Bronce: ingesta en streaming (Auto Loader)
# MAGIC
# MAGIC **Tarea `bronce_streaming`.** Structured Streaming con Auto Loader (`cloudFiles`, CSV con esquema explícito de texto, `_rescued_data`) hacia `bronze.ais_raw` (Delta append-only, `delta.appendOnly=true`).
# MAGIC
# MAGIC - **Checkpoint:** `/Volumes/<catálogo>/ops/checkpoints/bronze_ais_raw` registra archivos procesados y offsets.
# MAGIC - **Disparo:** `availableNow`: procesa todo lo pendiente y termina (apto para job programado).
# MAGIC - **Tolerancia a fallos:** el sumidero Delta es idempotente por `batchId`; ver `docs/evidencias/06 evidencia_tolerancia_fallos.md`.

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

from oceanwatch.streaming import run_bronze

resumen = run_bronze(lh)
print(json.dumps(resumen, indent=2))
display(spark.sql(f"DESCRIBE HISTORY {lh.name('bronze', 'ais_raw')}").limit(5))
