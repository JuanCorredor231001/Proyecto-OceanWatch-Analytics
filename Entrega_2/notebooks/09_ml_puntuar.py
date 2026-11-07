# Databricks notebook source
# MAGIC %md
# MAGIC # 09 - ML: puntuación diaria de anomalías
# MAGIC
# MAGIC **Tarea `ml_puntuar`.** Carga `models:/<modelo>@champion` y escribe `gold.anomalias_velocidad_diarias` (panel "anomalías del día" del tablero).

# COMMAND ----------

# MAGIC %pip install -q "mlflow>=3.1" scikit-learn skops

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

from oceanwatch.ml import score

for f in fechas(dbutils, lh):
    print(json.dumps(score(lh, f)))
