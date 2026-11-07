# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - ML: entrenamiento y registro en MLflow
# MAGIC
# MAGIC **Job `oceanwatch_backfill_historico`, tarea `ml_entrenar`.** IsolationForest en un `Pipeline` de scikit-learn, experimento MLflow con parámetros, métricas, artefactos de diagnóstico y firma; modelo registrado en Unity Catalog como `<catálogo>.ml.oceanwatch_anomalias_velocidad` con alias `champion`.

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

from oceanwatch.ml import train

contaminacion = float(widget(dbutils, "contaminacion", "0.01"))
print(json.dumps(train(lh, fechas(dbutils, lh), contamination=contaminacion), indent=2))
