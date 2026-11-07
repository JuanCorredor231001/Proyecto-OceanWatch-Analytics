# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - ML: ingeniería de características
# MAGIC
# MAGIC **Tarea `ml_features`.** Variables de velocidad por buque y día desde plata y oro hacia `ml.features_velocidad_buque_dia`. La misma función alimenta entrenamiento y puntuación.

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

from oceanwatch.ml import build_features

for f in fechas(dbutils, lh):
    print(f, build_features(lh, f))
