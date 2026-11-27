# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 09 - ML: puntuación diaria de anomalías
# MAGIC
# MAGIC **Tarea `ml_puntuar`.** Carga `models:/<modelo>@champion` y escribe `gold.anomalias_velocidad_diarias` (panel "anomalías del día" del tablero).

# COMMAND ----------

# MAGIC %pip install -q "mlflow>=3.1" scikit-learn skops

# COMMAND ----------

# Si Databricks muestra el mensaje: "you may need to restart the kernel..."
# ejecutar: 
dbutils.library.restartPython()
# y continuar desde la siguiente celda.

# COMMAND ----------

import os
import sys

# El código del pipeline vive en ../src (misma librería probada en local; ver README).
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from oceanwatch.lakehouse import get_lakehouse
from oceanwatch.notebook_params import fechas, widget

CATALOG = widget(dbutils, "catalogo", "oceanwatch_g06")
lh = get_lakehouse(spark, catalog=CATALOG)

# COMMAND ----------

import json

from oceanwatch.ml import score

#for f in fechas(dbutils, lh):
#    print(json.dumps(score(lh, f)))

for f in fechas(dbutils, lh):
    n_registros = spark.table(f"{CATALOG}.ml.features_velocidad_buque_dia") \
                       .filter(f"event_date = '{f}'") \
                       .count()
    
    if n_registros > 0:
        print(json.dumps(score(lh, f)))
    else:
        print(json.dumps({"fecha": str(f), "status": "sin_datos", "predicciones": 0}))