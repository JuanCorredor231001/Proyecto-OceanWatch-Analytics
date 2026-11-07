# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Validación del contrato de oro
# MAGIC
# MAGIC **Tarea `validar_contrato`.** Esquema, nulos, rangos, dominios y clave primaria de cada tabla oro según `contracts/gold_contract.yml`, más dos garantías de linaje: oro solo contiene buques presentes en plata y ninguna clave en cuarentena llegó a plata.

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

from oceanwatch.pipeline import validate_contract

for f in fechas(dbutils, lh):
    print(json.dumps(validate_contract(lh, f), indent=2, default=str))
