# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Resolver la fecha del feed
# MAGIC
# MAGIC **Tarea `resolver_fecha` del job.** Parámetro `fecha`: `auto` (siguiente día FEED del calendario sin trayectorias en oro), `AAAA-MM-DD` o lista separada por comas (backfill).
# MAGIC
# MAGIC **Salida.** Task value `fecha` que consumen todas las tareas siguientes; `NINGUNA` si no hay pendientes (la condición `hay_fecha_pendiente` corta el DAG).

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

from oceanwatch.pipeline import NO_PENDING, resolve_date

raw = widget(dbutils, "fecha", "auto")
resolved = ",".join(resolve_date(lh, f.strip()) for f in raw.split(",")) if raw.lower() != "auto" else resolve_date(lh, "auto")
dbutils.jobs.taskValues.set(key="fecha", value=resolved)
print(f"FECHA_RESUELTA={resolved}")
