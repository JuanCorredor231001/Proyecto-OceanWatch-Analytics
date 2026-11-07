# Databricks notebook source
# MAGIC %md
# MAGIC # 14 - Bono: Delta Sharing - consumidor en Databricks Free Edition
# MAGIC
# MAGIC **Objetivo.** Leer las tablas oro compartidas con el protocolo abierto (sin exportar archivos ni duplicar el dataset) y construir las vistas del tablero en Free Edition.
# MAGIC
# MAGIC **Pasos previos (manuales).**
# MAGIC 1. Abrir el enlace de activación recibido por canal privado y descargar `config.share` (credencial: no se versiona).
# MAGIC 2. Subirlo a un Volume privado de Free Edition, p. ej. `/Volumes/workspace/default/sharing/config.share`.
# MAGIC 3. Importar `dashboards/oceanwatch_consola.lvdash.json` cambiando el catálogo de los datasets por las vistas `oceanwatch_shared.*` creadas abajo.

# COMMAND ----------

# MAGIC %pip install -q delta-sharing

# COMMAND ----------

import delta_sharing

dbutils.widgets.text("perfil", "/Volumes/workspace/default/sharing/config.share")
dbutils.widgets.text("esquema_destino", "workspace.oceanwatch_shared")
PROFILE, DEST = dbutils.widgets.get("perfil"), dbutils.widgets.get("esquema_destino")

client = delta_sharing.SharingClient(PROFILE)
tablas = client.list_all_tables()
display(spark.createDataFrame([(t.share, t.schema, t.name) for t in tablas], "share string, schema string, tabla string"))

# COMMAND ----------

# MAGIC %md
# MAGIC Cada tabla se lee desde el proveedor con el conector Spark `deltaSharing` (protocolo abierto, sin exportar archivos). Los tableros de Databricks necesitan tablas de Unity Catalog, así que se refrescan en Free Edition **solo los agregados oro** que consume la consola (MB, no los ~60 M de posiciones crudas). Programar este notebook mantiene la copia al día con el proveedor.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DEST}")
for t in tablas:
    url = f"{PROFILE}#{t.share}.{t.schema}.{t.name}"
    df = spark.read.format("deltaSharing").load(url)
    df.createOrReplaceTempView(f"shared_{t.name}")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DEST}.{t.name}")
    print(t.name, spark.table(f"{DEST}.{t.name}").count())

# COMMAND ----------

# Consulta de verificación del panel "ranking de puertos" sobre datos compartidos.
display(spark.sql("""
  SELECT event_date, ranking, port_name, buques_distintos, buques_en_puerto_al_cierre
  FROM shared_puertos_resumen_diario
  WHERE event_date = (SELECT max(event_date) FROM shared_puertos_resumen_diario) AND ranking <= 10
  ORDER BY ranking"""))
