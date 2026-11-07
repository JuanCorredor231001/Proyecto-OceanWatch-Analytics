# Databricks notebook source
# MAGIC %md
# MAGIC # 13 - Bono: Delta Sharing (Open Sharing) - proveedor (workspace institucional)
# MAGIC
# MAGIC **Objetivo.** Publicar las tablas oro sin copiarlas: un *share* con las cinco tablas del contrato y un destinatario de **Open Sharing** (token) para el workspace de Databricks Free Edition.
# MAGIC
# MAGIC **Prerrequisitos.** Metastore con Delta Sharing habilitado y privilegios `CREATE SHARE`, `CREATE RECIPIENT` (los otorga el administrador del workspace institucional).
# MAGIC
# MAGIC **Seguridad.** El enlace de activación es una credencial: se entrega por un canal privado al equipo y **nunca** se versiona en Git.

# COMMAND ----------

dbutils.widgets.text("catalogo", "oceanwatch_g07")
dbutils.widgets.text("share", "oceanwatch_oro")
dbutils.widgets.text("destinatario", "oceanwatch_free_edition")
C, SHARE, RECIPIENT = (dbutils.widgets.get(k) for k in ("catalogo", "share", "destinatario"))
TABLAS = ["trayectorias_diarias_buque", "estadias_puerto", "puertos_resumen_diario", "trafico_celda_hora", "anomalias_velocidad_diarias"]

# COMMAND ----------

spark.sql(f"CREATE SHARE IF NOT EXISTS {SHARE} COMMENT 'Capa oro OceanWatch (contrato oceanwatch-gold v1.0.0)'")
existentes = {r["name"] for r in spark.sql(f"SHOW ALL IN SHARE {SHARE}").collect()}
for t in TABLAS:
    if f"gold.{t}" not in existentes:
        # Historial compartido para lecturas incrementales (CDF no es necesario para el tablero).
        spark.sql(f"ALTER SHARE {SHARE} ADD TABLE {C}.gold.{t} AS gold.{t} WITH HISTORY")
display(spark.sql(f"SHOW ALL IN SHARE {SHARE}"))

# COMMAND ----------

# Destinatario de Open Sharing (autenticación por token): consumidor fuera de este metastore.
spark.sql(f"CREATE RECIPIENT IF NOT EXISTS {RECIPIENT} COMMENT 'Workspace Databricks Free Edition del equipo'")
spark.sql(f"GRANT SELECT ON SHARE {SHARE} TO RECIPIENT {RECIPIENT}")
display(spark.sql(f"SHOW GRANTS ON SHARE {SHARE}"))
# El activation_link aparece en DESCRIBE RECIPIENT: copiarlo manualmente y entregarlo por canal privado.
display(spark.sql(f"DESCRIBE RECIPIENT {RECIPIENT}").filter("info_name <> 'activation_link'"))
