# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Configuración de Unity Catalog
# MAGIC
# MAGIC **Objetivo.** Dejar creados los activos base de OceanWatch Analytics en Unity Catalog: catálogo, esquemas y Volume.
# MAGIC
# MAGIC **Entradas.** Un identificador corto del equipo en `TEAM_ID` y permisos para crear catálogo, esquemas y Volume.
# MAGIC
# MAGIC **Salidas.** `oceanwatch_<equipo>`, los esquemas `landing`, `reference`, `analytics` y el Volume `landing.raw_ais`.
# MAGIC
# MAGIC **Prerrequisitos.** Ejecutar en un workspace Databricks con Unity Catalog y serverless habilitados; no requiere secretos ni datos locales.
# MAGIC
# MAGIC **Validación.** La última celda ejecuta `SHOW CATALOGS`, `SHOW SCHEMAS` y `DESCRIBE VOLUME`.

# COMMAND ----------

# Identificador provisional neutral; reemplazar únicamente tras acordarlo con el equipo.
TEAM_ID = "g06"

# Validación defensiva para usar el identificador de forma segura en sentencias SQL.
if not TEAM_ID.replace("_", "").isalnum() or TEAM_ID != TEAM_ID.lower():
    raise ValueError("TEAM_ID debe usar solo minúsculas, números y guiones bajos")

CATALOG = f"oceanwatch_{TEAM_ID}"
SCHEMA_COMMENTS = {
    "landing": "Datos AIS descargados y descomprimidos desde la fuente oficial NOAA.",
    "reference": "Datos de referencia: World Port Index y catálogo de tipos AIS.",
    "analytics": "Tablas optimizadas y resultados analíticos de OceanWatch.",
}
VOLUME_NAME = "raw_ais"
VOLUME_FQN = f"{CATALOG}.landing.{VOLUME_NAME}"
VOLUME_PATH = f"/Volumes/{CATALOG}/landing/{VOLUME_NAME}"

print(f"Catálogo objetivo: {CATALOG}")
print(f"Volume objetivo: {VOLUME_PATH}")

# COMMAND ----------

# Los comentarios documentan propósito y procedencia desde la creación de los activos.
spark.sql(
    f"CREATE CATALOG IF NOT EXISTS {CATALOG} "
    "COMMENT 'Lakehouse OceanWatch Analytics para tráfico marítimo AIS de NOAA.'"
)

for schema_name, comment in SCHEMA_COMMENTS.items():
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema_name} "
        f"COMMENT '{comment}'"
    )

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {VOLUME_FQN} "
    "COMMENT 'Archivos AIS crudos descargados y CSV descomprimidos; no es una tabla analítica.'"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación de activos y metadatos
# MAGIC
# MAGIC La salida de esta sección deja la evidencia de creación. Si falla, no se continúa con la ingesta.

# COMMAND ----------

display(spark.sql(f"SHOW CATALOGS LIKE '{CATALOG}'"))
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
display(spark.sql(f"DESCRIBE VOLUME {VOLUME_FQN}"))

# COMMAND ----------

# Verificación programática para que una ejecución incompleta no pase inadvertida.
existing_schemas = {next(iter(row.asDict().values())) for row in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()}
missing_schemas = set(SCHEMA_COMMENTS) - existing_schemas
if missing_schemas:
    raise RuntimeError(f"Esquemas no creados: {sorted(missing_schemas)}")

print("CONFIGURATION_OK")
print(f"catalog={CATALOG}")
print(f"volume={VOLUME_PATH}")