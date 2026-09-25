# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Gobernanza y documentación
# MAGIC
# MAGIC **Objetivo.** Reunir la evidencia de metadatos de Unity Catalog para catálogo, esquemas, Volumes y tabla analítica. Este notebook no modifica datos.
# MAGIC
# MAGIC **Validación.** Los comandos `DESCRIBE ... EXTENDED` muestran comentarios, propietario y ubicación. En Free Edition se reportan los permisos visibles para el usuario del workspace.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "oceanwatch_g06"
RAW_VOLUME = f"{CATALOG}.landing.raw_ais"
WPI_VOLUME = f"{CATALOG}.reference.world_port_index"
BENCHMARK_VOLUME = f"{CATALOG}.analytics.benchmark_files"
DELTA_TABLE = f"{CATALOG}.analytics.ais_positions_d07_delta"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completar y normalizar comentarios

# COMMAND ----------

# Los comentarios identifican propósito, procedencia y el carácter no productivo del benchmark.
spark.sql(f"COMMENT ON VOLUME {RAW_VOLUME} IS 'Archivos AIS NOAA crudos y CSV extraídos; fuente de ingesta, no tabla analítica.'")
spark.sql(f"COMMENT ON VOLUME {WPI_VOLUME} IS 'World Port Index NGA Pub. 150; referencia para asociar celdas H3 y puertos.'")
spark.sql(f"COMMENT ON VOLUME {BENCHMARK_VOLUME} IS 'Línea base Parquet del experimento de almacenamiento; evidencia reproducible, no artefacto productivo.'")
spark.sql(f"COMMENT ON TABLE {DELTA_TABLE} IS 'Tabla analítica oficial para historial temporal por MMSI; procede de AIS NOAA junio 2023 y aplica D-07: deduplicación exacta y MMSI válidos de nueve dígitos.'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidencia de catálogo, esquemas, Volumes y tabla

# COMMAND ----------

display(spark.sql(f"DESCRIBE CATALOG EXTENDED {CATALOG}"))
for schema_name in ("landing", "reference", "analytics"):
    display(spark.sql(f"DESCRIBE SCHEMA EXTENDED {CATALOG}.{schema_name}"))

for volume_name in (RAW_VOLUME, WPI_VOLUME, BENCHMARK_VOLUME):
    display(spark.sql(f"DESCRIBE VOLUME {volume_name}"))

display(spark.sql(f"DESCRIBE TABLE EXTENDED {DELTA_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Organización y permisos visibles
# MAGIC
# MAGIC En Databricks Free Edition el workspace se opera bajo el usuario propietario. Se consulta la metadata efectiva, sin inventar `GRANT` adicionales.

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON CATALOG {CATALOG}"))
display(spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.analytics"))
display(spark.sql(f"SHOW GRANTS ON TABLE {DELTA_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen ejecutivo
# MAGIC
# MAGIC El catálogo `oceanwatch_g06` organiza el proyecto AIS en tres esquemas funcionales. `landing.raw_ais` conserva la fuente y `reference.world_port_index` la referencia WPI. `analytics.ais_positions_d07_delta` es el artefacto analítico oficial y documenta procedencia y D-07; `analytics.benchmark_files` conserva únicamente la línea base Parquet del requisito 4. Propietarios y permisos se reportan desde Unity Catalog según la configuración efectiva de Free Edition.