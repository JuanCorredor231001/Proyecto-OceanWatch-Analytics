# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Almacenamiento optimizado
# MAGIC
# MAGIC **Propósito aprobado.** Recuperar el historial temporal de uno o varios MMSI, ordenado por `BaseDateTime`. La comparación usa exactamente los mismos registros D-07 en Parquet y Delta, y registra la línea base junto con el resultado post-`OPTIMIZE`.
# MAGIC
# MAGIC **Criterio de comparación.** La consulta selectiva es idéntica para ambos formatos. También se mide un rango semanal sin MMSI como caso adverso, que no corresponde al patrón favorecido por clustering por MMSI.

# COMMAND ----------

from time import perf_counter

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

CATALOG = "oceanwatch_g06"
RAW_ROOT = f"/Volumes/{CATALOG}/landing/raw_ais"
DATES = ["2023-06-01", "2023-06-02", "2023-06-03", "2023-06-04", "2023-06-05", "2023-06-06", "2023-06-07"]
DELTA_TABLE = f"{CATALOG}.analytics.ais_positions_d07_delta"
PARQUET_VOLUME = f"{CATALOG}.analytics.benchmark_files"
PARQUET_PATH = f"/Volumes/{CATALOG}/analytics/benchmark_files/ais_positions_d07_parquet"

AIS_SCHEMA = StructType([
    StructField("MMSI", StringType(), True), StructField("BaseDateTime", StringType(), True),
    StructField("LAT", DoubleType(), True), StructField("LON", DoubleType(), True),
    StructField("SOG", DoubleType(), True), StructField("COG", DoubleType(), True),
    StructField("Heading", DoubleType(), True), StructField("VesselName", StringType(), True),
    StructField("IMO", StringType(), True), StructField("CallSign", StringType(), True),
    StructField("VesselType", IntegerType(), True), StructField("Status", IntegerType(), True),
    StructField("Length", DoubleType(), True), StructField("Width", DoubleType(), True),
    StructField("Draft", DoubleType(), True), StructField("Cargo", IntegerType(), True),
    StructField("TransceiverClass", StringType(), True),
])

paths = [f"{RAW_ROOT}/extracted/ingestion_date={day}/AIS_{day.replace('-', '_')}.csv" for day in DATES]
raw_ais = (spark.read.option("header", "true").option("enforceSchema", "false")
    .option("mode", "FAILFAST").schema(AIS_SCHEMA).csv(paths)
    .withColumn("ingestion_date", F.regexp_extract(F.col("_metadata.file_path"), r"ingestion_date=(\d{4}-\d{2}-\d{2})", 1)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conjunto idéntico D-07 para ambos formatos
# MAGIC
# MAGIC De las 17 columnas AIS se eliminan únicamente duplicados exactos y se conservan MMSI numéricos de nueve dígitos. No se usa `cache()` ni `persist()`.

# COMMAND ----------

d07_ais = (raw_ais.dropDuplicates(AIS_SCHEMA.fieldNames())
    .filter(F.col("MMSI").rlike(r"^[0-9]{9}$")))
d07_ais.createOrReplaceTempView("d07_ais_source")

# El conteo se registra antes de escribir para auditar igualdad de contenido, no para optimizar el plan.
d07_rows = d07_ais.count()
print(f"Filas D-07 fuente para ambas tablas: {d07_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Parquet base y Delta con liquid clustering
# MAGIC
# MAGIC Ambos artefactos salen del mismo `d07_ais_source`. Unity Catalog administra tablas solo en Delta; Parquet se escribe como archivos en un Volume gobernado y se lee por ruta. No se registra como tabla porque una tabla no puede solaparse con un Volume. Los comentarios de la tabla Delta y el Volume registran procedencia y limpieza.

# COMMAND ----------

if spark.catalog.tableExists(DELTA_TABLE):
    raise RuntimeError(f"La tabla {DELTA_TABLE} ya existe; no se sobrescribe la línea base.")

spark.sql(f"""CREATE VOLUME IF NOT EXISTS {PARQUET_VOLUME}
COMMENT 'Línea base Parquet del experimento de almacenamiento; no es artefacto productivo. Procedencia: siete CSV AIS NOAA junio 2023; limpieza D-07.'""")
# Parquet es la línea base por archivos: no existe tabla UC Parquet administrada en este workspace.
# La ruta se valida antes de esta celda en Databricks; `errorifexists` no está soportado en Volumes.
d07_ais.write.mode("overwrite").parquet(PARQUET_PATH)

spark.sql(f"""
CREATE TABLE {DELTA_TABLE}
USING DELTA
CLUSTER BY (MMSI, BaseDateTime)
COMMENT 'Delta administrado para historial temporal por MMSI. Procede de los siete CSV AIS NOAA junio 2023 en landing.raw_ais; D-07 elimina duplicados exactos y MMSI no numéricos de nueve dígitos. Pendiente línea base previa a OPTIMIZE.'
AS SELECT * FROM d07_ais_source
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control de contenido y bytes/archivos de la línea base
# MAGIC
# MAGIC Para Delta, `DESCRIBE DETAIL` entrega `sizeInBytes`, `numFiles` y ubicación física. Para Parquet, los archivos del directorio del Volume se suman antes de `OPTIMIZE`.

# COMMAND ----------

def table_detail(table_name):
    return (spark.sql(f"DESCRIBE DETAIL {table_name}")
        .select("format", "location", "numFiles", "sizeInBytes", "createdAt"))

parquet_rows = spark.read.parquet(PARQUET_PATH).count()
delta_rows = spark.table(DELTA_TABLE).count()
assert parquet_rows == d07_rows == delta_rows, "Las tablas no contienen el mismo conjunto D-07."
print(f"Control de igualdad: fuente={d07_rows:,}, Parquet={parquet_rows:,}, Delta={delta_rows:,}")

parquet_directory = dbutils.fs.ls(PARQUET_PATH)
parquet_bytes = sum(item.size for item in parquet_directory)
parquet_data_files = [item for item in parquet_directory if item.name.endswith(".parquet")]
print(f"Parquet por ruta: bytes={parquet_bytes:,}; archivos .parquet={len(parquet_data_files)}; ruta={PARQUET_PATH}")
display(table_detail(DELTA_TABLE).withColumn("tabla", F.lit(DELTA_TABLE)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas idénticas y evidencia de archivos leídos
# MAGIC
# MAGIC El orden de formatos se alterna en dos rondas para reducir el sesgo de una segunda lectura caliente. Cada medición materializa el mismo `SELECT` por MMSI y rango de tiempo. La variante `_metadata.file_path` conserva esos filtros y cuenta los archivos alcanzados como evidencia complementaria; no reemplaza el tiempo del `SELECT` principal.

# COMMAND ----------

TARGET_MMSI = ["367638030", "367438630"]
START_TS = "2023-06-01T00:00:00"
END_TS = "2023-06-08T00:00:00"

parquet_base = spark.read.parquet(PARQUET_PATH)
parquet_base.createOrReplaceTempView("parquet_d07_base")

def selective_query(source_name):
    # Misma forma lógica para Parquet y Delta: filtro MMSI + rango y orden temporal.
    return spark.sql(f"""
        SELECT MMSI, BaseDateTime, LAT, LON, SOG, VesselName, VesselType
        FROM {source_name}
        WHERE MMSI IN ({','.join(repr(m) for m in TARGET_MMSI)})
          AND BaseDateTime >= '{START_TS}' AND BaseDateTime < '{END_TS}'
        ORDER BY MMSI, BaseDateTime
    """)

def broad_query(source_name):
    # Caso adverso: rango semanal sin predicado MMSI; no se espera data skipping por el clustering elegido.
    return spark.sql(f"""
        SELECT VesselType, COUNT(*) AS posiciones
        FROM {source_name}
        WHERE BaseDateTime >= '{START_TS}' AND BaseDateTime < '{END_TS}'
        GROUP BY VesselType
        ORDER BY posiciones DESC
    """)

def measure(label, dataframe):
    started = perf_counter()
    rows = dataframe.count()
    seconds = perf_counter() - started
    print(f"{label}: filas={rows:,}; tiempo_s={seconds:.3f}")
    return (label, rows, seconds)

def files_touched(source_name, predicate_sql):
    # `_metadata.file_path` se proyecta desde el lector, no desde la vista temporal, para serverless.
    source_df = spark.read.parquet(PARQUET_PATH) if source_name == "parquet_d07_base" else spark.table(source_name)
    return (source_df.filter(F.expr(predicate_sql))
        .select("_metadata.file_path").distinct().withColumnRenamed("file_path", "ruta_archivo"))

selective_predicate = f"MMSI IN ({','.join(repr(m) for m in TARGET_MMSI)}) AND BaseDateTime >= '{START_TS}' AND BaseDateTime < '{END_TS}'"
broad_predicate = f"BaseDateTime >= '{START_TS}' AND BaseDateTime < '{END_TS}'"

# COMMAND ----------

# Ronda 1: Parquet → Delta. Ronda 2 invierte el orden antes de promediar o comparar.
baseline_measurements = []
for label, source_name in [("Parquet selectiva R1", "parquet_d07_base"), ("Delta pre-OPTIMIZE selectiva R1", DELTA_TABLE),
                           ("Delta pre-OPTIMIZE selectiva R2", DELTA_TABLE), ("Parquet selectiva R2", "parquet_d07_base")]:
    baseline_measurements.append(measure(label, selective_query(source_name)))

display(spark.createDataFrame(baseline_measurements, "medicion string, filas long, tiempo_s double"))

print("=== PLAN Parquet, consulta selectiva ===")
selective_query("parquet_d07_base").explain("formatted")
print("=== PLAN Delta pre-OPTIMIZE, consulta selectiva ===")
selective_query(DELTA_TABLE).explain("formatted")

parquet_selective_files = files_touched("parquet_d07_base", selective_predicate)
delta_selective_files = files_touched(DELTA_TABLE, selective_predicate)
print(f"Archivos Parquet alcanzados (selectiva): {parquet_selective_files.count()}")
print(f"Archivos Delta pre-OPTIMIZE alcanzados (selectiva): {delta_selective_files.count()}")
display(parquet_selective_files.withColumn("formato", F.lit("Parquet")))
display(delta_selective_files.withColumn("formato", F.lit("Delta pre-OPTIMIZE")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Caso adverso sin MMSI
# MAGIC
# MAGIC Esta consulta se ejecuta después de cerrar la línea base selectiva. Se mantiene igual para ambos formatos y deja visible el trade-off del layout por MMSI.

# COMMAND ----------

broad_measurements = [
    measure("Delta pre-OPTIMIZE amplio R1", broad_query(DELTA_TABLE)),
    measure("Parquet amplio R1", broad_query("parquet_d07_base")),
    measure("Parquet amplio R2", broad_query("parquet_d07_base")),
    measure("Delta pre-OPTIMIZE amplio R2", broad_query(DELTA_TABLE)),
]
display(spark.createDataFrame(broad_measurements, "medicion string, filas long, tiempo_s double"))
print("=== PLAN Parquet, caso amplio ===")
broad_query("parquet_d07_base").explain("formatted")
print("=== PLAN Delta pre-OPTIMIZE, caso amplio ===")
broad_query(DELTA_TABLE).explain("formatted")
print(f"Archivos Parquet alcanzados (amplia): {files_touched('parquet_d07_base', broad_predicate).count()}")
print(f"Archivos Delta pre-OPTIMIZE alcanzados (amplia): {files_touched(DELTA_TABLE, broad_predicate).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZE y repetición post-optimización
# MAGIC
# MAGIC La tabla usa liquid clustering; por eso `OPTIMIZE` respeta `CLUSTER BY (MMSI, BaseDateTime)` y no se combina con `ZORDER BY`. Las mismas consultas se repiten en orden alternado. Si `OPTIMIZE` no reescribe archivos, un cambio de tiempo se interpreta como variación de ejecución y no como efecto causal del comando.

# COMMAND ----------

optimize_started = perf_counter()
optimize_result = spark.sql(f"OPTIMIZE {DELTA_TABLE}")
optimize_elapsed_s = perf_counter() - optimize_started
print(f"OPTIMIZE completado en {optimize_elapsed_s:.3f} s")
display(optimize_result)
display(table_detail(DELTA_TABLE))

# COMMAND ----------

def post_selective_query():
    # Conserva el mismo filtro, columnas y orden de la consulta representativa.
    return selective_query(DELTA_TABLE)

def post_broad_query():
    # Conserva el caso amplio sin MMSI para exponer el trade-off del layout.
    return broad_query(DELTA_TABLE)

# Alternar patrones distribuye el calentamiento de sesión entre selectiva y amplia.
post_measurements = [
    measure("Delta post-OPTIMIZE selectiva R1", post_selective_query()),
    measure("Delta post-OPTIMIZE amplia R1", post_broad_query()),
    measure("Delta post-OPTIMIZE selectiva R2", post_selective_query()),
    measure("Delta post-OPTIMIZE amplia R2", post_broad_query()),
]
display(spark.createDataFrame(post_measurements, "medicion string, filas long, tiempo_s double"))

# La evidencia de archivos conserva exactamente los predicados de las consultas medidas.
post_selective_files = files_touched(DELTA_TABLE, selective_predicate)
post_broad_files = files_touched(DELTA_TABLE, broad_predicate)
print(f"Archivos Delta post-OPTIMIZE (selectiva): {post_selective_files.count()}")
print(f"Archivos Delta post-OPTIMIZE (amplia): {post_broad_files.count()}")
print("=== PLAN Delta post-OPTIMIZE, consulta selectiva ===")
post_selective_query().explain("formatted")