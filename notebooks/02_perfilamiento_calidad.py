# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Perfilamiento y calidad de AIS
# MAGIC
# MAGIC Este notebook perfila los siete CSV AIS completos. No escribe tablas ni usa `cache()` o `persist()`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

TEAM_ID = "g06"
ROOT = f"/Volumes/oceanwatch_{TEAM_ID}/landing/raw_ais"
DATES = ["2023-06-01", "2023-06-02", "2023-06-03", "2023-06-04", "2023-06-05", "2023-06-06", "2023-06-07"]
EXPECTED_ROWS = 60_533_559
SCHEMA = StructType([
 StructField("MMSI",StringType(),True),StructField("BaseDateTime",StringType(),True),StructField("LAT",DoubleType(),True),StructField("LON",DoubleType(),True),StructField("SOG",DoubleType(),True),StructField("COG",DoubleType(),True),StructField("Heading",DoubleType(),True),StructField("VesselName",StringType(),True),StructField("IMO",StringType(),True),StructField("CallSign",StringType(),True),StructField("VesselType",IntegerType(),True),StructField("Status",IntegerType(),True),StructField("Length",DoubleType(),True),StructField("Width",DoubleType(),True),StructField("Draft",DoubleType(),True),StructField("Cargo",IntegerType(),True),StructField("TransceiverClass",StringType(),True)
])
paths = [f"{ROOT}/extracted/ingestion_date={d}/AIS_{d.replace('-', '_')}.csv" for d in DATES]
# Se agrega la fecha de ingesta a partir de la ruta del archivo para conservar el origen de cada registro y habilitar análisis por día sin depender del nombre del CSV.
ais = (spark.read.option("header","true").option("enforceSchema","false").option("mode","FAILFAST").schema(SCHEMA).csv(paths)
 .withColumn("ingestion_date",F.regexp_extract(F.col("_metadata.file_path"),r"ingestion_date=(\d{4}-\d{2}-\d{2})",1)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Perfilamiento general

# COMMAND ----------

# Se generan métricas de cobertura por día y un resumen semanal para validar que los siete archivos fueron cargados correctamente y conocer el volumen de posiciones AIS y buques únicos observados en el período analizado.
daily = ais.groupBy("ingestion_date").agg(F.count("*").alias("posiciones"),F.countDistinct("MMSI").alias("mmsi_unicos"))
weekly = ais.agg(F.count("*").alias("posiciones"),F.countDistinct("MMSI").alias("mmsi_unicos")).withColumn("ingestion_date",F.lit("TOTAL_SEMANA"))
display(daily.unionByName(weekly.select(daily.columns)).orderBy("ingestion_date"))

# COMMAND ----------

# Distribución de posiciones AIS por tipo de embarcación. 
# Los valores nulos se conservan explícitamente como "NULL" para evaluar la calidad del dato y cuantificar registros sin clasificación de buque.
display(ais.groupBy(F.coalesce(F.col("VesselType").cast("string"),F.lit("NULL")).alias("vessel_type")).count().withColumnRenamed("count","posiciones").orderBy(F.desc("posiciones")))

# COMMAND ----------

# Segmentación de las embarcaciones por eslora (Length) y manga (Width). Los valores se agrupan en rangos de tamaño para identificar la distribución de posiciones AIS por tipo de dimensión y evidenciar posibles valores faltantes.
sizes = ais.select(
 F.when(F.col("Length").isNull(),"NULL").when(F.col("Length")<20,"<20 m").when(F.col("Length")<50,"20-<50 m").when(F.col("Length")<100,"50-<100 m").when(F.col("Length")<200,"100-<200 m").otherwise(">=200 m").alias("rango_length"),
 F.when(F.col("Width").isNull(),"NULL").when(F.col("Width")<10,"<10 m").when(F.col("Width")<20,"10-<20 m").when(F.col("Width")<40,"20-<40 m").otherwise(">=40 m").alias("rango_width"))
display(sizes.groupBy("rango_length","rango_width").count().withColumnRenamed("count","posiciones").orderBy(F.desc("posiciones")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Calidad de datos

# COMMAND ----------

# Un solo agregado para nulos, límites y sentinelas sobre el corpus completo.
# Perfilamiento de calidad del dataset AIS:
# se cuantifican valores nulos, coordenadas fuera de rango, velocidades inválidas y códigos sentinela definidos por el estándar AIS, con el fin de evaluar la completitud y consistencia del corpus.
metrics=[F.count("*").alias("total")]
metrics += [F.sum(F.col(c).isNull().cast("long")).alias(f"null__{c}") for c in SCHEMA.fieldNames()]
metrics += [
 F.sum((~F.col("LAT").between(-90,90)).cast("long")).alias("lat_fuera"),
 F.sum((~F.col("LON").between(-180,180)).cast("long")).alias("lon_fuera"),
 F.sum((F.col("SOG")<0).cast("long")).alias("sog_negativa"),
 F.sum((F.col("SOG")>102.2).cast("long")).alias("sog_fuera_ais"),
 F.sum((F.col("SOG")==102.3).cast("long")).alias("sog_102_3"),
 F.sum((F.col("Heading")==511).cast("long")).alias("heading_511"),
 F.sum((F.col("COG")==360).cast("long")).alias("cog_360"),
 F.sum((~F.col("MMSI").rlike(r"^[0-9]{9}$")).cast("long")).alias("mmsi_no_conforme")]
q=ais.agg(*metrics).first().asDict()
assert q["total"] == EXPECTED_ROWS, f"Conteo inesperado: {q['total']:,}"

nulls=[(c,int(q[f"null__{c}"]),q["total"],round(100*q[f"null__{c}"]/q["total"],4)) for c in SCHEMA.fieldNames()]
display(spark.createDataFrame(nulls,"columna string,nulos long,total long,porcentaje_nulos double").orderBy(F.desc("porcentaje_nulos")))
display(spark.createDataFrame([
 ("LAT fuera de [-90,90]",q["lat_fuera"]),("LON fuera de [-180,180]",q["lon_fuera"]),("SOG negativa",q["sog_negativa"]),("SOG >102.2",q["sog_fuera_ais"]),("SOG=102.3 sentinela",q["sog_102_3"]),("Heading=511 sentinela",q["heading_511"]),("COG=360 sentinela",q["cog_360"]),("MMSI no cumple 9 digitos",q["mmsi_no_conforme"])],
 "regla string,registros long").withColumn("porcentaje",F.round(100*F.col("registros")/F.lit(q["total"]),4)))

# COMMAND ----------

# Status nulo por clase identifica la ausencia estructural esperada en transpondedor B.
display(ais.groupBy(F.coalesce(F.col("TransceiverClass"),F.lit("NULL")).alias("transceiver_class")).agg(F.count("*").alias("posiciones"),F.sum(F.col("Status").isNull().cast("long")).alias("status_nulo"),F.round(100*F.avg(F.col("Status").isNull().cast("double")),4).alias("porcentaje_status_nulo")).orderBy("transceiver_class"))

# COMMAND ----------

# Clasificación de registros con MMSI inválido para identificar la causa de incumplimiento del estándar AIS (9 dígitos numéricos).
# Se distinguen valores nulos, vacíos, con caracteres no numéricos y longitudes diferentes a las esperadas.
invalid=ais.filter(~F.col("MMSI").rlike(r"^[0-9]{9}$")).withColumn("patron_mmsi",
 F.when(F.col("MMSI").isNull()|(F.length(F.trim(F.col("MMSI")))==0),"nulo_o_vacio").when(~F.col("MMSI").rlike(r"^[0-9]+$"),"contiene_no_digitos").when(F.length("MMSI")<9,"numerico_menos_9_digitos").when(F.length("MMSI")>9,"numerico_mas_9_digitos").otherwise("otro"))
display(invalid.groupBy("patron_mmsi").agg(F.count("*").alias("posiciones"),F.countDistinct("MMSI").alias("mmsi_distintos")).orderBy(F.desc("posiciones")))
display(invalid.groupBy("MMSI","patron_mmsi").count().withColumnRenamed("count","posiciones").orderBy(F.desc("posiciones")).limit(50))

# COMMAND ----------

# Identificación de registros completamente duplicados.
# Se agrupan todas las columnas originales del esquema AIS para detectar filas idénticas y cuantificar cuántas copias adicionales existen en el corpus.
dups=ais.groupBy(*SCHEMA.fieldNames()).count().filter(F.col("count")>1)
display(dups.agg(F.count("*").alias("grupos_duplicados_exactos"),F.sum("count").alias("filas_en_grupos"),F.sum(F.col("count")-1).alias("copias_adicionales")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. D-04: pares consecutivos para haversine
# MAGIC
# MAGIC Se cuentan por separado los campos no calculables, los gaps no positivos, los gaps mayores a 2 h y las velocidades implícitas mayores a 60 kn.

# COMMAND ----------

# Construcción de trayectorias por embarcación a partir de posiciones AIS consecutivas.
# Para cada MMSI se calcula el intervalo de tiempo entre reportes, la distancia recorrida mediante la fórmula de Haversine y la velocidad implícita. Esto permite identificar movimientos físicamente plausibles y cuantificar observaciones que deben excluirse por brechas temporales o velocidades irrealistas.
events=ais.select("MMSI","LAT","LON",F.to_timestamp("BaseDateTime","yyyy-MM-dd'T'HH:mm:ss").alias("event_ts")).filter(F.col("MMSI").rlike(r"^[0-9]{9}$"))
w=Window.partitionBy("MMSI").orderBy("event_ts")
pairs=(events.withColumn("prev_ts",F.lag("event_ts").over(w)).withColumn("prev_lat",F.lag("LAT").over(w)).withColumn("prev_lon",F.lag("LON").over(w)).filter(F.col("prev_ts").isNotNull()).withColumn("gap_hours",(F.col("event_ts").cast("long")-F.col("prev_ts").cast("long"))/F.lit(3600.0)))
pairs=pairs.withColumn("distance_km",F.when(F.col("LAT").isNotNull()&F.col("LON").isNotNull()&F.col("prev_lat").isNotNull()&F.col("prev_lon").isNotNull(),12742.0176*F.asin(F.sqrt(F.pow(F.sin(F.radians(F.col("LAT")-F.col("prev_lat"))/2),2)+F.cos(F.radians("prev_lat"))*F.cos(F.radians("LAT"))*F.pow(F.sin(F.radians(F.col("LON")-F.col("prev_lon"))/2),2))))).withColumn("implied_knots",F.when(F.col("gap_hours")>0,F.col("distance_km")/F.col("gap_hours")/F.lit(1.852)))
display(pairs.agg(F.count("*").alias("pares_con_previo"),F.sum(F.col("distance_km").isNull().cast("long")).alias("excluir_campos_requeridos"),F.sum((F.col("gap_hours")<=0).cast("long")).alias("excluir_gap_no_positivo"),F.sum((F.col("gap_hours")>2).cast("long")).alias("excluir_gap_mayor_2h"),F.sum(((F.col("gap_hours")>0)&(F.col("gap_hours")<=2)&(F.col("implied_knots")>60)).cast("long")).alias("excluir_velocidad_mayor_60kn"),F.sum((F.col("distance_km").isNotNull()&(F.col("gap_hours")>0)&(F.col("gap_hours")<=2)&(F.col("implied_knots")<=60)).cast("long")).alias("pares_elegibles")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resumen ejecutivo
# MAGIC
# MAGIC Las tablas anteriores reúnen conteos diarios, MMSI únicos y distribución de tráfico. Nulos y sentinelas se calculan sobre todo el corpus. `Status` nulo en clase B se trata como ausencia estructural; `SOG=102.3`, `Heading=511` y `COG=360` quedan como no disponibles y no entran a sus promedios. Los MMSI no conformes se describen por patrón sin asignarles una clase AIS sin fuente externa. Antes de pasar a las preguntas de negocio se cuantifican duplicados exactos y exclusiones D-04, sin usar `cache()`, `persist()` ni una tabla intermedia.