# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Preguntas de negocio
# MAGIC
# MAGIC ## (a) ¿Cuántos buques distintos transmitieron cada día?
# MAGIC
# MAGIC Antes de esta pregunta se eliminan duplicados exactos y se exige un MMSI numérico de nueve dígitos, como indica D-07. No se usa `cache()` ni `persist()`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

ROOT = "/Volumes/oceanwatch_g07/landing/raw_ais"
DATES = ["2023-06-01","2023-06-02","2023-06-03","2023-06-04","2023-06-05","2023-06-06","2023-06-07"]
SCHEMA = StructType([StructField("MMSI",StringType(),True),StructField("BaseDateTime",StringType(),True),StructField("LAT",DoubleType(),True),StructField("LON",DoubleType(),True),StructField("SOG",DoubleType(),True),StructField("COG",DoubleType(),True),StructField("Heading",DoubleType(),True),StructField("VesselName",StringType(),True),StructField("IMO",StringType(),True),StructField("CallSign",StringType(),True),StructField("VesselType",IntegerType(),True),StructField("Status",IntegerType(),True),StructField("Length",DoubleType(),True),StructField("Width",DoubleType(),True),StructField("Draft",DoubleType(),True),StructField("Cargo",IntegerType(),True),StructField("TransceiverClass",StringType(),True)])
paths = [f"{ROOT}/extracted/ingestion_date={d}/AIS_{d.replace('-', '_')}.csv" for d in DATES]
raw_ais=(spark.read.option("header","true").option("enforceSchema","false").option("mode","FAILFAST").schema(SCHEMA).csv(paths).withColumn("ingestion_date",F.regexp_extract(F.col("_metadata.file_path"),r"ingestion_date=(\d{4}-\d{2}-\d{2})",1)))

# COMMAND ----------

# Conjunto canónico virtual D-07: no persiste ni escribe resultados intermedios.
canonical_ais=raw_ais.dropDuplicates(SCHEMA.fieldNames())
valid_mmsi_ais=canonical_ais.filter(F.col("MMSI").rlike(r"^[0-9]{9}$"))
with_invalid=canonical_ais.groupBy("ingestion_date").agg(F.countDistinct("MMSI").alias("mmsi_distintos_sin_excluir"))
valid_only=valid_mmsi_ais.groupBy("ingestion_date").agg(F.countDistinct("MMSI").alias("mmsi_distintos_validos"))
display(with_invalid.join(valid_only,"ingestion_date").withColumn("efecto_exclusion",F.col("mmsi_distintos_sin_excluir")-F.col("mmsi_distintos_validos")).orderBy("ingestion_date"))

# COMMAND ----------

exact_by_day=valid_mmsi_ais.groupBy("ingestion_date").agg(F.countDistinct("MMSI").alias("count_distinct_exacto"))
approx_by_day=valid_mmsi_ais.groupBy("ingestion_date").agg(F.approx_count_distinct("MMSI").alias("approx_count_distinct"))
comparison=(exact_by_day.join(approx_by_day,"ingestion_date").withColumn("diferencia_absoluta",F.abs(F.col("approx_count_distinct")-F.col("count_distinct_exacto"))).withColumn("diferencia_porcentaje",F.round(100*F.col("diferencia_absoluta")/F.col("count_distinct_exacto"),4)).orderBy("ingestion_date"))
display(comparison)

# COMMAND ----------

print("=== PLAN countDistinct exacto ===")
exact_by_day.explain("formatted")
print("=== PLAN approx_count_distinct ===")
approx_by_day.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decisión para producción
# MAGIC
# MAGIC Se usa `countDistinct`: la cardinalidad diaria ronda 20 mil MMSI y el resultado exacto es más defendible. Ambos planes leen el mismo volumen base; el menor estado de HyperLogLog no compensa introducir error en un cálculo diario de cardinalidad baja. `approx_count_distinct` queda como alternativa para una escala mayor o un SLA medido.

# COMMAND ----------

# MAGIC %md
# MAGIC ## (b) ¿Qué tipos de buque generan más tráfico?
# MAGIC
# MAGIC El catálogo proviene de NOAA Marine Cadastre, `VesselTypeCodes2018.pdf`, y se contrasta con la guía AIS USCG basada en ITU-R M.1371. D-07 no filtra MMSI aquí porque la métrica agrega posiciones por tipo, sin identificar ni agrupar buques individuales. La deduplicación exacta se mantiene como regla transversal.

# COMMAND ----------

VESSEL_TYPE_CATALOG = [
    (30, "Fishing"), (31, "Towing"), (32, "Towing: tow >200m or breadth >25m"),
    (33, "Dredging or underwater operations"), (34, "Diving operations"),
    (35, "Military operations"), (36, "Sailing"), (37, "Pleasure craft"),
    (40, "High speed craft"), (50, "Pilot vessel"), (51, "Search and rescue"),
    (52, "Tug"), (53, "Port tender"), (54, "Anti-pollution equipment"),
    (55, "Law enforcement"), (56, "Local vessel assignment (spare)"),
    (57, "Local vessel assignment (spare)"), (58, "Medical transport"),
    (59, "Noncombatant ship"), (60, "Passenger"), (70, "Cargo"),
    (71, "Cargo: hazard A"), (72, "Cargo: hazard B"), (73, "Cargo: hazard C"),
    (74, "Cargo: hazard D"), (79, "Cargo: no additional information"),
    (80, "Tanker"), (81, "Tanker: hazard A"), (82, "Tanker: hazard B"),
    (83, "Tanker: hazard C"), (84, "Tanker: hazard D"), (89, "Tanker: no additional information"),
    (90, "Other type"), (99, "Other type: no additional information"),
]
vessel_type_catalog = spark.createDataFrame(VESSEL_TYPE_CATALOG, "VesselType int, vessel_type_description string")

# COMMAND ----------

# SOG=102.3 es “no disponible”; se muestra con y sin excluirlo. Null SOG se ignora por avg.
traffic_by_type = (canonical_ais.groupBy("VesselType").agg(
    F.count("*").alias("posiciones"),
    F.avg("SOG").alias("sog_media_con_sentinela"),
    F.avg(F.when(F.col("SOG") != 102.3, F.col("SOG"))).alias("sog_media_sin_sentinela"),
    F.sum((F.col("SOG") == 102.3).cast("long")).alias("registros_sog_102_3"),
).join(vessel_type_catalog, "VesselType", "left")
 .withColumn("vessel_type_description", F.coalesce("vessel_type_description", F.lit("No disponible/reservado/no catalogado")))
 .withColumn("efecto_sentinela_nudos", F.round(F.col("sog_media_con_sentinela") - F.col("sog_media_sin_sentinela"), 4))
 .orderBy(F.desc("posiciones")))
display(traffic_by_type.limit(10))

# COMMAND ----------

print("=== PLAN 03b: tráfico y velocidad por VesselType ===")
traffic_by_type.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC **Costo.** Como en 03a, el plan escanea los siete CSV y hace la deduplicación global D-07. Luego agrega por `VesselType`, de cardinalidad baja, sin requerir el shuffle adicional de pares `(día, MMSI)` de `countDistinct` exacto. `SOG=102.3` se excluye del promedio, no del conteo de posiciones.

# COMMAND ----------

# MAGIC %md
# MAGIC ## (c) Diez buques con mayor distancia semanal
# MAGIC
# MAGIC Esta sección reutiliza D-04: MMSI válido, timestamp y coordenadas calculables, `0 < gap <= 2 h` y velocidad implícita `<= 60 kn`. D-07 aplica completo porque la métrica agrupa por MMSI.

# COMMAND ----------

from pyspark.sql.window import Window

# D-07 ya eliminó copias exactas y MMSI no conformes antes de crear trayectorias.
events = valid_mmsi_ais.select("MMSI", "VesselName", "VesselType", "LAT", "LON", F.to_timestamp("BaseDateTime", "yyyy-MM-dd'T'HH:mm:ss").alias("event_ts"))
window_mmsi = Window.partitionBy("MMSI").orderBy("event_ts")
pairs = (events.withColumn("prev_ts", F.lag("event_ts").over(window_mmsi))
    .withColumn("prev_lat", F.lag("LAT").over(window_mmsi))
    .withColumn("prev_lon", F.lag("LON").over(window_mmsi))
    .filter(F.col("prev_ts").isNotNull())
    .withColumn("gap_hours", (F.col("event_ts").cast("long") - F.col("prev_ts").cast("long")) / F.lit(3600.0))
    .withColumn("a", F.pow(F.sin((F.radians("LAT") - F.radians("prev_lat")) / 2), 2) + F.cos(F.radians("prev_lat")) * F.cos(F.radians("LAT")) * F.pow(F.sin((F.radians("LON") - F.radians("prev_lon")) / 2), 2))
    .withColumn("distance_km", F.lit(6371.0088) * 2 * F.asin(F.sqrt("a")))
    .withColumn("implied_knots", F.col("distance_km") / F.col("gap_hours") / F.lit(1.852))
    .withColumn("d04_eligible", (F.col("gap_hours") > 0) & (F.col("gap_hours") <= 2) & (F.col("implied_knots") <= 60)))

# Los pares no elegibles se retienen como auditoría, pero no suman distancia.
distance_by_mmsi = pairs.groupBy("MMSI").agg(
    F.sum(F.when(F.col("d04_eligible"), F.col("distance_km")).otherwise(F.lit(0.0))).alias("distance_km"),
    F.sum(F.col("d04_eligible").cast("long")).alias("pares_elegibles"),
    F.sum((~F.col("d04_eligible")).cast("long")).alias("pares_excluidos"),
    F.sum(F.when(F.col("d04_eligible"), F.col("gap_hours")).otherwise(F.lit(0.0))).alias("horas_elegibles"),
)
vessel_info = valid_mmsi_ais.groupBy("MMSI").agg(
    F.first("VesselName", ignorenulls=True).alias("VesselName"),
    F.first("VesselType", ignorenulls=True).alias("VesselType"),
    F.countDistinct("VesselName").alias("nombres_distintos"),
    F.countDistinct("VesselType").alias("tipos_distintos"),
)
top10_distance = (distance_by_mmsi.join(vessel_info, "MMSI", "left")
    .withColumn("velocidad_media_implicita_kn", F.col("distance_km") / F.col("horas_elegibles") / F.lit(1.852))
    .withColumn("velocidad_semana_calendario_kn", F.col("distance_km") / F.lit(168 * 1.852))
    .orderBy(F.desc("distance_km")).limit(10))
display(top10_distance.select("MMSI", "VesselName", "VesselType", F.round("distance_km", 2).alias("distance_km"), F.round("velocidad_media_implicita_kn", 2).alias("velocidad_media_implicita_kn"), F.round("velocidad_semana_calendario_kn", 2).alias("velocidad_semana_calendario_kn"), "pares_elegibles", "pares_excluidos", "nombres_distintos", "tipos_distintos"))

# COMMAND ----------

display(pairs.agg(F.count("*").alias("pares_totales_D07"), F.sum(F.col("d04_eligible").cast("long")).alias("pares_elegibles_D04"), F.sum((~F.col("d04_eligible")).cast("long")).alias("pares_excluidos_D04")))
display(events.groupBy("MMSI").count().agg(F.count("*").alias("mmsi"), F.expr("percentile_approx(count, array(0.5, 0.95, 0.99))").alias("p50_p95_p99_posiciones"), F.max("count").alias("max_posiciones")))
print("=== PLAN 03c: ventana por MMSI ===")
top10_distance.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (d) ¿Dónde se concentra el tráfico?
# MAGIC
# MAGIC D-07 elimina duplicados exactos. Los MMSI no conformes no se filtran porque se cuentan posiciones por celda, no buques individuales. H3 se calcula con funciones nativas de Databricks y conserva el plan Photon.

# COMMAND ----------

import hashlib
import os
import requests

WPI_URL = "https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv"
WPI_VOLUME = "/Volumes/oceanwatch_g07/reference/world_port_index"
WPI_PATH = f"{WPI_VOLUME}/UpdatedPub150.csv"
spark.sql("CREATE VOLUME IF NOT EXISTS oceanwatch_g07.reference.world_port_index COMMENT 'World Port Index (NGA Pub. 150); referencia para asociación de celdas H3 a puertos.'")
os.makedirs(WPI_VOLUME, exist_ok=True)
if not os.path.exists(WPI_PATH):
    response = requests.get(WPI_URL, timeout=120)
    response.raise_for_status()
    with open(WPI_PATH, "wb") as output:
        output.write(response.content)
with open(WPI_PATH, "rb") as source:
    print(f"WPI bytes={os.path.getsize(WPI_PATH)}; sha256={hashlib.sha256(source.read()).hexdigest()}")

ports = (spark.read.option("header", "true").option("multiLine", "true").option("escape", '\"').csv(WPI_PATH)
    .select(F.col("World Port Index Number").cast("string").alias("wpi_id"), F.col("Main Port Name").alias("port_name"), F.col("Country Code").alias("country"), F.col("Latitude").cast("double").alias("port_lat"), F.col("Longitude").cast("double").alias("port_lon"))
    .filter(F.col("port_lat").isNotNull() & F.col("port_lon").isNotNull()))
print(f"Puertos WPI con coordenadas: {ports.count()}")

# COMMAND ----------

h3_positions = canonical_ais.select(F.expr("h3_longlatash3(LON, LAT, 8)").alias("h3_r8"))
top_h3 = h3_positions.groupBy("h3_r8").count().withColumnRenamed("count", "posiciones").orderBy(F.desc("posiciones")).limit(10)
top_cells = (top_h3.withColumn("h3_hex", F.expr("h3_h3tostring(h3_r8)"))
    .withColumn("center_wkt", F.expr("h3_centeraswkt(h3_r8)"))
    .withColumn("center_lon", F.regexp_extract("center_wkt", r"POINT\\s*\\(([-0-9.]+)\\s+[-0-9.]+\\)", 1).cast("double"))
    .withColumn("center_lat", F.regexp_extract("center_wkt", r"POINT\\s*\\([-0-9.]+\\s+([-0-9.]+)\\)", 1).cast("double")))

# Solo se cruzan 10 centroides con la referencia pequeña WPI; el broadcast se verifica en el plan.
port_candidates = (top_cells.crossJoin(F.broadcast(ports))
    .withColumn("a", F.pow(F.sin((F.radians("port_lat") - F.radians("center_lat")) / 2), 2) + F.cos(F.radians("center_lat")) * F.cos(F.radians("port_lat")) * F.pow(F.sin((F.radians("port_lon") - F.radians("center_lon")) / 2), 2))
    .withColumn("distancia_puerto_km", F.lit(12742.0176) * F.asin(F.sqrt("a")))
    .withColumn("puerto_etiqueta", F.concat_ws(" | ", "port_name", "country", "wpi_id")))

top_cells_with_ports = (port_candidates.groupBy("h3_r8", "h3_hex", "posiciones", "center_lat", "center_lon").agg(
    F.round(F.min("distancia_puerto_km"), 3).alias("puerto_mas_cercano_km"),
    F.sort_array(F.collect_list(F.when(F.col("distancia_puerto_km") <= 1, F.col("puerto_etiqueta")))).alias("puertos_1km"),
    F.sort_array(F.collect_list(F.when(F.col("distancia_puerto_km") <= 2, F.col("puerto_etiqueta")))).alias("puertos_2km"),
    F.sort_array(F.collect_list(F.when(F.col("distancia_puerto_km") <= 5, F.col("puerto_etiqueta")))).alias("puertos_5km"),
).withColumn("asociada_1km", F.size("puertos_1km") > 0)
 .withColumn("asociada_2km", F.size("puertos_2km") > 0)
 .withColumn("asociada_5km", F.size("puertos_5km") > 0)
 .orderBy(F.desc("posiciones")))
display(top_cells_with_ports)

# COMMAND ----------

sensitivity_d03 = top_cells_with_ports.agg(
    F.sum(F.col("asociada_1km").cast("int")).alias("celdas_asociadas_1km"),
    F.sum(F.col("asociada_2km").cast("int")).alias("celdas_asociadas_2km"),
    F.sum(F.col("asociada_5km").cast("int")).alias("celdas_asociadas_5km"),
    F.sum(F.when(F.col("asociada_1km"), F.col("posiciones")).otherwise(0)).alias("posiciones_1km"),
    F.sum(F.when(F.col("asociada_2km"), F.col("posiciones")).otherwise(0)).alias("posiciones_2km"),
    F.sum(F.when(F.col("asociada_5km"), F.col("posiciones")).otherwise(0)).alias("posiciones_5km"),
    F.sum((F.col("asociada_1km") != F.col("asociada_5km")).cast("int")).alias("celdas_que_cambian_1_a_5km"),
    F.sum((F.col("asociada_2km") != F.col("asociada_5km")).cast("int")).alias("celdas_que_cambian_2_a_5km"),
)
display(sensitivity_d03)
print("=== PLAN 03d: H3 nativo y WPI broadcast ===")
sensitivity_d03.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (e) ¿Qué proporción transmitió los siete días y dónde están los visitantes?
# MAGIC
# MAGIC D-07 aplica completo porque la pregunta agrupa por MMSI. No se usa `cache()` ni `persist()`.

# COMMAND ----------

# Se usa una fila por MMSI válido. first(VesselType) permite caracterizar visitantes y tipos_distintos deja trazabilidad de mensajes inconsistentes.
mmsi_day_profile = valid_mmsi_ais.groupBy("MMSI").agg(
    F.countDistinct("ingestion_date").alias("dias_transmitidos"),
    F.min("ingestion_date").alias("dia_unico"),
    F.first("VesselType", ignorenulls=True).alias("VesselType"),
    F.countDistinct("VesselType").alias("tipos_distintos"),
)
distribution_days = mmsi_day_profile.groupBy("dias_transmitidos").count().withColumnRenamed("count", "buques").orderBy("dias_transmitidos")
total_valid_week = mmsi_day_profile.count()
seven_day_vessels = mmsi_day_profile.filter(F.col("dias_transmitidos") == 7).count()
display(distribution_days.withColumn("porcentaje", F.round(100 * F.col("buques") / F.lit(total_valid_week), 4)))
print(f"MMSI válidos={total_valid_week}; 7 días={seven_day_vessels}; proporción={100 * seven_day_vessels / total_valid_week:.4f}%")

# COMMAND ----------

one_day_visitors = mmsi_day_profile.filter(F.col("dias_transmitidos") == 1)
total_one_day = one_day_visitors.count()
visitor_types = (one_day_visitors.groupBy("VesselType").count().withColumnRenamed("count", "buques")
    .withColumn("porcentaje_visitantes", F.round(100 * F.col("buques") / F.lit(total_one_day), 4))
    .orderBy(F.desc("buques")))
visitor_days = (one_day_visitors.groupBy("dia_unico").count().withColumnRenamed("count", "buques")
    .withColumn("porcentaje_visitantes", F.round(100 * F.col("buques") / F.lit(total_one_day), 4))
    .orderBy(F.desc("buques")))
display(visitor_types)
display(visitor_days)

# COMMAND ----------

# Control explícito: cada MMSI aparece exactamente en una categoría de días transmitidos.
sanity_03e = distribution_days.agg(F.sum("buques").alias("suma_distribucion")).withColumn("mmsi_validos_semana", F.lit(total_valid_week))
display(sanity_03e.withColumn("control_ok", F.col("suma_distribucion") == F.col("mmsi_validos_semana")))
print("=== PLAN 03e: agregación inversa a 03a (por MMSI, luego por días) ===")
distribution_days.explain("formatted")
