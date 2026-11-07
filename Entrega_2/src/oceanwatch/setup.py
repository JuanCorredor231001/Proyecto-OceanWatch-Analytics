"""Creación idempotente de esquemas, Volumes, tablas (DDL explícito) y tablas de referencia."""

from __future__ import annotations

from pathlib import Path

from .contract import gold_ddl, gold_tables
from .lakehouse import SCHEMAS, Lakehouse
from .schemas import (
    BRONZE_COLUMNS_DDL,
    FEED_CALENDAR_COLUMNS,
    PIPELINE_RUNS_COLUMNS,
    QUALITY_METRICS_COLUMNS,
    QUARANTINE_COLUMNS,
    SILVER_COLUMNS,
    create_table_ddl,
)

FEATURE_COLUMNS = [
    ("event_date", "date", "Día UTC."), ("mmsi", "string", "MMSI."), ("vessel_name", "string", "Nombre."),
    ("vessel_type_group", "string", "Grupo de negocio."), ("lat_ultima", "double", "Latitud final."),
    ("lon_ultima", "double", "Longitud final."),
    ("n_posiciones", "double", "Posiciones válidas del día."), ("sog_media_kn", "double", "SOG medio."),
    ("sog_std_kn", "double", "Desviación estándar de SOG."), ("sog_p95_kn", "double", "Percentil 95 de SOG."),
    ("sog_max_kn", "double", "SOG máximo."), ("frac_sog_mayor_25kn", "double", "Fracción de posiciones con SOG > 25 kn."),
    ("velocidad_implicita_max_kn", "double", "Máxima velocidad implícita D-04."),
    ("distancia_km", "double", "Distancia D-04."), ("frac_pares_excluidos_d04", "double", "Fracción de pares excluidos por D-04."),
    ("length_m", "double", "Eslora."), ("_feature_processed_at", "timestamp", "Instante de cálculo."),
]

TABLES = {
    ("bronze", "ais_raw"): dict(
        columns=BRONZE_COLUMNS_DDL, partition_by="ingestion_date",
        comment="Bronce: AIS crudo e inmutable tal como llegó del feed diario NOAA (todas las columnas como texto). Append-only.",
        properties={"delta.appendOnly": "true", "oceanwatch.capa": "bronce"}),
    ("silver", "ais_posiciones"): dict(
        columns=SILVER_COLUMNS, partition_by="event_date",
        comment="Plata: posiciones AIS que pasaron todas las compuertas CUARENTENA, deduplicadas por (mmsi, event_ts).",
        properties={"oceanwatch.capa": "plata"}),
    ("silver", "ais_cuarentena"): dict(
        columns=QUARANTINE_COLUMNS, partition_by="ingestion_date",
        comment="Plata: registros rechazados por compuertas de calidad o duplicados, con la causa documentada.",
        properties={"delta.appendOnly": "true", "oceanwatch.capa": "plata"}),
    ("silver", "metricas_calidad"): dict(
        columns=QUALITY_METRICS_COLUMNS, partition_by=None,
        comment="Plata: métricas de calidad por micro-lote, fecha y regla (formato largo).",
        properties={"delta.appendOnly": "true", "oceanwatch.capa": "plata"}),
    ("ml", "features_velocidad_buque_dia"): dict(
        columns=FEATURE_COLUMNS, partition_by="event_date",
        comment="ML: características de velocidad por buque y día para el detector de anomalías.",
        properties={"oceanwatch.capa": "ml"}),
    ("ops", "pipeline_runs"): dict(
        columns=PIPELINE_RUNS_COLUMNS, partition_by=None,
        comment="Operación: bitácora de tareas del job (intentos, estado, duración, métricas).",
        properties={"delta.appendOnly": "true"}),
    ("ops", "feed_calendar"): dict(
        columns=FEED_CALENDAR_COLUMNS, partition_by=None,
        comment="Operación: días del feed NOAA y su rol (HISTORICO o FEED).",
        properties={}),
}

VOLUMES = {
    ("landing", "raw_ais"): "Archivos AIS NOAA: downloads/ (ZIP) y feed/<fecha>/ (CSV que dispara el stream bronce).",
    ("ops", "checkpoints"): "Checkpoints de Structured Streaming (bronce y plata).",
    ("reference", "files"): "Archivos de referencia: WPI, Natural Earth.",
}


def create_all(lh: Lakehouse) -> list[str]:
    created = []
    if not lh.is_local:
        lh.sql(f"CREATE CATALOG IF NOT EXISTS {lh.catalog} COMMENT 'Lakehouse OceanWatch Analytics para tráfico marítimo AIS de NOAA.'")
        for schema, comment in SCHEMAS.items():
            lh.sql(f"CREATE SCHEMA IF NOT EXISTS {lh.catalog}.{lh.schema_names[schema]} COMMENT '{comment}'")
        for (schema, volume), comment in VOLUMES.items():
            lh.sql(f"CREATE VOLUME IF NOT EXISTS {lh.catalog}.{lh.schema_names[schema]}.{volume} COMMENT '{comment}'")
    else:
        for (schema, volume) in VOLUMES:
            Path(lh.volume_dir(schema, volume)).mkdir(parents=True, exist_ok=True)
        Path(lh.landing_dir()).mkdir(parents=True, exist_ok=True)

    for (schema, table), spec in TABLES.items():
        lh.sql(create_table_ddl(lh.name(schema, table), spec["columns"], spec["comment"],
                                spec["partition_by"], spec["properties"]))
        created.append(f"{schema}.{table}")
    for table in gold_tables():
        lh.sql(gold_ddl(lh.name("gold", table), table))
        created.append(f"gold.{table}")
    return created


def build_reference_tables(lh: Lakehouse, ports_source: str, natural_earth_dir: str,
                           port_radius_km: float = 2.0) -> dict:
    """Publica catálogo VesselType, puertos, celdas portuarias H3 r8 y máscara de tierra H3 r5."""
    from .reference import land_cells, load_ports, port_cells, vessel_type_frame

    spark = lh.spark
    ports = load_ports(ports_source)
    cells = port_cells(ports, radius_km=port_radius_km)
    land = land_cells(natural_earth_dir)
    ports = ports.astype({"country_code": "object"}).where(ports.notna(), None)
    frames = {
        "vessel_type_catalog": spark.createDataFrame(vessel_type_frame(), "vessel_type int, descripcion string, grupo string"),
        "ports": spark.createDataFrame(ports, "port_id string, port_name string, country_code string, lat double, lon double, fuente string"),
        "port_cells_h3r8": spark.createDataFrame(cells, "h3_r8 string, port_id string, port_name string, distancia_km double"),
        "land_cells_h3r5": spark.createDataFrame(land, "h3_r5 string"),
    }
    counts = {}
    for name, df in frames.items():
        lh.overwrite(df, "reference", name)
        counts[name] = lh.read("reference", name).count()
    return counts


def build_waterway_exceptions(lh: Lakehouse, fechas: list[str], min_vessels: int = 2) -> dict:
    """Celdas de la máscara de tierra con tráfico recurrente en el histórico => vías navegables.

    Natural Earth 1:10m no contiene la red navegable interior fina (p. ej. Gulf Intracoastal
    Waterway, Atchafalaya). Una celda "de tierra" donde transmitieron >= `min_vessels` MMSI
    válidos distintos durante el corpus histórico se reclasifica como agua (D-E2-04). Un error
    de GPS aislado rara vez se repite en la misma celda para buques distintos.
    """
    from pyspark.sql import functions as F

    from .geo import H3_RES_LAND, h3_cell

    land = lh.read("reference", "land_cells_h3r5")
    raw = (lh.read("bronze", "ais_raw")
           .filter(F.col("ingestion_date").cast("string").isin(list(fechas)))
           .filter(F.trim("MMSI").rlike(r"^[0-9]{9}$"))
           .select(F.trim("MMSI").alias("mmsi"),
                   F.expr("try_cast(LAT AS DOUBLE)").alias("lat"), F.expr("try_cast(LON AS DOUBLE)").alias("lon"))
           .withColumn("h3_r5", h3_cell("lat", "lon", H3_RES_LAND)))
    traffic = (raw.join(F.broadcast(land), "h3_r5")
               .groupBy("h3_r5").agg(F.countDistinct("mmsi").alias("buques_distintos"), F.count(F.lit(1)).alias("posiciones")))
    exceptions = traffic.filter(F.col("buques_distintos") >= min_vessels).withColumn(
        "criterio", F.lit(f">= {min_vessels} MMSI distintos en {min(fechas)}..{max(fechas)}"))
    lh.overwrite(exceptions, "reference", "waterway_cells_h3r5")
    summary = traffic.agg(F.count(F.lit(1)).alias("celdas_tierra_con_trafico"),
                          F.sum(F.when(F.col("buques_distintos") >= min_vessels, 1).otherwise(0)).alias("celdas_reclasificadas"),
                          F.sum("posiciones").alias("posiciones_en_tierra"),
                          F.sum(F.when(F.col("buques_distintos") >= min_vessels, F.col("posiciones")).otherwise(0)).alias("posiciones_reclasificadas"),
                          ).first().asDict()
    return summary


def effective_land_cells(lh: Lakehouse):
    """Máscara usada por la compuerta E06: tierra Natural Earth menos excepciones navegables."""
    land = lh.read("reference", "land_cells_h3r5")
    if lh.exists("reference", "waterway_cells_h3r5"):
        land = land.join(lh.read("reference", "waterway_cells_h3r5").select("h3_r5"), "h3_r5", "left_anti")
    return land
