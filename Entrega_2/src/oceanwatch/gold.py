"""Capa oro: modelo de negocio de OceanWatch, construido EXCLUSIVAMENTE desde plata.

Cada tabla se recalcula por fecha (`replaceWhere event_date = fecha`), así que re-ejecutar un
día es idempotente. Un registro en cuarentena nunca está en plata y, por lo tanto, nunca en oro.

Tablas (contrato en contracts/gold_contract.yml):
- trayectorias_diarias_buque
- estadias_puerto (presencia en puertos)
- puertos_resumen_diario (ranking y buques en puerto)
- trafico_celda_hora (matriz de tráfico por celda H3 r8 y hora)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .geo import KM_PER_NAUTICAL_MILE, h3_center, haversine_km
from .lakehouse import Lakehouse

# D-04 (Entrega 1): un par consecutivo suma distancia si 0 < gap <= 2 h y velocidad implícita <= 60 kn.
D04_MAX_GAP_HOURS = 2.0
D04_MAX_IMPLIED_KN = 60.0
# Estadía en puerto: posiciones del buque en celdas del mismo puerto; se abre una estadía nueva
# solo si el buque estuvo más de 1 h sin reportar desde ese puerto (evita que el "parpadeo" en el
# borde de las celdas portuarias infle los arribos).
STAY_MAX_GAP_HOURS = 1.0
BERTHED_MEDIAN_SOG_KN = 1.0
MAX_PATH_POINTS = 300


def silver_day(lh: Lakehouse, fecha: str) -> DataFrame:
    return lh.read("silver", "ais_posiciones").filter(F.col("event_date") == F.lit(fecha).cast("date"))


def positions_with_pairs(positions: DataFrame, port_cells: DataFrame) -> DataFrame:
    """Posiciones del día ordenadas por buque con el par consecutivo (D-04) y el puerto de la celda."""
    ports = port_cells.select("h3_r8", "port_id", "port_name")
    w = Window.partitionBy("mmsi").orderBy("event_ts")
    pos = positions.join(F.broadcast(ports), "h3_r8", "left")
    pos = (
        pos.withColumn("prev_ts", F.lag("event_ts").over(w))
        .withColumn("prev_lat", F.lag("lat").over(w))
        .withColumn("prev_lon", F.lag("lon").over(w))
        .withColumn("prev_port", F.lag("port_id").over(w))
        .withColumn("gap_h", (F.col("event_ts").cast("long") - F.col("prev_ts").cast("long")) / 3600.0)
        .withColumn("dist_km", haversine_km(F.col("prev_lat"), F.col("prev_lon"), F.col("lat"), F.col("lon")))
        .withColumn("implied_kn", F.when(F.col("gap_h") > 0, F.col("dist_km") / F.col("gap_h") / KM_PER_NAUTICAL_MILE))
        .withColumn("d04_ok", F.coalesce((F.col("gap_h") > 0) & (F.col("gap_h") <= D04_MAX_GAP_HOURS)
                                         & (F.col("implied_kn") <= D04_MAX_IMPLIED_KN), F.lit(False)))
        .withColumn("seq", F.row_number().over(w))
        .withColumn("n_dia", F.count(F.lit(1)).over(Window.partitionBy("mmsi")))
    )
    return pos


def trajectories(pos: DataFrame, fecha: str) -> DataFrame:
    has_prev = F.col("prev_ts").isNotNull()
    # Trayectoria simplificada: como máximo MAX_PATH_POINTS puntos equiespaciados + el último.
    path_points = F.expr(
        f"""
        transform(
          filter(_all, (p, i) -> i % greatest(1, cast(ceil(size(_all) / {MAX_PATH_POINTS}.0) AS INT)) = 0
                                 OR i = size(_all) - 1),
          p -> concat(cast(round(p.lon, 5) AS STRING), ' ', cast(round(p.lat, 5) AS STRING)))
        """
    )
    agg = pos.groupBy("mmsi").agg(
        F.max_by("vessel_name", "event_ts").alias("vessel_name"),
        F.max_by("vessel_type", "event_ts").alias("vessel_type"),
        F.max_by("vessel_type_group", "event_ts").alias("vessel_type_group"),
        F.max("length_m").alias("length_m"),
        F.count(F.lit(1)).alias("n_posiciones"),
        F.min("event_ts").alias("primera_posicion_ts"),
        F.max("event_ts").alias("ultima_posicion_ts"),
        F.round(F.sum(F.when(F.col("d04_ok"), F.col("dist_km")).otherwise(0.0)), 3).alias("distancia_km"),
        F.round(F.sum(F.when(F.col("d04_ok"), F.col("gap_h")).otherwise(0.0)), 3).alias("horas_cubiertas"),
        F.sum((has_prev & F.col("d04_ok")).cast("int")).alias("pares_validos_d04"),
        F.sum((has_prev & ~F.col("d04_ok")).cast("int")).alias("pares_excluidos_d04"),
        F.round(F.avg("sog"), 3).alias("sog_media_kn"),
        F.round(F.max("sog"), 3).alias("sog_max_kn"),
        F.round(F.max(F.when(F.col("d04_ok"), F.col("implied_kn"))), 3).alias("velocidad_implicita_max_kn"),
        F.min_by("lat", "event_ts").alias("lat_inicio"),
        F.min_by("lon", "event_ts").alias("lon_inicio"),
        F.max_by("lat", "event_ts").alias("lat_fin"),
        F.max_by("lon", "event_ts").alias("lon_fin"),
        F.countDistinct("h3_r8").alias("celdas_h3_visitadas"),
        F.array_sort(F.collect_list(F.struct("event_ts", "lon", "lat"))).alias("_all"),
    ).withColumn("_pts", path_points).drop("_all")
    wkt = F.when(F.size("_pts") >= 2, F.concat(F.lit("LINESTRING("), F.array_join("_pts", ", "), F.lit(")"))) \
        .otherwise(F.concat(F.lit("POINT("), F.element_at("_pts", 1), F.lit(")")))
    return (
        agg.withColumn("trayectoria_wkt", wkt).drop("_pts")
        .withColumn("event_date", F.lit(fecha).cast("date"))
        .withColumn("_gold_processed_at", F.current_timestamp())
    )


def port_stays(pos: DataFrame, fecha: str) -> DataFrame:
    """Sesiones de presencia en puerto: posiciones en el mismo puerto sin ausencias > 1 h."""
    w = Window.partitionBy("mmsi", "port_id").orderBy("event_ts")
    in_port = pos.filter(F.col("port_id").isNotNull())
    prev_in = F.lag("event_ts").over(w)
    new_stay = (prev_in.isNull()
                | ((F.col("event_ts").cast("long") - prev_in.cast("long")) / 3600.0 > STAY_MAX_GAP_HOURS)).cast("int")
    stays = (
        in_port.withColumn("_new", new_stay)
        .withColumn("stay_seq", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0)))
        .groupBy("mmsi", "stay_seq", "port_id")
        .agg(
            F.first("port_name").alias("port_name"),
            F.max_by("vessel_name", "event_ts").alias("vessel_name"),
            F.max_by("vessel_type_group", "event_ts").alias("vessel_type_group"),
            F.min("event_ts").alias("arribo_ts"),
            F.max("event_ts").alias("salida_ts"),
            F.count(F.lit(1)).alias("n_posiciones"),
            F.round(F.avg("sog"), 3).alias("sog_media_kn"),
            F.percentile_approx("sog", 0.5).alias("sog_mediana_kn"),
            F.min("seq").alias("_min_seq"),
            F.max("seq").alias("_max_seq"),
            F.first("n_dia").alias("_n_dia"),
        )
    )
    return (
        stays.withColumn("duracion_horas", F.round((F.col("salida_ts").cast("long") - F.col("arribo_ts").cast("long")) / 3600.0, 3))
        .withColumn("estado", F.when(F.col("sog_mediana_kn") < BERTHED_MEDIAN_SOG_KN, "ATRACADO_O_FONDEADO").otherwise("EN_TRANSITO"))
        .withColumn("inicia_dia_en_puerto", F.col("_min_seq") == 1)
        .withColumn("en_puerto_al_cierre", F.col("_max_seq") == F.col("_n_dia"))
        .withColumn("stay_id", F.concat_ws("-", F.lit(fecha), "mmsi", "port_id", "stay_seq"))
        .withColumn("event_date", F.lit(fecha).cast("date"))
        .withColumn("_gold_processed_at", F.current_timestamp())
        .drop("_min_seq", "_max_seq", "_n_dia", "stay_seq")
    )


def port_daily_summary(stays: DataFrame, ports: DataFrame, fecha: str) -> DataFrame:
    summary = stays.groupBy("port_id").agg(
        F.first("port_name").alias("port_name"),
        F.countDistinct("mmsi").alias("buques_distintos"),
        F.count(F.lit(1)).alias("estadias"),
        F.sum((~F.col("inicia_dia_en_puerto")).cast("int")).alias("arribos"),
        F.countDistinct(F.when(F.col("estado") == "ATRACADO_O_FONDEADO", F.col("mmsi"))).alias("buques_atracados"),
        F.countDistinct(F.when(F.col("en_puerto_al_cierre"), F.col("mmsi"))).alias("buques_en_puerto_al_cierre"),
        F.round(F.sum("duracion_horas"), 3).alias("horas_buque_totales"),
        F.round(F.avg("duracion_horas"), 3).alias("duracion_media_horas"),
        F.countDistinct(F.when(F.col("vessel_type_group") == "Carga", F.col("mmsi"))).alias("buques_carga"),
        F.countDistinct(F.when(F.col("vessel_type_group") == "Tanquero", F.col("mmsi"))).alias("buques_tanquero"),
        F.countDistinct(F.when(F.col("vessel_type_group") == "Pasajeros", F.col("mmsi"))).alias("buques_pasajeros"),
    )
    rank_w = Window.orderBy(F.desc("buques_distintos"), F.desc("horas_buque_totales"), F.asc("port_id"))
    return (
        summary.join(ports.select("port_id", "country_code", F.col("lat").alias("port_lat"), F.col("lon").alias("port_lon")), "port_id", "left")
        .withColumn("ranking", F.row_number().over(rank_w))
        .withColumn("event_date", F.lit(fecha).cast("date"))
        .withColumn("_gold_processed_at", F.current_timestamp())
    )


def traffic_cell_hour(positions: DataFrame, fecha: str) -> DataFrame:
    groups = ["Carga", "Tanquero", "Pasajeros", "Pesca", "Remolque", "Recreo"]
    aggs = [
        F.count(F.lit(1)).alias("posiciones"),
        F.countDistinct("mmsi").alias("buques_distintos"),
        F.round(F.avg("sog"), 3).alias("sog_media_kn"),
    ]
    aggs += [F.countDistinct(F.when(F.col("vessel_type_group") == g, F.col("mmsi"))).alias(f"buques_{g.lower()}") for g in groups]
    aggs.append(F.countDistinct(F.when(~F.col("vessel_type_group").isin(groups), F.col("mmsi"))).alias("buques_otros"))
    cells = positions.groupBy("h3_r8", F.date_trunc("hour", "event_ts").alias("hora_ts")).agg(*aggs)
    centers = cells.select("h3_r8").distinct().withColumn("_c", h3_center("h3_r8")).select(
        "h3_r8", F.col("_c.lat").alias("centro_lat"), F.col("_c.lon").alias("centro_lon"))
    return (
        cells.join(centers, "h3_r8")
        .withColumn("hora", F.hour("hora_ts"))
        .withColumn("event_date", F.lit(fecha).cast("date"))
        .withColumn("_gold_processed_at", F.current_timestamp())
    )


def build_gold_for_date(lh: Lakehouse, fecha: str) -> dict:
    """Construye las cuatro tablas oro de una fecha. Devuelve conteos para el monitoreo."""
    from .contract import conform

    positions = silver_day(lh, fecha)
    port_cells = lh.read("reference", "port_cells_h3r8")
    ports = lh.read("reference", "ports")
    pos = positions_with_pairs(positions, port_cells)
    predicate = f"event_date = '{fecha}'"

    tables = {
        "trayectorias_diarias_buque": trajectories(pos, fecha),
        "estadias_puerto": port_stays(pos, fecha),
        "trafico_celda_hora": traffic_cell_hour(positions, fecha),
    }
    counts = {}
    for name, df in tables.items():
        lh.replace_partition(conform(df, name), "gold", name, predicate)
        counts[name] = lh.read("gold", name).filter(F.col("event_date") == F.lit(fecha).cast("date")).count()

    stays = lh.read("gold", "estadias_puerto").filter(F.col("event_date") == F.lit(fecha).cast("date"))
    lh.replace_partition(conform(port_daily_summary(stays, ports, fecha), "puertos_resumen_diario"),
                         "gold", "puertos_resumen_diario", predicate)
    counts["puertos_resumen_diario"] = (
        lh.read("gold", "puertos_resumen_diario").filter(F.col("event_date") == F.lit(fecha).cast("date")).count())
    counts["posiciones_plata"] = positions.count()
    return counts
