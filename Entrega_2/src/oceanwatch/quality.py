"""Compuertas de calidad (expectations) de la capa plata.

Cada regla es una expresión booleana declarativa con severidad:

- CUARENTENA: la fila no pasa a plata (ni, por construcción, a oro). Se envía a
  `silver.ais_cuarentena` con todas las reglas incumplidas.
- ADVERTENCIA: la fila pasa; el campo afectado se anula y la regla queda en `quality_warnings`.
- DEDUPLICACION: segunda llegada de la misma pareja (MMSI, BaseDateTime).

Todas las reglas se cuentan por micro-lote en `silver.metricas_calidad`, sin detener el stream.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .geo import H3_RES_LAND, H3_RES_TRAFFIC, h3_cell
from .reference import VALID_VESSEL_TYPE_CODES

# Umbrales documentados en docs/decisiones (D-E2-03).
SOG_SENTINEL = 102.3
SOG_MAX_PLAUSIBLE_KN = 60.0  # Igual al tope de velocidad implícita D-04 de la Entrega 1.
HEADING_SENTINEL = 511.0
COG_SENTINEL = 360.0
LENGTH_MAX_M = 500.0
WIDTH_MAX_M = 80.0


@dataclass(frozen=True)
class Expectation:
    rule_id: str
    severity: str  # CUARENTENA | ADVERTENCIA
    description: str
    passes: Column  # True si la fila cumple


def _typed(df: DataFrame) -> DataFrame:
    """Conversión tolerante: `try_cast` produce NULL en lugar de fallar el stream."""
    num = lambda c, t="double": F.expr(f"try_cast(trim(`{c}`) AS {t})")  # noqa: E731
    return (
        df.withColumn("_mmsi", F.trim("MMSI"))
        .withColumn("_event_ts", F.expr("try_to_timestamp(trim(BaseDateTime), \"yyyy-MM-dd'T'HH:mm:ss\")"))
        .withColumn("_lat", num("LAT"))
        .withColumn("_lon", num("LON"))
        .withColumn("_sog", num("SOG"))
        .withColumn("_cog", num("COG"))
        .withColumn("_heading", num("Heading"))
        .withColumn("_vessel_type", num("VesselType", "int"))
        .withColumn("_status", num("Status", "int"))
        .withColumn("_length", num("Length"))
        .withColumn("_width", num("Width"))
        .withColumn("_draft", num("Draft"))
        .withColumn("_cargo", num("Cargo", "int"))
        .withColumn("_imo", F.upper(F.trim("IMO")))
    )


def _non_empty(c: str) -> Column:
    return F.col(c).isNotNull() & (F.length(F.trim(F.col(c))) > 0)


def expectations() -> list[Expectation]:
    """Catálogo ordenado de expectativas (el orden define `motivo_principal`)."""
    sog = F.col("_sog")
    return [
        Expectation("E01_MMSI_FORMATO", "CUARENTENA",
                    "MMSI numérico de exactamente 9 dígitos (D-07).",
                    F.coalesce(F.col("_mmsi").rlike(r"^[0-9]{9}$"), F.lit(False))),
        Expectation("E02_TIMESTAMP_VALIDO", "CUARENTENA",
                    "BaseDateTime presente y con formato yyyy-MM-ddTHH:mm:ss.",
                    F.col("_event_ts").isNotNull()),
        Expectation("E03_TIMESTAMP_EN_VENTANA", "CUARENTENA",
                    "La fecha UTC de la posición coincide con el día del archivo del feed.",
                    F.coalesce(F.to_date("_event_ts") == F.col("ingestion_date"), F.lit(False))),
        Expectation("E04_COORDENADAS_PRESENTES", "CUARENTENA",
                    "LAT y LON presentes y numéricas.",
                    F.col("_lat").isNotNull() & F.col("_lon").isNotNull()),
        Expectation("E05_COORDENADAS_EN_RANGO", "CUARENTENA",
                    "LAT en [-90, 90], LON en [-180, 180] y distinta de (0, 0) (isla nula GPS).",
                    F.coalesce(F.col("_lat").between(-90, 90) & F.col("_lon").between(-180, 180)
                               & ~((F.col("_lat") == 0) & (F.col("_lon") == 0)), F.lit(False))),
        Expectation("E06_COORDENADAS_EN_AGUA", "CUARENTENA",
                    "La posición no cae en una celda H3 r5 de tierra firme (máscara Natural Earth erosionada).",
                    F.col("_en_tierra").isNull() | ~F.col("_en_tierra")),
        Expectation("E07_SOG_PLAUSIBLE", "CUARENTENA",
                    f"SOG presente y en [0, {SOG_MAX_PLAUSIBLE_KN:g}] nudos, o el sentinela {SOG_SENTINEL} (no disponible).",
                    F.coalesce(sog.between(0, SOG_MAX_PLAUSIBLE_KN) | (sog == SOG_SENTINEL), F.lit(False))),
        Expectation("E08_COG_RANGO", "ADVERTENCIA",
                    "COG en [0, 360]; 360 es el sentinela no disponible (se anula sin contarse como error).",
                    F.coalesce(F.col("_cog").between(0, COG_SENTINEL), F.lit(False))),
        Expectation("E09_HEADING_RANGO", "ADVERTENCIA",
                    "Heading en [0, 359] o el sentinela 511 no disponible (se anula sin contarse como error).",
                    F.coalesce(F.col("_heading").between(0, 359) | (F.col("_heading") == HEADING_SENTINEL), F.lit(False))),
        Expectation("E10_IMO_FORMATO", "ADVERTENCIA",
                    "IMO ausente o con formato IMOnnnnnnn (7 dígitos).",
                    ~_non_empty("IMO") | F.coalesce(F.col("_imo").rlike(r"^IMO[0-9]{7}$"), F.lit(False))),
        Expectation("E11_VESSELTYPE_CATALOGO", "ADVERTENCIA",
                    "VesselType ausente o perteneciente a los códigos AIS ITU-R M.1371.",
                    F.col("_vessel_type").isNull() | F.col("_vessel_type").isin(VALID_VESSEL_TYPE_CODES)),
        Expectation("E12_DIMENSIONES_PLAUSIBLES", "ADVERTENCIA",
                    f"Eslora en (0, {LENGTH_MAX_M:g}] m y manga en (0, {WIDTH_MAX_M:g}] m cuando se reportan.",
                    (F.col("_length").isNull() | ((F.col("_length") > 0) & (F.col("_length") <= LENGTH_MAX_M)))
                    & (F.col("_width").isNull() | ((F.col("_width") > 0) & (F.col("_width") <= WIDTH_MAX_M)))),
    ]


DEDUP_RULES = {
    "D01_DUPLICADO_EXACTO": "Segunda llegada de (MMSI, BaseDateTime) con contenido idéntico.",
    "D02_DUPLICADO_CONFLICTIVO": "Segunda llegada de (MMSI, BaseDateTime) con contenido distinto; se conserva la primera.",
}


def evaluate(df: DataFrame, land_cells: DataFrame) -> DataFrame:
    """Agrega columnas tipadas, las banderas `_ok_<regla>` y `_motivos` (reglas CUARENTENA incumplidas).

    `land_cells` tiene una única columna `h3_r5`; se une por igualdad con broadcast.
    """
    typed = _typed(df)
    typed = typed.withColumn("_h3_r5", h3_cell(F.col("_lat"), F.col("_lon"), H3_RES_LAND))
    land = land_cells.select(F.col(f"h3_r{H3_RES_LAND}").alias("_land_cell"), F.lit(True).alias("_en_tierra"))
    typed = typed.join(F.broadcast(land), typed["_h3_r5"] == land["_land_cell"], "left").drop("_land_cell")

    rules = expectations()
    for rule in rules:
        typed = typed.withColumn(f"_ok_{rule.rule_id}", rule.passes)
    quarantine_rules = [r for r in rules if r.severity == "CUARENTENA"]
    warning_rules = [r for r in rules if r.severity == "ADVERTENCIA"]
    motivos = F.filter(
        F.array(*[F.when(~F.col(f"_ok_{r.rule_id}"), F.lit(r.rule_id)) for r in quarantine_rules]),
        lambda x: x.isNotNull(),
    )
    warnings = F.filter(
        F.array(*[F.when(~F.col(f"_ok_{r.rule_id}"), F.lit(r.rule_id)) for r in warning_rules]),
        lambda x: x.isNotNull(),
    )
    return typed.withColumn("_motivos", motivos).withColumn("_advertencias", warnings)


def content_hash() -> Column:
    """Huella del contenido crudo para distinguir duplicados exactos de conflictivos."""
    from .schemas import AIS_COLUMNS

    return F.sha2(F.concat_ws("\u0001", *[F.coalesce(F.col(c), F.lit("\u0000")) for c in AIS_COLUMNS]), 256)


def to_silver_columns(valid: DataFrame, batch_id: int) -> DataFrame:
    """Proyección conformada de plata: sentinelas a NULL, normalización y celda H3 r8.

    `group_expr` replica `reference.vessel_group` en SQL para no usar un UDF de Python.
    """
    group_expr = (
        F.when(F.col("_vessel_type").isNull(), "Desconocido")
        .when(F.col("_vessel_type").between(70, 79), "Carga")
        .when(F.col("_vessel_type").between(80, 89), "Tanquero")
        .when(F.col("_vessel_type").between(60, 69), "Pasajeros")
        .when(F.col("_vessel_type") == 30, "Pesca")
        .when(F.col("_vessel_type").isin(31, 32, 52), "Remolque")
        .when(F.col("_vessel_type").isin(36, 37), "Recreo")
        .when(F.col("_vessel_type").isin(33, 34, 35, 40, 50, 51, 53, 54, 55, 58, 59), "Servicio")
        .otherwise("Otro")
    )
    ok = lambda r: F.col(f"_ok_{r}")  # noqa: E731
    return valid.select(
        F.col("_mmsi").alias("mmsi"),
        F.col("_event_ts").alias("event_ts"),
        F.to_date("_event_ts").alias("event_date"),
        F.col("_lat").alias("lat"),
        F.col("_lon").alias("lon"),
        F.when(F.col("_sog") != SOG_SENTINEL, F.col("_sog")).alias("sog"),
        F.when(F.col("_cog").between(0, 359.9999), F.col("_cog")).alias("cog"),
        F.when(F.col("_heading").between(0, 359), F.col("_heading")).alias("heading"),
        F.nullif(F.trim("VesselName"), F.lit("")).alias("vessel_name"),
        F.when(ok("E10_IMO_FORMATO") & _non_empty("IMO"), F.col("_imo")).alias("imo"),
        F.nullif(F.trim("CallSign"), F.lit("")).alias("call_sign"),
        F.col("_vessel_type").alias("vessel_type"),
        group_expr.alias("vessel_type_group"),
        F.col("_status").alias("status"),
        F.when(ok("E12_DIMENSIONES_PLAUSIBLES"), F.col("_length")).alias("length_m"),
        F.when(ok("E12_DIMENSIONES_PLAUSIBLES"), F.col("_width")).alias("width_m"),
        F.col("_draft").alias("draft_m"),
        F.col("_cargo").alias("cargo"),
        F.nullif(F.trim("TransceiverClass"), F.lit("")).alias("transceiver_class"),
        h3_cell(F.col("_lat"), F.col("_lon"), H3_RES_TRAFFIC).alias("h3_r8"),
        F.col("_advertencias").alias("quality_warnings"),
        F.col("ingestion_date"),
        F.col("_source_file"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_hash").alias("_content_hash"),
        F.lit(batch_id).cast("bigint").alias("_silver_batch_id"),
        F.current_timestamp().alias("_silver_processed_at"),
    )


def to_quarantine_columns(rejected: DataFrame, batch_id: int, motivos_col: str = "_motivos") -> DataFrame:
    from .schemas import AIS_COLUMNS

    return rejected.select(
        *[F.col(c) for c in AIS_COLUMNS],
        F.col("ingestion_date"),
        F.col("_source_file"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col(motivos_col).alias("motivos"),
        F.element_at(F.col(motivos_col), 1).alias("motivo_principal"),
        F.lit(batch_id).cast("bigint").alias("_silver_batch_id"),
        F.current_timestamp().alias("_quarantined_at"),
    )


def metrics_rows(evaluated: DataFrame, dedup_counts: DataFrame, batch_id: int) -> DataFrame:
    """Métricas por (fecha de archivo, regla) en formato largo.

    `evaluated`: salida de `evaluate`; `dedup_counts`: (ingestion_date, regla, fallidos) de la
    deduplicación, evaluada solo sobre filas que pasaron las compuertas.
    """
    rules = expectations()
    aggs = [F.count(F.lit(1)).alias("total")]
    aggs += [F.sum((~F.col(f"_ok_{r.rule_id}")).cast("long")).alias(r.rule_id) for r in rules]
    aggs.append(F.sum((F.size("_motivos") > 0).cast("long")).alias("RESUMEN_CUARENTENA_COMPUERTAS"))
    wide = evaluated.groupBy("ingestion_date").agg(*aggs)

    stack_items = [(r.rule_id, r.severity, r.description) for r in rules]
    stack_items.append(("RESUMEN_CUARENTENA_COMPUERTAS", "RESUMEN", "Filas con al menos una compuerta CUARENTENA incumplida."))
    stack_expr = "stack({n}, {items}) AS (regla, severidad, descripcion, registros_fallidos)".format(
        n=len(stack_items),
        items=", ".join(f"'{rid}', '{sev}', '{desc.replace(chr(39), '')}', `{rid}`" for rid, sev, desc in stack_items),
    )
    long_rules = wide.select("ingestion_date", F.col("total").alias("registros_evaluados"), F.expr(stack_expr))

    long_dedup = dedup_counts.select(
        "ingestion_date",
        F.col("evaluados").alias("registros_evaluados"),
        "regla",
        F.lit("DEDUPLICACION").alias("severidad"),
        "descripcion",
        F.col("fallidos").alias("registros_fallidos"),
    )
    cols = ["ingestion_date", "registros_evaluados", "regla", "severidad", "descripcion", "registros_fallidos"]
    return (
        long_rules.select(*cols).unionByName(long_dedup.select(*cols))
        .withColumn("batch_id", F.lit(batch_id).cast("bigint"))
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("registros_fallidos", F.coalesce(F.col("registros_fallidos").cast("bigint"), F.lit(0).cast("bigint")))
        .withColumn("registros_evaluados", F.col("registros_evaluados").cast("bigint"))
        .withColumn("porcentaje_fallidos",
                    F.when(F.col("registros_evaluados") > 0,
                           F.round(100.0 * F.col("registros_fallidos") / F.col("registros_evaluados"), 4))
                    .otherwise(F.lit(0.0)))
        .select("batch_id", "processed_at", "ingestion_date", "regla", "severidad", "descripcion",
                "registros_evaluados", "registros_fallidos", "porcentaje_fallidos")
    )
