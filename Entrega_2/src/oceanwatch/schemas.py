"""Esquemas explícitos de cada capa y DDL de creación de tablas."""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Las 17 columnas de los CSV NOAA (verificadas en la Entrega 1 con encabezado estricto).
AIS_COLUMNS = [
    "MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG", "Heading", "VesselName", "IMO",
    "CallSign", "VesselType", "Status", "Length", "Width", "Draft", "Cargo", "TransceiverClass",
]

# Bronce conserva el texto tal como llegó: ninguna conversión de tipos puede perder información
# ni detener el stream. La tipificación y las compuertas ocurren en plata.
RAW_STRING_SCHEMA = StructType([StructField(c, StringType(), True) for c in AIS_COLUMNS])

BRONZE_METADATA = [
    ("_rescued_data", "string", "Columnas o valores que no encajaron en el esquema (Auto Loader); NULL si la fila es conforme."),
    ("_source_file", "string", "Ruta del archivo del feed del que proviene la fila."),
    ("_source_modified_at", "timestamp", "Fecha de modificación del archivo de origen."),
    ("_ingested_at", "timestamp", "Instante en que el micro-lote de streaming escribió la fila."),
    ("ingestion_date", "date", "Fecha del archivo diario NOAA (AIS_AAAA_MM_DD), clave de partición."),
]

SILVER_COLUMNS = [
    ("mmsi", "string", "Identificador MMSI de 9 dígitos (compuerta E01)."),
    ("event_ts", "timestamp", "BaseDateTime en UTC."),
    ("event_date", "date", "Fecha UTC de la posición; partición."),
    ("lat", "double", "Latitud WGS84 validada (E04-E06)."),
    ("lon", "double", "Longitud WGS84 validada (E04-E06)."),
    ("sog", "double", "Velocidad sobre el fondo (nudos); NULL si llegó el sentinela 102.3."),
    ("cog", "double", "Rumbo sobre el fondo (grados); NULL si 360 (sentinela) o fuera de rango."),
    ("heading", "double", "Rumbo verdadero (grados); NULL si 511 (sentinela) o fuera de rango."),
    ("vessel_name", "string", "Nombre del buque, recortado."),
    ("imo", "string", "Número IMO normalizado 'IMOnnnnnnn'; NULL si no conforme."),
    ("call_sign", "string", "Indicativo de llamada."),
    ("vessel_type", "int", "Código AIS VesselType."),
    ("vessel_type_group", "string", "Grupo de negocio del VesselType (Carga, Tanquero, Pasajeros...)."),
    ("status", "int", "Estado de navegación AIS (NULL estructural en clase B)."),
    ("length_m", "double", "Eslora (m); NULL si no plausible."),
    ("width_m", "double", "Manga (m); NULL si no plausible."),
    ("draft_m", "double", "Calado (m)."),
    ("cargo", "int", "Código de carga AIS."),
    ("transceiver_class", "string", "Clase de transpondedor A/B."),
    ("h3_r8", "string", "Celda H3 resolución 8 de la posición (grilla de negocio)."),
    ("quality_warnings", "array<string>", "Expectativas de severidad ADVERTENCIA incumplidas (la fila pasa, el campo se anula)."),
    ("ingestion_date", "date", "Fecha del archivo de origen."),
    ("_source_file", "string", "Archivo del feed de origen."),
    ("_bronze_ingested_at", "timestamp", "Instante de escritura en bronce."),
    ("_content_hash", "string", "SHA-256 del registro crudo; distingue duplicados exactos de conflictivos."),
    ("_silver_batch_id", "bigint", "Micro-lote de streaming que escribió la fila (idempotencia)."),
    ("_silver_processed_at", "timestamp", "Instante de escritura en plata."),
]

QUARANTINE_COLUMNS = (
    [(c, "string", f"Valor crudo de {c}.") for c in AIS_COLUMNS]
    + [
        ("ingestion_date", "date", "Fecha del archivo de origen; partición."),
        ("_source_file", "string", "Archivo del feed de origen."),
        ("_bronze_ingested_at", "timestamp", "Instante de escritura en bronce."),
        ("motivos", "array<string>", "Todas las compuertas incumplidas por la fila."),
        ("motivo_principal", "string", "Primera compuerta incumplida según el orden del catálogo."),
        ("_silver_batch_id", "bigint", "Micro-lote que envió la fila a cuarentena."),
        ("_quarantined_at", "timestamp", "Instante de cuarentena."),
    ]
)

QUALITY_METRICS_COLUMNS = [
    ("batch_id", "bigint", "Micro-lote del stream plata."),
    ("processed_at", "timestamp", "Instante del cálculo."),
    ("ingestion_date", "date", "Fecha del archivo evaluado."),
    ("regla", "string", "Identificador de la expectativa (E01..E12, D01, D02) o RESUMEN_*."),
    ("severidad", "string", "CUARENTENA, ADVERTENCIA, DEDUPLICACION o RESUMEN."),
    ("descripcion", "string", "Qué verifica la regla."),
    ("registros_evaluados", "bigint", "Filas evaluadas por la regla en el lote."),
    ("registros_fallidos", "bigint", "Filas que incumplen la regla."),
    ("porcentaje_fallidos", "double", "100 * fallidos / evaluados."),
]

PIPELINE_RUNS_COLUMNS = [
    ("run_id", "string", "Identificador de la ejecución del job."),
    ("fecha_datos", "date", "Fecha del feed procesada (parámetro del job)."),
    ("tarea", "string", "Tarea del DAG."),
    ("intento", "int", "Número de intento (reintentos)."),
    ("estado", "string", "EXITOSO o FALLIDO."),
    ("inicio", "timestamp", "Inicio de la tarea."),
    ("fin", "timestamp", "Fin de la tarea."),
    ("duracion_s", "double", "Duración en segundos."),
    ("detalle", "string", "Métricas o error en JSON."),
]

FEED_CALENDAR_COLUMNS = [
    ("fecha_datos", "date", "Día del feed NOAA."),
    ("rol", "string", "HISTORICO (carga inicial) o FEED (llegada diaria)."),
    ("url_origen", "string", "URL oficial del ZIP diario."),
]


def struct_from_columns(columns) -> StructType:
    mapping = {
        "string": StringType(), "double": DoubleType(), "int": IntegerType(), "bigint": LongType(),
        "date": DateType(), "timestamp": TimestampType(), "boolean": BooleanType(),
        "array<string>": ArrayType(StringType(), True),
    }
    return StructType([StructField(n, mapping[t], True) for n, t, _ in columns])


BRONZE_SCHEMA = StructType(
    list(RAW_STRING_SCHEMA.fields) + list(struct_from_columns(BRONZE_METADATA).fields)
)
SILVER_SCHEMA = struct_from_columns(SILVER_COLUMNS)
QUARANTINE_SCHEMA = struct_from_columns(QUARANTINE_COLUMNS)
QUALITY_METRICS_SCHEMA = struct_from_columns(QUALITY_METRICS_COLUMNS)
PIPELINE_RUNS_SCHEMA = struct_from_columns(PIPELINE_RUNS_COLUMNS)


def _sql_literal(text: str) -> str:
    return text.replace("'", "\\'")


def create_table_ddl(name: str, columns, comment: str, partition_by: str | None = None,
                     properties: dict | None = None, with_comments: bool = True) -> str:
    cols = []
    for col_name, col_type, col_comment in columns:
        piece = f"`{col_name}` {col_type.upper()}"
        if with_comments:
            piece += f" COMMENT '{_sql_literal(col_comment)}'"
        cols.append(piece)
    ddl = f"CREATE TABLE IF NOT EXISTS {name} (\n  " + ",\n  ".join(cols) + "\n) USING DELTA"
    if partition_by:
        ddl += f"\nPARTITIONED BY ({partition_by})"
    if with_comments:
        ddl += f"\nCOMMENT '{_sql_literal(comment)}'"
    if properties:
        props = ", ".join(f"'{k}' = '{v}'" for k, v in properties.items())
        ddl += f"\nTBLPROPERTIES ({props})"
    return ddl


BRONZE_COLUMNS_DDL = [(c, "string", f"Texto crudo de la columna NOAA {c}.") for c in AIS_COLUMNS] + BRONZE_METADATA
