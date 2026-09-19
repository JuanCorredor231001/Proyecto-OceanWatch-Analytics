# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Ingesta AIS desde NOAA hacia Unity Catalog Volume
# MAGIC
# MAGIC **Objetivo.** Descargar los ZIP oficiales AIS, comprobar su integridad, extraer los CSV en un Volume y leerlos con un esquema explícito.
# MAGIC
# MAGIC **Entradas.** URLs oficiales del enunciado, catálogo configurado por `00_configuracion_unity_catalog` y el Volume `landing.raw_ais`.
# MAGIC
# MAGIC **Salidas.** ZIP en `downloads/`, CSV en `extracted/` dentro del Volume, y métricas de descarga, extracción y lectura.
# MAGIC
# MAGIC **Prerrequisitos.** Ejecutar antes `00_configuracion_unity_catalog`. La primera corrida se limita a 01-jun-2023.
# MAGIC
# MAGIC **Validación.** Confirmar `INGESTION_OK`, `spark.version`, tiempos y conteo antes de habilitar los siete días.

# COMMAND ----------

import hashlib
import shutil
import time
import urllib.request
import zipfile
from pathlib import Path

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# Identificador provisional neutral; debe coincidir con el notebook 00.
TEAM_ID = "g07"
if not TEAM_ID.replace("_", "").isalnum() or TEAM_ID != TEAM_ID.lower():
    raise ValueError("TEAM_ID debe usar solo minúsculas, números y guiones bajos")

CATALOG = f"oceanwatch_{TEAM_ID}"
VOLUME_ROOT = Path(f"/Volumes/{CATALOG}/landing/raw_ais")

# 01-jun ya fue validado end-to-end contra el conteo local; ejecutar los siete días.
DATES_TO_INGEST = [
    "2023-06-01", "2023-06-02", "2023-06-03", "2023-06-04",
    "2023-06-05", "2023-06-06", "2023-06-07",
]

OFFICIAL_URLS = {
    "2023-06-01": "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_01.zip",
    "2023-06-02": "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_02.zip",
    "2023-06-03": "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_03.zip",
    "2023-06-04": "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_04.zip",
    "2023-06-05": "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_05.zip",
    "2023-06-06": "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_06.zip",
    "2023-06-07": "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_07.zip",
}

# No hay checksums oficiales publicados en el enunciado; el notebook registra SHA-256 observado.
EXPECTED_SHA256 = {}
RETRIES = 3
TIMEOUT_SECONDS = 120

# COMMAND ----------

# El esquema se declara explícitamente; MMSI se conserva como identificador de texto.
AIS_SCHEMA = StructType(
    [
        StructField("MMSI", StringType(), True),
        StructField("BaseDateTime", StringType(), True),
        StructField("LAT", DoubleType(), True),
        StructField("LON", DoubleType(), True),
        StructField("SOG", DoubleType(), True),
        StructField("COG", DoubleType(), True),
        StructField("Heading", DoubleType(), True),
        StructField("VesselName", StringType(), True),
        StructField("IMO", StringType(), True),
        StructField("CallSign", StringType(), True),
        StructField("VesselType", IntegerType(), True),
        StructField("Status", IntegerType(), True),
        StructField("Length", DoubleType(), True),
        StructField("Width", DoubleType(), True),
        StructField("Draft", DoubleType(), True),
        StructField("Cargo", IntegerType(), True),
        StructField("TransceiverClass", StringType(), True),
    ]
)


def sha256_file(path: Path, block_size: int = 1_048_576) -> str:
    """Calcula SHA-256 por bloques, apto para archivos grandes en el Volume."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(block_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_file(path: Path, expected_sha256: str | None = None) -> dict:
    """Verifica presencia, tamaño no nulo y checksum cuando exista una referencia oficial."""
    if not path.is_file():
        raise FileNotFoundError(f"Archivo no encontrado: {path}")
    size_bytes = path.stat().st_size
    if size_bytes == 0:
        raise ValueError(f"Archivo vacío: {path}")
    checksum = sha256_file(path)
    if expected_sha256 and checksum.lower() != expected_sha256.lower():
        raise ValueError(f"SHA-256 inesperado para {path.name}")
    return {"path": str(path), "bytes": size_bytes, "sha256": checksum}


def download_with_retries(url: str, destination: Path, expected_sha256: str | None) -> tuple[dict, float]:
    """Descarga a un archivo temporal y lo mueve solo después de recibir HTTP 200."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    last_error = None
    started = time.perf_counter()

    for attempt in range(1, RETRIES + 1):
        try:
            with urllib.request.urlopen(url, timeout=TIMEOUT_SECONDS) as response, temporary.open("wb") as output:
                if response.status != 200:
                    raise OSError(f"HTTP inesperado: {response.status}")
                shutil.copyfileobj(response, output, length=1_048_576)
            temporary.replace(destination)
            return verify_file(destination, expected_sha256), time.perf_counter() - started
        except Exception as error:
            last_error = error
            temporary.unlink(missing_ok=True)
            if attempt < RETRIES:
                time.sleep(2 ** (attempt - 1))

    raise RuntimeError(f"Descarga fallida tras {RETRIES} intentos: {url}") from last_error


def extract_single_csv(archive: Path, destination_dir: Path) -> tuple[Path, float]:
    """Extrae exactamente un CSV y elimina componentes de ruta no confiables del ZIP."""
    if not zipfile.is_zipfile(archive):
        raise ValueError(f"ZIP inválido: {archive}")
    started = time.perf_counter()

    with zipfile.ZipFile(archive) as zipped:
        csv_members = [member for member in zipped.infolist() if not member.is_dir() and member.filename.lower().endswith(".csv")]
        if len(csv_members) != 1:
            raise ValueError(f"Se esperaba exactamente un CSV en {archive.name}; encontrados: {len(csv_members)}")
        member = csv_members[0]
        destination = destination_dir / Path(member.filename).name
        destination.parent.mkdir(parents=True, exist_ok=True)
        with zipped.open(member) as source, destination.open("wb") as target:
            shutil.copyfileobj(source, target, length=1_048_576)

    verify_file(destination)
    return destination, time.perf_counter() - started


def read_ais_csv(csv_path: Path):
    """Lee con esquema y valida que el encabezado del CSV coincida con las 17 columnas esperadas."""
    return (
        spark.read.option("header", True)
        .option("enforceSchema", "false")
        .option("mode", "FAILFAST")
        .schema(AIS_SCHEMA)
        .csv(str(csv_path))
    )


def union_daily_frames(daily_frames):
    """Unión preparada para siete días; no invocar hasta aprobar la prueba de un día."""
    unioned = daily_frames[0]
    for frame in daily_frames[1:]:
        unioned = unioned.unionByName(frame, allowMissingColumns=False)
    return unioned

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingesta controlada: únicamente 01-jun-2023
# MAGIC
# MAGIC `DATES_TO_INGEST` se amplía a los siete días después de revisar esta salida. La función de unión queda lista, pero no se invoca en esta primera corrida.

# COMMAND ----------

daily_frames = []
ingestion_metrics = []

for day in DATES_TO_INGEST:
    if day not in OFFICIAL_URLS:
        raise KeyError(f"Fecha sin URL oficial: {day}")

    archive_path = VOLUME_ROOT / "downloads" / f"AIS_{day.replace('-', '_')}.zip"
    csv_dir = VOLUME_ROOT / "extracted" / f"ingestion_date={day}"
    download_info, download_seconds = download_with_retries(
        OFFICIAL_URLS[day], archive_path, EXPECTED_SHA256.get(day)
    )
    csv_path, extract_seconds = extract_single_csv(archive_path, csv_dir)

    read_started = time.perf_counter()
    daily_frame = read_ais_csv(csv_path)
    rows = daily_frame.count()
    read_count_seconds = time.perf_counter() - read_started
    if daily_frame.schema != AIS_SCHEMA:
        raise RuntimeError(f"Esquema inesperado en {day}")

    daily_frames.append(daily_frame)
    ingestion_metrics.append(
        {
            "day": day,
            "archive_bytes": download_info["bytes"],
            "archive_sha256": download_info["sha256"],
            "csv_path": str(csv_path),
            "download_seconds": round(download_seconds, 2),
            "extract_seconds": round(extract_seconds, 2),
            "rows": rows,
            "read_count_seconds": round(read_count_seconds, 2),
        }
    )

display(spark.createDataFrame(ingestion_metrics))
print("INGESTION_OK")
print(f"spark_version={spark.version}")
print(f"volume_root={VOLUME_ROOT}")
print(f"frames_prepared={len(daily_frames)}")

# COMMAND ----------

# La unión exige el mismo contrato de columnas en los siete DataFrames.
union_started = time.perf_counter()
all_days = union_daily_frames(daily_frames)
total_rows = all_days.count()
union_seconds = time.perf_counter() - union_started
expected_total_rows = 60_533_559
if total_rows != expected_total_rows:
    raise RuntimeError(f"Total inesperado: {total_rows:,} vs {expected_total_rows:,}")

display(spark.createDataFrame([(total_rows, expected_total_rows, total_rows - expected_total_rows, round(union_seconds, 2))],
    "filas_unidas long, filas_esperadas long, diferencia long, segundos_union double"))
print("UNION_OK")
