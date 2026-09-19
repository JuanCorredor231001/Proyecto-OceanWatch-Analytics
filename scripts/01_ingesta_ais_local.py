"""Ingesta AIS parametrizable para prueba local y futura adaptación a Databricks."""

import argparse
import hashlib
import shutil
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# El esquema se declara explícitamente para evitar inferencia inconsistente entre días.
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
    """Calcula SHA-256 por bloques para no cargar archivos grandes en memoria."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(block_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_file(
    path: Path,
    *,
    expected_bytes: int | None,
    minimum_bytes: int | None,
    expected_sha256: str | None,
) -> dict[str, str | int]:
    """Valida presencia, tamaño y, cuando se provee, checksum de una descarga o CSV."""
    if not path.is_file():
        raise FileNotFoundError(f"Archivo no encontrado: {path}")

    observed_bytes = path.stat().st_size
    if observed_bytes == 0:
        raise ValueError(f"Archivo vacío: {path}")
    if expected_bytes is not None and observed_bytes != expected_bytes:
        raise ValueError(f"Tamaño inesperado: {observed_bytes} != {expected_bytes}")
    if minimum_bytes is not None and observed_bytes < minimum_bytes:
        raise ValueError(f"Archivo menor al mínimo: {observed_bytes} < {minimum_bytes}")

    checksum = sha256_file(path)
    if expected_sha256 is not None and checksum.lower() != expected_sha256.lower():
        raise ValueError("SHA-256 no coincide con el valor esperado")
    return {"path": str(path), "bytes": observed_bytes, "sha256": checksum}


def download_with_retries(
    url: str,
    destination: Path,
    *,
    retries: int,
    timeout_seconds: int,
    expected_bytes: int | None,
    minimum_bytes: int | None,
    expected_sha256: str | None,
) -> dict[str, str | int]:
    """Descarga atómicamente y reintenta; se usará sin cambios contra un Volume."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout_seconds) as response, temporary.open("wb") as output:
                if response.status != 200:
                    raise OSError(f"HTTP inesperado: {response.status}")
                shutil.copyfileobj(response, output, length=1_048_576)
            temporary.replace(destination)
            return verify_file(
                destination,
                expected_bytes=expected_bytes,
                minimum_bytes=minimum_bytes,
                expected_sha256=expected_sha256,
            )
        except Exception as error:
            last_error = error
            temporary.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(2**(attempt - 1))

    raise RuntimeError(f"La descarga falló tras {retries} intentos: {url}") from last_error


def extract_csv_from_zip(archive: Path, destination_dir: Path) -> Path:
    """Extrae exactamente un CSV desde el ZIP y rechaza rutas inseguras o ambiguas."""
    if not zipfile.is_zipfile(archive):
        raise ValueError(f"El archivo no es un ZIP válido: {archive}")

    with zipfile.ZipFile(archive) as zipped:
        csv_members = [member for member in zipped.infolist() if member.filename.lower().endswith(".csv")]
        if len(csv_members) != 1:
            raise ValueError(f"Se esperaba un CSV en {archive}; encontrados: {len(csv_members)}")
        member = csv_members[0]
        output_path = destination_dir / Path(member.filename).name
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with zipped.open(member) as source, output_path.open("wb") as target:
            shutil.copyfileobj(source, target, length=1_048_576)

    return output_path


def create_csv_sample(source: Path, destination: Path, rows: int) -> Path:
    """Copia encabezado y primeras N filas físicas para una iteración local rápida."""
    if rows <= 0:
        raise ValueError("--sample-rows debe ser mayor que cero")

    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as input_stream, destination.open("wb") as output_stream:
        header = input_stream.readline()
        if not header:
            raise ValueError(f"CSV sin encabezado: {source}")
        output_stream.write(header)
        for _ in range(rows):
            row = input_stream.readline()
            if not row:
                break
            output_stream.write(row)
    return destination


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--source-csv", type=Path, help="CSV local existente para prototipado")
    source.add_argument("--source-url", help="URL oficial de un ZIP para Databricks")
    parser.add_argument("--work-dir", type=Path, required=True, help="Directorio local o Volume de trabajo")
    parser.add_argument("--dataset-date", required=True, help="Fecha ISO usada en la partición de salida")
    parser.add_argument("--sample-rows", type=int, help="Si se define, crea y procesa primeras N filas")
    parser.add_argument("--archive-name", default="ais_source.zip", help="Nombre para ZIP descargado")
    parser.add_argument("--expected-bytes", type=int, help="Tamaño exacto esperado del archivo fuente")
    parser.add_argument("--minimum-bytes", type=int, help="Tamaño mínimo aceptable del archivo fuente")
    parser.add_argument("--expected-sha256", help="SHA-256 esperado del archivo fuente")
    parser.add_argument("--retries", type=int, default=3, help="Intentos máximos de descarga")
    parser.add_argument("--timeout-seconds", type=int, default=120, help="Timeout HTTP por intento")
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    work_dir = args.work_dir.resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    if args.source_csv:
        source_csv = args.source_csv.resolve()
        verification = verify_file(
            source_csv,
            expected_bytes=args.expected_bytes,
            minimum_bytes=args.minimum_bytes,
            expected_sha256=args.expected_sha256,
        )
    else:
        archive = work_dir / "downloads" / args.archive_name
        verification = download_with_retries(
            args.source_url,
            archive,
            retries=args.retries,
            timeout_seconds=args.timeout_seconds,
            expected_bytes=args.expected_bytes,
            minimum_bytes=args.minimum_bytes,
            expected_sha256=args.expected_sha256,
        )
        source_csv = extract_csv_from_zip(archive, work_dir / "extracted")
        verify_file(source_csv, expected_bytes=None, minimum_bytes=1, expected_sha256=None)

    if args.sample_rows is not None:
        input_csv = create_csv_sample(source_csv, work_dir / "samples" / source_csv.name, args.sample_rows)
    else:
        input_csv = source_csv

    # TEMP/TMP explícitos previenen un problema de sockets Java heredado en Windows.
    spark_tmp = work_dir / ".spark_tmp"
    spark_tmp.mkdir(exist_ok=True)
    import os

    os.environ["TEMP"] = str(spark_tmp)
    os.environ["TMP"] = str(spark_tmp)

    spark = (
        SparkSession.builder.master("local[2]")
        .appName(f"oceanwatch-ingesta-{args.dataset_date}")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        started = time.perf_counter()
        ais = (
            spark.read.option("header", True)
            .option("enforceSchema", "false")
            .option("mode", "FAILFAST")
            .schema(AIS_SCHEMA)
            .csv(str(input_csv))
        )
        rows = ais.count()
        read_seconds = time.perf_counter() - started

        print("INGESTION_OK")
        print(f"source_bytes={verification['bytes']}")
        print(f"source_sha256={verification['sha256']}")
        print(f"input_csv={input_csv}")
        print(f"rows={rows}")
        print(f"read_count_seconds={read_seconds:.2f}")
        print(f"spark_version={spark.version}")
        ais.printSchema()
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"INGESTION_FAILED: {error}", file=sys.stderr)
        raise
