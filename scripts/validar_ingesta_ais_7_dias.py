"""Valida ingesta local AIS para siete CSV diarios, sin cache ni persist."""

import argparse
import importlib.util
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession


def load_ingestion_module():
    """Carga el esquema y la verificación desde el script de ingesta sin duplicarlos."""
    script_path = Path(__file__).with_name("01_ingesta_ais_local.py")
    spec = importlib.util.spec_from_file_location("ingesta_ais_local", script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"No se pudo cargar {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, required=True, help="Directorio con los siete CSV")
    parser.add_argument("--work-dir", type=Path, required=True, help="Directorio de temporales y reporte")
    parser.add_argument("--pattern", default="AIS_2023_06_*.csv", help="Patrón de archivos diarios")
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    source_dir = args.source_dir.resolve()
    work_dir = args.work_dir.resolve()
    files = sorted(source_dir.glob(args.pattern))
    if len(files) != 7:
        raise ValueError(f"Se esperaban exactamente 7 archivos y se encontraron {len(files)}")

    # TEMP/TMP explícitos permiten a Java crear sockets locales válidos en Windows.
    spark_tmp = work_dir / ".spark_tmp"
    spark_tmp.mkdir(parents=True, exist_ok=True)
    os.environ["TEMP"] = str(spark_tmp)
    os.environ["TMP"] = str(spark_tmp)

    ingestion = load_ingestion_module()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("oceanwatch-ingesta-local-7-dias")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    started = time.perf_counter()
    results = []
    frames = []

    try:
        for source_file in files:
            verification = ingestion.verify_file(
                source_file,
                expected_bytes=None,
                minimum_bytes=1,
                expected_sha256=None,
            )

            # `enforceSchema=false` exige que el encabezado coincida con las 17 columnas esperadas.
            read_started = time.perf_counter()
            frame = (
                spark.read.option("header", True)
                .option("enforceSchema", "false")
                .option("mode", "FAILFAST")
                .schema(ingestion.AIS_SCHEMA)
                .csv(str(source_file))
            )
            rows = frame.count()
            elapsed = time.perf_counter() - read_started
            schema_matches = frame.schema == ingestion.AIS_SCHEMA
            if not schema_matches:
                raise ValueError(f"Esquema inesperado después de leer {source_file.name}")

            frames.append(frame)
            result = {
                "file": source_file.name,
                "bytes": verification["bytes"],
                "sha256": verification["sha256"],
                "rows": rows,
                "read_count_seconds": round(elapsed, 2),
                "schema_matches_explicit_schema": schema_matches,
            }
            results.append(result)
            print(json.dumps(result, ensure_ascii=False))

        union_started = time.perf_counter()
        unioned = frames[0]
        for frame in frames[1:]:
            unioned = unioned.unionByName(frame, allowMissingColumns=False)
        union_rows = unioned.count()
        union_seconds = time.perf_counter() - union_started
        expected_rows = sum(result["rows"] for result in results)
        if union_rows != expected_rows:
            raise ValueError(f"Unión inconsistente: {union_rows} != {expected_rows}")

        report = {
            "validated_at_utc": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version,
            "explicit_schema": ingestion.AIS_SCHEMA.simpleString(),
            "files": results,
            "sum_rows": expected_rows,
            "sum_individual_read_count_seconds": round(sum(item["read_count_seconds"] for item in results), 2),
            "union_rows": union_rows,
            "union_seconds": round(union_seconds, 2),
            "total_wall_seconds": round(time.perf_counter() - started, 2),
            "union_schema_matches_explicit_schema": unioned.schema == ingestion.AIS_SCHEMA,
        }
        report_path = work_dir / "ingesta_local_7_dias.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print("BATCH_INGESTION_OK")
        print(json.dumps(report, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
