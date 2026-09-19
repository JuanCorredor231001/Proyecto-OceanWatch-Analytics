"""Prueba mínima local: lectura explícita de una muestra AIS con PySpark y H3."""

import os
from pathlib import Path

import h3
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# Mantener el esquema explícito desde la primera lectura; MMSI es identificador, no medida.
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


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    source_file = project_root / "Datos" / "AIS_2023_06_01.csv"
    spark_tmp = project_root / ".spark_tmp"

    # Java en Windows usa TEMP/TMP para sockets internos; usar una ruta local explícita evita rutas heredadas inválidas.
    spark_tmp.mkdir(exist_ok=True)
    os.environ["TEMP"] = str(spark_tmp)
    os.environ["TMP"] = str(spark_tmp)

    if not source_file.is_file():
        raise FileNotFoundError(f"No se encontró el CSV esperado: {source_file}")

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("oceanwatch-local-smoke-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        # La prueba usa una muestra acotada para no ejecutar análisis sobre todo el día.
        ais = spark.read.option("header", True).schema(AIS_SCHEMA).csv(str(source_file))
        sample = ais.limit(1_000)

        print(f"Spark version: {spark.version}")
        print(f"Rows in deterministic sample: {sample.count()}")
        sample.printSchema()
        sample.select("MMSI", "BaseDateTime", "LAT", "LON", "SOG").show(5, truncate=False)

        # Confirma que la biblioteca H3 está disponible antes de usarla en el análisis espacial.
        first_position = sample.select("LAT", "LON").first()
        cell = h3.latlng_to_cell(first_position.LAT, first_position.LON, 8)
        print(f"H3 resolution-8 cell for first position: {cell}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
