"""Pruebas unitarias del pipeline con datos sintéticos (sin descargar nada).

Ejecutar dentro del contenedor:  python -m pytest -q tests
"""

from __future__ import annotations

import datetime as dt
import shutil
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from oceanwatch import gold, setup
from oceanwatch.contract import conform, gold_tables, validate_table
from oceanwatch.lakehouse import Lakehouse, build_local_spark
from oceanwatch.quality import evaluate
from oceanwatch.schemas import AIS_COLUMNS, BRONZE_SCHEMA
from oceanwatch.streaming import process_silver_batch

TEST_ROOT = "/tmp/oceanwatch_pytest"
DAY = dt.date(2023, 6, 8)

GOOD = ["367000001", "2023-06-08T10:00:00", "29.30", "-94.80", "12.0", "90.0", "88", "BUQUE A", "IMO1234567",
        "WXYZ", "70", "0", "150", "25", "8", "70", "A"]


def row(**overrides):
    values = dict(zip(AIS_COLUMNS, GOOD))
    values.update(overrides)
    return [values[c] for c in AIS_COLUMNS] + [None, "file:/feed/2023-06-08/AIS_2023_06_08.csv",
                                                dt.datetime(2023, 6, 9), dt.datetime(2023, 6, 9), DAY]


@pytest.fixture(scope="session")
def lh():
    shutil.rmtree(TEST_ROOT, ignore_errors=True)
    spark = build_local_spark("pytest", shuffle_partitions=2, driver_memory="2g")
    lake = Lakehouse(spark=spark, local_root=TEST_ROOT)
    setup.create_all(lake)
    # Máscara mínima: la celda r5 de un punto en el interior de Texas es "tierra".
    import h3
    lake.overwrite(spark.createDataFrame([(h3.latlng_to_cell(31.0, -99.0, 5),)], "h3_r5 string"), "reference", "land_cells_h3r5")
    port_cell = h3.latlng_to_cell(29.30, -94.80, 8)
    lake.overwrite(spark.createDataFrame([(port_cell, "T-1", "PUERTO PRUEBA", 0.1)],
                                         "h3_r8 string, port_id string, port_name string, distancia_km double"),
                   "reference", "port_cells_h3r8")
    lake.overwrite(spark.createDataFrame([("T-1", "PUERTO PRUEBA", "US", 29.30, -94.80, "test")],
                                         "port_id string, port_name string, country_code string, lat double, lon double, fuente string"),
                   "reference", "ports")
    yield lake
    spark.stop()
    shutil.rmtree(TEST_ROOT, ignore_errors=True)


def motivos(lh, **overrides):
    df = lh.spark.createDataFrame([row(**overrides)], BRONZE_SCHEMA)
    out = evaluate(df, lh.read("reference", "land_cells_h3r5")).select("_motivos", "_advertencias").first()
    return out[0], out[1]


@pytest.mark.parametrize("overrides, expected", [
    ({}, []),
    ({"MMSI": "12345"}, ["E01_MMSI_FORMATO"]),
    ({"BaseDateTime": "08/06/2023 10:00"}, ["E02_TIMESTAMP_VALIDO", "E03_TIMESTAMP_EN_VENTANA"]),
    ({"BaseDateTime": "2023-06-07T23:59:59"}, ["E03_TIMESTAMP_EN_VENTANA"]),
    ({"LAT": "abc"}, ["E04_COORDENADAS_PRESENTES", "E05_COORDENADAS_EN_RANGO"]),
    ({"LAT": "95.0"}, ["E05_COORDENADAS_EN_RANGO"]),
    ({"LAT": "0", "LON": "0"}, ["E05_COORDENADAS_EN_RANGO"]),
    ({"LAT": "31.0", "LON": "-99.0"}, ["E06_COORDENADAS_EN_AGUA"]),
    ({"SOG": "85.1"}, ["E07_SOG_PLAUSIBLE"]),
    ({"SOG": "102.3"}, []),
])
def test_compuertas_cuarentena(lh, overrides, expected):
    assert motivos(lh, **overrides)[0] == expected


def test_advertencias_no_bloquean(lh):
    bloqueo, advertencias = motivos(lh, IMO="IMO12", COG="-5", Heading="511", Length="900")
    assert bloqueo == []
    assert advertencias == ["E08_COG_RANGO", "E10_IMO_FORMATO", "E12_DIMENSIONES_PLAUSIBLES"]


def test_lote_enruta_deduplica_y_es_idempotente(lh):
    rows = [
        row(),                                              # válido
        row(),                                              # duplicado exacto (D01)
        row(SOG="11.0"),                                    # misma clave, otro contenido (D01/D02)
        row(MMSI="1"),                                      # cuarentena E01
        row(BaseDateTime="2023-06-08T10:05:00", LAT="29.32"),  # válido
    ]
    batch = lh.spark.createDataFrame(rows, BRONZE_SCHEMA)
    land = lh.read("reference", "land_cells_h3r5")
    process_silver_batch(lh, land, batch, 0)
    process_silver_batch(lh, land, batch, 0)  # reproceso del mismo lote: no debe duplicar

    silver = lh.read("silver", "ais_posiciones")
    quarantine = lh.read("silver", "ais_cuarentena")
    assert silver.count() == 2
    assert silver.filter(F.col("sog").isNull()).count() == 0
    reasons = sorted(r[0] for r in quarantine.select("motivo_principal").collect())
    # La fila conservada es la de menor hash; según cuál sea, las copias son D01+D02 o D02+D02.
    assert reasons in (["D01_DUPLICADO_EXACTO", "D02_DUPLICADO_CONFLICTIVO", "E01_MMSI_FORMATO"],
                       ["D02_DUPLICADO_CONFLICTIVO", "D02_DUPLICADO_CONFLICTIVO", "E01_MMSI_FORMATO"])
    assert silver.count() + quarantine.count() == len(rows)

    # Un lote posterior con la misma clave es duplicado entre lotes.
    process_silver_batch(lh, land, lh.spark.createDataFrame([row()], BRONZE_SCHEMA), 1)
    assert lh.read("silver", "ais_posiciones").count() == 2
    assert lh.read("silver", "ais_cuarentena").filter(F.col("_silver_batch_id") == 1).count() == 1


def test_oro_cumple_contrato(lh):
    counts = gold.build_gold_for_date(lh, str(DAY))
    assert counts["trayectorias_diarias_buque"] == 1
    traj = lh.read("gold", "trayectorias_diarias_buque").first()
    assert traj.n_posiciones == 2 and traj.trayectoria_wkt.startswith("LINESTRING(")
    assert counts["estadias_puerto"] == 1
    for table in gold_tables():
        if table == "anomalias_velocidad_diarias":
            continue
        results = validate_table(lh.read("gold", table), table)
        assert all(r["ok"] for r in results), [r for r in results if not r["ok"]]


def test_conform_rechaza_columnas_faltantes(lh):
    with pytest.raises(ValueError):
        conform(lh.spark.createDataFrame([(1,)], "x int"), "trafico_celda_hora")


def test_bronce_es_append_only(lh):
    with pytest.raises(Exception, match="APPEND_ONLY"):
        lh.sql(f"DELETE FROM {lh.name('bronze', 'ais_raw')}")


def test_ddl_de_oro_generado_desde_contrato(lh):
    detail = lh.sql(f"DESCRIBE DETAIL {lh.name('gold', 'trafico_celda_hora')}").first()
    assert detail.partitionColumns == ["event_date"]
    assert detail.properties.get("oceanwatch.contrato.version") == "1.0.0"
    assert Path(lh.path("gold", "trafico_celda_hora")).exists()
