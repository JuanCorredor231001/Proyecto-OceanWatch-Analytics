"""Funciones espaciales con la misma semántica en Databricks y en local.

- Databricks: funciones H3 nativas (`h3_longlatash3string`, `h3_centeraswkt`), ejecutadas por Photon
  (decisión H3 de la Entrega 1).
- Local: pandas UDF sobre la librería `h3` 4.x. Ambas implementan el mismo índice H3 v4, por lo
  que los identificadores de celda coinciden entre plataformas.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from .lakehouse import IS_DATABRICKS

EARTH_DIAMETER_KM = 12742.0176  # 2 * 6371.0088 km, igual que D-04 en la Entrega 1.
KM_PER_NAUTICAL_MILE = 1.852

# Resolución de la grilla de negocio (Entrega 1, 03d): H3 r8, ~0.74 km² por celda.
H3_RES_TRAFFIC = 8
# Resolución de la máscara de tierra (compuerta "en el agua"): H3 r5, ~252 km² por celda.
H3_RES_LAND = 5


@pandas_udf(StringType())
def _h3_cell_local(lat: pd.Series, lon: pd.Series, res: pd.Series) -> pd.Series:
    import h3

    out = []
    for la, lo, r in zip(lat, lon, res):
        if pd.isna(la) or pd.isna(lo) or not (-90 <= la <= 90) or not (-180 <= lo <= 180):
            out.append(None)
        else:
            out.append(h3.latlng_to_cell(float(la), float(lo), int(r)))
    return pd.Series(out, dtype="object")


@pandas_udf(StructType([StructField("lat", DoubleType()), StructField("lon", DoubleType())]))
def _h3_center_local(cell: pd.Series) -> pd.DataFrame:
    import h3

    lats, lons = [], []
    for c in cell:
        if c is None or (isinstance(c, float) and pd.isna(c)):
            lats.append(None)
            lons.append(None)
        else:
            la, lo = h3.cell_to_latlng(c)
            lats.append(la)
            lons.append(lo)
    return pd.DataFrame({"lat": lats, "lon": lons})


def h3_cell(lat: str | Column, lon: str | Column, res: int) -> Column:
    """Celda H3 (string hexadecimal) de un punto; NULL si las coordenadas están fuera de rango."""
    lat_c = F.col(lat) if isinstance(lat, str) else lat
    lon_c = F.col(lon) if isinstance(lon, str) else lon
    if IS_DATABRICKS:
        valid = lat_c.between(-90, 90) & lon_c.between(-180, 180)
        return F.when(valid, F.call_function("h3_longlatash3string", lon_c, lat_c, F.lit(res)))
    return _h3_cell_local(lat_c, lon_c, F.lit(res))


def h3_center(cell: str | Column) -> Column:
    """Centroide (lat, lon) de una celda H3 como struct."""
    cell_c = F.col(cell) if isinstance(cell, str) else cell
    if IS_DATABRICKS:
        wkt = F.call_function("h3_centeraswkt", cell_c)
        lon = F.regexp_extract(wkt, r"POINT\s*\(\s*(-?[0-9.]+)\s+(-?[0-9.]+)", 1).cast("double")
        lat = F.regexp_extract(wkt, r"POINT\s*\(\s*(-?[0-9.]+)\s+(-?[0-9.]+)", 2).cast("double")
        return F.struct(lat.alias("lat"), lon.alias("lon"))
    return _h3_center_local(cell_c)


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Distancia geodésica (km) con la misma fórmula usada en D-04."""
    a = F.pow(F.sin(F.radians(lat2 - lat1) / 2), 2) + F.cos(F.radians(lat1)) * F.cos(
        F.radians(lat2)
    ) * F.pow(F.sin(F.radians(lon2 - lon1) / 2), 2)
    return F.lit(EARTH_DIAMETER_KM) * F.asin(F.sqrt(a))
