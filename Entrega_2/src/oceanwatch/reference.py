"""Tablas de referencia: catálogo VesselType, puertos, celdas portuarias H3 y máscara de tierra H3.

Las construcciones geométricas se hacen en Python puro (h3, shapely, pyshp) porque son tablas
pequeñas que se recalculan rara vez; el resultado se publica como tabla Delta y el pipeline solo
hace joins de igualdad por celda (broadcast), que escalan a decenas de millones de posiciones.
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from .geo import H3_RES_LAND, H3_RES_TRAFFIC

# Catálogo NOAA Marine Cadastre / USCG (fuente documentada en la Entrega 1, 03b).
VESSEL_TYPE_CATALOG = [
    (30, "Fishing"), (31, "Towing"), (32, "Towing: tow >200m or breadth >25m"),
    (33, "Dredging or underwater operations"), (34, "Diving operations"),
    (35, "Military operations"), (36, "Sailing"), (37, "Pleasure craft"),
    (40, "High speed craft"), (50, "Pilot vessel"), (51, "Search and rescue"),
    (52, "Tug"), (53, "Port tender"), (54, "Anti-pollution equipment"),
    (55, "Law enforcement"), (56, "Local vessel assignment (spare)"),
    (57, "Local vessel assignment (spare)"), (58, "Medical transport"),
    (59, "Noncombatant ship"), (60, "Passenger"), (70, "Cargo"),
    (71, "Cargo: hazard A"), (72, "Cargo: hazard B"), (73, "Cargo: hazard C"),
    (74, "Cargo: hazard D"), (79, "Cargo: no additional information"),
    (80, "Tanker"), (81, "Tanker: hazard A"), (82, "Tanker: hazard B"),
    (83, "Tanker: hazard C"), (84, "Tanker: hazard D"), (89, "Tanker: no additional information"),
    (90, "Other type"), (99, "Other type: no additional information"),
]
# Los rangos 40-49, 60-69, 70-79 y 80-89 completos son válidos en el estándar ITU-R M.1371.
VALID_VESSEL_TYPE_CODES = sorted(
    {c for c, _ in VESSEL_TYPE_CATALOG}
    | set(range(20, 30)) | set(range(40, 50)) | set(range(60, 70)) | set(range(70, 80))
    | set(range(80, 90)) | set(range(90, 100)) | {0}
)

VESSEL_GROUPS = ["Carga", "Tanquero", "Pasajeros", "Pesca", "Remolque", "Recreo", "Servicio", "Otro", "Desconocido"]


def vessel_group(code: int | None) -> str:
    """Agrupa códigos AIS en categorías de negocio usadas por el tablero y la matriz de tráfico."""
    if code is None:
        return "Desconocido"
    if 70 <= code <= 79:
        return "Carga"
    if 80 <= code <= 89:
        return "Tanquero"
    if 60 <= code <= 69:
        return "Pasajeros"
    if code == 30:
        return "Pesca"
    if code in (31, 32, 52):
        return "Remolque"
    if code in (36, 37):
        return "Recreo"
    if code in (33, 34, 35, 40, 50, 51, 53, 54, 55, 58, 59):
        return "Servicio"
    return "Otro"


def vessel_type_frame() -> pd.DataFrame:
    rows = [(c, d, vessel_group(c)) for c, d in VESSEL_TYPE_CATALOG]
    return pd.DataFrame(rows, columns=["vessel_type", "descripcion", "grupo"])


# ----------------------------------------------------------------------------- puertos
def load_ports(path: str) -> pd.DataFrame:
    """Carga puertos desde el World Port Index (CSV NGA Pub. 150) o Natural Earth `ne_10m_ports`.

    Ambos se normalizan a (port_id, port_name, country_code, lat, lon, fuente).
    """
    p = Path(path)
    if p.suffix.lower() == ".csv":
        df = pd.read_csv(p, dtype=str)
        out = pd.DataFrame({
            "port_id": "WPI-" + df["World Port Index Number"].str.strip(),
            "port_name": df["Main Port Name"].str.strip(),
            "country_code": df["Country Code"].str.strip(),
            "lat": pd.to_numeric(df["Latitude"], errors="coerce"),
            "lon": pd.to_numeric(df["Longitude"], errors="coerce"),
        })
        out["fuente"] = "NGA World Port Index Pub. 150"
    else:
        import shapefile  # pyshp

        reader = shapefile.Reader(str(p))
        fields = [f[0] for f in reader.fields[1:]]
        rows = []
        for idx, rec in enumerate(reader.iterShapeRecords()):
            attrs = dict(zip(fields, rec.record))
            lon, lat = rec.shape.points[0]
            rows.append((f"NE-{idx:05d}", str(attrs.get("name") or "").strip(), None, lat, lon))
        out = pd.DataFrame(rows, columns=["port_id", "port_name", "country_code", "lat", "lon"])
        out["fuente"] = "Natural Earth 10m ports (subconjunto WPI, dominio público)"
        # Natural Earth repite algunos puertos con varios puntos (p. ej. Port Everglades, Port Arthur):
        # se fusionan los homónimos a menos de ~0.5° conservando el primero.
        out["_k"] = out["port_name"].str.upper() + "|" + (out["lat"] * 2).round().astype(str) + "|" + (out["lon"] * 2).round().astype(str)
        out = out.drop_duplicates("_k").drop(columns="_k")
    out = out.dropna(subset=["lat", "lon"])
    out = out[out["port_name"].fillna("") != ""]
    return out.drop_duplicates("port_id").reset_index(drop=True)


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 12742.0176 * math.asin(math.sqrt(a))


def port_cells(ports: pd.DataFrame, radius_km: float = 2.0, res: int = H3_RES_TRAFFIC) -> pd.DataFrame:
    """Celdas H3 r8 cuyo centroide está a <= radius_km de un puerto (D-03: 2 km).

    Si una celda queda cerca de dos puertos se asigna al más cercano, de modo que cada posición
    pertenece a lo sumo a un puerto.
    """
    import h3

    edge_km = h3.average_hexagon_edge_length(res, unit="km")
    k = int(math.ceil(radius_km / (edge_km * 1.5))) + 1
    best: dict[str, tuple] = {}
    for port in ports.itertuples(index=False):
        origin = h3.latlng_to_cell(port.lat, port.lon, res)
        for cell in h3.grid_disk(origin, k):
            clat, clon = h3.cell_to_latlng(cell)
            dist = _haversine_km(port.lat, port.lon, clat, clon)
            if dist <= radius_km and (cell not in best or dist < best[cell][3]):
                best[cell] = (cell, port.port_id, port.port_name, round(dist, 4))
    return pd.DataFrame(list(best.values()), columns=["h3_r8", "port_id", "port_name", "distancia_km"])


# ----------------------------------------------------------------------- máscara de tierra
# Ventanas que cubren las aguas de EE. UU. publicadas por Marine Cadastre (continental, Alaska,
# Hawái, Puerto Rico/USVI y Guam/CNMI). Fuera de ellas la compuerta "en el agua" no marca tierra.
LAND_MASK_WINDOWS = [(-180.0, -15.0, -50.0, 75.0), (130.0, 0.0, 180.0, 30.0)]


def land_cells(natural_earth_dir: str, res: int = H3_RES_LAND, land_erosion_deg: float = 0.2,
               water_buffer_deg: float = 0.05, simplify_deg: float = 0.01) -> pd.DataFrame:
    """Celdas H3 que están completamente en tierra firme, lejos de costas, lagos y ríos principales.

    Construcción (Natural Earth 1:10m):
      1. tierra = ne_10m_land recortada a las ventanas EE. UU.;
      2. se restan lagos (ne_10m_lakes) y ríos (ne_10m_rivers_lake_centerlines) con un buffer de
         `water_buffer_deg` (~5 km) para no marcar vías navegables interiores (Misisipi, Grandes Lagos);
      3. se erosiona `land_erosion_deg` (~20 km) para que una celda r5 (radio ~10 km) cuyo centro
         cae dentro quede entera sobre tierra.
    Una posición dentro de estas celdas es físicamente imposible para un buque: compuerta E06.
    """
    import h3
    import shapefile
    from shapely.geometry import box, mapping, shape
    from shapely.ops import unary_union

    base = Path(natural_earth_dir)

    def read(name):
        reader = shapefile.Reader(str(base / f"{name}.zip"))
        return [shape(s.__geo_interface__) for s in reader.shapes()]

    windows = [box(*w) for w in LAND_MASK_WINDOWS]
    window_union = unary_union(windows)
    land = unary_union([g for g in read("ne_10m_land") if g.intersects(window_union)])
    land = land.intersection(window_union).simplify(simplify_deg)
    water = [g for g in read("ne_10m_lakes") + read("ne_10m_rivers_lake_centerlines")
             if g.intersects(window_union)]
    water = unary_union(water).buffer(water_buffer_deg)
    core = land.difference(water).buffer(-land_erosion_deg)

    cells: set[str] = set()
    geoms = getattr(core, "geoms", [core])
    for poly in geoms:
        if poly.is_empty or poly.geom_type != "Polygon":
            continue
        cells.update(h3.geo_to_cells(mapping(poly), res))
    return pd.DataFrame(sorted(cells), columns=[f"h3_r{res}"])

