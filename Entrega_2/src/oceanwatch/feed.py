"""Llegada del feed diario: descarga del ZIP oficial NOAA y publicación atómica del CSV en landing.

El stream bronce observa `landing/raw_ais/feed/<fecha>/AIS_*.csv`. El CSV se escribe primero en
`staging/` y luego se mueve con `os.replace`, para que la fuente de streaming nunca vea un
archivo a medio escribir.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import urllib.request
import zipfile
import zlib
from pathlib import Path

from .lakehouse import Lakehouse

BOM = bytes([0xEF, 0xBB, 0xBF])  # marca UTF-8 opcional al inicio del encabezado
NOAA_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{y}/AIS_{y}_{m}_{d}.zip"
HISTORICAL_DATES = [f"2023-06-0{d}" for d in range(1, 8)]
FEED_DATES = ["2023-06-08", "2023-06-09", "2023-06-10", "2023-06-11", "2023-06-12", "2023-06-13", "2023-06-14"]


def noaa_url(fecha: str) -> str:
    y, m, d = fecha.split("-")
    return NOAA_URL.format(y=y, m=m, d=d)


def file_stem(fecha: str) -> str:
    return "AIS_" + fecha.replace("-", "_")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url: str, destination: Path, retries: int = 3, timeout: int = 120) -> None:
    """Descarga a `.part` y renombra solo si termina con HTTP 200 y ZIP válido (Entrega 1, D-02)."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp = destination.with_suffix(destination.suffix + ".part")
    last = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp, tmp.open("wb") as out:
                if resp.status != 200:
                    raise OSError(f"HTTP {resp.status}")
                shutil.copyfileobj(resp, out, length=1 << 20)
            if not zipfile.is_zipfile(tmp):
                raise ValueError("ZIP inválido")
            os.replace(tmp, destination)
            return
        except Exception as error:  # noqa: BLE001
            last = error
            tmp.unlink(missing_ok=True)
            time.sleep(2 ** (attempt - 1))
    raise RuntimeError(f"Descarga fallida tras {retries} intentos: {url}") from last


def _keep(mmsi: str, modulo: int | None) -> bool:
    if not modulo:
        return True
    mmsi = (mmsi or "").strip()
    if not (len(mmsi) == 9 and mmsi.isdigit()):
        return True  # se conservan los MMSI no conformes para ejercitar la cuarentena
    return zlib.crc32(mmsi.encode()) % modulo == 0


def _extract_sample(archive: Path, destination: Path, sample_modulo: int | None) -> tuple[int, int]:
    """Extrae el único CSV del ZIP filtrando por buque sobre bytes crudos.

    No se re-serializa con `csv`: cada línea se copia byte a byte, lo que preserva el archivo
    original exacto y es ~5x más rápido. MMSI es la primera columna y nunca va entre comillas.
    """
    rows_in = rows_out = 0
    with zipfile.ZipFile(archive) as zipped:
        members = [m for m in zipped.infolist() if m.filename.lower().endswith(".csv") and not m.is_dir()]
        if len(members) != 1:
            raise ValueError(f"Se esperaba un CSV en {archive.name}, hay {len(members)}")
        with zipped.open(members[0]) as raw, destination.open("wb") as out:
            header = raw.readline()
            if header.split(b",", 1)[0].strip().removeprefix(BOM) != b"MMSI":
                raise ValueError(f"Encabezado inesperado en {archive.name}: {header[:80]!r}")
            out.write(header)
            buffer = []
            for line in raw:
                rows_in += 1
                if _keep(line.split(b",", 1)[0].decode("ascii", "replace"), sample_modulo):
                    buffer.append(line)
                    rows_out += 1
                    if len(buffer) >= 50_000:
                        out.writelines(buffer)
                        buffer.clear()
            out.writelines(buffer)
    return rows_in, rows_out


def land_day(lh: Lakehouse, fecha: str, zip_dir: str | None = None, sample_modulo: int | None = None,
             copy_name_suffix: str = "") -> dict:
    """Publica el CSV del día en el directorio observado por bronce.

    - `zip_dir`: carpeta con ZIP ya descargados (pruebas locales); si falta el ZIP se descarga de NOAA.
    - `sample_modulo`: muestreo por buque (crc32(MMSI) % m == 0) que conserva trayectorias completas.
    - `copy_name_suffix`: publica una copia con otro nombre (simula un reenvío del proveedor).
    """
    stem = file_stem(fecha)
    downloads = Path(zip_dir) if zip_dir else Path(lh.volume_dir("landing", "raw_ais")) / "downloads"
    archive = downloads / f"{stem}.zip"
    started = time.perf_counter()
    if not archive.exists():
        if zip_dir:
            # En pruebas locales solo se usan los ZIP autorizados y ya descargados: nunca se descarga.
            raise FileNotFoundError(f"ZIP del feed no disponible localmente: {archive}")
        download(noaa_url(fecha), archive)
    archive_sha = sha256_file(archive)

    staging = Path(lh.volume_dir("landing", "raw_ais")) / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    target_dir = Path(lh.landing_dir()) / fecha
    target_dir.mkdir(parents=True, exist_ok=True)
    final = target_dir / f"{stem}{copy_name_suffix}.csv"
    tmp = staging / f"{stem}{copy_name_suffix}.csv.tmp"

    # Caché local de la muestra (solo pruebas locales): clave = SHA-256 del ZIP + módulo de muestreo.
    # Repetir un día ya no descomprime ni filtra 8.8 M de filas: se copia el CSV ya muestreado.
    cache = None
    if zip_dir:
        cache_dir = Path(zip_dir).parent / "cache_muestras"
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache = cache_dir / f"{stem}_m{sample_modulo or 0}_{archive_sha[:12]}.csv"
    if cache is not None and cache.exists() and cache.with_suffix(".json").exists():
        meta = json.loads(cache.with_suffix(".json").read_text(encoding="utf-8"))
        rows_in, rows_out = meta["filas_origen"], meta["filas_publicadas"]
        shutil.copyfile(cache, tmp)
        from_cache = True
    else:
        rows_in, rows_out = _extract_sample(archive, tmp, sample_modulo)
        if cache is not None:
            shutil.copyfile(tmp, cache)
            cache.with_suffix(".json").write_text(json.dumps({"filas_origen": rows_in, "filas_publicadas": rows_out}),
                                                  encoding="utf-8")
        from_cache = False
    os.replace(tmp, final)
    return {
        "fecha": fecha, "zip": archive.name, "zip_bytes": archive.stat().st_size, "zip_sha256": archive_sha,
        "csv_publicado": str(final), "filas_origen": rows_in, "filas_publicadas": rows_out,
        "muestreo_modulo": sample_modulo, "desde_cache": from_cache,
        "segundos": round(time.perf_counter() - started, 2),
    }
