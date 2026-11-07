"""Demostración reproducible de tolerancia a fallos del streaming bronce -> plata (requisito 1).

Usa un lakehouse aislado (`local_lakehouse_ft`) para no tocar el principal. Escenarios:

  A. Caída abrupta del proceso (os._exit) tras confirmar el lote 0 de bronce; se reinicia el
     stream y continúa desde el checkpoint sin perder ni repetir archivos.
  B. Caída entre la escritura Delta y el commit del checkpoint (se elimina `commits/<último>`):
     Spark re-ejecuta el lote y el sumidero Delta lo descarta por idempotencia (txn por batchId).
  C. Excepción dentro de `foreachBatch` de plata después de escribir plata y antes de cuarentena
     y métricas: al reiniciar, el lote se reprocesa; `txnAppId/txnVersion` evita duplicar plata.
  D. Reenvío del proveedor: el archivo del 08-jun llega dos veces con otro nombre; la
     deduplicación por (MMSI, BaseDateTime) envía la copia completa a cuarentena (D01).

Invariantes verificados al final: bronce = filas publicadas; plata sin duplicados de clave;
bronce = plata + cuarentena; cada archivo aparece exactamente una vez en bronce.

Uso: python scripts/demo_tolerancia_fallos.py   (dentro del contenedor)
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = "/workspace/local_lakehouse_ft"
MAIN_ROOT = "/workspace/local_lakehouse"
ZIP_DIR = "/workspace/Datos/downloads"
SAMPLE = 20


def child(mode: str, extra_env: dict | None = None) -> int:
    env = dict(os.environ, OW_LOCAL_ROOT=ROOT, **(extra_env or {}))
    proc = subprocess.run([sys.executable, __file__, mode], env=env)
    return proc.returncode


def run_child(mode: str) -> None:
    from oceanwatch import streaming
    from oceanwatch.lakehouse import get_lakehouse

    lh = get_lakehouse(local_root=ROOT)
    if mode == "bronce_crash":
        streaming.run_bronze(lh, max_files_per_trigger=1, crash_after_batch=0)
    elif mode == "bronce":
        print(json.dumps(streaming.run_bronze(lh, max_files_per_trigger=1)))
    elif mode == "plata":
        print(json.dumps(streaming.run_silver(lh, max_files_per_trigger=2)))


def main() -> None:
    from pyspark.sql import functions as F

    from oceanwatch import feed, setup
    from oceanwatch.lakehouse import Lakehouse, build_local_spark
    from oceanwatch.streaming import BRONZE_STREAM

    shutil.rmtree(ROOT, ignore_errors=True)
    spark = build_local_spark("demo-tolerancia")
    lh = Lakehouse(spark=spark, local_root=ROOT)
    main_lh = Lakehouse(spark=spark, local_root=MAIN_ROOT)
    setup.create_all(lh)
    for ref in ["vessel_type_catalog", "ports", "port_cells_h3r8", "land_cells_h3r5", "waterway_cells_h3r5"]:
        lh.overwrite(main_lh.read("reference", ref), "reference", ref)

    published = [
        feed.land_day(lh, "2023-06-08", zip_dir=ZIP_DIR, sample_modulo=SAMPLE),
        feed.land_day(lh, "2023-06-09", zip_dir=ZIP_DIR, sample_modulo=SAMPLE),
        feed.land_day(lh, "2023-06-08", zip_dir=ZIP_DIR, sample_modulo=SAMPLE, copy_name_suffix="_reenvio"),
    ]
    expected = sum(p["filas_publicadas"] for p in published)
    report = {"archivos_publicados": {Path(p["csv_publicado"]).name: p["filas_publicadas"] for p in published},
              "filas_esperadas_bronce": expected}
    bronze_count = lambda: lh.read("bronze", "ais_raw").count()  # noqa: E731

    # A. caída abrupta tras el lote 0
    code = child("bronce_crash")
    after_crash = bronze_count()
    code2 = child("bronce")
    after_restart = bronze_count()
    report["A_caida_proceso"] = {"codigo_salida_caida": code, "bronce_tras_caida": after_crash,
                                 "codigo_reinicio": code2, "bronce_tras_reinicio": after_restart,
                                 "ok": code != 0 and after_crash < expected and after_restart == expected}

    # B. commit de checkpoint perdido para el último lote
    commits = Path(lh.checkpoint(BRONZE_STREAM)) / "commits"
    last = max(int(p.name) for p in commits.iterdir() if p.name.isdigit())
    (commits / str(last)).unlink()
    child("bronce")
    after_replay = bronze_count()
    report["B_commit_perdido"] = {"lote_reejecutado": last, "bronce_tras_reejecucion": after_replay,
                                  "ok": after_replay == expected}

    # C. falla dentro de foreachBatch de plata
    code = child("plata", {"OW_FALLA_PLATA_EN_LOTE": "1"})
    silver_mid = lh.read("silver", "ais_posiciones").count()
    quarantine_mid = lh.read("silver", "ais_cuarentena").count()
    code2 = child("plata")
    silver = lh.read("silver", "ais_posiciones")
    quarantine = lh.read("silver", "ais_cuarentena")
    s_count, q_count = silver.count(), quarantine.count()
    dup_keys = silver.groupBy("mmsi", "event_ts").count().filter("count > 1").count()
    report["C_falla_foreachBatch"] = {
        "codigo_salida_falla": code, "plata_tras_falla": silver_mid, "cuarentena_tras_falla": quarantine_mid,
        "codigo_reinicio": code2, "plata_final": s_count, "cuarentena_final": q_count,
        "claves_mmsi_ts_duplicadas_en_plata": dup_keys,
        "lotes_metricas": sorted(r[0] for r in lh.read("silver", "metricas_calidad").select("batch_id").distinct().collect()),
        "ok": code != 0 and dup_keys == 0 and s_count + q_count == expected,
    }

    # D. reenvío del archivo del 08-jun
    resend = quarantine.filter(F.col("_source_file").contains("_reenvio"))
    report["D_reenvio_duplicado"] = {
        "filas_reenvio": report["archivos_publicados"]["AIS_2023_06_08_reenvio.csv"],
        "reenvio_en_cuarentena_por_motivo": {r[0]: r[1] for r in resend.groupBy("motivo_principal").count().collect()},
        "reenvio_en_plata": silver.filter(F.col("_source_file").contains("_reenvio")).count(),
    }
    report["D_reenvio_duplicado"]["ok"] = report["D_reenvio_duplicado"]["reenvio_en_plata"] == 0

    per_file = {r[0].split("/")[-1]: r[1] for r in lh.read("bronze", "ais_raw").groupBy("_source_file").count().collect()}
    report["invariantes"] = {
        "bronce_por_archivo": per_file,
        "cada_archivo_una_vez": per_file == report["archivos_publicados"],
        "bronce_igual_plata_mas_cuarentena": bronze_count() == s_count + q_count,
    }
    report["todo_ok"] = all(v.get("ok", True) for v in report.values() if isinstance(v, dict)) and all(
        v for k, v in report["invariantes"].items() if k != "bronce_por_archivo")
    print("REPORTE_TOLERANCIA=" + json.dumps(report, ensure_ascii=False, indent=2))
    out = Path("/workspace/docs/evidencias/logs/tolerancia_fallos.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_child(sys.argv[1])
    else:
        main()
