"""Tareas del job end-to-end. Cada función es una tarea del DAG (Databricks Jobs o el orquestador local).

DAG diario (parámetro `fecha`, 'auto' = siguiente día pendiente del feed):

  resolver_fecha -> aterrizar_feed -> bronce_streaming -> plata_calidad -> monitorear_calidad
      -> oro_modelo -> ml_features -> ml_puntuar -> validar_contrato
"""

from __future__ import annotations

import datetime as dt
import json
import uuid

from pyspark.sql import functions as F

from .lakehouse import Lakehouse

NO_PENDING = "NINGUNA"


# ---------------------------------------------------------------------------- tareas
def seed_feed_calendar(lh: Lakehouse) -> int:
    from .feed import FEED_DATES, HISTORICAL_DATES, noaa_url

    rows = [(dt.date.fromisoformat(d), "HISTORICO", noaa_url(d)) for d in HISTORICAL_DATES]
    rows += [(dt.date.fromisoformat(d), "FEED", noaa_url(d)) for d in FEED_DATES]
    df = lh.spark.createDataFrame(rows, "fecha_datos date, rol string, url_origen string")
    lh.overwrite(df, "ops", "feed_calendar")
    return len(rows)


def resolve_date(lh: Lakehouse, fecha: str | None) -> str:
    """'auto' -> primer día FEED del calendario que aún no tiene trayectorias en oro."""
    if fecha and fecha.lower() != "auto":
        dt.date.fromisoformat(fecha)  # valida el formato
        return fecha
    done = lh.read("gold", "trayectorias_diarias_buque").select("event_date").distinct()
    pending = (lh.read("ops", "feed_calendar").filter(F.col("rol") == "FEED")
               .join(done, F.col("fecha_datos") == F.col("event_date"), "left_anti")
               .agg(F.min("fecha_datos")).first()[0])
    return str(pending) if pending else NO_PENDING


def monitor_quality(lh: Lakehouse, fecha: str, max_pct_quarantine: float = 5.0) -> dict:
    """Compuerta operativa: reconcilia bronce = plata + cuarentena y limita la tasa de cuarentena."""
    day = F.lit(fecha).cast("date")
    bronze = lh.read("bronze", "ais_raw").filter(F.col("ingestion_date") == day).count()
    silver = lh.read("silver", "ais_posiciones").filter(F.col("ingestion_date") == day).count()
    quarantine_df = lh.read("silver", "ais_cuarentena").filter(F.col("ingestion_date") == day)
    quarantine = quarantine_df.count()
    gates = quarantine_df.filter(~F.col("motivo_principal").startswith("D0")).count()
    dups = quarantine - gates
    pct_gates = round(100.0 * gates / bronze, 4) if bronze else 0.0
    by_reason = {r["motivo_principal"]: r["n"] for r in
                 quarantine_df.groupBy("motivo_principal").agg(F.count(F.lit(1)).alias("n")).collect()}
    result = {
        "fecha": fecha, "filas_bronce": bronze, "filas_plata": silver, "filas_cuarentena": quarantine,
        "cuarentena_compuertas": gates, "cuarentena_duplicados": dups, "pct_cuarentena_compuertas": pct_gates,
        "cuarentena_por_motivo": by_reason, "reconciliacion_ok": bronze == silver + quarantine,
    }
    problems = []
    if bronze == 0:
        problems.append("no llegaron filas a bronce para la fecha")
    if silver == 0:
        problems.append("ninguna fila pasó a plata")
    if bronze != silver + quarantine:
        problems.append(f"reconciliación fallida: bronce={bronze} plata+cuarentena={silver + quarantine}")
    if pct_gates > max_pct_quarantine:
        problems.append(f"cuarentena {pct_gates}% supera el umbral {max_pct_quarantine}%")
    if problems:
        raise RuntimeError("Monitoreo de calidad: " + "; ".join(problems) + f" | {json.dumps(result)}")
    return result


def validate_contract(lh: Lakehouse, fecha: str) -> dict:
    """Valida el contrato de oro para la fecha y que oro solo contenga buques/fechas presentes en plata."""
    from .contract import gold_tables, validate_table

    day = F.lit(fecha).cast("date")
    results = []
    for table in gold_tables():
        results += validate_table(lh.read("gold", table).filter(F.col("event_date") == day), table)

    silver_keys = lh.read("silver", "ais_posiciones").filter(F.col("event_date") == day).select("mmsi").distinct()
    orphan = (lh.read("gold", "trayectorias_diarias_buque").filter(F.col("event_date") == day)
              .select("mmsi").join(silver_keys, "mmsi", "left_anti").count())
    results.append({"tabla": "trayectorias_diarias_buque", "chequeo": "solo_desde_plata", "ok": orphan == 0,
                    "detalle": f"mmsi_sin_respaldo_en_plata={orphan}"})
    leaked = (lh.read("silver", "ais_cuarentena").filter(F.col("ingestion_date") == day)
              .filter(~F.col("motivo_principal").startswith("D0"))
              .select(F.trim("MMSI").alias("mmsi"),
                      F.expr("try_to_timestamp(trim(BaseDateTime), \"yyyy-MM-dd'T'HH:mm:ss\")").alias("event_ts"))
              .join(lh.read("silver", "ais_posiciones").filter(F.col("event_date") == day), ["mmsi", "event_ts"], "left_semi")
              .count())
    results.append({"tabla": "silver.ais_posiciones", "chequeo": "cuarentena_no_llega_a_plata_ni_oro", "ok": leaked == 0,
                    "detalle": f"claves_en_cuarentena_presentes_en_plata={leaked}"})
    failed = [r for r in results if not r["ok"]]
    summary = {"fecha": fecha, "chequeos": len(results), "fallidos": len(failed), "detalle_fallidos": failed}
    if failed:
        raise RuntimeError("Contrato de oro incumplido: " + json.dumps(summary, default=str))
    return summary


# ------------------------------------------------------------------------- bitácora
def log_task(lh: Lakehouse, run_id: str, fecha: str | None, task: str, attempt: int, status: str,
             started: dt.datetime, finished: dt.datetime, detail: dict | str) -> None:
    fecha_date = dt.date.fromisoformat(fecha) if fecha and fecha != NO_PENDING and fecha.lower() != "auto" else None
    row = [(run_id, fecha_date, task, attempt, status, started, finished,
            round((finished - started).total_seconds(), 3),
            detail if isinstance(detail, str) else json.dumps(detail, default=str, ensure_ascii=False)[:20000])]
    schema = ("run_id string, fecha_datos date, tarea string, intento int, estado string, inicio timestamp, "
              "fin timestamp, duracion_s double, detalle string")
    lh.append(lh.spark.createDataFrame(row, schema), "ops", "pipeline_runs")


def new_run_id() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
