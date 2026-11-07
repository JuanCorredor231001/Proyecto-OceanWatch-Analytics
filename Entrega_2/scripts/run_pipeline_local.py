"""Orquestador local del job diario de OceanWatch (equivalente al job de Databricks en resources/).

Reproduce las propiedades exigidas por el requisito 3 sin depender del workspace:
  * DAG de tareas con dependencias (misma lista y nombres que `resources/oceanwatch_diario.job.yml`).
  * Parámetro `--fecha` (AAAA-MM-DD o `auto` = siguiente día pendiente del feed).
  * Reintentos por tarea con espera exponencial; las tareas dependientes se omiten si una falla.
  * Monitoreo: cada intento queda en `ops.pipeline_runs`; la tarea `monitorear_calidad` y
    `validar_contrato` fallan el job si se violan umbrales.
  * Ejecución programada: `--programar-cada SEG --max-ejecuciones N` dispara el job periódicamente.

Uso (dentro del contenedor, ver README):
  python scripts/run_pipeline_local.py --preparar
  python scripts/run_pipeline_local.py --backfill 2023-06-01 --muestreo 10
  python scripts/run_pipeline_local.py --fecha auto --programar-cada 30 --max-ejecuciones 2
  python scripts/run_pipeline_local.py --fecha 2023-06-01,2023-06-08,2023-06-09   # reproceso en una sola sesión
  python scripts/run_pipeline_local.py --fecha auto --zip-dir /workspace/Datos/feed_ausente  # falla persistente: el feed no llegó
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import traceback

from oceanwatch import feed, gold, ml, pipeline, setup, streaming
from oceanwatch.lakehouse import get_lakehouse


def build_dag(lh, args):
    """(tarea, dependencias, función(ctx) -> dict)."""
    ctx = {"fecha": args.fecha}

    def resolver(_):
        ctx["fecha"] = pipeline.resolve_date(lh, args.fecha)
        return {"fecha": ctx["fecha"]}

    def guard(fn):
        def run(_):
            if ctx["fecha"] == pipeline.NO_PENDING:
                return {"omitida": "sin fechas pendientes en el feed"}
            return fn(ctx["fecha"])
        return run

    return ctx, [
        ("resolver_fecha", [], resolver),
        ("aterrizar_feed", ["resolver_fecha"],
         guard(lambda f: feed.land_day(lh, f, zip_dir=args.zip_dir, sample_modulo=args.muestreo))),
        ("bronce_streaming", ["aterrizar_feed"], guard(lambda f: streaming.run_bronze(lh))),
        ("plata_calidad", ["bronce_streaming"], guard(lambda f: streaming.run_silver(lh))),
        ("monitorear_calidad", ["plata_calidad"],
         guard(lambda f: pipeline.monitor_quality(lh, f, max_pct_quarantine=args.umbral_cuarentena))),
        ("oro_modelo", ["monitorear_calidad"], guard(lambda f: gold.build_gold_for_date(lh, f))),
        ("ml_features", ["oro_modelo"], guard(lambda f: {"filas": ml.build_features(lh, f)})),
        ("ml_puntuar", ["ml_features"], guard(lambda f: ml.score(lh, f) if not args.sin_ml else {"omitida": True})),
        ("validar_contrato", ["ml_puntuar"], guard(lambda f: pipeline.validate_contract(lh, f))),
    ]


def build_backfill_dag(lh, args):
    """Carga histórica (job `oceanwatch_backfill_historico`): varios días, excepciones navegables y entrenamiento."""
    dates = [d.strip() for d in args.backfill.split(",") if d.strip()]
    ctx = {"fecha": dates[0]}

    def each(fn):
        return lambda _: {d: fn(d) for d in dates}

    return ctx, [
        ("aterrizar_historico", [], each(lambda f: feed.land_day(lh, f, zip_dir=args.zip_dir, sample_modulo=args.muestreo))),
        ("bronce_streaming", ["aterrizar_historico"], lambda _: streaming.run_bronze(lh)),
        ("excepciones_navegables", ["bronce_streaming"], lambda _: setup.build_waterway_exceptions(lh, dates)),
        ("plata_calidad", ["excepciones_navegables"], lambda _: streaming.run_silver(lh)),
        ("monitorear_calidad", ["plata_calidad"],
         each(lambda f: pipeline.monitor_quality(lh, f, max_pct_quarantine=args.umbral_cuarentena))),
        ("oro_modelo", ["monitorear_calidad"], each(lambda f: gold.build_gold_for_date(lh, f))),
        ("ml_features", ["oro_modelo"], each(lambda f: {"filas": ml.build_features(lh, f)})),
        ("ml_entrenar", ["ml_features"], lambda _: ml.train(lh, dates)),
        ("ml_puntuar", ["ml_entrenar"], each(lambda f: ml.score(lh, f))),
        ("validar_contrato", ["ml_puntuar"], each(lambda f: pipeline.validate_contract(lh, f))),
    ]


def run_job(lh, args) -> bool:
    run_id = pipeline.new_run_id()
    ctx, dag = build_backfill_dag(lh, args) if args.backfill else build_dag(lh, args)
    state: dict[str, str] = {}
    print(f"\n=== Ejecución {run_id} | fecha={args.backfill or args.fecha} ===", flush=True)
    for task, deps, fn in dag:
        if any(state.get(d) != "EXITOSO" for d in deps):
            state[task] = "OMITIDO"
            now = dt.datetime.now(dt.timezone.utc)
            pipeline.log_task(lh, run_id, ctx["fecha"], task, 0, "OMITIDO", now, now, {"motivo": "dependencia fallida"})
            print(f"  - {task:<20} OMITIDO (dependencia fallida)", flush=True)
            continue
        for attempt in range(1, args.reintentos + 2):
            started = dt.datetime.now(dt.timezone.utc)
            try:
                if args.inyectar_falla == task and attempt == 1:
                    raise RuntimeError(f"Falla transitoria inyectada en {task} (intento 1)")
                detail = fn(ctx) or {}
                finished = dt.datetime.now(dt.timezone.utc)
                pipeline.log_task(lh, run_id, ctx["fecha"], task, attempt, "EXITOSO", started, finished, detail)
                state[task] = "EXITOSO"
                secs = (finished - started).total_seconds()
                print(f"  ✓ {task:<20} intento={attempt} {secs:7.1f}s {json.dumps(detail, default=str, ensure_ascii=False)[:300]}", flush=True)
                break
            except Exception as error:  # noqa: BLE001
                finished = dt.datetime.now(dt.timezone.utc)
                pipeline.log_task(lh, run_id, ctx["fecha"], task, attempt, "FALLIDO", started, finished,
                                  {"error": str(error)[:4000], "traza": traceback.format_exc()[-4000:]})
                print(f"  ✗ {task:<20} intento={attempt} ERROR: {str(error)[:300]}", flush=True)
                if attempt <= args.reintentos:
                    wait = args.espera_reintento * (2 ** (attempt - 1))
                    print(f"    reintento en {wait}s", flush=True)
                    time.sleep(wait)
                else:
                    state[task] = "FALLIDO"
    ok = all(v == "EXITOSO" for v in state.values())
    print(f"=== Resultado {run_id}: {'EXITOSO' if ok else 'FALLIDO'} (fecha={ctx['fecha']}) ===", flush=True)
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fecha", default="auto", help="auto, AAAA-MM-DD o lista separada por comas (una sesión Spark)")
    parser.add_argument("--backfill", default=None, help="fechas históricas separadas por coma (carga inicial)")
    parser.add_argument("--zip-dir", default="/workspace/Datos/downloads",
                        help="carpeta con los ZIP NOAA; una carpeta vacía o inexistente simula que el feed no llegó")
    parser.add_argument("--muestreo", type=int, default=10, help="crc32(MMSI) %% m == 0; 0 = día completo")
    parser.add_argument("--reintentos", type=int, default=2)
    parser.add_argument("--espera-reintento", type=int, default=5)
    parser.add_argument("--umbral-cuarentena", type=float, default=5.0)
    parser.add_argument("--inyectar-falla", default=None, help="tarea que falla en su primer intento")
    parser.add_argument("--sin-ml", action="store_true")
    parser.add_argument("--preparar", action="store_true", help="crea tablas y referencias y termina")
    parser.add_argument("--puertos", default="/workspace/Datos/reference/natural_earth/ne_10m_ports.zip")
    parser.add_argument("--natural-earth", default="/workspace/Datos/reference/natural_earth")
    parser.add_argument("--programar-cada", type=int, default=0, help="segundos entre ejecuciones programadas")
    parser.add_argument("--max-ejecuciones", type=int, default=1)
    args = parser.parse_args()
    args.muestreo = args.muestreo or None

    lh = get_lakehouse()
    if args.preparar:
        created = setup.create_all(lh)
        print("Tablas:", created)
        print("Referencias:", setup.build_reference_tables(lh, args.puertos, args.natural_earth))
        print("Calendario feed:", pipeline.seed_feed_calendar(lh))
        return 0

    results = []
    for i in range(args.max_ejecuciones):
        if i > 0:
            print(f"\n[programador] esperando {args.programar_cada}s hasta la siguiente ejecución programada", flush=True)
            time.sleep(args.programar_cada)
        print(f"[programador] disparo {i + 1}/{args.max_ejecuciones} a las {dt.datetime.now(dt.timezone.utc).isoformat()}", flush=True)
        # Varias fechas separadas por coma se procesan en serie dentro de la MISMA sesión Spark:
        # se evita pagar el arranque de JVM + Spark + Delta (~15-20 s) por cada día.
        fechas = [f.strip() for f in args.fecha.split(",")] if not args.backfill else [args.fecha]
        for fecha in fechas:
            args.fecha = fecha
            results.append(run_job(lh, args))
        args.fecha = ",".join(fechas)
    return 0 if all(results) else 1


if __name__ == "__main__":
    os.environ.setdefault("OW_LOCAL_ROOT", "/workspace/local_lakehouse")
    sys.exit(main())
