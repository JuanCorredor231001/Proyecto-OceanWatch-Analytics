"""Genera la evidencia de la prueba local en docs/evidencias/local/ (regenerable).

- resultados_locales.md: conteos por capa, métricas de calidad, cuarentena, contrato, ML y ejecuciones.
- tablero/*.csv: resultado de cada consulta de dashboards/sql contra la capa oro local.
- tablero/consola_vista_previa.png: vista previa de los paneles del tablero para la última fecha.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from pyspark.sql import functions as F

from oceanwatch.lakehouse import get_lakehouse

ROOT = Path("/workspace")
OUT = ROOT / "docs" / "evidencias" / "local"


def md_table(pdf, max_rows=40) -> str:
    pdf = pdf.head(max_rows)
    cols = list(pdf.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for row in pdf.itertuples(index=False):
        lines.append("| " + " | ".join("" if v is None else (f"{v:,.4f}".rstrip("0").rstrip(".") if isinstance(v, float) else (f"{v:,}" if isinstance(v, int) else str(v))) for v in row) + " |")
    return "\n".join(lines)


def local_sql(lh, sql: str) -> str:
    return re.sub(r"__CATALOGO__\.(\w+)\.(\w+)", lambda m: lh.name(m.group(1), m.group(2)), sql)


def main() -> None:
    lh = get_lakehouse()
    spark = lh.spark
    (OUT / "tablero").mkdir(parents=True, exist_ok=True)
    sections = ["# Resultados de la prueba local (generado por `scripts/evidencias_locales.py`)\n",
                "Muestra por buque `crc32(MMSI) % 10 == 0` (~10 % de los buques, trayectorias completas) más todos los MMSI no conformes. "
                "Días: 2023-06-01 (histórico) y 2023-06-08, 2023-06-09 (feed).\n"]

    # 1. Conteos por capa
    rows = []
    for schema, table, date_col in [("bronze", "ais_raw", "ingestion_date"), ("silver", "ais_posiciones", "ingestion_date"),
                                    ("silver", "ais_cuarentena", "ingestion_date"), ("gold", "trayectorias_diarias_buque", "event_date"),
                                    ("gold", "estadias_puerto", "event_date"), ("gold", "puertos_resumen_diario", "event_date"),
                                    ("gold", "trafico_celda_hora", "event_date"), ("gold", "anomalias_velocidad_diarias", "event_date"),
                                    ("ml", "features_velocidad_buque_dia", "event_date")]:
        for r in lh.read(schema, table).groupBy(date_col).count().collect():
            rows.append((f"{schema}.{table}", str(r[0]), r[1]))
    counts = spark.createDataFrame(rows, "tabla string, fecha string, filas long").groupBy("tabla").pivot("fecha").sum("filas").orderBy("tabla").toPandas()
    sections += ["## 1. Filas por capa y fecha\n", md_table(counts), ""]

    # 2. Métricas de calidad
    metrics = (lh.read("silver", "metricas_calidad").groupBy("ingestion_date", "regla", "severidad")
               .agg(F.sum("registros_evaluados").alias("evaluados"), F.sum("registros_fallidos").alias("fallidos"))
               .withColumn("pct", F.round(100 * F.col("fallidos") / F.col("evaluados"), 4))
               .orderBy("ingestion_date", "regla").toPandas())
    metrics["ingestion_date"] = metrics["ingestion_date"].astype(str)
    sections += ["## 2. Métricas de calidad por regla (`silver.metricas_calidad`)\n", md_table(metrics, 60), ""]

    q = (lh.read("silver", "ais_cuarentena").groupBy("ingestion_date", "motivo_principal").count()
         .orderBy("ingestion_date", F.desc("count")).toPandas())
    q["ingestion_date"] = q["ingestion_date"].astype(str)
    sections += ["## 3. Cuarentena por motivo principal\n", md_table(q), ""]
    e6 = (lh.read("silver", "ais_cuarentena").filter(F.array_contains("motivos", "E06_COORDENADAS_EN_AGUA"))
          .groupBy("MMSI", "VesselName", "VesselType").agg(F.count(F.lit(1)).alias("posiciones"),
          F.round(F.avg(F.col("LAT").cast("double")), 3).alias("lat_media"), F.round(F.avg(F.col("LON").cast("double")), 3).alias("lon_media"),
          F.max(F.col("SOG").cast("double")).alias("sog_max")).orderBy(F.desc("posiciones")).limit(15).toPandas())
    sections += ["### Compuerta E06 (en el agua): buques afectados\n", md_table(e6), ""]
    if lh.exists("reference", "waterway_cells_h3r5"):
        ww = lh.read("reference", "waterway_cells_h3r5").toPandas()
        sections += ["### Excepciones navegables (`reference.waterway_cells_h3r5`)\n", md_table(ww), ""]

    # 4. Ejecuciones del orquestador
    runs = (lh.read("ops", "pipeline_runs").orderBy("inicio")
            .select("run_id", F.col("fecha_datos").cast("string").alias("fecha"), "tarea", "intento", "estado",
                    F.round("duracion_s", 1).alias("seg"), F.substring("detalle", 1, 110).alias("detalle")).toPandas())
    runs["detalle"] = runs["detalle"].str.replace("|", "/", regex=False)
    sections += ["## 4. Bitácora de ejecuciones (`ops.pipeline_runs`)\n", md_table(runs, 120), ""]

    # 5. ML
    import mlflow

    from oceanwatch.ml import configure_mlflow, model_name
    configure_mlflow(lh)
    client = mlflow.MlflowClient()
    versions = client.search_model_versions(f"name='{model_name(lh)}'")
    ml_lines = []
    for v in versions:
        run = client.get_run(v.run_id)
        aliases = ",".join(a for a in ["champion"] if client.get_model_version_by_alias(v.name, a).version == v.version)
        ml_lines.append(f"- Modelo `{v.name}` v{v.version} (alias: `{aliases}`), run `{v.run_id}`; "
                        f"parámetros: {json.dumps({k: run.data.params[k] for k in ('algoritmo', 'contamination', 'n_estimators', 'fechas_entrenamiento')}, ensure_ascii=False)}; "
                        f"métricas: {json.dumps({k: round(val, 4) for k, val in run.data.metrics.items()})}")
    sections += ["## 5. MLflow: experimento y registro\n", "\n".join(ml_lines), ""]

    # 6. Consultas del tablero
    sections.append("## 6. Consultas del tablero sobre oro (`dashboards/sql`)\n")
    results = {}
    for sql_file in sorted((ROOT / "dashboards" / "sql").glob("*.sql")):
        pdf = spark.sql(local_sql(lh, sql_file.read_text(encoding="utf-8"))).toPandas()
        pdf.to_csv(OUT / "tablero" / f"{sql_file.stem}.csv", index=False)
        results[sql_file.stem] = pdf
        sections += [f"### {sql_file.stem} ({len(pdf):,} filas)\n", md_table(pdf.drop(columns=[c for c in pdf.columns if c in ("port_lat", "port_lon")]), 12), ""]

    preview(results)
    sections += ["![Vista previa local de la consola](tablero/consola_vista_previa.png)\n"]
    (OUT / "resultados_locales.md").write_text("\n".join(sections), encoding="utf-8")
    print("EVIDENCIAS_OK", OUT)


def preview(results) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    last = max(results["01_kpis_dia"]["event_date"])
    sel = {k: v[v["event_date"] == last] if "event_date" in v.columns else v for k, v in results.items()}
    kpi = sel["01_kpis_dia"].iloc[0]
    fig = plt.figure(figsize=(18, 11))
    fig.suptitle(f"OceanWatch - Consola diaria (vista previa local, oro)  |  fecha {last}  |  "
                 f"buques {kpi.buques_activos:,} · puertos {kpi.puertos_con_actividad:,} · en puerto al cierre "
                 f"{kpi.buques_en_puerto_al_cierre:,} · anomalías {kpi.anomalias:,}", fontsize=13)
    ax = fig.add_subplot(2, 2, 1)
    m = sel["02_mapa_intensidad_celda"]
    sc = ax.scatter(m.celda_lon, m.celda_lat, c=m.intensidad_log10, s=2, cmap="inferno")
    a = sel["06_anomalias_dia"]
    ax.scatter(a.lon_ultima, a.lat_ultima, marker="x", c="cyan", s=30, label="anomalías")
    ax.set_xlim(-180, -60); ax.set_ylim(10, 72); ax.set_title("Intensidad de tráfico, rejilla 0.05° desde H3 r8 (log10 posiciones)")
    ax.legend(loc="lower left"); fig.colorbar(sc, ax=ax)
    ax = fig.add_subplot(2, 2, 2)
    r = sel["04_ranking_puertos"].sort_values("ranking").head(15)[::-1]
    ax.barh(r.port_name, r.buques_distintos, color="#1f6feb"); ax.set_title("Ranking de puertos (buques distintos)")
    ax = fig.add_subplot(2, 2, 3)
    h = sel["03_trafico_por_hora"]
    for col in ["carga", "tanquero", "pasajeros", "pesca", "remolque", "recreo"]:
        ax.plot(h.hora, h[col], label=col)
    ax.set_title("Buques por hora y grupo (suma de celdas)"); ax.set_xlabel("hora UTC"); ax.legend(ncol=3)
    ax = fig.add_subplot(2, 2, 4); ax.axis("off")
    top = a.sort_values("anomaly_score", ascending=False).head(12)
    cell = [[str(x.vessel_name)[:22], x.vessel_type_group, f"{x.anomaly_score:.3f}", x.motivo_principal,
             f"{x.sog_max_kn}", f"{x.velocidad_implicita_max_kn}"] for x in top.itertuples()]
    if cell:
        t = ax.table(cellText=cell, colLabels=["buque", "grupo", "score", "variable", "SOG máx", "vel. impl. máx"], loc="center")
        t.auto_set_font_size(False); t.set_fontsize(8); t.scale(1, 1.3)
    ax.set_title("Anomalías de velocidad del día (top 12)")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(OUT / "tablero" / "consola_vista_previa.png", dpi=110)


if __name__ == "__main__":
    main()
