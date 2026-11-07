"""Genera dashboards/oceanwatch_consola.lvdash.json (Databricks AI/BI) desde dashboards/sql/*.sql.

Las consultas SQL son la fuente única: las mismas se ejecutan en local contra oro
(scripts/evidencias_locales.py) y en el tablero. Uso: python scripts/generar_tablero.py --catalogo oceanwatch_g07
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "dashboards" / "sql"

DATASETS = {
    "ds_kpis": ("KPIs del día", "01_kpis_dia.sql"),
    "ds_mapa": ("Intensidad por celda H3", "02_mapa_intensidad_celda.sql"),
    "ds_hora": ("Tráfico por hora", "03_trafico_por_hora.sql"),
    "ds_puertos": ("Ranking de puertos", "04_ranking_puertos.sql"),
    "ds_en_puerto": ("Buques en puerto", "05_buques_en_puerto.sql"),
    "ds_anomalias": ("Anomalías del día", "06_anomalias_dia.sql"),
}


def query(name, dataset, fields, disaggregated=True):
    return {"name": name, "query": {"datasetName": dataset, "disaggregated": disaggregated,
                                    "fields": [{"name": f, "expression": f"`{f}`"} for f in fields]}}


def counter(name, field, title):
    return {"name": name, "queries": [query("main_query", "ds_kpis", [field])],
            "spec": {"version": 2, "widgetType": "counter",
                     "encodings": {"value": {"fieldName": field, "displayName": title}},
                     "frame": {"showTitle": True, "title": title}}}


def table(name, dataset, columns, title):
    return {"name": name, "queries": [query("main_query", dataset, [c for c, _ in columns])],
            "spec": {"version": 1, "widgetType": "table",
                     "encodings": {"columns": [{"fieldName": c, "displayName": d} for c, d in columns]},
                     "frame": {"showTitle": True, "title": title}}}


def build(catalog: str) -> dict:
    datasets = []
    for key, (display, file) in DATASETS.items():
        sql = (SQL_DIR / file).read_text(encoding="utf-8").replace("__CATALOGO__", catalog)
        lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
        datasets.append({"name": key, "displayName": display, "queryLines": [line + "\n" for line in lines]})

    date_filter = {
        "name": "filtro_fecha",
        "queries": [{"name": f"fecha_{k}", "query": {"datasetName": k, "disaggregated": False, "fields": [
            {"name": "event_date", "expression": "`event_date`"},
            {"name": "event_date_associativity", "expression": "COUNT_IF(`associative_filter_predicate_group`)"}]}}
            for k in DATASETS],
        "spec": {"version": 2, "widgetType": "filter-single-select",
                 "encodings": {"fields": [{"fieldName": "event_date", "displayName": "Fecha (UTC)", "queryName": f"fecha_{k}"} for k in DATASETS]},
                 "frame": {"showTitle": True, "title": "Fecha del feed"}}}

    mapa = {"name": "mapa_intensidad",
            "queries": [query("main_query", "ds_mapa", ["centro_lat", "centro_lon", "posiciones", "intensidad_log10", "h3_r8"])],
            "spec": {"version": 2, "widgetType": "point-map",
                     "encodings": {"coordinates": {"latitude": {"fieldName": "centro_lat", "displayName": "Latitud"},
                                                   "longitude": {"fieldName": "centro_lon", "displayName": "Longitud"}},
                                   "color": {"fieldName": "intensidad_log10", "scale": {"type": "quantitative"},
                                             "displayName": "log10(posiciones)"},
                                   "extra": [{"fieldName": "posiciones"}, {"fieldName": "h3_r8"}]},
                     "frame": {"showTitle": True, "title": "Mapa de intensidad de tráfico (celdas H3 r8)"}}}

    ranking = {"name": "ranking_puertos",
               "queries": [query("main_query", "ds_puertos", ["port_name", "buques_distintos", "ranking"])],
               "spec": {"version": 3, "widgetType": "bar",
                        "encodings": {"x": {"fieldName": "buques_distintos", "scale": {"type": "quantitative"}, "displayName": "Buques distintos"},
                                      "y": {"fieldName": "port_name", "scale": {"type": "categorical", "sort": {"by": "x-reversed"}}, "displayName": "Puerto"}},
                        "frame": {"showTitle": True, "title": "Ranking de puertos (top 20)"}}}

    hora = {"name": "trafico_hora",
            "queries": [query("main_query", "ds_hora", ["hora", "posiciones"])],
            "spec": {"version": 3, "widgetType": "line",
                     "encodings": {"x": {"fieldName": "hora", "scale": {"type": "quantitative"}, "displayName": "Hora UTC"},
                                   "y": {"fieldName": "posiciones", "scale": {"type": "quantitative"}, "displayName": "Posiciones"}},
                     "frame": {"showTitle": True, "title": "Perfil horario del tráfico"}}}

    layout = [
        (date_filter, 0, 0, 2, 1),
        (counter("kpi_buques", "buques_activos", "Buques activos"), 2, 0, 1, 2),
        (counter("kpi_puertos", "puertos_con_actividad", "Puertos con actividad"), 3, 0, 1, 2),
        (counter("kpi_en_puerto", "buques_en_puerto_al_cierre", "Buques en puerto al cierre"), 4, 0, 1, 2),
        (counter("kpi_anomalias", "anomalias", "Anomalías del día"), 5, 0, 1, 2),
        (mapa, 0, 2, 4, 8),
        (ranking, 4, 2, 2, 8),
        (hora, 0, 10, 3, 5),
        (table("tabla_anomalias", "ds_anomalias", [("vessel_name", "Buque"), ("mmsi", "MMSI"), ("vessel_type_group", "Grupo"),
                                                   ("anomaly_score", "Puntaje"), ("motivo_principal", "Variable principal"),
                                                   ("sog_max_kn", "SOG máx (kn)"), ("velocidad_implicita_max_kn", "Vel. implícita máx (kn)")],
               "Anomalías de velocidad del día"), 3, 10, 3, 5),
        (table("tabla_en_puerto", "ds_en_puerto", [("port_name", "Puerto"), ("vessel_name", "Buque"), ("mmsi", "MMSI"),
                                                   ("vessel_type_group", "Grupo"), ("arribo_ts", "Arribo"), ("salida_ts", "Última posición"),
                                                   ("duracion_horas", "Horas"), ("estado", "Estado")],
               "Buques en puerto"), 0, 15, 6, 6),
    ]
    page = {"name": "consola", "displayName": "Consola diaria", "pageType": "PAGE_TYPE_CANVAS",
            "layout": [{"widget": w, "position": {"x": x, "y": y, "width": wd, "height": h}} for w, x, y, wd, h in layout]}
    return {"datasets": datasets, "pages": [page], "uiSettings": {"theme": {"widgetHeaderAlignment": "LEFT"}}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalogo", default="oceanwatch_g07")
    args = parser.parse_args()
    out = ROOT / "dashboards" / "oceanwatch_consola.lvdash.json"
    # newline="\n": en Windows evita reescribir el JSON con CRLF (diff limpio entre plataformas).
    out.write_text(json.dumps(build(args.catalogo), indent=2, ensure_ascii=False), encoding="utf-8", newline="\n")
    print(f"Tablero escrito en {out}")
