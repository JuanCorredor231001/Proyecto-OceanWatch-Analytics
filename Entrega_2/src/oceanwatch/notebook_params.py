"""Parámetros comunes de los notebooks de Databricks (widgets + task values del job)."""

from __future__ import annotations

from .pipeline import NO_PENDING, resolve_date


def widget(dbutils, name: str, default: str) -> str:
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name).strip()


def fechas(dbutils, lh) -> list[str]:
    """Fechas a procesar: task value de `resolver_fecha` > widget `fecha` ('auto', 'AAAA-MM-DD' o lista)."""
    raw = widget(dbutils, "fecha", "auto")
    try:
        resolved = dbutils.jobs.taskValues.get(taskKey="resolver_fecha", key="fecha", debugValue="")
    except Exception:  # noqa: BLE001  (fuera de un job no hay task values)
        resolved = ""
    value = resolved or raw
    if value.lower() == "auto":
        value = resolve_date(lh, "auto")
    if value == NO_PENDING:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]
