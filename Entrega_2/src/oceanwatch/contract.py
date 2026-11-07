"""Contrato de datos de oro: DDL, conformación de columnas y validación automática."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .schemas import create_table_ddl

CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contracts" / "gold_contract.yml"


@lru_cache(maxsize=1)
def load_contract(path: str | None = None) -> dict:
    with open(path or CONTRACT_PATH, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def table_spec(table: str) -> dict:
    return load_contract()["tablas"][table]


def gold_tables() -> list[str]:
    return list(load_contract()["tablas"].keys())


def gold_ddl(name: str, table: str, with_comments: bool = True) -> str:
    spec = table_spec(table)
    columns = [(c["nombre"], c["tipo"], c["descripcion"]) for c in spec["columnas"]]
    contract = load_contract()["contrato"]
    comment = f"{spec['descripcion']} Contrato {contract['id']} v{contract['version']}."
    return create_table_ddl(name, columns, comment, partition_by=spec.get("particion"),
                            properties={"oceanwatch.contrato.version": contract["version"],
                                        "oceanwatch.capa": "oro"},
                            with_comments=with_comments)


def conform(df: DataFrame, table: str) -> DataFrame:
    """Selecciona y tipifica exactamente las columnas del contrato, en su orden."""
    spec = table_spec(table)
    missing = [c["nombre"] for c in spec["columnas"] if c["nombre"] not in df.columns]
    if missing:
        raise ValueError(f"{table}: columnas del contrato ausentes en el cálculo: {missing}")
    return df.select(*[F.col(c["nombre"]).cast(c["tipo"]).alias(c["nombre"]) for c in spec["columnas"]])


def validate_table(df: DataFrame, table: str) -> list[dict]:
    """Verifica esquema, nulos, rangos, dominios y unicidad de la clave primaria."""
    spec = table_spec(table)
    results = []
    expected = [(c["nombre"], c["tipo"]) for c in spec["columnas"]]
    actual = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    results.append({"tabla": table, "chequeo": "esquema", "ok": expected == actual,
                    "detalle": "" if expected == actual else f"esperado={expected} real={actual}"})

    aggs = [F.count(F.lit(1)).alias("__filas")]
    for c in spec["columnas"]:
        name = c["nombre"]
        if not c.get("nulo", True):
            aggs.append(F.sum(F.col(name).isNull().cast("long")).alias(f"null__{name}"))
        if "minimo" in c:
            aggs.append(F.sum((F.col(name) < c["minimo"]).cast("long")).alias(f"min__{name}"))
        if "maximo" in c:
            aggs.append(F.sum((F.col(name) > c["maximo"]).cast("long")).alias(f"max__{name}"))
        if "valores" in c:
            aggs.append(F.sum((~F.col(name).isin(c["valores"])).cast("long")).alias(f"dom__{name}"))
    pk = spec["clave_primaria"]
    stats = df.agg(*aggs, F.count_distinct(*[F.col(k) for k in pk]).alias("__pk_distintas")).first().asDict()
    total = stats.pop("__filas")
    pk_distinct = stats.pop("__pk_distintas")
    for key, value in stats.items():
        kind, col = key.split("__", 1)
        label = {"null": "no_nulo", "min": "minimo", "max": "maximo", "dom": "dominio"}[kind]
        results.append({"tabla": table, "chequeo": f"{label}:{col}", "ok": (value or 0) == 0,
                        "detalle": f"violaciones={value or 0} de {total}"})
    results.append({"tabla": table, "chequeo": f"clave_primaria:{'+'.join(pk)}", "ok": pk_distinct == total,
                    "detalle": f"filas={total} claves_distintas={pk_distinct}"})
    return results
