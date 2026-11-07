# Databricks notebook source
# MAGIC %md
# MAGIC # 12 - Gobernanza: permisos por rol, linaje y contrato de oro
# MAGIC
# MAGIC **Objetivo.** Requisito 5: (1) esquemas con permisos por rol, (2) linaje capturado por Unity Catalog y (3) documentación del contrato de datos de oro aplicada como metadatos.
# MAGIC
# MAGIC **Roles (grupos de cuenta).** Los nombres se parametrizan porque el workspace institucional define sus propios grupos:
# MAGIC
# MAGIC | Rol | Grupo por defecto | Acceso |
# MAGIC |---|---|---|
# MAGIC | Ingeniería de datos | `oceanwatch_ingenieros` | Dueño operativo: todos los privilegios en todas las capas y Volumes |
# MAGIC | Ciencia de datos | `oceanwatch_cientificos` | Lectura de plata, oro y referencias; escritura en `ml` (tablas y modelos) |
# MAGIC | Analítica / negocio | `oceanwatch_analistas` | Solo lectura de oro y referencias |
# MAGIC | Tablero | `oceanwatch_tablero` | Solo lectura de oro (identidad del dashboard publicado) |
# MAGIC
# MAGIC Bronce, `landing` y `ops` quedan restringidos a ingeniería (principio de mínimo privilegio). Unity Catalog es aditivo (no hay DENY): por eso los permisos se otorgan por esquema y nunca a nivel de catálogo completo.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from oceanwatch.contract import gold_tables, load_contract
from oceanwatch.lakehouse import get_lakehouse
from oceanwatch.notebook_params import widget

CATALOG = widget(dbutils, "catalogo", "oceanwatch_g07")
G_ING = widget(dbutils, "grupo_ingenieros", "oceanwatch_ingenieros")
G_CIE = widget(dbutils, "grupo_cientificos", "oceanwatch_cientificos")
G_ANA = widget(dbutils, "grupo_analistas", "oceanwatch_analistas")
G_TAB = widget(dbutils, "grupo_tablero", "oceanwatch_tablero")
lh = get_lakehouse(spark, catalog=CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Permisos por rol
# MAGIC Cada `GRANT` se ejecuta por separado; si un grupo no existe en la cuenta se reporta sin detener el notebook.

# COMMAND ----------

C = CATALOG
GRANTS = [
    f"GRANT USE CATALOG ON CATALOG {C} TO `{g}`" for g in (G_ING, G_CIE, G_ANA, G_TAB)
] + [
    f"GRANT ALL PRIVILEGES ON SCHEMA {C}.{s} TO `{G_ING}`" for s in ("landing", "reference", "bronze", "silver", "gold", "ml", "ops")
] + [
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.{s} TO `{G_CIE}`" for s in ("silver", "gold", "reference")
] + [
    f"GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MODEL, EXECUTE ON SCHEMA {C}.ml TO `{G_CIE}`",
] + [
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.{s} TO `{G_ANA}`" for s in ("gold", "reference")
] + [
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.gold TO `{G_TAB}`",
]

estado = []
for stmt in GRANTS:
    try:
        spark.sql(stmt)
        estado.append((stmt, "OK", ""))
    except Exception as error:  # noqa: BLE001
        estado.append((stmt, "ERROR", str(error).splitlines()[0][:200]))
display(spark.createDataFrame(estado, "sentencia string, estado string, detalle string"))

# COMMAND ----------

for s in ("bronze", "silver", "gold", "ml"):
    display(spark.sql(f"SHOW GRANTS ON SCHEMA {C}.{s}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Contrato de oro como metadatos (comentarios, propiedades y etiquetas)
# MAGIC Los comentarios de columnas ya se crean desde `contracts/gold_contract.yml` en el DDL; aquí se agregan etiquetas consultables en `information_schema`.

# COMMAND ----------

contrato = load_contract()["contrato"]
for table in gold_tables():
    fqn = lh.name("gold", table)
    spark.sql(f"""ALTER TABLE {fqn} SET TAGS ('capa' = 'oro', 'contrato' = '{contrato['id']}',
                  'contrato_version' = '{contrato['version']}', 'propietario' = '{G_ING}',
                  'clasificacion' = 'uso_interno')""")
display(spark.sql(f"""
    SELECT table_name, tag_name, tag_value FROM {C}.information_schema.table_tags
    WHERE schema_name = 'gold' ORDER BY table_name, tag_name"""))
display(spark.sql(f"""
    SELECT table_name, column_name, data_type, comment FROM {C}.information_schema.columns
    WHERE table_schema = 'gold' ORDER BY table_name, ordinal_position"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Linaje (Unity Catalog)
# MAGIC Unity Catalog captura automáticamente el linaje de tablas y columnas de las escrituras del job. Se consulta en las tablas de sistema (requiere `system.access` habilitado) y se ve gráficamente en *Catalog Explorer > Lineage*.

# COMMAND ----------

try:
    display(spark.sql(f"""
        SELECT DISTINCT source_table_full_name, target_table_full_name, entity_type, max(event_time) AS ultimo_evento
        FROM system.access.table_lineage
        WHERE target_table_catalog = '{C}' OR source_table_catalog = '{C}'
        GROUP BY ALL ORDER BY target_table_full_name"""))
    display(spark.sql(f"""
        SELECT source_table_name, source_column_name, target_table_name, target_column_name
        FROM system.access.column_lineage
        WHERE target_table_catalog = '{C}' AND target_table_schema = 'gold'
        GROUP BY ALL ORDER BY target_table_name, target_column_name LIMIT 200"""))
except Exception as error:  # noqa: BLE001
    print("Tablas de sistema de linaje no disponibles para este usuario:", str(error).splitlines()[0])
