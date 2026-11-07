# Evidencia requisito 5 - Gobernanza

## 1. Esquemas con permisos por rol

El catálogo `oceanwatch_g07` separa las capas en esquemas (`setup.SCHEMAS`). El notebook `notebooks/12_gobernanza.py` otorga permisos **por esquema** a cuatro grupos parametrizables:

| Esquema | `oceanwatch_ingenieros` | `oceanwatch_cientificos` | `oceanwatch_analistas` | `oceanwatch_tablero` |
|---|---|---|---|---|
| `landing` (Volume) | ALL PRIVILEGES | - | - | - |
| `bronze` | ALL PRIVILEGES | - | - | - |
| `silver` | ALL PRIVILEGES | USE SCHEMA, SELECT | - | - |
| `gold` | ALL PRIVILEGES | USE SCHEMA, SELECT | USE SCHEMA, SELECT | USE SCHEMA, SELECT |
| `ml` | ALL PRIVILEGES | SELECT, MODIFY, CREATE TABLE, CREATE MODEL, EXECUTE | - | - |
| `reference` | ALL PRIVILEGES | USE SCHEMA, SELECT | USE SCHEMA, SELECT | - |
| `ops` | ALL PRIVILEGES | - | - | - |

- Todos reciben `USE CATALOG`. Ningún permiso se da a nivel de catálogo, porque Unity Catalog es aditivo y no tiene DENY.
- Cada `GRANT` se registra con estado OK/ERROR, y la evidencia la completan `SHOW GRANTS ON SCHEMA`.
- En la Entrega 1 (Free Edition) los `SHOW GRANTS` devolvían cero filas porque había un solo usuario. En el workspace institucional los grupos deben existir en la cuenta, por eso sus nombres son widgets.

## 2. Linaje

- Unity Catalog captura automáticamente el linaje de tablas y columnas de cada escritura del job: `landing → bronze.ais_raw → silver.* → gold.* → ml.* → gold.anomalias_velocidad_diarias`.
- El notebook 12 lo consulta en `system.access.table_lineage` y `system.access.column_lineage`. Se ve gráficamente en *Catalog Explorer → Lineage*.
- Linaje declarado en el código:
  - Cada fila de plata y de cuarentena conserva `_source_file`, `_bronze_ingested_at` y `_silver_batch_id`.
  - Cada fila de oro lleva `_gold_processed_at`.
  - Cada anomalía lleva `modelo` y `modelo_version`.
  - Cada intento del job queda en `ops.pipeline_runs`.
- **Verificación automática:** `validar_contrato` comprueba que oro solo tenga buques presentes en plata y que ninguna clave en cuarentena aparezca en plata. Resultado local: 0 violaciones en 01, 08 y 09 de junio.

## 3. Contrato de datos de oro

- Fuente: [`contracts/gold_contract.yml`](../../contracts/gold_contract.yml). Documento legible: [`docs/contrato_datos_oro.md`](../contrato_datos_oro.md).
- **Se hace cumplir, no solo se documenta:**
  1. el DDL de oro se genera del contrato, con comentarios de columna y la propiedad `oceanwatch.contrato.version = 1.0.0`;
  2. `conform()` aplica el esquema exacto en cada escritura;
  3. `validar_contrato` revisa por fecha esquema, nulos, rangos, dominios y clave primaria. Local: **102/102 chequeos OK** en cada fecha;
  4. el notebook 12 agrega etiquetas UC (`capa`, `contrato`, `contrato_version`, `propietario`, `clasificacion`) consultables en `information_schema.table_tags`.
- Prueba unitaria `test_ddl_de_oro_generado_desde_contrato`: la tabla creada tiene la partición y la versión del contrato.

## Pendiente en el workspace institucional

Ejecutar `12_gobernanza.py` con los grupos reales y guardar las salidas de `SHOW GRANTS`, `table_tags` y linaje.
