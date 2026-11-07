# Evidencia requisito 1 - Ingesta en streaming y tolerancia a fallos

**Ejecución:** 2026-11-06, entorno local (contenedor `oceanwatch-local:e2`, Spark 4.2.0, Delta 4.4.1). Script: `scripts/demo_tolerancia_fallos.py`. Salida cruda: [`logs/tolerancia_fallos.json`](logs/tolerancia_fallos.json).

## Diseño evaluado

| Elemento | Implementación |
|---|---|
| Fuente | Archivos del feed `landing/raw_ais/feed/<fecha>/AIS_*.csv` (Auto Loader `cloudFiles` en Databricks; fuente `csv` en local) |
| Checkpoints | `ops/checkpoints/bronze_ais_raw` y `ops/checkpoints/silver_ais_posiciones` (offsets + commits) |
| Disparo | `trigger(availableNow=True)`: procesa lo pendiente y termina, apto para job programado |
| Bronce | Sumidero Delta append-only, idempotente por `batchId` |
| Plata | `foreachBatch` con `txnAppId`/`txnVersion = batchId` en plata, cuarentena y métricas |
| Duplicados | Por (MMSI, BaseDateTime): dentro del lote (`row_number` determinista) y entre lotes (anti-join contra `_silver_batch_id < batchId`) |

## Datos de la prueba

Tres archivos publicados en landing (muestra `crc32(MMSI) % 20 == 0` + MMSI no conformes), procesados con `maxFilesPerTrigger = 1` para forzar varios micro-lotes:

| Archivo | Filas |
|---|---:|
| `AIS_2023_06_08.csv` | 439,221 |
| `AIS_2023_06_09.csv` | 426,003 |
| `AIS_2023_06_08_reenvio.csv` (reenvío idéntico del proveedor) | 439,221 |
| **Total esperado en bronce** | **1,304,445** |

## Escenarios y resultados

| Escenario | Falla simulada | Observado | Resultado |
|---|---|---|---|
| **A. Caída abrupta del proceso** | `os._exit(137)` cuando se confirma el lote 0 de bronce (equivale a perder el driver) | Bronce tras la caída: 439,221 (solo el lote 0). Tras reiniciar: 1,304,445 = esperado | ✅ ningún archivo perdido ni repetido |
| **B. Commit de checkpoint perdido** | Se borra `commits/2`: la caída ocurrió después de escribir en Delta y antes de confirmar el checkpoint | Spark re-ejecuta el lote 2; bronce sigue en 1,304,445 | ✅ el sumidero Delta descarta el lote ya escrito |
| **C. Excepción en `foreachBatch` de plata** | `RuntimeError` en el lote 1 **después** de escribir plata y **antes** de cuarentena y métricas (`OW_FALLA_PLATA_EN_LOTE=1`) | Tras la falla: plata 117,855 y cuarentena 627 (falta la del lote 1). Tras reiniciar: plata 850,067 + cuarentena 454,378 = 1,304,445; **0 claves (MMSI, ts) duplicadas en plata**; métricas de los lotes 0 a 17 sin huecos | ✅ el lote se reprocesa y `txnVersion` evita duplicar plata |
| **D. Reenvío duplicado** | El archivo del 08-jun llega dos veces con otro nombre | Ninguna fila del reenvío llegó a plata. Por motivo: 431,991 `D01_DUPLICADO_EXACTO` + 6,362 `E01` + 867 `E06` + 1 `E07` = 439,221 | ✅ deduplicación por buque y timestamp |

**Invariantes finales:** cada archivo aparece exactamente una vez en bronce y `bronce = plata + cuarentena` (1,304,445 = 850,067 + 454,378). `todo_ok = true`.

## Hallazgo durante la prueba

En el primer intento, el escenario A falló al reiniciar con `SparkConcurrentModificationException ... commits/.1.crc already exists`. El sistema de archivos local de Hadoop (`ChecksumFileSystem`/`ChecksumFs`) escribe archivos `.crc` junto a cada commit, y la caída abrupta dejó uno huérfano. Los Volumes de Databricks usan almacenamiento de objetos, que no genera esos archivos. Por eso el entorno local se configuró con `RawLocalFileSystem`/`RawLocalFs` (`lakehouse.build_local_spark`). Con ese ajuste los cuatro escenarios pasan. El fallo queda registrado porque explica por qué el checkpoint debe vivir en almacenamiento confiable.

## Cómo repetir

```powershell
.\scripts\docker_run.ps1 python scripts/demo_tolerancia_fallos.py
```

En Databricks, la misma prueba consiste en cancelar el job durante `bronce_streaming` o `plata_calidad` y relanzarlo: las tareas tienen `max_retries: 2` y comparten los checkpoints del Volume `ops.checkpoints`.
