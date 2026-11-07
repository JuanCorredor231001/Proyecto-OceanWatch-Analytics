# Evidencia - Ejecución en el workspace de Databricks

**Workspace:** `https://dbc-18ae9c2d-d518.cloud.databricks.com` · **Usuario:** j.corredor2@uniandes.edu.co · **Catálogo:** `oceanwatch_g07` (el que ya contenía los activos de la Entrega 1) · **Cómputo:** serverless (jobs) y *Serverless Starter Warehouse* (tablero).

## Despliegue (Databricks Asset Bundle)

`databricks bundle validate` → *Validation OK*. `databricks bundle deploy -t institucional` → 77 archivos y 3 recursos creados:

| Recurso | Enlace |
|---|---|
| Job backfill | https://dbc-18ae9c2d-d518.cloud.databricks.com/jobs/888154898396381 |
| Job diario | https://dbc-18ae9c2d-d518.cloud.databricks.com/jobs/579383963389763 |
| Tablero AI/BI | https://dbc-18ae9c2d-d518.cloud.databricks.com/dashboardsv3/01f1d9f8b9351847a578d7ab844fbbe7/published |

El target `institucional` está en modo *development*: Databricks antepone `[dev j_corredor2]` a los nombres y deja la programación en pausa. Para dejar la ejecución programada activa hay que desplegar con `-t produccion` o reanudar el schedule desde la UI.

## Backfill histórico (corpus completo 01-07 jun 2023)

Corrida `1033973699100243`. Terminó **SUCCESS** después de tres reparaciones (*repair run*, que re-ejecuta solo las tareas fallidas). Cada falla corresponde a una diferencia de plataforma que la prueba local no podía mostrar:

| Tarea | Falla en Databricks | Corrección |
|---|---|---|
| `bronce_streaming` | En serverless, `recentProgress` devuelve `numInputRows = None`; el stream sí terminó | `_progress_summary` trata `None` como 0. El reintento no duplicó filas (checkpoint) |
| `plata_calidad` | El worker de `foreachBatch` de serverless no hereda `DATABRICKS_RUNTIME_VERSION`, así que el código se creyó local e intentó `import h3` | `IS_DATABRICKS` también detecta `/databricks/spark` |
| `ml_entrenar` (1) | El experimento `/Shared/oceanwatch/...` no tenía carpeta padre | `configure_mlflow` crea la carpeta (`WorkspaceClient().workspace.mkdirs`) |
| `ml_entrenar` (2) | Unity Catalog limita las etiquetas de versión de modelo a 256 caracteres | Las estadísticas de variables se leen del artefacto `diagnostico/estadisticas_features.json` del run |

### Resultados sobre 60.5 M posiciones

| Métrica | Valor | Contraste con la Entrega 1 |
|---|---:|---|
| Filas en bronce | 60,533,559 | = conteo de la Entrega 1 (D-01) |
| Filas en plata | 60,476,456 | |
| Filas en cuarentena | 57,103 | plata + cuarentena = bronce ✅ |
| E01 MMSI no conforme | 49,897 (0.0824 %) | idéntico al perfilamiento 02 |
| E06 fuera del agua | 5,032 (0.0083 %) | 65 celdas reclasificadas como navegables con el histórico completo |
| D01 duplicado exacto | 1,386 | 1,388 copias exactas en la Entrega 1; las otras 2 tienen MMSI no conforme y caen antes en E01 |
| D02 duplicado conflictivo | 284 | nuevo: misma clave (MMSI, ts) con contenido distinto |
| E07 SOG no plausible | 504 | |
| Trayectorias buque-día | 140,972 | |
| Anomalías marcadas (7 días) | 1,458 | contamination 1 % |

## Job diario (feed 08-jun)

Corrida `556259866884543`, `fecha=auto` → 2023-06-08: **SUCCESS en el primer intento**, 11/11 tareas, incluida la condición `hay_fecha_pendiente` y `refrescar_tablero`.

| Tarea | Duración |
|---|---:|
| resolver_fecha | 18 s |
| aterrizar_feed (descarga NOAA + publicación) | 70 s |
| bronce_streaming | 42 s |
| plata_calidad | 95 s |
| monitorear_calidad | 10 s |
| oro_modelo | 39 s |
| ml_features / ml_puntuar | 10 s / 42 s |
| validar_contrato | 13 s |
| refrescar_tablero | 21 s |

## Feed completo 08-14 jun (verificado el 2026-11-07)

Siete corridas manuales (`ONE_TIME`) del job diario con `fecha=auto`, el 2026-11-06 entre 19:03 y 19:51 (hora de Bogotá), todas **TERMINATED/SUCCESS** (5-10 min cada una): `556259866884543`, `322374998217722`, `207369622664612`, `729633314393099`, `686480947309748`, `160977464606576`, `218049826264968`. Consultas sobre el SQL warehouse:

| Día | Filas CSV (ZIP NOAA) | Bronce | Plata | Cuarentena | Buque-día | Anomalías | Puertos |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2023-06-08 | 8,920,444 | 8,920,444 | 8,912,350 | 8,094 | 20,462 | 248 | 513 |
| 2023-06-09 | 8,791,124 | 8,791,124 | 8,782,654 | 8,470 | 21,267 | 314 | 513 |
| 2023-06-10 | 7,794,117 | 7,794,117 | 7,786,343 | 7,774 | 21,619 | 326 | 512 |
| 2023-06-11 | 8,390,342 | 8,390,342 | 8,382,743 | 7,599 | 21,730 | 253 | 518 |
| 2023-06-12 | 8,797,495 | 8,797,495 | 8,790,746 | 6,749 | 20,291 | 173 | 512 |
| 2023-06-13 | 8,495,677 | 8,495,677 | 8,487,992 | 7,685 | 20,213 | 164 | 511 |
| 2023-06-14 | 8,858,291 | 8,858,291 | 8,850,579 | 7,712 | 20,591 | 162 | 518 |

- Bronce = filas del CSV en los 7 días: ningún lote se ingirió dos veces (checkpoint de Auto Loader).
- Plata + cuarentena = bronce en los 7 días: ningún registro se pierde entre capas.
- El 2026-11-07 se volvió a desplegar el bundle con los cambios de la Fase 0 (`bundle validate` OK; 3 recursos actualizados). Las alertas `on_failure` y de duración quedaron en `j.corredor2@uniandes.edu.co`.
- Causas de cuarentena 08-14 jun (`motivo_principal`, suman 54,083): `E01_MMSI_FORMATO` 45,882; `E06_COORDENADAS_EN_AGUA` 5,449; `D01_DUPLICADO_EXACTO` 1,550; `E07_SOG_PLAUSIBLE` 773; `D02_DUPLICADO_CONFLICTIVO` 429.
- `ops.pipeline_runs` solo se llena en el orquestador local. En el workspace, el historial por tarea y los reintentos se consultan en la UI de Jobs.

## Ejecución programada y fin del feed

Corrida [`576486414020707`](https://dbc-18ae9c2d-d518.cloud.databricks.com/?o=7474654323473441#job/579383963389763/run/576486414020707), **trigger `PERIODIC`**: la disparó el cron `0 0 6 * * ?` (America/Bogota) el 2026-11-07 a las 06:00, sin intervención manual. Duró 42 s y terminó **TERMINATED/SUCCESS**.

| Tarea | Estado |
|---|---|
| `resolver_fecha` | SUCCESS (30 s): no quedan días FEED sin oro → `NINGUNA` |
| `hay_fecha_pendiente` | SUCCESS, resultado de la condición = `false` |
| `aterrizar_feed` … `refrescar_tablero` (9 tareas) | `SKIPPED/EXCLUDED` |

La misma corrida demuestra la ejecución programada (requisito 3) y que el job no reprocesa ni falla cuando el feed se agota: la `condition_task` corta el DAG.

## Cierre (2026-11-07)

- Capturas del job, el tablero, el linaje y MLflow: [`capturas/`](capturas/), enlazadas desde las evidencias 08 a 11.
- `12_gobernanza.py` ejecutado con service principals por rol (corrida `159612244398872`, evidencia 10).
- Bono Delta Sharing: el share está creado, pero el destinatario está bloqueado por el metastore (evidencia 13).
- Sigue pendiente una corrida fallida en el workspace con reintentos y correo de alerta (evidencia 08).
