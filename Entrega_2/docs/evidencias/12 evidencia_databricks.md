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

## Pendiente

- Capturas de pantalla del grafo del job, del tablero publicado y del linaje en Catalog Explorer, para la sustentación.
- Ejecutar `12_gobernanza.py` con los grupos reales del workspace.
- Correr el job diario para 09-14 jun (cada ejecución procesa el siguiente día pendiente).
