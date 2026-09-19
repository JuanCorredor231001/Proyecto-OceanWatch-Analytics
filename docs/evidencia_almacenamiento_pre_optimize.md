# Evidencia inicial — requisito 4, antes de `OPTIMIZE`

Fecha: 2026-09-15. Propósito: historial temporal por `MMSI` y `BaseDateTime`.

## Conjunto comparable

Ambos artefactos contienen el mismo conjunto D-07: **60,482,276** filas. Antes de ambas escrituras se eliminaron duplicados exactos y MMSI no conformes.

Unity Catalog Free Edition rechazó una tabla administrada `USING PARQUET` porque solo admite Delta. La línea base Parquet se conservó como archivos en el Volume gobernado `oceanwatch_g07.analytics.benchmark_files`; Delta quedó como la tabla administrada `oceanwatch_g07.analytics.ais_positions_d07_delta`, con comentario de procedencia y D-07. Un Volume no puede registrarse simultáneamente como tabla UC.

## Tamaño y archivos iniciales

| Artefacto | Bytes | Archivos |
|---|---:|---:|
| Parquet por ruta | 2,155,133,156 | 57 |
| Delta pre-OPTIMIZE | 1,454,406,725 | 24 |

La diferencia de bytes entre Parquet y Delta mezcla formato, compresión, metadatos, escritura y número inicial de archivos. Por eso no aísla el efecto de clustering ni de `OPTIMIZE`. La comparación más limpia para ese efecto es Delta pre-OPTIMIZE frente a Delta post-OPTIMIZE: mismo formato y mismo contenido lógico.

## Misma consulta selectiva

Filtro: MMSI `367638030`, `367438630`; `BaseDateTime >= 2023-06-01T00:00:00` y `< 2023-06-08T00:00:00`; salida ordenada por `MMSI, BaseDateTime`. Devuelve 11,209 filas en ambos formatos.

| Medición | Tiempo (s) |
|---|---:|
| Parquet R1 | 1.434 |
| Delta pre-OPTIMIZE R1 | 1.399 |
| Delta pre-OPTIMIZE R2 | 0.764 |
| Parquet R2 | 0.733 |

El orden se alternó y la primera medición no mostró una ventaja concluyente por tiempo. Los lectores y planes reportaron 57 archivos Parquet y 4 archivos Delta alcanzados por la consulta selectiva; el plan conserva filtros `MMSI` y `BaseDateTime` y usa Photon.

## Caso amplio sin MMSI

La agregación semanal por `VesselType` devolvió 73 filas.

| Medición | Tiempo (s) |
|---|---:|
| Delta pre-OPTIMIZE R1 | 1.894 |
| Parquet R1 | 1.048 |
| Parquet R2 | 0.793 |
| Delta pre-OPTIMIZE R2 | 1.202 |

El resultado deja visible el trade-off: antes de `OPTIMIZE`, el layout Delta por MMSI no favorece necesariamente un acceso amplio sin MMSI.

## Delta post-OPTIMIZE

Se ejecutó `OPTIMIZE oceanwatch_g07.analytics.ais_positions_d07_delta` sin `ZORDER BY`, pues la tabla fue creada con liquid clustering `CLUSTER BY (MMSI, BaseDateTime)`. Duró **7.523 s**. Sus métricas informaron `numFilesAdded=0`, `numFilesRemoved=0` y `partitionsOptimized=0`: no hubo reescritura física.

`DESCRIBE DETAIL` se mantuvo en **1,454,406,725 bytes** y **24 archivos**. Las diferencias de tiempo posteriores se interpretan como variación de ejecución o calentamiento de sesión, no como una mejora atribuible a `OPTIMIZE`.

| Consulta | Etapa | Tiempo R1/R2 (s) | Promedio (s) | Archivos alcanzados | Bytes almacenados |
|---|---|---:|---:|---:|---:|
| Selectiva por MMSI | Parquet base | 1.434 / 0.733 | 1.084 | 57 | 2,155,133,156 |
| Selectiva por MMSI | Delta pre-OPTIMIZE | 1.399 / 0.764 | 1.082 | 4 | 1,454,406,725 |
| Selectiva por MMSI | Delta post-OPTIMIZE | 0.843 / 0.690 | 0.767 | 4 | 1,454,406,725 |
| Amplia por `VesselType` | Parquet base | 1.048 / 0.793 | 0.920 | 57 | 2,155,133,156 |
| Amplia por `VesselType` | Delta pre-OPTIMIZE | 1.894 / 1.202 | 1.548 | 24 | 1,454,406,725 |
| Amplia por `VesselType` | Delta post-OPTIMIZE | 1.011 / 0.980 | 0.996 | 24 | 1,454,406,725 |

Los bytes de la tabla son evidencia de almacenamiento (`DESCRIBE DETAIL`/directorio), no una afirmación de bytes físicos transferidos por cada ejecución. Los archivos efectivamente alcanzados se verificaron tanto con `inputFiles()` como con `_metadata.file_path`; la consulta selectiva devuelve 11,209 filas y la amplia 73 en todas las etapas.

Conclusión: el clustering ya entrega data skipping para la consulta selectiva (4 frente a 57 archivos Parquet); `OPTIMIZE` no produjo un efecto adicional medible porque no encontró archivos para reescribir. El caso amplio sigue leyendo los 24 archivos Delta y, aun con variación favorable post, no debe presentarse como beneficio causado por `OPTIMIZE`.
