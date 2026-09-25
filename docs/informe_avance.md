# Informe de avance consolidado — Entrega 1

## 1. Resumen ejecutivo

- El trabajo organiza mensajes AIS de NOAA / Marine Cadastre en un lakehouse para OceanWatch Analytics.
- El corpus cubre del 01 al 07 de junio de 2023 y usa los enlaces oficiales `.zip`.
- Cada archivo se descargó, verificó, extrajo a un Volume de Unity Catalog y leyó con un `StructType` explícito de 17 columnas.
- El conteo observado fue **60,533,559 posiciones**, frente a ~150 millones estimadas en el enunciado.
- Esta diferencia de volumen quedó registrada como hallazgo y no alteró el alcance de los siete archivos solicitados.
- La ejecución se hizo en Databricks Free Edition serverless con Spark 4.2.0; el prototipo local usa PySpark 4.2.0.
- El lakehouse usa el catálogo provisional `oceanwatch_g06` y los esquemas `landing`, `reference` y `analytics`.
- La entrega cubre ingesta, perfilamiento, cinco preguntas de negocio, almacenamiento óptimo y gobernanza de Unity Catalog.
- El repositorio reúne notebooks exportados, decisiones, fuentes, evidencias, bitácora y reglas para excluir artefactos pesados.
- Los seis requisitos de la Entrega 1 están documentados como completos; quedan decisiones docentes no bloqueantes para una eventual corrección.

Fuentes: `docs/evidencia_ingesta_databricks_7_dias.md`, `docs/decisiones_pendientes_de_confirmar.md`, `BITACORA.md` y `README.md`.

## 2. Avance por requisito

### 1. Ingesta

- Los siete CSV se validaron localmente y en Databricks con el mismo `StructType`, encabezado estricto y unión por nombre sin columnas faltantes.
- En Databricks, la unión final dio **60,533,559 filas**, igual al conteo local; no hubo diferencias de esquema ni encoding. Cada ZIP tuvo tamaño y SHA-256 observados, mientras que el CSV extraído se verificó con lectura y conteo.
- La primera ejecución end-to-end del 01-jun confirmó 8,808,904 filas, sin diferencia frente a local; Spark reportó versión 4.2.0.
- Evidencia: `docs/evidencia_ingesta_local_7_dias.md`, `docs/evidencia_ingesta_databricks_7_dias.md` y notebook `01_ingesta_ais`.

### 2. Perfilamiento de calidad

- El corpus completo se perfiló sin `cache()` ni `persist()`: 60,533,559 posiciones y 31,871 MMSI únicos antes de D-07.
- El análisis cuantificó nulos, sentinelas AIS, coordenadas y velocidades fuera de rango, MMSI no conformes, duplicados y exclusiones para Haversine.
- D-04 dejó **60,369,975 pares elegibles** para trayectorias después de excluir gaps no positivos, gaps mayores de dos horas y velocidades implícitas mayores de 60 kn.
- Evidencia: `docs/evidencia_perfilamiento_calidad_databricks.md` y notebook `02_perfilamiento_calidad`.

### 3. Preguntas de negocio

- Las cinco preguntas (03a–03e) se respondieron con planes Photon, reglas D-04/D-07 cuando correspondía y evidencia individual.
- Los resultados cubren cardinalidad diaria, tráfico por tipo AIS, distancia Haversine por buque, concentración H3 con WPI y permanencia semanal con visitantes.
- Evidencia: `docs/resumen_preguntas_negocio.md`, `docs/evidencia_pregunta_03a.md` a `docs/evidencia_pregunta_03e.md` y notebook `03_preguntas_negocio`.

### 4. Almacenamiento óptimo

- El propósito elegido fue recuperar historial temporal por MMSI y tiempo, representativo de 03c y 03e.
- La comparación usó el mismo conjunto D-07 de **60,482,276 filas** como Parquet en Volume y Delta administrada con `CLUSTER BY (MMSI, BaseDateTime)`.
- Para la consulta selectiva por MMSI, Delta alcanzó 4 archivos frente a 57 de Parquet. `OPTIMIZE` no reescribió archivos, por lo que no se atribuyen a él variaciones posteriores de tiempo.
- Evidencia: `docs/evidencia_almacenamiento_pre_optimize.md`, `docs/resumen_requisito_4_almacenamiento.md` y notebook `04_almacenamiento_optimizado`.

### 5. Gobernanza

- Se verificaron los comentarios, la jerarquía y la metadata de `oceanwatch_g06`, sus esquemas, los Volumes `landing.raw_ais`, `reference.world_port_index`, `analytics.benchmark_files` y la tabla Delta oficial.
- La evidencia reporta la propiedad efectiva del usuario del workspace Free Edition y cero grants explícitos en catálogo, esquema `analytics` y tabla Delta.
- Evidencia: `docs/evidencia_gobernanza.md` y notebook `05_gobernanza_documentacion`.

### 6. Repositorio y reproducibilidad

- El repositorio contiene los seis notebooks en formato legible, documentación de decisiones, fuentes y evidencias, bitácora, entorno local reproducible y reglas de exclusión para datos crudos, artefactos Spark, entornos y el reporte local de validación.
- `README.md` define el orden de ejecución, la convención de notebooks, el entorno local y las restricciones serverless relevantes.
- Evidencia: `README.md`, `BITACORA.md`, `.gitignore` y notebooks `00_configuracion_unity_catalog` a `05_gobernanza_documentacion`.

## 3. Decisiones técnicas clave

| Decisión | Decisión vigente | Justificación y evidencia |
|---|---|---|
| D-01 | Usar los siete CSV observados: 60,533,559 registros. | Son los siete días exigidos; el conteo físico difiere de ~150M estimados, pero no cambia método ni alcance. (`docs/decisiones_pendientes_de_confirmar.md`, `docs/evidencia_ingesta_local_7_dias.md`.) |
| D-02 | Descargar los `.zip` oficiales del enunciado. | Se verifican reintentos, ZIP, tamaño, SHA-256 observado, extracción y conteo con esquema; ZIP y CSV son artefactos distintos. (`docs/decisiones_pendientes_de_confirmar.md`, `docs/evidencia_ingesta_databricks_7_dias.md`.) |
| D-03 | Asociar centroide H3 r8 con punto WPI mediante buffer geodésico de 2 km. | Sensibilidad Top 10: 1 km asocia 1 celda, 2 km 2 y 5 km 5; 2 km se conserva como proximidad directa conservadora. (`docs/evidencia_pregunta_03d.md`.) |
| D-04 | Formar pares consecutivos por MMSI; exigir gap `0 < t <= 2 h` y velocidad implícita `<= 60 kn`. | Evita saltos/tramos desconectados; quedaron 60,369,975 pares elegibles en el perfilamiento. (`docs/evidencia_perfilamiento_calidad_databricks.md`.) |
| D-05 | Evidenciar bytes, archivos leídos y efecto de `OPTIMIZE`. | Es la evidencia literal solicitada para almacenamiento; se midió en Parquet y Delta antes/después. (`docs/evidencia_almacenamiento_pre_optimize.md`.) |
| D-06 | Usar catálogo provisional `oceanwatch_g06` y esquemas `landing`, `reference`, `analytics`. | La convención separa función de cada activo sin afirmar capas fuera de alcance; `g06` sigue pendiente de confirmar con el equipo. (`docs/decisiones_pendientes_de_confirmar.md`.) |
| D-07 | Deduplicar filas exactas; excluir MMSI no conformes de métricas por buque. | Había 1,388 copias adicionales y 49,897 posiciones con MMSI no conformes; la regla evita sesgar conteos, trayectorias y presencia. (`docs/evidencia_perfilamiento_calidad_databricks.md`.) |

## 4. Hallazgos y descubrimientos relevantes

- El corpus real tiene **60,533,559** posiciones, frente a ~150M enunciadas; es una diferencia de volumen registrada, no una estimación recalculada. (`docs/decisiones_pendientes_de_confirmar.md`.)
- `Status` es nulo en el 100% de `TransceiverClass=B` y no nulo en clase A; se clasificó como ausencia estructural, no como error general. (`docs/evidencia_perfilamiento_calidad_databricks.md`.)
- Los sentinelas AIS son frecuentes: `Heading=511` en 33,535,628 filas (55.4001%), `COG=360` en 10,291,131 (17.0007%) y `SOG=102.3` en 159,987 (0.2643%). (`docs/evidencia_perfilamiento_calidad_databricks.md`.)
- `approx_count_distinct` presentó diferencias de 1.10% a **9.21%**. El plan usó el RSD predeterminado 0.05 y la variación por grupos diarios de ~20 mil MMSI explica que una realización puntual supere el valor esperado; se prefirió el conteo exacto. (`docs/evidencia_pregunta_03a.md`.)
- El control físico del primer ranking de distancia fue consistente: JUSTIN PAUL ECKSTEIN recorrió 5,770.92 km en una semana, equivalente a 18.55 kn, con 8 pares excluidos frente a 2,865 válidos. (`docs/evidencia_pregunta_03c.md`.)
- Solo **2 de las 10** celdas H3 con más tráfico estuvieron a 2 km o menos de un puerto WPI; ampliar a 5 km cambia la clasificación de tres celdas adicionales. (`docs/evidencia_pregunta_03d.md`.)
- Los 5,964 visitantes de un solo día se concentraron en 01-jun (1,326) y 07-jun (1,244), no en el fin de semana; Pleasure craft y Sailing reúnen 65.5265%. (`docs/evidencia_pregunta_03e.md`.)
- `OPTIMIZE` no agregó ni removió archivos porque la tabla Delta ya estaba organizada por liquid clustering desde la escritura. (`docs/evidencia_almacenamiento_pre_optimize.md`.)

## 5. Dificultades y cómo se resolvieron

| Dificultad documentada | Resolución aplicada |
|---|---|
| `input_file_name()` no está disponible en serverless con Unity Catalog. | Se usa `_metadata.file_path` para identificar origen y para la evidencia de archivos leídos. (`README.md`.) |
| Free Edition rechazó `USING PARQUET` para tabla administrada. | Parquet se conservó como línea base en el Volume gobernado `analytics.benchmark_files`; Delta fue la tabla administrada de producción. (`docs/evidencia_almacenamiento_pre_optimize.md`.) |
| La biblioteca Python H3 no debía asumirse persistente en Free Edition. | El análisis espacial usó funciones H3 nativas de Databricks, ejecutadas por Photon; `h3==4.5.0` se conserva solo para el prototipo local. (`docs/decisiones_pendientes_de_confirmar.md`.) |
| El ZIP y el CSV local tienen hashes distintos. | Se verifican por separado: tamaño/hash observado y validez del ZIP; extracción y conteo con `StructType` para el CSV. (`docs/decisiones_pendientes_de_confirmar.md`.) |
| Un DataFrame lazy reutilizado en varias salidas de 03e produjo varias ramas `Scan csv`. | Se registró en el plan; no se materializó una tabla intermedia ni se usó cache fuera del alcance. (`docs/evidencia_pregunta_03e.md`.) |

## 6. Técnicas y herramientas utilizadas

- PySpark y Apache Spark 4.2.0 en Databricks Free Edition serverless; planes ejecutados con Photon donde se documenta.
- Esquema AIS explícito, lectura CSV, unión estricta, reintentos HTTP, comprobación de ZIP y SHA-256 observado.
- Funciones Window por MMSI y Haversine para trayectorias; `countDistinct` y `approx_count_distinct`/HyperLogLog para comparación de cardinalidad.
- H3 nativo de Databricks para grilla r8 y broadcast del World Port Index.
- Delta Lake administrado con liquid clustering `CLUSTER BY (MMSI, BaseDateTime)`, Parquet de línea base en Volume y `OPTIMIZE` medido.
- Unity Catalog, catálogo/esquemas/Volumes, comentarios, `DESCRIBE ... EXTENDED` y `SHOW GRANTS`.
- Git/GitHub como mecanismo de versionado del código y documentación; `.gitignore` excluye datos y artefactos reproducibles locales.

## 7. Resultados de las cinco preguntas de negocio

| Pregunta | Resultado documentado |
|---|---|
| 03a. Buques distintos por día | Entre **19,597 y 21,086 MMSI válidos** diarios. Se usa `countDistinct`; el aproximado tuvo 1.10%–9.21% de diferencia. |
| 03b. Tipos con más tráfico | Towing (31) lidera con **16,557,760** posiciones, seguido de Pleasure craft (37) con **14,476,457**. SOG=102.3 se excluye de sus promedios. |
| 03c. Mayor distancia | JUSTIN PAUL ECKSTEIN (367638030, tipo 31) acumuló **5,770.92 km**; D-04 aceptó 60,369,977 pares en la base D-07 de esta pregunta. |
| 03d. Concentración | La celda `8828d555a1fffff` registró **235,260** posiciones. A 2 km, 2 de las 10 celdas líderes se asociaron a WPI. |
| 03e. Permanencia/visitantes | **12,656 de 31,780** MMSI válidos transmitieron los siete días (39.8238%); hubo 5,964 visitantes de un día. |

Detalle y planes de ejecución: `docs/resumen_preguntas_negocio.md` y `docs/evidencia_pregunta_03a.md` a `docs/evidencia_pregunta_03e.md`.

## 8. Estado y próximos pasos

La Entrega 1 está documentada como completa en sus seis requisitos. Quedan decisiones no bloqueantes para confirmar con el profesor: el conteo real frente a ~150M (D-01), el uso de enlaces `.zip` frente a la distribución `.csv.zst` actual (D-02), una definición docente distinta para asociación a puertos (D-03), los umbrales D-04 y la naturaleza de los MMSI no conformes (D-07). El equipo también debe confirmar si conserva `g06` como identificador de catálogo o lo sustituye antes de la entrega. (Fuente: `docs/decisiones_pendientes_de_confirmar.md`.)

Según el alcance ya documentado, la Entrega 2 debe reutilizar el perfilamiento de calidad de esta entrega como insumo para definir y justificar reglas de tratamiento. Cualquier ajuste posterior debe conservar trazabilidad en `BITACORA.md` y actualizar la decisión afectada, sin reescribir la evidencia histórica.
