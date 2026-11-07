# Decisiones técnicas - Entrega 2

Las decisiones D-01 a D-07 de la Entrega 1 siguen vigentes ([`docs/decisiones/decisiones_pendientes_de_confirmar.md`](../../../docs/decisiones/decisiones_pendientes_de_confirmar.md), en la raíz del repositorio). En particular, D-03 (2 km para asociar celdas a puertos), D-04 (pares haversine con gap ≤ 2 h y velocidad implícita ≤ 60 kn) y D-07 (MMSI de 9 dígitos) se convierten aquí en compuertas y reglas del modelo oro. Cada decisión cita la evidencia que la sostiene.

## D-E2-01 - Corpus extendido y feed diario

- **Decisión:** los días 01-07 jun 2023 (corpus de la Entrega 1) son el histórico (`rol = HISTORICO` en `ops.feed_calendar`) y los días 08-14 jun 2023 llegan como feed diario (`rol = FEED`), uno por ejecución del job.
- **Por qué:** el enunciado no fija fechas. Reutilizar el histórico permite entrenar el modelo y calcular las excepciones navegables (D-E2-04) antes de que llegue el feed.
- **Pendiente:** cambiar el calendario si el profesor define otras fechas; el código solo depende de `ops.feed_calendar`.

## D-E2-02 - Bronce como texto crudo, append-only

- **Decisión:** bronce guarda las 17 columnas **como texto**, más `_rescued_data`, archivo de origen, instante de ingesta e `ingestion_date`. La tabla tiene `delta.appendOnly = true`.
- **Por qué:** un `cast` en bronce podría perder valores (por ejemplo `LAT = "abc"`) o detener el stream con `FAILFAST`. La tipificación con `try_cast` ocurre en plata, donde cada fallo queda como causa de cuarentena. `appendOnly` hace que la inmutabilidad la imponga Delta: `DELETE` falla con `DELTA_CANNOT_MODIFY_APPEND_ONLY` (prueba `test_bronce_es_append_only`).

## D-E2-03 - Catálogo de compuertas y umbrales

| Regla | Severidad | Criterio | Fuente del umbral |
|---|---|---|---|
| E01 | CUARENTENA | MMSI de 9 dígitos | D-07 |
| E02-E03 | CUARENTENA | Timestamp válido y del mismo día que el archivo | Formato NOAA; evita mezclar días |
| E04-E05 | CUARENTENA | LAT/LON presentes, en rango y ≠ (0,0) | Estándar WGS84; "isla nula" GPS |
| E06 | CUARENTENA | Posición fuera de tierra firme | D-E2-04 |
| E07 | CUARENTENA | SOG en [0, 60] kn o sentinela 102.3 | Mismo tope que D-04; 102.3 = no disponible (03b) |
| E08-E09 | ADVERTENCIA | COG en [0, 360], Heading en [0, 359] o 511 | Sentinelas del perfilamiento 02 |
| E10 | ADVERTENCIA | IMO con formato `IMOnnnnnnn` | IMO tiene 42.8 % de nulos (02): no se exige |
| E11 | ADVERTENCIA | VesselType dentro de los códigos ITU-R M.1371 | Catálogo NOAA/USCG (03b) |
| E12 | ADVERTENCIA | Eslora en (0, 500] m y manga en (0, 80] m | El buque más grande en servicio mide ~460 m |
| D01/D02 | DEDUPLICACION | Segunda llegada de (MMSI, BaseDateTime) | Requisito 1 |

- **ADVERTENCIA** no bloquea la fila: el campo se anula en plata y la regla queda en `quality_warnings`. Un sentinela (102.3, 360, 511) no es un error. Se anula sin contarse como violación.
- **Deduplicación:** se conserva la primera llegada según un orden determinista (archivo, hash del contenido). D01 marca copias idénticas y D02 copias con contenido distinto. En las dos, la copia va a cuarentena para dejar trazabilidad.

## D-E2-04 - "En el agua": máscara de tierra H3 r5 con excepciones navegables

- **v1 (descartada):** celdas H3 r5 completamente dentro de tierra según Natural Earth 1:10m. Se restaron lagos y ríos principales con un buffer de 0.05° y se erosionó 0.2°.
- **Evidencia que la descartó (prueba local, 01-jun, muestra 10 %):** 2,163 posiciones quedaron en E06. Casi todas eran remolcadores en vías navegables interiores que Natural Earth no dibuja: Gulf Intracoastal Waterway cerca de Houma (29.55, -90.72), Atchafalaya (30.49, -91.75), Sabine-Neches (30.07, -94.07) y río Nanticoke (38.35, -75.63). Solo 3 buques eran errores reales, con SOG de 43 a 85 kn tierra adentro (p. ej. STEELROD, 85.1 kn en el valle de San Fernando, CA).
- **v2 (vigente):** una celda de la máscara con ≥ 2 MMSI válidos distintos en el histórico se reclasifica como navegable (`reference.waterway_cells_h3r5`). Un error de GPS aislado rara vez se repite en la misma celda para buques distintos.
- **Limitación declarada:** en local, con solo 1 día y 10 % de los buques, solo 2 celdas se reclasificaron (803 posiciones). Siguen en cuarentena tramos legítimos recorridos por un único buque de la muestra, como el remolcador A.B. CENAC en el Atchafalaya. Con los 7 días completos del backfill en Databricks, la lista de excepciones crece y esos falsos positivos bajan. La alternativa con más fidelidad es la National Waterway Network de USACE, que queda como mejora futura.

## D-E2-05 - Idempotencia de streaming

- **Bronce:** checkpoint de Auto Loader + sumidero Delta, idempotente por `batchId`.
- **Plata:** `foreachBatch` con `txnAppId`/`txnVersion = batchId` en las tres tablas de salida. La deduplicación entre lotes solo compara contra `_silver_batch_id < batchId`, así que reprocesar un lote da el mismo resultado.
- **Oro y ML:** `replaceWhere event_date = fecha`. Re-ejecutar un día lo reemplaza y no lo duplica.
- **Evidencia:** `docs/evidencias/06 evidencia_tolerancia_fallos.md` y el reproceso del backfill (bronce y plata leyeron 0 filas nuevas).

## D-E2-06 - Particionamiento por fecha

- **Decisión:** bronce, plata, cuarentena, oro y features se particionan por fecha (`ingestion_date` o `event_date`), no con liquid clustering.
- **Por qué:** todas las operaciones del pipeline son por día: `replaceWhere`, anti-join de deduplicación y lecturas del tablero. Un día completo pesa unos 300 MB en Parquet (Entrega 1: 2.15 GB / 7 días), un tamaño de partición razonable. El mismo DDL funciona igual en Delta OSS local y en Databricks. La tabla de historial por MMSI de la Entrega 1 (`analytics.ais_positions_d07_delta`, `CLUSTER BY (MMSI, BaseDateTime)`) se mantiene para su propio propósito.

## D-E2-07 - Grilla espacial y puertos

- H3 r8 para la matriz de tráfico (igual que la pregunta 03d), con funciones nativas en Databricks y la librería `h3` en local; las dos implementan H3 v4 y dan los mismos IDs.
- **Mapa del tablero (2026-11-07):** oro conserva H3 r8. Solo la consulta del mapa agrega los centroides r8 en una rejilla regular de 0.05° (~5.5 km). Con r8, un día tiene hasta 444,917 celdas y el widget de AI/BI trunca el resultado; con la rejilla quedan como máximo 59,560 puntos y se conserva el total de posiciones (evidencia 09).
- Puertos: WPI (NGA Pub. 150) en Databricks. En local NGA respondió HTTP 503, así que se usa Natural Earth `ne_10m_ports` (subconjunto del WPI, dominio público), con la misma interfaz `load_ports`. Natural Earth repite algunos puertos con varios puntos; la primera corrida mostró "Port Everglades" y "Port Arthur" dos veces en el ranking. Por eso los homónimos a menos de ~0.5° se fusionan.
- Celdas portuarias: centroide a ≤ 2 km del puerto (D-03). Si una celda queda cerca de dos puertos se asigna al más cercano.
- **Estadía (v2):** posiciones del buque en celdas del mismo puerto. Se abre una estadía nueva solo si el buque pasó más de 1 h sin reportar desde ese puerto. Se marca `ATRACADO_O_FONDEADO` si la mediana de SOG es < 1 kn.
  - **Evidencia del cambio:** en v1 una salida de las celdas portuarias abría una estadía nueva. Bellingham registró 46 arribos para 12 buques el 01-jun: buques atracados en el borde de las celdas "parpadeaban". Los números v2 están en `docs/evidencias/07 evidencia_capas_calidad.md`.

## D-E2-08 - ML: anomalías de velocidad con IsolationForest

- **Unidad:** buque-día. **Variables:** 10 de SOG (media, desviación, p95, máximo, fracción > 25 kn), velocidad implícita máxima D-04, distancia, fracción de pares excluidos, número de posiciones y eslora.
- **Modelo:** `Pipeline(SimpleImputer → StandardScaler → IsolationForest(contamination=0.01, n_estimators=200, random_state=42))`. Es simple y no supervisado, porque no hay etiquetas. El objetivo es la integración reproducible y no la precisión (sección 5 del enunciado).
- **Registro:** Unity Catalog `<catálogo>.ml.oceanwatch_anomalias_velocidad` con alias `champion`. La puntuación siempre carga el alias, así que promover un modelo no exige cambiar código.
- **Explicabilidad:** `motivo_principal` es la variable con mayor |z| frente a las estadísticas de entrenamiento, guardadas como artefacto del run de entrenamiento (UC limita las etiquetas a 256 caracteres).

## D-E2-09 - Entorno local en contenedor

- Spark 4.2 en Windows exige `winutils.exe`/`HADOOP_HOME`, un binario de terceros que no se instaló. Las pruebas corren en `docker/Dockerfile` (Debian + OpenJDK 21 + pyspark 4.2.0 + delta-spark 4.4.1 + MLflow 3.18). El código es el mismo de los notebooks; solo cambian `lakehouse.py` (rutas frente a Unity Catalog) y `geo.py` (H3 nativo frente a la librería).
