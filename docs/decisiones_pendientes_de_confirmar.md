# Decisiones de trabajo pendientes de confirmar

Estas decisiones permitieron avanzar sin detener el proyecto. Si el profesor indica algo distinto, el ajuste se registra en `BITACORA.md` y se conserva el antecedente.

## D-01 - Tamaño efectivo del corpus

- **Decisión vigente:** usar los siete CSV disponibles tal como están.
- **Evidencia:** 60,533,559 registros de datos y 6.04 GiB descomprimidos, contados localmente el 2026-09-14.
- **Justificación:** el enunciado solicita esos siete días. La diferencia frente a la estimación de ~150M filas no cambia las preguntas ni el método.
- **Pendiente de confirmar:** no bloqueante; informar el conteo real en el README y los resultados.

## D-02 - Formato y enlaces de descarga

- **Decisión vigente:** usar los siete enlaces `.zip` de junio de 2023 incluidos en el enunciado.
- **Justificación:** son los enlaces concretos evaluables para esta entrega, aunque la distribución reciente descrita en la web use `.csv.zst`.
- **Verificación en ingesta (dos pasos):** (1) reintentos HTTP, código 200, archivo ZIP no vacío, tamaño y SHA-256 observados, más comprobación de ZIP; (2) extracción del único CSV esperado y lectura con `StructType`, encabezado estricto y conteo de filas. El SHA-256 del ZIP no se compara con el SHA-256 del CSV local: son artefactos distintos (comprimido frente a descomprimido). Cuando NOAA publique una referencia verificable, se agregará como checksum esperado del ZIP.
- **Pendiente de confirmar:** no bloqueante.

## D-03 - Asociación entre celdas de tráfico y puertos (cerrada)

- **Decisión vigente:** usar un buffer geodésico de 2 km entre el centroide de cada celda H3 r8 y el punto de cada puerto del World Port Index. Una celda se asocia a puerto cuando su centroide cae dentro de ese buffer; también se reportan el puerto más cercano y la distancia mínima.
- **Opción A - misma celda H3 de resolución 8:** asignar cada puerto a su celda H3 y marcar coincidencia exacta.
  - **Pros:** determinista, sin parámetro arbitrario, rápida y coherente con el agregado.
  - **Contras:** una celda H3 r8 (~0.74 km² promedio) puede no captar un fondeadero o canal cercano.
- **Opción B - buffer geodésico de 2 km alrededor del puerto:** marcar una celda como portuaria si su centroide o puntos AIS están dentro del buffer.
  - **Pros:** representa mejor la cercanía operacional del tráfico a instalaciones portuarias.
  - **Contras:** introduce un umbral que debe justificarse; puertos cercanos pueden solaparse.
- **Justificación adoptada:** 2 km da una definición interpretable de "corresponden a puertos" y reduce falsos negativos en los bordes de celda. El top 10 se calcula exclusivamente por celda H3 r8.
- **Sensibilidad ejecutada (2026-09-15):** sobre las diez celdas principales, 1 km asocia 1 celda (123,782 posiciones), 2 km asocia 2 (224,630) y 5 km asocia 5 (618,180). Entre 2 y 5 km cambian 3 celdas: `8829a411d7fffff`/San Diego, `8829127827fffff`/Ventura y `8844a13b51fffff`/Port Everglades. A 2 km quedan Bellingham (0.960 km) y San Diego (1.141 km).
- **Conclusión de cierre:** el resultado no es invariante a ampliar el radio a 5 km; por ello 2 km se conserva como umbral conservador de proximidad directa, no como frontera geográfica absoluta. La sustentación debe reportar la sensibilidad y esta definición de centroide-punto.
- **Revisión futura:** solo cambiar si el profesor define otra semántica de "corresponden a puertos"; registrar entonces la sustitución en la bitácora.

## D-04 - Segmentación para distancia haversine

- **Decisión vigente:** evaluar pares consecutivos dentro de cada MMSI, ordenados por `BaseDateTime`.
- **Reglas de elegibilidad:** MMSI de nueve dígitos; timestamp parseable; LAT/LON dentro de rango; diferencia temporal mayor que 0 y menor o igual a 2 horas; velocidad implícita menor o igual a 60 nudos.
- **Tratamiento:** excluir pares no elegibles del acumulado y cuantificarlos en el perfilamiento. `SOG=102.3`, `COG=360` y `Heading=511` se registran como sentinelas AIS y no se usan como mediciones válidas.
- **Justificación:** el AIS está filtrado a un minuto. Un corte de 2 horas evita unir tramos desconectados y 60 nudos sirve como límite conservador para detectar saltos de posición en este corpus mercante.
- **Evidencia de perfilamiento (2026-09-14):** de 60,451,882 pares con evento previo, se excluyeron 1,670 por gap no positivo, 52,418 por gap > 2 h y 27,819 por velocidad implícita > 60 kn; 60,369,975 pares quedaron elegibles. No hubo pares excluidos por campos requeridos. Las categorías son disjuntas porque la velocidad se evalúa solo cuando `0 < gap <= 2 h`.
- **Pendiente de confirmar:** el umbral puede recalibrarse con la distribución observada y la retroalimentación docente, conservando la versión anterior en la bitácora.

## D-05 - Evidencia de almacenamiento

- **Decisión vigente:** demostrar bytes antes/después, archivos leídos por consulta mediante plan o `input_file_name`, y efecto de `OPTIMIZE`.
- **Justificación:** reproduce literalmente la evidencia requerida por el enunciado.
- **Pendiente de confirmar:** ninguno.

## D-06 - Convención de Unity Catalog

- **Decisión vigente:** catálogo provisional `oceanwatch_g07`; esquemas `landing`, `reference` y `analytics`; Volume `landing.raw_ais`.
- **Uso:** `landing` conserva la tabla AIS con esquema explícito; `reference` alberga World Port Index y catálogo de tipos; `analytics` contiene resultados y la tabla optimizada de consulta.
- **Justificación:** la convención expresa la función de cada activo sin presentar un pipeline bronze/silver/gold, que no hace parte del alcance de la Entrega 1.
- **Pendiente de confirmar con el equipo:** `g07` es un placeholder neutral sin datos personales. Sustituirlo por un nombre corto o iniciales acordadas, en minúsculas y con guiones bajos, antes de una entrega final. Si se cambia después de crear activos, registrar la migración y actualizar README/bitácora.

## H3 en Databricks Free Edition

- **Decisión vigente:** usar funciones H3 nativas de Databricks (`h3_longlatash3`, `h3_h3tostring`, `h3_centeraswkt`) en la ejecución serverless.
- **Evidencia:** el plan de 03d muestra `static_invoke(com.databricks.geo.H3Utils.dbxH3LongLatAsH3(...))` y ejecución totalmente Photon. La biblioteca Python `h3==4.5.0` sigue disponible para el prototipo local, pero no es necesaria ni se asume instalada entre sesiones serverless.
- **Contingencia:** usar una rejilla regular lat/lon, permitida por el enunciado, solo si las funciones nativas no están disponibles.

## D-07 - Duplicados exactos y MMSI no conformes

- **Decisión vigente:** crear un conjunto analítico canónico que elimina duplicados exactos sobre las 17 columnas de origen antes de todas las preguntas de negocio, sin escribir ni persistir una tabla intermedia. Toda pregunta que agrupe, ordene o forme pares por buque (03a, 03c, 03e y D-04) exige MMSI numérico de exactamente nueve dígitos.
- **Evidencia:** el perfilamiento completo encontró 1,388 grupos duplicados, 2,776 filas involucradas y 1,388 copias adicionales (0.0023% del corpus); 49,897 posiciones (0.0824%) tienen MMSI no conforme, todos numéricos y distribuidos en 86 IDs cortos y 5 largos.
- **Justificación:** un duplicado exacto no añade señal y puede sesgar conteos, promedios y distancias; retirarlo es reproducible y su efecto marginal quedó cuantificado. Los MMSI no conformes no pueden identificarse con seguridad como buques, por lo que se excluyen de métricas por buque. Las métricas que no agrupan por buque conservan el conjunto canónico y declaran cuándo esta regla no aplica.
- **Transparencia:** 03a presenta el conteo con y sin exclusión MMSI. Si se requiere conservar los registros excluidos, se reportan como categoría separada, nunca fusionados con buques válidos.
- **Pendiente de confirmar:** ajustar solo si el profesor provee una definición formal de esos IDs.
