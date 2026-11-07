# Contrato de datos de la capa oro

> Documento generado desde [`contracts/gold_contract.yml`](../contracts/gold_contract.yml), fuente única del DDL, los comentarios de Unity Catalog y la tarea `validar_contrato` del job. Si cambia el YAML, se regenera este documento.

## Identificación

| Campo | Valor |
|---|---|
| id | oceanwatch-gold |
| version | 1.0.0 |
| estado | vigente |
| propietario | Equipo de datos OceanWatch (grupo oceanwatch_ingenieros) |
| dominio | Tráfico marítimo AIS - aguas de EE. UU. (NOAA Marine Cadastre) |
| frescura | Cada fecha del feed disponible en oro a más tardar 2 h después de que el archivo diario llega a landing. |
| granularidad_temporal | event_date (UTC). Las tablas se reescriben por fecha (replaceWhere): re-ejecutar un día es idempotente. |
| linaje | landing.raw_ais (Volume) -> bronze.ais_raw -> silver.ais_posiciones -> gold.*; ml.features_velocidad_buque_dia -> modelo MLflow -> gold.anomalias_velocidad_diarias |
| consumidores | oceanwatch_analistas, oceanwatch_tablero, oceanwatch_cientificos |

## Garantías

- Ningún registro en silver.ais_cuarentena contribuye a oro: oro solo lee silver.ais_posiciones.
- MMSI siempre de 9 dígitos; posiciones dentro de rango y en el agua; SOG en [0, 60] kn o NULL.
- Distancias y velocidades implícitas aplican D-04 (0 < gap <= 2 h, velocidad implícita <= 60 kn).
- Asociación a puertos por celdas H3 r8 a <= 2 km del punto del puerto (D-03).

## Cómo se hace cumplir

1. **DDL desde el contrato:** `setup.create_all` crea cada tabla oro con las columnas, tipos, comentarios y partición del YAML, más la propiedad `oceanwatch.contrato.version`.
2. **Conformación en escritura:** `contract.conform` selecciona y tipifica exactamente las columnas del contrato antes de cada `replaceWhere`; si falta una columna, la tarea falla.
3. **Validación en el job:** la tarea `validar_contrato` revisa por fecha el esquema, los nulos, los rangos (`minimo`/`maximo`), los dominios (`valores`) y la unicidad de la clave primaria. Además comprueba que oro solo tenga buques de plata y que ninguna clave en cuarentena aparezca en plata. Si algo falla, el job falla y se notifica.
4. **Metadatos de gobierno:** el notebook `12_gobernanza` agrega las etiquetas `capa`, `contrato`, `contrato_version`, `propietario` y `clasificacion`, consultables en `information_schema`.
5. **Versionado:** un cambio compatible (columna nueva nullable) sube la versión menor; quitar o renombrar columnas, o cambiar tipos, sube la versión mayor y se anuncia a los consumidores.

## `gold.trayectorias_diarias_buque`

Una fila por buque (MMSI) y día: resumen de la trayectoria diaria y su geometría simplificada.

- **Clave primaria:** `event_date, mmsi`
- **Partición:** `event_date`

| Columna | Tipo | Nulos | Reglas | Descripción |
|---|---|---|---|---|
| `event_date` | date | no |  | Día UTC de la trayectoria. |
| `mmsi` | string | no |  | Identificador MMSI de 9 dígitos. |
| `vessel_name` | string | sí |  | Último nombre reportado en el día. |
| `vessel_type` | int | sí |  | Último código AIS VesselType del día. |
| `vessel_type_group` | string | no |  | Grupo de negocio (Carga, Tanquero, Pasajeros, Pesca, Remolque, Recreo, Servicio, Otro, Desconocido). |
| `length_m` | double | sí |  | Eslora máxima plausible reportada (m). |
| `n_posiciones` | bigint | no | ≥ 1 | Posiciones válidas del buque en el día. |
| `primera_posicion_ts` | timestamp | no |  | Primera posición del día (UTC). |
| `ultima_posicion_ts` | timestamp | no |  | Última posición del día (UTC). |
| `distancia_km` | double | no | ≥ 0 | Distancia haversine sumada sobre pares D-04 (km). |
| `horas_cubiertas` | double | no | ≥ 0; ≤ 24 | Horas cubiertas por pares D-04. |
| `pares_validos_d04` | int | no |  | Pares consecutivos que cumplen D-04. |
| `pares_excluidos_d04` | int | no |  | Pares consecutivos excluidos por D-04 (gap o salto). |
| `sog_media_kn` | double | sí |  | SOG medio (nudos) sin sentinelas. |
| `sog_max_kn` | double | sí | ≤ 60 | SOG máximo (nudos) sin sentinelas. |
| `velocidad_implicita_max_kn` | double | sí | ≤ 60 | Máxima velocidad implícita entre pares D-04 (nudos). |
| `lat_inicio` | double | no |  | Latitud de la primera posición. |
| `lon_inicio` | double | no |  | Longitud de la primera posición. |
| `lat_fin` | double | no |  | Latitud de la última posición. |
| `lon_fin` | double | no |  | Longitud de la última posición. |
| `celdas_h3_visitadas` | bigint | no |  | Celdas H3 r8 distintas visitadas. |
| `trayectoria_wkt` | string | no |  | Trayectoria simplificada (<= 301 puntos) en WKT LINESTRING/POINT, lon lat WGS84. |
| `_gold_processed_at` | timestamp | no |  | Instante de cálculo. |

## `gold.estadias_puerto`

Presencia en puertos: una fila por estadía (posiciones del buque en celdas del mismo puerto sin ausencias > 1 h).

- **Clave primaria:** `stay_id`
- **Partición:** `event_date`

| Columna | Tipo | Nulos | Reglas | Descripción |
|---|---|---|---|---|
| `event_date` | date | no |  | Día UTC. |
| `stay_id` | string | no |  | Identificador fecha-mmsi-puerto-secuencia. |
| `mmsi` | string | no |  | MMSI del buque. |
| `vessel_name` | string | sí |  | Nombre del buque. |
| `vessel_type_group` | string | no |  | Grupo de negocio del buque. |
| `port_id` | string | no |  | Identificador del puerto (WPI-n o NE-n). |
| `port_name` | string | no |  | Nombre del puerto. |
| `arribo_ts` | timestamp | no |  | Primera posición en el puerto. |
| `salida_ts` | timestamp | no |  | Última posición en el puerto. |
| `duracion_horas` | double | no | ≥ 0; ≤ 24 | salida_ts - arribo_ts en horas. |
| `n_posiciones` | bigint | no |  | Posiciones en la estadía. |
| `sog_media_kn` | double | sí |  | SOG medio en la estadía. |
| `sog_mediana_kn` | double | sí |  | SOG mediano en la estadía. |
| `estado` | string | no | ATRACADO_O_FONDEADO / EN_TRANSITO | ATRACADO_O_FONDEADO si SOG mediano < 1 kn; si no, EN_TRANSITO. |
| `inicia_dia_en_puerto` | boolean | no |  | La estadía empieza con la primera posición del día (el arribo pudo ser el día anterior). |
| `en_puerto_al_cierre` | boolean | no |  | La última posición del día del buque está en este puerto. |
| `_gold_processed_at` | timestamp | no |  | Instante de cálculo. |

## `gold.puertos_resumen_diario`

Ranking diario de puertos y conteo de buques en puerto.

- **Clave primaria:** `event_date, port_id`
- **Partición:** `event_date`

| Columna | Tipo | Nulos | Reglas | Descripción |
|---|---|---|---|---|
| `event_date` | date | no |  | Día UTC. |
| `ranking` | int | no | ≥ 1 | Posición por buques distintos (desempate: horas-buque). |
| `port_id` | string | no |  | Identificador del puerto. |
| `port_name` | string | no |  | Nombre del puerto. |
| `country_code` | string | sí |  | País (WPI). |
| `port_lat` | double | no |  | Latitud del puerto. |
| `port_lon` | double | no |  | Longitud del puerto. |
| `buques_distintos` | bigint | no |  | Buques con al menos una estadía en el día. |
| `estadias` | bigint | no |  | Número de estadías. |
| `arribos` | bigint | no |  | Estadías que no empiezan con la primera posición del día. |
| `buques_atracados` | bigint | no |  | Buques con alguna estadía ATRACADO_O_FONDEADO. |
| `buques_en_puerto_al_cierre` | bigint | no |  | Buques cuya última posición del día está en el puerto. |
| `horas_buque_totales` | double | no |  | Suma de duraciones de estadía (h). |
| `duracion_media_horas` | double | no |  | Duración media de estadía (h). |
| `buques_carga` | bigint | no |  | Buques de carga distintos. |
| `buques_tanquero` | bigint | no |  | Tanqueros distintos. |
| `buques_pasajeros` | bigint | no |  | Buques de pasajeros distintos. |
| `_gold_processed_at` | timestamp | no |  | Instante de cálculo. |

## `gold.trafico_celda_hora`

Matriz de tráfico: una fila por celda H3 r8 y hora UTC con posiciones y buques por grupo.

- **Clave primaria:** `event_date, hora_ts, h3_r8`
- **Partición:** `event_date`

| Columna | Tipo | Nulos | Reglas | Descripción |
|---|---|---|---|---|
| `event_date` | date | no |  | Día UTC. |
| `hora_ts` | timestamp | no |  | Inicio de la hora UTC. |
| `hora` | int | no | ≥ 0; ≤ 23 | Hora del día 0-23. |
| `h3_r8` | string | no |  | Celda H3 resolución 8 (~0.74 km²). |
| `centro_lat` | double | no |  | Latitud del centroide de la celda. |
| `centro_lon` | double | no |  | Longitud del centroide de la celda. |
| `posiciones` | bigint | no | ≥ 1 | Posiciones válidas en la celda-hora. |
| `buques_distintos` | bigint | no | ≥ 1 | MMSI distintos en la celda-hora. |
| `sog_media_kn` | double | sí |  | SOG medio en la celda-hora. |
| `buques_carga` | bigint | no |  | Buques de carga distintos. |
| `buques_tanquero` | bigint | no |  | Tanqueros distintos. |
| `buques_pasajeros` | bigint | no |  | Buques de pasajeros distintos. |
| `buques_pesca` | bigint | no |  | Pesqueros distintos. |
| `buques_remolque` | bigint | no |  | Remolcadores distintos. |
| `buques_recreo` | bigint | no |  | Embarcaciones de recreo distintas. |
| `buques_otros` | bigint | no |  | Buques de otros grupos. |
| `_gold_processed_at` | timestamp | no |  | Instante de cálculo. |

## `gold.anomalias_velocidad_diarias`

Puntaje de anomalía de velocidad por buque y día (modelo MLflow registrado, alias champion).

- **Clave primaria:** `event_date, mmsi`
- **Partición:** `event_date`

| Columna | Tipo | Nulos | Reglas | Descripción |
|---|---|---|---|---|
| `event_date` | date | no |  | Día UTC. |
| `mmsi` | string | no |  | MMSI del buque. |
| `vessel_name` | string | sí |  | Nombre del buque. |
| `vessel_type_group` | string | no |  | Grupo de negocio. |
| `n_posiciones` | bigint | no |  | Posiciones del día. |
| `sog_media_kn` | double | sí |  | SOG medio. |
| `sog_p95_kn` | double | sí |  | Percentil 95 de SOG. |
| `sog_max_kn` | double | sí |  | SOG máximo. |
| `velocidad_implicita_max_kn` | double | sí |  | Máxima velocidad implícita D-04. |
| `distancia_km` | double | no |  | Distancia diaria D-04. |
| `anomaly_score` | double | no |  | Puntaje IsolationForest (mayor = más anómalo). |
| `es_anomalia` | boolean | no |  | Predicción del modelo (contaminación configurada). |
| `motivo_principal` | string | sí |  | Variable con mayor desviación estandarizada del buque frente al entrenamiento. |
| `lat_ultima` | double | no |  | Latitud de la última posición del día. |
| `lon_ultima` | double | no |  | Longitud de la última posición del día. |
| `modelo` | string | no |  | Nombre del modelo registrado. |
| `modelo_version` | string | no |  | Versión del modelo usada. |
| `_gold_processed_at` | timestamp | no |  | Instante de puntuación. |
