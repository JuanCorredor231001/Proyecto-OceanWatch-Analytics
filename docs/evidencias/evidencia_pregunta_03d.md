# Evidencia — Pregunta 03d: ¿dónde se concentra el tráfico?

Ejecución: 2026-09-15, Databricks Free Edition serverless, Spark 4.2.0. La fuente AIS son los siete CSV del Volume `oceanwatch_g06.landing.raw_ais`, con 60,533,559 posiciones de origen. Se aplicó D-07 para duplicados exactos; el filtro de MMSI no conformes **no** aplica porque la métrica cuenta posiciones por celda, no por buque.

## Método

- H3 resolución 8 con `h3_longlatash3(LON, LAT, 8)`, función nativa de Databricks.
- Se convirtió el identificador con `h3_h3tostring` y el centroide con `h3_centeraswkt`.
- Se cruzaron solo las 10 celdas líderes con los 3,807 puntos WPI usando Haversine y `broadcast(ports)`.
- La regla D-03 asocia un puerto cuando el **centroide de la celda** está a no más de 2 km del **punto WPI**.

## Top 10 de celdas H3 r8

| H3 | Posiciones | Centroide (lat, lon) | Puerto WPI más cercano (km) | Resultado 2 km |
|---|---:|---|---:|---|
| `8828d555a1fffff` | 235,260 | 47.629803, -122.387803 | 5.259 | Sin asociación |
| `8828d54715fffff` | 184,408 | 47.679755, -122.407096 | 9.882 | Sin asociación |
| `8829a411d7fffff` | 180,097 | 32.717596, -117.231276 | 4.486 | Sin asociación |
| `8828d17b59fffff` | 123,782 | 48.758286, -122.503656 | 0.960 | Bellingham, Estados Unidos (WPI 18050) |
| `8829a19a35fffff` | 115,457 | 33.980381, -118.450953 | 7.269 | Sin asociación |
| `88446e0359fffff` | 114,643 | 29.966253, -93.861539 | 9.310 | Sin asociación |
| `8829127827fffff` | 109,522 | 34.244790, -119.263224 | 4.667 | Sin asociación |
| `8844a13b51fffff` | 103,931 | 26.099540, -80.161615 | 4.489 | Sin asociación |
| `8828d5476bfffff` | 101,036 | 47.659698, -122.374343 | 7.315 | Sin asociación |
| `8829a411a9fffff` | 100,848 | 32.725240, -117.190044 | 1.141 | San Diego, Estados Unidos (WPI 16010) |

## Sensibilidad de D-03

| Radio | Celdas asociadas de las 10 | Posiciones en celdas asociadas | Cambio frente a 2 km |
|---:|---:|---:|---|
| 1 km | 1 | 123,782 | Bellingham solamente |
| 2 km | 2 | 224,630 | Agrega San Diego (`8829a411a9fffff`) |
| 5 km | 5 | 618,180 | Agrega San Diego (`8829a411d7fffff`), Ventura y Port Everglades |

Entre 1 y 5 km cambian 4 de 10 celdas; entre 2 y 5 km cambian 3. La clasificación cambia al ampliar el radio a 5 km. Por eso se mantiene 2 km como umbral conservador de proximidad directa y se reporta la sensibilidad; no se presenta como una delimitación física de puerto.

## Plan y costo

La ejecución tardó **2 min 20 s**. El plan usa `PhotonGroupingAgg` para el agregado H3 y `PhotonBroadcastNestedLoopJoin Cross BuildRight` para WPI (`EXECUTOR_BROADCAST`). El costo dominante está en leer los CSV, deduplicar las 17 columnas con shuffle ancho y agregar/ordenar las celdas; H3 es una invocación nativa Photon, no una UDF Python. El cruce posterior solo considera 10 × 3,807 candidatos.
