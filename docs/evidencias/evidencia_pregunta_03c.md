# Evidencia — pregunta 03c: mayor distancia semanal

Fecha de ejecución: 2026-09-15. Notebook: `03_preguntas_negocio`, Databricks serverless.

## Método reproducible

- La base canónica D-07 elimina duplicados exactos y admite únicamente MMSI con la expresión `^[0-9]{9}$`.
- Para cada MMSI, la ventana se ordena por `BaseDateTime` y calcula Haversine entre cada posición y su predecesora.
- D-04 se aplica sin modificar sus umbrales: `0 < gap <= 2 h` y velocidad implícita `<= 60 kn`.
- Los pares excluidos no suman distancia; se reportan por MMSI para auditar el Top 10.

## Ranking final

| # | MMSI | VesselName | VesselType | Distancia (km) | Vel. implícita (kn) | Vel. semana (kn) | Pares válidos | Pares excluidos |
|---:|---|---|---:|---:|---:|---:|---:|---:|
| 1 | 367638030 | JUSTIN PAUL ECKSTEIN | 31 | 5,770.92 | 18.61 | 18.55 | 2,865 | 8 |
| 2 | 367438630 | A STEVE CROWLEY | 31 | 3,749.18 | 12.12 | 12.05 | 8,324 | 10 |
| 3 | 366991412 | USNS APALACHICOLA | 35 | 3,638.87 | 11.70 | 11.70 | 5,301 | 0 |
| 4 | 368097380 | KOALAFIED CRUISER | 60 | 3,632.87 | 16.46 | 11.68 | 6,794 | 7 |
| 5 | 366941210 | LAKE EXPRESS | 60 | 3,614.35 | 11.76 | 11.62 | 6,293 | 224 |
| 6 | 316011408 | COASTAL INSPIRATION | 60 | 3,499.10 | 11.25 | 11.25 | 7,208 | 0 |
| 7 | 316001245 | QUEEN OF ALBERNI | 60 | 3,428.77 | 11.03 | 11.02 | 7,197 | 4 |
| 8 | 316001251 | QUEEN OF COWICHAN | 60 | 3,222.26 | 10.36 | 10.36 | 6,657 | 2 |
| 9 | 368067110 | LADY SWIFT | 40 | 3,139.82 | 10.10 | 10.09 | 6,260 | 1 |
| 10 | 316001257 | QUEEN OF OAK BAY | 60 | 3,099.40 | 10.11 | 9.96 | 5,998 | 3 |

Cada integrante del Top 10 tuvo un solo nombre y un solo tipo reportado (`nombres_distintos = tipos_distintos = 1`), sin ambigüedad de identificación en el ranking.

## Controles

- Con D-07 se formaron 60,450,496 pares; D-04 aceptó 60,369,977 y excluyó 80,519. El perfilamiento antes de deduplicar registró 60,451,882 pares y 60,369,975 elegibles. La diferencia de 1,386 pares totales corresponde exactamente a las copias eliminadas por D-07; dos pares adicionales pasan el umbral después de reconstruir la secuencia sin esas copias.
- Distribución por MMSI: 31,780 MMSI; percentiles de posiciones `p50=957`, `p95=8,197`, `p99=8,607`, máximo `9,581`. El máximo es solo 11.3% superior a p99: no hay evidencia de un MMSI desproporcionado que produzca skew severo.
- Sanity check del primer puesto: 5,770.92 km en 168 h implica 18.55 kn sostenidos durante la semana; la media por pares es 18.61 kn y coincide casi exactamente. Sus 8 pares excluidos frente a 2,865 válidos descartan que la distancia sea producto de pocos saltos grandes. El tipo AIS 31 es `Towing`; una velocidad de tránsito cercana a 19 kn es alta pero físicamente plausible para una embarcación de remolque sin remolque pesado, y queda muy por debajo del límite D-04 de 60 kn.

## Plan y costo

El plan Photon contiene `PhotonShuffleExchange` por MMSI, `PhotonSort(MMSI, event_ts)` y `PhotonWindow` para los `lag`; ese conjunto tiene un costo mayor que 03a, de agregación diaria, y 03b, de agregación por tipo AIS. También aparecen la deduplicación global D-07, una agregación por MMSI, un segundo escaneo para `VesselName`/`VesselType` y el `PhotonTopK` final. Photon soportó íntegramente el plan. La corrida completa tomó 3 min 18 s sin `cache()` ni `persist()`.
