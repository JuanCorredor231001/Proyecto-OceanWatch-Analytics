# Evidencia — pregunta 03e: permanencia semanal y visitantes de un día

Ejecución: 2026-09-15 en Databricks Free Edition serverless, Spark 4.2.0. Se aplicó D-07 completo: `dropDuplicates` de las 17 columnas y MMSI que cumple `^[0-9]{9}$`.

## Distribución por días con transmisión

| Días transmitidos | Buques válidos | % del total semanal |
|---:|---:|---:|
| 1 | 5,964 | 18.7665% |
| 2 | 4,361 | 13.7225% |
| 3 | 2,888 | 9.0875% |
| 4 | 2,200 | 6.9226% |
| 5 | 2,002 | 6.2996% |
| 6 | 1,709 | 5.3776% |
| 7 | 12,656 | 39.8238% |

El total post-D-07 es **31,780 MMSI válidos**; 12,656 transmitieron los siete días, equivalentes a **39.8238%**. El control de suma confirmó `31,780 = 31,780`, sin doble conteo.

## Visitantes de un solo día

Hay **5,964 visitantes**. El patrón por tipo AIS se calculó con el primer `VesselType` no nulo de cada MMSI, como queda explícito en el notebook. La concentración principal es:

| VesselType | Descripción | Visitantes | % de visitantes |
|---:|---|---:|---:|
| 37 | Pleasure craft | 2,795 | 46.8645% |
| 36 | Sailing | 1,113 | 18.6620% |
| nulo | Sin tipo AIS | 504 | 8.4507% |
| 30 | Fishing | 365 | 6.1201% |
| 31 | Towing | 246 | 4.1247% |
| 70 | Cargo | 178 | 2.9846% |

Los tipos recreativos 37 y 36 reúnen 65.5265% de los visitantes, un patrón compatible con tráfico local o recreativo de corta aparición. El mayor número no ocurrió el fin de semana: fue el **01-jun** con 1,326 (22.2334%), seguido de 07-jun con 1,244 (20.8585%).

| Día único | Visitantes | % de visitantes |
|---|---:|---:|
| 2023-06-01 | 1,326 | 22.2334% |
| 2023-06-02 | 831 | 13.9336% |
| 2023-06-03 | 759 | 12.7264% |
| 2023-06-04 | 723 | 12.1227% |
| 2023-06-05 | 501 | 8.4004% |
| 2023-06-06 | 580 | 9.7250% |
| 2023-06-07 | 1,244 | 20.8585% |

## Plan y costo

La consulta terminó en **1 min 58 s**, sin `cache()` ni `persist()`, y Photon soportó íntegramente el plan. Como en 03a, el costo fundamental está en la deduplicación D-07 y los intercambios (`PhotonShuffleExchange`). La diferencia está en la dirección del agregado: 03a agrupa por `(ingestion_date, MMSI)` y termina por día; 03e agrupa por `MMSI` y calcula `countDistinct(ingestion_date)` antes de derivar la distribución.

El `PhotonUnion` de las salidas de distribución, tipos, días y control expuso varias ramas `Scan csv`: una DataFrame lazy referenciada en varias agregaciones no se materializa automáticamente. El tiempo fue aceptable para el cierre analítico; no se escribió una tabla intermedia fuera del alcance. En el requisito 4 se evaluará materializar una tabla optimizada si estas métricas deben ejecutarse recurrentemente.
