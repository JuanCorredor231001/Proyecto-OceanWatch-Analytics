# Evidencia - Pregunta 03b: tipos que generan más tráfico

El catálogo se tomó de [NOAA Marine Cadastre Vessel Type Codes](https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf) y se contrastó con la [guía AIS de USCG](https://www.navcen.uscg.gov/sites/default/files/pdf/AIS/AISGuide.pdf). El detalle está en `docs/fuentes_catalogo_ais.md`.

D-07 elimina duplicados exactos como regla transversal. El filtro de MMSI no conformes no aplica porque la pregunta agrega posiciones por `VesselType`, sin agrupar ni identificar buques individuales.

| Código | Tipo | Posiciones | SOG media con 102.3 | SOG media sin 102.3 | Registros 102.3 | Efecto (kn) |
|---:|---|---:|---:|---:|---:|---:|
| 31 | Towing | 16,557,760 | 1.899 | 1.677 | 36,505 | 0.2218 |
| 37 | Pleasure craft | 14,476,457 | 1.642 | 1.348 | 42,231 | 0.2945 |
| 60 | Passenger | 4,795,082 | 4.434 | 4.109 | 15,869 | 0.3250 |
| 30 | Fishing | 4,010,116 | 2.466 | 2.245 | 8,872 | 0.2214 |
| 36 | Sailing | 3,876,193 | 1.979 | 1.626 | 13,611 | 0.3535 |
| 90 | Other type | 3,848,064 | 2.028 | 1.765 | 10,065 | 0.2630 |
| 70 | Cargo | 3,837,757 | 6.370 | 6.330 | 1,591 | 0.0398 |
| 52 | Tug | 1,921,509 | 2.093 | 1.724 | 7,059 | 0.3695 |
| 80 | Tanker | 1,789,094 | 5.875 | 5.862 | 234 | 0.0126 |
| 57 | Local vessel assignment (spare) | 1,323,745 | 1.911 | 1.904 | 88 | 0.0067 |

El plan escanea los siete CSV y hace la deduplicación global D-07, su primer shuffle grande. Después agrega por `VesselType`, de cardinalidad baja, cruza el catálogo local mediante broadcast hash join y ordena el Top 10. A diferencia de 03a, no requiere el shuffle adicional por `(día, MMSI)` del conteo distinto exacto. El costo observado fue 53.37 s.
