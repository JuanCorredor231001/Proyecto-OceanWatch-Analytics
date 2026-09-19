# Evidencia de perfilamiento de calidad - Databricks

Ejecución: 2026-09-14 en Databricks Free Edition serverless con Spark 4.2.0. Fuente: los siete CSV extraídos en `/Volumes/oceanwatch_g07/landing/raw_ais`. No se usó `cache()` ni `persist()` ni se materializó una tabla intermedia.

## Perfil general

| Día | Posiciones | MMSI únicos |
|---|---:|---:|
| 2023-06-01 | 8,808,904 | 20,448 |
| 2023-06-02 | 9,052,241 | 21,153 |
| 2023-06-03 | 8,036,348 | 20,505 |
| 2023-06-04 | 8,522,645 | 19,720 |
| 2023-06-05 | 8,613,757 | 19,615 |
| 2023-06-06 | 8,587,938 | 19,717 |
| 2023-06-07 | 8,911,726 | 20,086 |
| **Total semana** | **60,533,559** | **31,871** |

Los códigos `VesselType` con más posiciones fueron 31 (16,558,120), 37 (14,476,696), 60 (4,795,209), 30 (4,010,188), 36 (3,876,264), 90 (3,848,162) y 70 (3,837,926). Los tamaños más frecuentes fueron `<20 m` / `<10 m` (19,518,882 posiciones), `20-<50 m` / `<10 m` (14,147,617) y `20-<50 m` / `10-<20 m` (6,609,397).

## Nulos y sentinelas

| Campo | Nulos | % |
|---|---:|---:|
| Draft | 39,000,865 | 64.4285 |
| IMO | 25,891,448 | 42.7721 |
| Status | 19,952,397 | 32.9609 |
| Cargo | 19,898,489 | 32.8718 |
| CallSign | 10,157,915 | 16.7806 |
| Width | 9,219,567 | 15.2305 |
| Length | 3,664,725 | 6.0540 |

MMSI, BaseDateTime, LAT, LON, SOG, COG, Heading y TransceiverClass no tuvieron nulos. Las tasas observadas sostienen el diagnóstico inicial de la muestra.

| Regla | Registros | % |
|---|---:|---:|
| LAT fuera de [-90, 90] | 0 | 0 |
| LON fuera de [-180, 180] | 0 | 0 |
| SOG negativa | 0 | 0 |
| SOG = 102.3 (sentinela AIS) | 159,987 | 0.2643 |
| Heading = 511 (sentinela AIS) | 33,535,628 | 55.4001 |
| COG = 360 (sentinela AIS) | 10,291,131 | 17.0007 |
| MMSI no conforme a 9 dígitos | 49,897 | 0.0824 |

`Status` es nulo en el 100% de las 19,952,397 posiciones de `TransceiverClass=B`; en clase A no tiene nulos. Se trata como una ausencia estructural, no como un error que obligue a descartar esas filas. Los sentinelas se marcan como no disponibles y se excluyen solo de los promedios de su propia variable, en particular `SOG=102.3` para velocidad media.

## MMSI, duplicados y D-04

Los MMSI no conformes son únicamente numéricos: 46,772 posiciones de 86 IDs con menos de nueve dígitos y 3,125 posiciones de 5 IDs con más de nueve. No se les atribuye una clase AIS sin una fuente externa. Para análisis por buque y pares haversine se exige MMSI de nueve dígitos.

Se encontraron 1,388 grupos de duplicados exactos, con 2,776 filas involucradas y 1,388 copias adicionales. La muestra inicial había sobrerrepresentado el problema por proporción; aun así se conserva la métrica y se decidirá la deduplicación antes de preguntas analíticas.

| Regla de pares consecutivos | Pares |
|---|---:|
| Pares con evento previo | 60,451,882 |
| Excluir campos requeridos | 0 |
| Excluir gap no positivo | 1,670 |
| Excluir gap > 2 h | 52,418 |
| Excluir velocidad implícita > 60 kn | 27,819 |
| **Pares elegibles D-04** | **60,369,975** |

Las exclusiones D-04 son disjuntas: la velocidad solo se evalúa cuando `0 < gap <= 2 h`.

## Resumen ejecutivo

- El corpus completo contiene 60,533,559 posiciones y 31,871 MMSI únicos en la semana.
- Los porcentajes de nulos a escala sostienen el patrón de la muestra; Draft e IMO son los campos más incompletos.
- No hay coordenadas fuera de rango ni SOG negativas.
- `Status` ausente en clase B es estructural; no se debe usar como criterio de exclusión general.
- Los tres sentinelas AIS están presentes y deben marcarse como no disponibles en métricas de SOG, COG y Heading.
- Los MMSI no conformes son pocos (0.0824%) y solo numéricos, pero se excluyen de agrupaciones por buque y D-04 hasta aclarar su naturaleza.
- Los duplicados exactos suman 1,388 copias adicionales; el tratamiento se declarará antes de resultados de negocio.
- D-04 conserva 60,369,975 pares para distancia; las reglas de gap y velocidad quedan respaldadas cuantitativamente.
