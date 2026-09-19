# Evidencia - Pregunta 03a: buques distintos por día

Pregunta: ¿cuántos buques distintos transmitieron cada día? Ejecución 2026-09-14 en Databricks serverless Spark 4.2.0.

La pregunta aplica D-07: `dropDuplicates` sobre las 17 columnas antes del análisis y `MMSI RLIKE '^[0-9]{9}$'` para la métrica por buque. Se presentan el conteo sin excluir MMSI no conformes, el exacto con la exclusión y la estimación por `approx_count_distinct`.

| Día | Sin excluir | Exacto válido | Efecto exclusión | Aproximado | Diferencia abs. | Diferencia % |
|---|---:|---:|---:|---:|---:|---:|
| 2023-06-01 | 20,448 | 20,422 | 26 | 21,782 | 1,360 | 6.6595 |
| 2023-06-02 | 21,153 | 21,086 | 67 | 23,028 | 1,942 | 9.2099 |
| 2023-06-03 | 20,505 | 20,456 | 49 | 20,680 | 224 | 1.0950 |
| 2023-06-04 | 19,720 | 19,698 | 22 | 20,275 | 577 | 2.9292 |
| 2023-06-05 | 19,615 | 19,597 | 18 | 20,358 | 761 | 3.8832 |
| 2023-06-06 | 19,717 | 19,691 | 26 | 20,288 | 597 | 3.0318 |
| 2023-06-07 | 20,086 | 20,056 | 30 | 21,184 | 1,128 | 5.6243 |

## Planes observados

Ambos planes leen los siete CSV desde el Volume, aplican el filtro MMSI y usan Photon. El plan exacto hace la deduplicación completa y agrupa por `(ingestion_date, MMSI)` antes del conteo final; observó tres intercambios Shuffle relevantes. El aproximado también paga la deduplicación y usa `partial_approx_count_distinct(MMSI, 0.05)` con buffer HyperLogLog. Reduce una fase de agregación posterior, pero mantiene el escaneo y el shuffle dominante de D-07.

## Decisión de producción

Para esta pregunta se usa `countDistinct`. La cardinalidad real ronda 20 mil MMSI por día y el cálculo cubre solo siete días; el resultado exacto es defendible frente a un error observado entre 1.10% y 9.21%. `approx_count_distinct` queda como alternativa si el corpus crece de forma material o un SLA medido justifica tolerar error.

El plan mostró `partial_approx_count_distinct(MMSI, 0.05)`: se usó el RSD predeterminado de 5%, no una configuración de 1-2%. Con solo ~20 mil IDs por grupo diario, la variación estocástica por grupo puede hacer que una realización puntual exceda el RSD esperado; el 9.21% observado es precisamente evidencia para no usar la estimación en esta pregunta.
