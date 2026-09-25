# Cierre — requisito 4: almacenamiento óptimo

- **Propósito elegido:** reconstruir el historial temporal de uno o varios buques mediante `MMSI` y `BaseDateTime`. Representa directamente 03c y 03e, las preguntas de mayor costo observado.
- **Conjunto comparable:** ambos artefactos contienen 60,482,276 posiciones tras D-07, con deduplicación exacta y exclusión de MMSI no conformes. La comparación separa así formato y layout de la limpieza aplicada.
- **Producción:** `oceanwatch_g06.analytics.ais_positions_d07_delta` es una tabla Delta administrada con `CLUSTER BY (MMSI, BaseDateTime)`, comentario de procedencia y limpieza D-07.
- **Línea base:** Parquet se conserva como archivos en el Volume gobernado `oceanwatch_g06.analytics.benchmark_files` y no es productivo. Databricks Free Edition no permite tablas administradas `USING PARQUET`; solo Delta. Esta es una restricción de plataforma, no una omisión del experimento.
- **Beneficio demostrado:** con el mismo filtro por MMSI y tiempo, Delta alcanza 4 archivos frente a 57 en Parquet. El data skipping proviene del `CLUSTER BY` aplicado durante la escritura, no de una mejora posterior artificial.
- **OPTIMIZE y trade-off:** `OPTIMIZE` duró 7.523 s y reescribió 0 archivos; Delta siguió en 24 archivos y 1,454,406,725 bytes. El acceso amplio sin predicado MMSI alcanza los 24 archivos Delta. El layout se justifica por el propósito temporal por MMSI, no como optimización universal.

La evidencia de tamaños, planes, archivos alcanzados y mediciones está en `docs/evidencia_almacenamiento_pre_optimize.md`.
