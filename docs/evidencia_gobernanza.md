# Evidencia — requisito 5: gobernanza y documentación

Fecha de verificación: 2026-09-15. Notebook ejecutado: `05_gobernanza_documentacion` en Databricks Free Edition.

## Organización y metadatos

- Catálogo: `oceanwatch_g07`, tipo `Regular`, comentario **Lakehouse OceanWatch Analytics para tráfico marítimo AIS de NOAA**.
- Esquemas: `landing` (datos AIS NOAA descargados y extraídos), `reference` (datos de referencia) y `analytics` (artefactos analíticos).
- Volumes: `landing.raw_ais`, `reference.world_port_index` y `analytics.benchmark_files`; este último tiene el comentario **Línea base Parquet del experimento de almacenamiento; evidencia reproducible, no artefacto productivo**.
- Tabla oficial: `analytics.ais_positions_d07_delta`, Delta administrada con `CLUSTER BY (MMSI, BaseDateTime)` y comentario que registra fuente AIS NOAA y limpieza D-07.

La evidencia proviene de `DESCRIBE CATALOG EXTENDED`, `DESCRIBE SCHEMA EXTENDED`, `DESCRIBE VOLUME` y `DESCRIBE TABLE EXTENDED` ejecutados en el notebook. Los resultados muestran la jerarquía `catalog > schema > volume/table`, tipo `MANAGED` en los Volumes y los comentarios aplicados.

## Propiedad y permisos efectivos en Free Edition

- El owner que reporta Unity Catalog para el catálogo y los objetos revisados es el **usuario propietario del workspace Free Edition del equipo**.
- `SHOW GRANTS ON CATALOG oceanwatch_g07`, `SHOW GRANTS ON SCHEMA oceanwatch_g07.analytics` y `SHOW GRANTS ON TABLE oceanwatch_g07.analytics.ais_positions_d07_delta` devolvieron cero filas: no hay grants explícitos adicionales registrados en esos niveles.
- El proyecto queda documentado bajo la propiedad efectiva del usuario del workspace Free Edition. No se agregaron permisos artificiales para la evidencia; una colaboración futura debe materializarse con grants explícitos y verificarse de nuevo con los mismos comandos.
