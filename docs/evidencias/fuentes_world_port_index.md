# Fuente — World Port Index

- **Proveedor:** National Geospatial-Intelligence Agency (NGA), *World Port Index*, publicación 150.
- **URL de descarga:** `https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv`
- **Consulta y descarga:** 2026-09-15.
- **Archivo conservado en Databricks:** `/Volumes/oceanwatch_g06/reference/world_port_index/UpdatedPub150.csv`.
- **Integridad observada:** 3,508,891 bytes; SHA-256 `315644f1e77966291633145fd351c2762b37702d115926b9ed074b39e8e21667`.
- **Preparación usada en 03d:** 3,807 puertos con latitud y longitud no nulas; se usan `Main Port Name`, `Country Code`, `World Port Index Number`, `Latitude` y `Longitude`.

La asociación no afirma que cada posición esté dentro de un recinto portuario. Compara el centroide de la celda H3 r8 con el punto WPI y reporta la sensibilidad de radio en `docs/evidencia_pregunta_03d.md`.
