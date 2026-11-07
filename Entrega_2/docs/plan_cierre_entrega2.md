# Plan de cierre - Entrega 2

Estado al 2026-11-06. El código de los requisitos 1 a 6 está implementado y probado en local (Docker). En el workspace institucional ya corrieron el backfill completo (01-07 jun, 60.5 M posiciones) y el job diario del 08-jun. Este plan lista lo que falta para cerrar la entrega, en orden de ejecución.

Los 14 ZIP NOAA (01-14 jun 2023) ya están en `Datos/downloads/`.

## Fase 0 - Ajustes previos en el repositorio (local, ~1 h)

**Estado (2026-11-07): hecha**, salvo 0.7 (opcional) y el commit de 0.8. Además se corrigió un error que no estaba en la lista: el `.lvdash.json` consultaba `oceanwatch_g06` y no `oceanwatch_g07`. Las alertas usan `${workspace.current_user.userName}` en lugar de un correo fijo, y el target `produccion` recibió su `host`. Verificación: YAML válidos, pytest 16/16 y demo de feed ausente repetida.

| # | Tarea | Por qué | Archivo |
|---|---|---|---|
| 0.1 | Mantener verificable la demo de "feed ausente": cambiar la falla por un día sin ZIP real (p. ej. `2023-06-15`) o por `--inyectar-falla aterrizar_feed`, en vez de depender de que falte el ZIP de 06-10 | Con los 14 ZIP descargados, `fecha=auto → 2023-06-10` ya no falla y la evidencia 08 deja de ser reproducible | `scripts/run_pipeline_local.py`, `docs/evidencias/08 evidencia_orquestacion.md` |
| 0.2 | Actualizar la sección "Prueba local": ahora hay 14 ZIP y la nota "si falta un ZIP la tarea falla" debe decir cómo se prueba la falla | Documentación coherente con los datos | `README.md` |
| 0.3 | Quitar la advertencia "Pendiente de validar en el workspace": la evidencia 12 muestra el deploy y las corridas | Contradice la evidencia | `README.md` |
| 0.4 | Corregir el enlace `../Entrega_1/...`: la Entrega 1 vive en la raíz del repositorio, no en una carpeta `Entrega_1/` | Enlaces rotos en README y decisiones | `README.md`, `docs/decisiones/decisiones_entrega2.md` |
| 0.5 | Unificar el catálogo: la bitácora dice `oceanwatch_g06` en D-06 y el resto usa `oceanwatch_g07` | Inconsistencia | `docs/decisiones/BITACORA.md` |
| 0.6 | Reemplazar `correo_alertas = equipo-oceanwatch@example.com` por un correo real del equipo | Sin él, la notificación de falla (req. 3) no llega a nadie | `databricks.yml` |
| 0.7 | Opcional: correr en local el feed 10-14 con `--muestreo 10` para ampliar las métricas de calidad locales | Más días de evidencia local; ~2 min por día | `scripts/run_pipeline_local.py` |
| 0.8 | Primer commit de `Entrega_2/`. Antes, revisar con `git status` que no entren datos ni artefactos (`Datos/`, `local_lakehouse*/`, `.ivy2/`, `spark-warehouse/` y `.databricks/` ya están en `.gitignore`) | La carpeta completa sigue sin versionar | git |

## Fase 1 - Feed diario completo en Databricks (req. 1 y 3, 35 %)

**Estado (2026-11-07): hecha salvo capturas y la falla en el workspace (1.4).**
- 1.1: bundle desplegado.
- 1.2: el feed 08-14 ya estaba procesado (7 corridas SUCCESS).
- 1.3 y 1.6: corrida `PERIODIC` del cron a las 06:00, con el DAG cortado por `hay_fecha_pendiente = false`.
- 1.5: bronce = CSV y plata + cuarentena = bronce en los 7 días.

En 1.4 falta una corrida fallida en el workspace que dispare el correo; hoy los reintentos solo están documentados en el backfill. Detalle en las evidencias 08 y 12.

| # | Tarea | Evidencia que debe quedar |
|---|---|---|
| 1.1 | `databricks bundle deploy -t institucional` con los cambios de la fase 0 | Salida del deploy |
| 1.2 | Correr `oceanwatch_diario` con `fecha=auto` para 09, 10, 11, 12, 13 y 14 jun (una corrida por día) | IDs de corrida y duración por tarea, igual que en la evidencia 12 |
| 1.3 | **Ejecución programada demostrada:** desplegar `-t produccion` o reanudar el schedule y dejar que al menos una corrida la dispare el cron (se puede bajar el cron temporalmente a cada 15-30 min) | Captura del run con *Trigger: Scheduled* |
| 1.4 | **Reintentos y falla:** una corrida con un día sin datos o una falla inyectada, que muestre reintentos, tareas omitidas y el correo de falla | Captura del historial de reintentos y del correo |
| 1.5 | Verificar idempotencia: re-ejecutar un día ya procesado y confirmar que bronce y plata leen 0 filas nuevas y oro no duplica | Conteos antes y después |
| 1.6 | Correr `oceanwatch_diario` una vez más con todo procesado: la condición `hay_fecha_pendiente` debe cortar el DAG | Captura del run |

## Fase 2 - Tablero (req. 4, 15 %)

**Estado (2026-11-07): hecha salvo capturas.**
- 2.1: los 4 paneles pedidos se ven en el tablero publicado.
- 2.2: el filtro cubre 01-14 jun y abre en 14-jun; el refresco corrió en las 7 corridas del feed.
- 2.3: se corrigieron el mapa (`symbol-map` sobre rejilla 0.05°, antes era inválido), las tablas (spec v2) y el filtro sin valor por defecto. Detalle en la evidencia 09.

| # | Tarea | Evidencia |
|---|---|---|
| 2.1 | Abrir el tablero publicado y revisar los cuatro elementos exigidos: mapa de intensidad por celda, ranking de puertos, buques en puerto y anomalías del día | Captura de cada widget |
| 2.2 | Confirmar que el filtro de fecha cubre 01-14 jun y que el refresco automático del job diario actualiza los datos | Captura antes y después de un día del feed |
| 2.3 | Si un widget no carga (por ejemplo el mapa), recrearlo en la UI sobre el mismo dataset y exportar de nuevo el `.lvdash.json` | JSON actualizado en `dashboards/` |

## Fase 3 - Gobernanza (req. 5, 10 %)

**Estado (2026-11-07): hecha salvo la captura del linaje y una decisión.**
- 3.1: sin permisos de administrador de la cuenta no hay grupos de cuenta, así que se crearon 4 *service principals* por rol.
- 3.2: el notebook 12 terminó SUCCESS y los permisos coinciden con el diseño.
- 3.3: el linaje se consultó por SQL; faltan la captura y declarar los huecos bronce → plata y features → anomalías.
- 3.4: etiquetas en 5/5 tablas y comentarios en 93/93 columnas.

**Decisión (2026-11-07):** se mantiene el grant de `account users` (`ALL PRIVILEGES` + `MANAGE` sobre `oceanwatch_g07`) como riesgo aceptado, para no quitar el acceso a los integrantes. La evidencia 10 documenta que los roles no son restrictivos mientras exista y el `REVOKE` que se usaría en producción.

| # | Tarea | Evidencia |
|---|---|---|
| 3.1 | Pedir o crear los grupos de cuenta (ingenieros, científicos, analistas, tablero), o mapearlos a grupos existentes del workspace | Lista de grupos |
| 3.2 | Ejecutar `notebooks/12_gobernanza.py` con esos nombres | Salidas de `SHOW GRANTS` por esquema |
| 3.3 | Capturar el linaje en Catalog Explorer: bronce → plata → oro → tablero y oro → features → modelo | Capturas del grafo de linaje |
| 3.4 | Verificar comentarios y tags del contrato en las tablas oro (`DESCRIBE TABLE EXTENDED`, `information_schema.table_tags`) | Salida del notebook |

## Fase 4 - ML con MLflow (req. 6, 10 %)

**Estado (2026-11-07): hecha.**
- 4.1: run `ca6f949f…` capturado con sus parámetros y métricas.
- 4.2: modelo en UC capturado, con v3 `@champion`.
- 4.3: el feed 08-14 (y los 14 días) se puntuó con v3: 3,098 anomalías.

Capturas en `docs/evidencias/capturas/11*.jpg`. El linaje de la Fase 3 está en `10a_*.jpg` (lista) y `10b_*.jpg` (grafo).

| # | Tarea | Evidencia |
|---|---|---|
| 4.1 | Capturar el experimento (parámetros, métricas, artefactos) del entrenamiento del backfill | Captura de la UI de MLflow |
| 4.2 | Capturar el modelo en Unity Catalog con la versión y el alias `champion` | Captura de Catalog Explorer |
| 4.3 | Mostrar que `ml_puntuar` del job diario carga `@champion` y escribe `anomalias_velocidad_diarias` para cada día del feed | Conteo de anomalías por día 08-14 |

## Fase 5 - Bono Delta Sharing (+5 %)

**Estado (2026-11-07): bloqueado por el administrador.**
- 5.1: el equipo sí tiene `CREATE SHARE` y `CREATE RECIPIENT`, pero el metastore tiene `delta_sharing_scope = INTERNAL`.
- 5.2: `oceanwatch_oro` se creó con las 5 tablas oro. `CREATE RECIPIENT` falló con "External Delta Sharing is not enabled on the metastore".
- 5.3 y 5.4: no se pueden hacer hasta que un administrador de la cuenta habilite el sharing externo.

Detalle y pasos para completarlo en la evidencia 13.

| # | Tarea | Evidencia |
|---|---|---|
| 5.1 | Confirmar con el administrador del workspace institucional los privilegios `CREATE SHARE` y `CREATE RECIPIENT` (bloqueante: sin ellos el bono no se puede hacer) | Respuesta del administrador |
| 5.2 | Ejecutar `13_delta_sharing_proveedor.py`; descargar el archivo de credenciales desde el enlace de activación y guardarlo **fuera de Git** | `SHOW ALL IN SHARE`, `SHOW GRANTS ON SHARE` |
| 5.3 | En Free Edition, ejecutar `14_delta_sharing_consumidor_free.py` y rehacer los cuatro widgets del tablero sobre los datos compartidos | Capturas del tablero en Free Edition |
| 5.4 | Documentar en una evidencia nueva (`13 evidencia_delta_sharing.md`) que no hubo copia manual de datos | Archivo de evidencia |

## Fase 6 - Documentación y cierre (req. 7, 5 %, y sustentación, 10 %)

**Estado (2026-11-07).**
- 6.1 hecha: evidencias 08 a 13 completas y 8 capturas en `docs/evidencias/capturas/`.
- 6.2 hecha: la bitácora tiene una fila por fase y los riesgos al día.
- 6.4 hecha: README al día con el estado y la evidencia 13.

**Lo que solo puede hacer el equipo:**
- 6.3: aprobar en equipo D-E2-01 a D-E2-09, más las notas del mapa (D-E2-07) y el riesgo aceptado de `account users`.
- 6.5: preparar la sustentación.
- 6.6: push y entrega en Bloque Neón.

**Pendientes técnicos:**
- 1.4: corrida fallida en el workspace con correo de alerta.
- Bono: pedir al administrador de la cuenta que habilite External Delta Sharing.

| # | Tarea |
|---|---|
| 6.1 | Completar las secciones "Pendiente" de las evidencias 08 a 12 con los resultados y las capturas (guardar las imágenes en `docs/evidencias/capturas/`) |
| 6.2 | Agregar a la bitácora una fila por sesión de trabajo (fases 1-5) con integrante, hallazgo y evidencia; cerrar los riesgos abiertos |
| 6.3 | Aprobar en equipo las decisiones D-E2-01 a D-E2-09 y registrar la aprobación |
| 6.4 | Revisar forma y redacción de README y evidencias (la calidad de forma se califica) |
| 6.5 | Preparar la sustentación: cada integrante explica de punta a punta un requisito y rota a otros, ya que cualquiera puede ser preguntado sobre cualquier parte |
| 6.6 | Commit y push final; subir la entrega a Bloque Neón |

## Orden sugerido y dependencias

```text
Fase 0 ─► Fase 1 ─► Fase 2 ─┐
               ├──► Fase 4 ─┼─► Fase 6
               └──► Fase 3 ─┘
Fase 5 (en paralelo, depende solo de la respuesta del administrador en 5.1)
```

Las fases 2, 3 y 4 pueden repartirse entre los tres integrantes una vez termine la fase 1.
