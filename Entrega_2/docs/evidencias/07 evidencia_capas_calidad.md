# Evidencia requisito 2 - Capas con la calidad como compuerta

**Ejecución local:** 2026-11-06. Muestra por buque `crc32(MMSI) % 10 == 0` (~10 % de los buques, con trayectorias completas) más todos los MMSI no conformes, para ejercitar la cuarentena. Datos completos: [`local/resultados_locales.md`](local/resultados_locales.md).

## Flujo de filas por capa

| Tabla | 2023-06-01 | 2023-06-08 | 2023-06-09 |
|---|---:|---:|---:|
| `bronze.ais_raw` (crudo inmutable) | 846,338 | 872,650 | 859,555 |
| `silver.ais_posiciones` (pasan las compuertas) | 838,048 | 864,428 | 851,120 |
| `silver.ais_cuarentena` (con causa) | 8,290 | 8,222 | 8,435 |
| **Reconciliación bronce = plata + cuarentena** | ✅ | ✅ | ✅ |
| `gold.trayectorias_diarias_buque` | 1,996 | 2,054 | 2,150 |
| `gold.estadias_puerto` | 503 | 532 | 586 |
| `gold.puertos_resumen_diario` | 78 | 81 | 86 |
| `gold.trafico_celda_hora` | 114,493 | 123,300 | 127,873 |
| `gold.anomalias_velocidad_diarias` | 1,996 | 2,054 | 2,150 |

## Bronce: crudo inmutable

Las 17 columnas se guardan como texto, junto con `_rescued_data`, `_source_file`, `_source_modified_at`, `_ingested_at` e `ingestion_date`. La tabla es `delta.appendOnly = true`: un `DELETE` falla con `DELTA_CANNOT_MODIFY_APPEND_ONLY` (prueba `test_bronce_es_append_only`).

## Plata: compuertas (expectations), cuarentena y métricas

`silver.metricas_calidad` guarda, por micro-lote, fecha y regla, los registros evaluados y fallidos. Resumen por día:

| Regla | Severidad | 01-jun | 08-jun | 09-jun |
|---|---|---:|---:|---:|
| E01 MMSI de 9 dígitos | CUARENTENA | 6,911 | 6,362 | 7,249 |
| E02-E05 timestamp, ventana y coordenadas | CUARENTENA | 0 | 0 | 0 |
| E06 en el agua | CUARENTENA | 1,360 | 1,843 | 1,147 |
| E07 SOG plausible (≤ 60 kn o 102.3) | CUARENTENA | 11 | 4 | 14 |
| D01 duplicado exacto (MMSI, ts) | DEDUPLICACION | 13 | 12 | 20 |
| D02 duplicado conflictivo (MMSI, ts) | DEDUPLICACION | 0 | 4 | 9 |
| **Cuarentena por compuertas** | RESUMEN | **0.978 %** | **0.940 %** | **0.978 %** |
| E10 IMO mal formado | ADVERTENCIA | 8,094 | 6,034 | 5,263 |
| E11 VesselType fuera de catálogo | ADVERTENCIA | 1,608 | 339 | 1,064 |
| E12 dimensiones no plausibles | ADVERTENCIA | 28,089 | 30,192 | 34,768 |

- **Cuarentena sin detener el stream:** los registros inválidos se enrutan dentro de `foreachBatch` a `silver.ais_cuarentena`, con `motivos` (todas las reglas incumplidas) y `motivo_principal`. El stream sigue.
- **Coherencia con la Entrega 1:** E01 es D-07. Sobre el corpus completo, la Entrega 1 midió 0.0824 % de MMSI no conformes. En la muestra la tasa es mayor (≈ 0.8 %) porque conserva el 100 % de esos MMSI y solo el 10 % de los válidos. Igual que en el perfilamiento, no hay coordenadas fuera de rango ni SOG negativa.
- **E06 en el agua:** la máscara v1 generaba falsos positivos en vías interiores y se corrigió con evidencia (D-E2-04). Siguen en cuarentena tramos de un solo buque en la muestra (p. ej. REDDING C y A.B. CENAC en canales de Luisiana, LUCAS PAUL DEVALL en el Victoria Barge Canal, TX), junto con errores reales (STEELROD, 102.2 kn en el interior de California; NANEA, 64.3 kn en Oregón). Limitación declarada: con los 7 días completos crecen las excepciones navegables.

## Oro: solo desde plata

- `gold.py` lee únicamente `silver.ais_posiciones` y reescribe por fecha con `replaceWhere`.
- `validar_contrato` comprueba en cada fecha que (a) todos los MMSI de oro existen en plata y (b) ninguna clave (MMSI, ts) en cuarentena por compuertas aparece en plata. Resultado: **0 violaciones, 102/102 chequeos por fecha**.
- Las pruebas `test_lote_enruta_deduplica_y_es_idempotente` y `test_oro_cumple_contrato` lo verifican con datos sintéticos.

### Corrección de la regla de estadías (v1 → v2)

| Métrica 01-jun | v1 (corte al salir de las celdas) | v2 (corte tras > 1 h fuera del puerto) |
|---|---:|---:|
| Estadías | 783 | 503 |
| Bellingham: arribos / buques | 46 / 12 | 4 / 11 (09-jun) |
| Puertos repetidos en el ranking | Port Everglades ×2, Port Arthur ×2 | ninguno (homónimos fusionados) |
