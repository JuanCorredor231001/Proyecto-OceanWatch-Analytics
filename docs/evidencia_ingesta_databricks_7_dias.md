# Evidencia de ingesta Databricks - siete días AIS

Fecha de ejecución: 2026-09-14. Entorno: Databricks Free Edition serverless, Spark 4.2.0. Destino: `/Volumes/oceanwatch_g06/landing/raw_ais`.

## Cierre previo: 01-jun

El CSV extraído se leyó con el `StructType` explícito y produjo **8,808,904** filas, exactamente el conteo local. La primera ejecución end-to-end tomó 14.06 s en descarga, 7.88 s en extracción, ~13 s en lectura/conteo y 46.995 s en total.

El ZIP de 01-jun tuvo 343,949,892 bytes y SHA-256 `e6287c47f9659fd3bc6c0456579e93f07e3ec099c347514757a86413d0c99d6a`; el CSV extraído tuvo 943,431,681 bytes. Sus hashes no se comparan entre sí: uno corresponde al archivo comprimido descargado y el otro al CSV descomprimido.

## Ejecución escalada

| Día | ZIP bytes | SHA-256 ZIP observado | CSV bytes | Filas | Descarga (s) | Extracción (s) | Lectura + conteo (s) |
|---|---:|---|---:|---:|---:|---:|---:|
| 2023-06-01 | 343,949,892 | `e6287c47f9659fd3bc6c0456579e93f07e3ec099c347514757a86413d0c99d6a` | 943,431,681 | 8,808,904 | 19.56 | 7.26 | 2.35 |
| 2023-06-02 | 354,145,683 | `9f01ff4c3a21240c41ed0ca64a945be7fb8bee0f5bff049dc5c3e727aa02b8ac` | 969,444,078 | 9,052,241 | 9.80 | 13.09 | 2.45 |
| 2023-06-03 | 312,248,025 | `070ccec839de290cba810308753b56d2bac6839a9fd076df6ab089b525e046c8` | 860,511,883 | 8,036,348 | 9.33 | 6.72 | 2.06 |
| 2023-06-04 | 332,457,486 | `396efd491ea992aded336ba85bfed1cd8079baf294fb1fadf215e69d40ed42a0` | 913,283,650 | 8,522,645 | 12.74 | 7.50 | 2.20 |
| 2023-06-05 | 335,658,758 | `00899e6450a4cc3170059ed990c94e060257e86f22edd5c6683f282e5ce7f4c8` | 922,808,494 | 8,613,757 | 9.47 | 7.44 | 2.16 |
| 2023-06-06 | 335,300,028 | `c8b4acd430a675fbffe3665d9ed9843ce0696c947e4d35411a504ad2ffc595f4` | 918,827,336 | 8,587,938 | 28.71 | 7.61 | 2.14 |
| 2023-06-07 | 347,918,296 | `03b9b5ddb1247f08a4c3bab738c7b6c569c93094f82e8830cfea4853f9a5bed7` | 953,614,322 | 8,911,726 | 10.07 | 7.64 | 2.14 |

La celda escalada terminó en 3 min 8 s. La unión por nombre, con contrato de 17 columnas y sin columnas faltantes, tomó 12.35 s y produjo **60,533,559** filas, el total esperado. No aparecieron inconsistencias de esquema ni de encoding.
