# Evidencia - ingesta local de los siete días

Ejecución: 2026-09-14 local, PySpark 4.2.0, sin `cache()` ni `persist()`.

El lector usó las 17 columnas del `StructType` definido en `scripts/01_ingesta_ais_local.py`, con `enforceSchema=false` y modo `FAILFAST`. Antes de contar filas, cada encabezado debía coincidir con el esquema explícito.

| Archivo | Bytes | Filas | Lectura + conteo (s) | SHA-256 | Esquema |
|---|---:|---:|---:|---|---|
| AIS_2023_06_01.csv | 943,431,681 | 8,808,904 | 8.89 | `1fb46f4f10872a63413f80ff0be8ff0439d720e216525969528ed10a65c3b26d` | Coincide |
| AIS_2023_06_02.csv | 969,444,078 | 9,052,241 | 2.18 | `3b07e90250f0b86cec06dbd07066f744768d333b65a885066ab4429dfe9d9a8e` | Coincide |
| AIS_2023_06_03.csv | 860,511,883 | 8,036,348 | 2.03 | `4b15d1b8b63cc994835c2c59132b2676fbcef7ca5ab48397b66509bdef54a0ea` | Coincide |
| AIS_2023_06_04.csv | 913,283,650 | 8,522,645 | 2.74 | `938a933c8ec3f6a059b286a63fccfe97e3440e8f18cd07151fda9f7af9697629` | Coincide |
| AIS_2023_06_05.csv | 922,808,494 | 8,613,757 | 1.91 | `ef90dd267333538da947b935f323721b0b6c464df6497229caeae34edf23824a` | Coincide |
| AIS_2023_06_06.csv | 918,827,336 | 8,587,938 | 2.86 | `8fd06262a2013ce2df744855a07df9ca2d8db4b847d1679ef27fcae11a4bc82f` | Coincide |
| AIS_2023_06_07.csv | 953,614,322 | 8,911,726 | 1.85 | `6e08dbc6b612ec03e39531605eb2ab12b24ca01ee74a46ec7d70f50107017399` | Coincide |
| **Total** | **6,481,921,444** | **60,533,559** | **22.46** | — | **Coincide** |

## Unión

La unión `unionByName(..., allowMissingColumns=False)` de los siete DataFrames produjo 60,533,559 filas en 12.39 s y conservó el `StructType` explícito.

No hubo fallos de encabezado, lectura CSV ni unión. El `StructType` se aplicó igual a los siete archivos; el perfilamiento de calidad concentra la cuantificación de nulos y valores no convertibles. La ejecución completa, incluidos SHA-256, inicialización Spark, conteos y unión, tomó 47.71 s. Esta línea base local no se extrapola directamente a Databricks serverless.
