# OceanWatch Analytics - MINE 4213

Este repositorio reúne el lakehouse de tráfico marítimo construido con mensajes AIS de NOAA / Marine Cadastre. Aquí quedan los notebooks, las decisiones y la evidencia de la Entrega 1; los datos crudos se mantienen fuera de Git.

## Equipo

| Integrante | Código |
|---|---|
| Juan Sebastián Corredor Sánchez | 202014956 |
| Karina Marita Aranguren Espinoza | 202326292 |
| Estefania Quijano Garcia | 202311445 |

## Estado

La ingesta y el perfilamiento cubren 60,533,559 posiciones en Databricks Free Edition. Las cinco preguntas de negocio tienen planes de ejecución y evidencia en `docs/`; el requisito 4 de almacenamiento óptimo sigue a continuación.

## Contexto y fuente

El corpus cubre del 1 al 7 de junio de 2023 y reúne posiciones AIS de aguas costeras de Estados Unidos. La fuente oficial es [Marine Cadastre - Vessel Traffic](https://hub.marinecadastre.gov/pages/vesseltraffic).

El notebook de ingesta descarga los archivos diarios desde los enlaces oficiales. Los CSV usados para el prototipado local viven en `Datos/`, directorio excluido de Git.

## Reproducibilidad en Databricks Free Edition

1. Crear un catálogo y esquemas propios en Unity Catalog; registrar sus nombres en `config/` sin incluir secretos.
2. Crear o seleccionar un Volume para los archivos descargados y descomprimidos.
3. Ejecutar los notebooks en este orden:
   - `00_configuracion_unity_catalog`
   - `01_ingesta_ais`
   - `02_perfilamiento_calidad`
   - `03_preguntas_negocio`
   - `04_almacenamiento_optimizado`
   - `05_gobernanza_documentacion`
4. Cada notebook debe incluir sus parámetros, celdas Markdown, validaciones y evidencia de planes de ejecución cuando aplique.

Free Edition serverless no permite `cache()` ni `persist()`, por lo que los notebooks no dependen de esas APIs.

En serverless con Unity Catalog, `input_file_name()` no está disponible. El origen se identifica con `_metadata.file_path`, incluida la evidencia de archivos leídos del requisito 4.

## Entorno local de desarrollo

El prototipo local usa Python 3.10, JDK 21 y `pyspark==4.2.0`, alineado con Apache Spark 4.2.0 de Databricks Runtime 19. El serverless de Databricks se actualiza automáticamente; verificar la versión de `spark.version` en el primer notebook antes de ejecutar el corpus completo.

```powershell
# Desde la raíz del proyecto (Windows PowerShell)
winget install --id Microsoft.OpenJDK.21 --exact --accept-package-agreements --accept-source-agreements
$env:JAVA_HOME = (Get-ChildItem 'C:\Program Files\Microsoft\jdk-21*' -Directory | Select-Object -First 1).FullName
$env:Path = "$env:JAVA_HOME\bin;$env:Path"

python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
python scripts/verificar_entorno_local.py
```

La prueba abre `Datos/AIS_2023_06_01.csv` con un esquema explícito, cuenta una muestra determinista de 1.000 filas, imprime el esquema y genera una celda H3 de resolución 8. El entorno virtual, los temporales de Spark y los CSV se excluyen de Git.

El entorno local conserva `h3==4.5.0` para la prueba mínima. En Databricks Free Edition, la pregunta espacial usa `h3_longlatash3`, `h3_h3tostring` y `h3_centeraswkt`, funciones nativas ejecutadas por Photon. Así no depende de una instalación `%pip` en cada sesión; la rejilla lat/lon queda como contingencia permitida por el enunciado.

## Prototipo local de ingesta

El script `scripts/01_ingesta_ais_local.py` recibe las rutas como parámetros, revisa tamaño y SHA-256, y aplica el esquema AIS explícito. Para una iteración rápida:

```powershell
.\.venv\Scripts\python.exe scripts\01_ingesta_ais_local.py `
  --source-csv Datos\AIS_2023_06_01.csv `
  --work-dir local_artifacts\ingesta_01jun_sample `
  --dataset-date 2023-06-01 `
  --sample-rows 100000 `
  --expected-bytes 943431681
```

Para revisar un día completo, omitir `--sample-rows` y usar otro `--work-dir`. Los artefactos locales se ignoran en Git. El mismo script incluye descarga con reintentos y extracción segura de ZIP mediante `--source-url`; en Databricks se ejecuta desde un Volume.

Para validar los siete días y su unión antes de usar Databricks:

```powershell
.\.venv\Scripts\python.exe scripts\validar_ingesta_ais_7_dias.py `
  --source-dir Datos `
  --work-dir local_artifacts\ingesta_7_dias
```

El comando deja un reporte JSON regenerable e ignorado por Git en el directorio de trabajo. Los hashes y tiempos de la ejecución versionada están en `docs/evidencia_ingesta_local_7_dias.md`.

## Estructura

```text
.
|- notebooks/       # notebooks PySpark exportados en formato legible en GitHub
|- docs/            # decisiones, evidencias, fuentes y material de apoyo ligero
|- config/          # ejemplos de configuración sin secretos
|- Datos/           # CSV locales de prototipado; ignorados por Git
|- README.md
|- BITACORA.md
`- .gitignore
```

## Convención de notebooks

Los notebooks usan el prefijo de dos dígitos y nombres en minúsculas con guiones bajos: `NN_etapa_descriptiva`. Cada uno abre con objetivo, entradas/salidas, prerrequisitos y criterio de validación. Las salidas versionadas no incluyen secretos, rutas personales ni datos crudos.

## Uso de asistencia de IA

El equipo utilizó Codex como apoyo durante el desarrollo iterativo de los notebooks en Databricks/PySpark y al redactar evidencia técnica. Cada resultado fue ejecutado y validado por el equipo en Databricks o en el entorno local antes de documentarlo. Las decisiones D-01 a D-07 fueron revisadas y aprobadas por el equipo a partir de la evidencia cuantitativa registrada; no se adoptaron automáticamente por una sugerencia de IA.

## Fuentes

- NOAA / Marine Cadastre, AIS Vessel Traffic.
- World Port Index (NGA Pub. 150): [fuente, fecha, tamaño y hash](docs/fuentes_world_port_index.md).
