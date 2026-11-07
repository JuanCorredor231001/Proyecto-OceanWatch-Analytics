# OceanWatch Analytics - Entrega 2 (MINE 4213)

Pipeline completo del lakehouse de tráfico marítimo AIS (NOAA / Marine Cadastre): **ingesta en streaming**, **capas bronce → plata → oro con la calidad como compuerta**, **orquestación**, **consola de visualización**, **gobernanza**, **ML con MLflow** y bono de **Delta Sharing**. Continúa la [Entrega 1](../README.md), que vive en la raíz del repositorio: conserva sus decisiones D-01 a D-07 y el catálogo `oceanwatch_g07`.

## Equipo

| Integrante | Código |
|---|---|
| Juan Sebastián Corredor Sánchez | 202014956 |
| Karina Marita Aranguren Espinoza | 202326292 |
| Estefania Quijano Garcia | 202311445 |

## Mapa de requisitos

| # | Requisito (%) | Dónde está | Evidencia |
|---|---|---|---|
| 1 | Ingesta en streaming (15) | `src/oceanwatch/streaming.py`, `notebooks/03_bronce_streaming.py`, `04_plata_calidad.py` | [06 tolerancia a fallos](docs/evidencias/06%20evidencia_tolerancia_fallos.md) |
| 2 | Capas con calidad como compuerta (15) | `quality.py`, `gold.py`, `notebooks/04-06` | [07 capas y calidad](docs/evidencias/07%20evidencia_capas_calidad.md) |
| 3 | Orquestación (20) | `databricks.yml`, `resources/*.job.yml`, `scripts/run_pipeline_local.py` | [08 orquestación](docs/evidencias/08%20evidencia_orquestacion.md) |
| 4 | Consola de visualización (15) | `dashboards/oceanwatch_consola.lvdash.json`, `dashboards/sql/` | [09 tablero](docs/evidencias/09%20evidencia_tablero.md) |
| 5 | Gobernanza (10) | `notebooks/12_gobernanza.py`, `contracts/gold_contract.yml` | [contrato de oro](docs/contrato_datos_oro.md), [10 gobernanza](docs/evidencias/10%20evidencia_gobernanza.md) |
| 6 | ML con MLflow (10) | `src/oceanwatch/ml.py`, `notebooks/07-09` | [11 ML](docs/evidencias/11%20evidencia_ml.md) |
| 7 | Repositorio y bitácora (5) | este repositorio | [bitácora](docs/decisiones/BITACORA.md), [decisiones E2](docs/decisiones/decisiones_entrega2.md) |
| Despliegue | Databricks (`oceanwatch_g07`, serverless) | `databricks.yml` | [12 Databricks](docs/evidencias/12%20evidencia_databricks.md) |
| Bono | Delta Sharing (+5) | `notebooks/13_*`, `14_*` | [13 Delta Sharing](docs/evidencias/13%20evidencia_delta_sharing.md): share creado; destinatario bloqueado porque el metastore no tiene habilitado el sharing externo |

Las capturas del workspace están en [`docs/evidencias/capturas/`](docs/evidencias/capturas/). Lo que falta está en el [plan de cierre](docs/plan_cierre_entrega2.md).

La arquitectura y el flujo están en [docs/arquitectura.md](docs/arquitectura.md).

## Estructura

```text
Entrega_2/
|- src/oceanwatch/       # librería única del pipeline (Databricks y local)
|- notebooks/            # notebooks Databricks: envoltorios delgados por tarea del job
|- resources/            # Databricks Asset Bundle: jobs diario y backfill, tablero
|- databricks.yml        # definición del bundle (variables: catálogo, warehouse)
|- contracts/            # contrato de datos de oro (YAML, fuente del DDL y la validación)
|- dashboards/           # tablero AI/BI (.lvdash.json) y sus consultas SQL
|- scripts/              # orquestador local, demo de tolerancia a fallos, evidencias, generador del tablero
|- tests/                # pytest con datos sintéticos
|- docker/               # imagen local Spark 4.2 + Delta 4.4 + MLflow 3.18
|- docs/                 # arquitectura, contrato, decisiones, bitácora y evidencias
|- Datos/                # ZIP NOAA y referencias (excluido de Git)
```

## Despliegue en el workspace institucional (Databricks)

1. Clonar el repositorio como *Git folder* y revisar las variables de `databricks.yml` (`catalogo`, `warehouse_id`). Las alertas de falla y de duración llegan al usuario que despliega (`${workspace.current_user.userName}`); para avisar a todo el equipo, agregar sus correos a `email_notifications` en `resources/*.job.yml`.
2. Copiar el WPI de la Entrega 1 a `/Volumes/<catalogo>/reference/world_port_index/UpdatedPub150.csv` si el catálogo es nuevo.
3. Desplegar y ejecutar:

```bash
databricks bundle validate -t institucional
```

```bash
databricks bundle deploy -t institucional
```

```bash
databricks bundle run oceanwatch_backfill_historico -t institucional
```

```bash
databricks bundle run oceanwatch_diario -t institucional --params fecha=auto
```

4. El job diario queda programado a las 06:00 (America/Bogota) con `fecha=auto`. Cada ejecución procesa el siguiente día pendiente del feed (08 a 14 de junio de 2023) y, al terminar, refresca el tablero.
5. Ejecutar `notebooks/12_gobernanza.py` con un principal por rol. Sin administrador de la cuenta no hay grupos de cuenta; se usaron service principals ([evidencia 10](docs/evidencias/10%20evidencia_gobernanza.md)).

> **Estado en el workspace (2026-11-07):**
> - Backfill 01-07 jun y feed 08-14 jun procesados (bronce = CSV NOAA; plata + cuarentena = bronce).
> - El cron de las 06:00 disparó una corrida `PERIODIC` que cortó el DAG al no quedar días pendientes ([evidencia 12](docs/evidencias/12%20evidencia_databricks.md)).
> - Tablero publicado sin widgets inválidos ([evidencia 09](docs/evidencias/09%20evidencia_tablero.md)).
> - Gobernanza aplicada ([evidencia 10](docs/evidencias/10%20evidencia_gobernanza.md)).
> - Modelo `champion` v3 puntuando los 14 días ([evidencia 11](docs/evidencias/11%20evidencia_ml.md)).
>
> Si la importación del tablero falla por cambios de esquema de AI/BI, las consultas de `dashboards/sql/` sirven para reconstruirlo; `scripts/generar_tablero.py --catalogo <catálogo>` regenera el `.lvdash.json`.

## Prueba local (Docker)

Spark 4.x en Windows exige `winutils.exe`, por eso la prueba local corre en un contenedor Linux con el mismo código (decisión D-E2-09).

```powershell
docker build -t oceanwatch-local:e2 docker
```

```powershell
.\scripts\docker_run.ps1 python scripts/run_pipeline_local.py --preparar
```

```powershell
.\scripts\docker_run.ps1 python scripts/run_pipeline_local.py --backfill 2023-06-01
```

```powershell
.\scripts\docker_run.ps1 python scripts/run_pipeline_local.py --fecha auto --programar-cada 20 --max-ejecuciones 2 --inyectar-falla bronce_streaming
```

```powershell
.\scripts\docker_run.ps1 python scripts/run_pipeline_local.py --fecha auto --zip-dir /workspace/Datos/feed_ausente --espera-reintento 3
```

```powershell
.\scripts\docker_run.ps1 python scripts/demo_tolerancia_fallos.py
```

```powershell
.\scripts\docker_run.ps1 python -m pytest -q tests
```

```powershell
.\scripts\docker_run.ps1 python scripts/evidencias_locales.py
```

- Datos en `Datos/downloads/`: los 14 ZIP oficiales NOAA `AIS_2023_06_01.zip` a `AIS_2023_06_14.zip` (~4.7 GB). Los SHA-256 de 01-07 coinciden con los registrados en la Entrega 1. Las evidencias locales usan 01, 08 y 09; los demás permiten extender la prueba al feed completo.
- En local, si falta un ZIP la tarea `aterrizar_feed` falla y **no descarga**. La orden con `--zip-dir /workspace/Datos/feed_ausente` usa una carpeta sin ZIP (`--zip-dir .../feed_ausente`) para demostrar reintentos agotados y tareas dependientes omitidas.
- Referencias en `Datos/reference/natural_earth/`: `ne_10m_land`, `ne_10m_lakes`, `ne_10m_rivers_lake_centerlines` y `ne_10m_ports` (naciscdn.org, dominio público).
- **Velocidad:** el CSV muestreado de cada día se guarda en `Datos/cache_muestras/` (la clave es el SHA-256 del ZIP más el módulo). Las repeticiones no vuelven a descomprimir el ZIP: el aterrizaje baja de ~68 s a ~4 s por día. Con `--fecha 2023-06-01,2023-06-08,2023-06-09` los días se procesan en una sola sesión Spark. Reprocesar los 3 días pasó de ~12 min a ~7 min ([log](docs/evidencias/logs/optimizacion_local.log)). El resto del tiempo es cómputo real de oro, monitoreo y contrato (~2 min por día).
- `--muestreo 10` (por defecto) conserva ~10 % de los buques con trayectorias completas (`crc32(MMSI) % 10 == 0`), más todos los MMSI no conformes. `--muestreo 0` procesa el día completo.

## Fuentes

- NOAA / Marine Cadastre, AIS Vessel Traffic: `https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_DD.zip`.
- NGA World Port Index, Pub. 150 (Databricks; ver Entrega 1).
- Natural Earth 1:10m: land, lakes, rivers_lake_centerlines, ports (dominio público).
- Catálogo VesselType: NOAA `VesselTypeCodes2018.pdf` y guía AIS de USCG (Entrega 1).

## Uso de asistencia de IA

El equipo usó Claude Code como apoyo para estructurar la librería, los notebooks, el bundle y la redacción de la evidencia. Cada resultado reportado proviene de ejecuciones locales registradas en `docs/evidencias/`. Las decisiones D-E2-01 a D-E2-09 deben ser revisadas y aprobadas por el equipo antes de la entrega.
