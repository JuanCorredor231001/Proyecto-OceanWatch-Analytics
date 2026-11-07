# Arquitectura de la Entrega 2

```mermaid
flowchart LR
    NOAA[(NOAA Marine Cadastre<br/>AIS_AAAA_MM_DD.zip)] -->|02 aterrizar_feed<br/>reintentos + SHA-256 + publicación atómica| LND[/landing.raw_ais<br/>feed/&lt;fecha&gt;/AIS_*.csv/]
    LND -->|03 Auto Loader<br/>checkpoint, availableNow| BR[(bronze.ais_raw<br/>texto crudo, appendOnly)]
    BR -->|04 foreachBatch<br/>12 expectativas + dedup MMSI,ts<br/>txnAppId/txnVersion| SV[(silver.ais_posiciones)]
    BR --> QR[(silver.ais_cuarentena<br/>motivos)]
    BR --> QM[(silver.metricas_calidad)]
    SV -->|05 monitorear_calidad<br/>bronce = plata + cuarentena<br/>umbral %| GATE{OK?}
    GATE -->|sí| GD[06 oro_modelo<br/>replaceWhere fecha]
    GATE -->|no: falla + correo| STOP[[job en error<br/>oro no se actualiza]]
    REF[(reference.*<br/>puertos WPI, celdas H3 r8,<br/>máscara tierra H3 r5)] --> GD
    REF --> SV
    GD --> T1[(gold.trayectorias_diarias_buque)]
    GD --> T2[(gold.estadias_puerto)]
    GD --> T3[(gold.puertos_resumen_diario)]
    GD --> T4[(gold.trafico_celda_hora)]
    T1 --> FE[(ml.features_velocidad_buque_dia)]
    SV --> FE
    FE -->|08 entrenar: MLflow run + registro UC| MOD{{modelo @champion}}
    MOD -->|09 puntuar| T5[(gold.anomalias_velocidad_diarias)]
    T1 & T2 & T3 & T4 & T5 -->|10 validar_contrato| DASH[[Tablero AI/BI<br/>Consola OceanWatch]]
    T1 & T2 & T3 & T4 & T5 -.->|13 Delta Sharing abierto| FREE[[Databricks Free Edition]]
```

## Capas y responsabilidades

| Capa | Tablas | Garantía | Cómo se implementa |
|---|---|---|---|
| Landing | Volume `landing.raw_ais` | El archivo está completo antes de ser visible | ZIP verificado; CSV publicado con `os.replace` desde `staging/` |
| Bronce | `bronze.ais_raw` | Crudo e inmutable; cada archivo entra exactamente una vez | Auto Loader + checkpoint; Delta `appendOnly`; columnas como texto |
| Plata | `silver.ais_posiciones`, `silver.ais_cuarentena`, `silver.metricas_calidad` | Solo filas que pasan las compuertas; clave única (MMSI, timestamp); cada fila de bronce termina en plata o en cuarentena | `foreachBatch` idempotente; `quality.py` |
| Oro | 5 tablas del contrato | Modelo de negocio calculado solo desde plata; contrato validado | `gold.py`, `contract.py`, `replaceWhere` por fecha |
| ML | `ml.features_velocidad_buque_dia`, modelo UC | Las mismas variables para entrenar y puntuar | `ml.py`, MLflow |
| Ops | `ops.pipeline_runs`, `ops.feed_calendar`, Volume `ops.checkpoints` | Trazabilidad de cada intento | orquestador / Databricks Jobs |

## Código compartido Databricks ↔ local

| Módulo | Responsabilidad | Diferencia de plataforma |
|---|---|---|
| `lakehouse.py` | Nombres de tablas, rutas, escrituras Delta (`append` con txn, `replaceWhere`) | UC `catalogo.esquema.tabla` frente a ``delta.`ruta` `` |
| `geo.py` | H3 y haversine | `h3_longlatash3string` nativo frente a pandas UDF `h3` |
| `streaming.py` | Bronce y plata | `cloudFiles` frente a la fuente `csv` de archivos |
| `quality.py`, `gold.py`, `ml.py`, `contract.py`, `pipeline.py`, `feed.py`, `setup.py` | Lógica de negocio | Ninguna |

Los notebooks (`notebooks/NN_*.py`) son envoltorios de 5 a 10 líneas que leen los parámetros del job y llaman a la librería. El orquestador local (`scripts/run_pipeline_local.py`) llama las mismas funciones con el mismo DAG.
