"""Ingesta en streaming: feed diario -> bronce (crudo inmutable) -> plata (compuertas de calidad).

Garantías de tolerancia a fallos
--------------------------------
* Bronce: la fuente de archivos (Auto Loader en Databricks) registra en el checkpoint qué
  archivos ya entraron; el sumidero Delta es idempotente por `batchId`. Si el proceso muere
  entre la escritura y el commit del checkpoint, el lote se re-ejecuta y Delta lo descarta.
* Plata: `foreachBatch` escribe con `txnAppId`/`txnVersion = batchId` en las tres tablas
  (plata, cuarentena, métricas). Un lote reprocesado tras una falla no duplica filas.
* Deduplicación por (MMSI, BaseDateTime): dentro del lote con `row_number` determinista y
  entre lotes con un anti-join contra plata restringido a las fechas del lote y a lotes previos
  (`_silver_batch_id < batchId`), de modo que el reproceso de un lote es determinista.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .lakehouse import IS_DATABRICKS, Lakehouse
from .quality import DEDUP_RULES, content_hash, evaluate, metrics_rows, to_quarantine_columns, to_silver_columns
from .schemas import RAW_STRING_SCHEMA

BRONZE_STREAM = "bronze_ais_raw"
SILVER_STREAM = "silver_ais_posiciones"
FILE_DATE_REGEX = r"AIS_(\d{4})_(\d{2})_(\d{2})"


# ----------------------------------------------------------------------------- bronce
def bronze_source(lh: Lakehouse, max_files_per_trigger: int | None = None) -> DataFrame:
    spark = lh.spark
    landing = lh.landing_dir()
    if IS_DATABRICKS:
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", lh.checkpoint(BRONZE_STREAM) + "/_schema")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("rescuedDataColumn", "_rescued_data")
            .option("pathGlobFilter", "AIS_*.csv")
            .schema(RAW_STRING_SCHEMA)
        )
        if max_files_per_trigger:
            reader = reader.option("cloudFiles.maxFilesPerTrigger", max_files_per_trigger)
        raw = reader.load(landing)
    else:
        reader = spark.readStream.format("csv").option("header", "true").schema(RAW_STRING_SCHEMA)
        if max_files_per_trigger:
            reader = reader.option("maxFilesPerTrigger", max_files_per_trigger)
        raw = reader.load(f"{landing}/*/AIS_*.csv").withColumn("_rescued_data", F.lit(None).cast("string"))

    file_date = F.regexp_extract(F.col("_metadata.file_name"), FILE_DATE_REGEX, 0)
    return raw.select(
        *[F.col(c) for c in RAW_STRING_SCHEMA.fieldNames()],
        F.col("_rescued_data"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_source_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
        F.to_date(F.regexp_replace(F.substring(file_date, 5, 10), "_", "-")).alias("ingestion_date"),
    )


def run_bronze(lh: Lakehouse, max_files_per_trigger: int | None = None, query_name: str = BRONZE_STREAM,
               crash_after_batch: int | None = None) -> dict:
    """Ejecuta el stream bronce en modo `availableNow` (procesa lo pendiente y termina).

    `crash_after_batch` solo se usa en la demostración de tolerancia a fallos: mata el proceso
    (os._exit) apenas el lote indicado queda confirmado, como una caída abrupta del driver.
    """
    if crash_after_batch is not None:
        _install_crash_listener(lh, query_name, crash_after_batch)
    writer = (
        bronze_source(lh, max_files_per_trigger).writeStream.format("delta").outputMode("append")
        .queryName(query_name)
        .option("checkpointLocation", lh.checkpoint(BRONZE_STREAM))
        .trigger(availableNow=True)
    )
    query = writer.start(lh.path("bronze", "ais_raw")) if lh.is_local else writer.toTable(lh.name("bronze", "ais_raw"))
    query.awaitTermination()
    return _progress_summary(query)


# ------------------------------------------------------------------------------ plata
def _persist(df: DataFrame) -> DataFrame:
    # Databricks serverless no permite persist(); en clusters clásicos y en local sí.
    try:
        return df.persist()
    except Exception:  # noqa: BLE001
        return df


def _unpersist(df: DataFrame) -> None:
    try:
        df.unpersist()
    except Exception:  # noqa: BLE001
        pass


def process_silver_batch(lh: Lakehouse, land_cells: DataFrame, batch_df: DataFrame, batch_id: int) -> dict:
    """Aplica compuertas, deduplica y enruta un micro-lote a plata / cuarentena / métricas."""
    evaluated = _persist(evaluate(batch_df, land_cells).withColumn("_hash", content_hash()))
    rejected = evaluated.filter(F.size("_motivos") > 0)
    passed = evaluated.filter(F.size("_motivos") == 0)

    # 1) Duplicados dentro del lote: se conserva la primera fila con orden determinista.
    w = Window.partitionBy("_mmsi", "_event_ts").orderBy("_source_file", "_hash")
    ranked = passed.withColumn("_rn", F.row_number().over(w)).withColumn("_first_hash", F.first("_hash").over(w))
    first_rows = ranked.filter(F.col("_rn") == 1).drop("_rn", "_first_hash")
    intra_dups = ranked.filter(F.col("_rn") > 1).withColumn(
        "_dup_rule", F.when(F.col("_hash") == F.col("_first_hash"), "D01_DUPLICADO_EXACTO")
        .otherwise("D02_DUPLICADO_CONFLICTIVO")).drop("_rn", "_first_hash")

    # 2) Duplicados frente a lotes anteriores (incluye reenvíos del mismo archivo o días solapados).
    dates = [r[0] for r in first_rows.select("ingestion_date").distinct().collect()]
    existing = (
        lh.read("silver", "ais_posiciones")
        .filter(F.col("event_date").isin(dates) & (F.col("_silver_batch_id") < batch_id))
        .select(F.col("mmsi").alias("_x_mmsi"), F.col("event_ts").alias("_x_ts"), F.col("_content_hash").alias("_x_hash"))
    )
    joined = first_rows.join(existing, (F.col("_mmsi") == F.col("_x_mmsi")) & (F.col("_event_ts") == F.col("_x_ts")), "left")
    new_rows = joined.filter(F.col("_x_hash").isNull()).drop("_x_mmsi", "_x_ts", "_x_hash")
    cross_dups = joined.filter(F.col("_x_hash").isNotNull()).withColumn(
        "_dup_rule", F.when(F.col("_hash") == F.col("_x_hash"), "D01_DUPLICADO_EXACTO")
        .otherwise("D02_DUPLICADO_CONFLICTIVO")).drop("_x_mmsi", "_x_ts", "_x_hash")
    dups = intra_dups.unionByName(cross_dups).withColumn("_motivos", F.array(F.col("_dup_rule")))

    # 3) Métricas por regla (compuertas + deduplicación).
    passed_by_date = passed.groupBy("ingestion_date").agg(F.count(F.lit(1)).alias("evaluados"))
    rules = lh.spark.createDataFrame(list(DEDUP_RULES.items()), "regla string, descripcion string")
    dup_counts = dups.groupBy("ingestion_date", F.col("_dup_rule").alias("regla")).agg(F.count(F.lit(1)).alias("fallidos"))
    dedup_metrics = (passed_by_date.crossJoin(rules)
                     .join(dup_counts, ["ingestion_date", "regla"], "left")
                     .fillna({"fallidos": 0}))
    metrics = metrics_rows(evaluated, dedup_metrics, batch_id)

    # 4) Escrituras idempotentes por lote.
    lh.append(to_silver_columns(new_rows, batch_id), "silver", "ais_posiciones", txn=("oceanwatch_silver", batch_id))
    _maybe_inject_failure(lh, batch_id)
    quarantine = to_quarantine_columns(rejected, batch_id).unionByName(to_quarantine_columns(dups, batch_id))
    lh.append(quarantine, "silver", "ais_cuarentena", txn=("oceanwatch_quarantine", batch_id))
    lh.append(metrics, "silver", "metricas_calidad", txn=("oceanwatch_quality_metrics", batch_id))
    _unpersist(evaluated)
    return {"batch_id": batch_id, "fechas": [str(d) for d in dates]}


def run_silver(lh: Lakehouse, max_files_per_trigger: int | None = None, query_name: str = SILVER_STREAM) -> dict:
    from .setup import effective_land_cells

    land_cells = effective_land_cells(lh)
    source =lh.read_stream("bronze", "ais_raw", **({"maxFilesPerTrigger": max_files_per_trigger} if max_files_per_trigger else {}))
    query = (
        source.writeStream.queryName(query_name)
        .foreachBatch(lambda df, bid: process_silver_batch(lh, land_cells, df, bid))
        .option("checkpointLocation", lh.checkpoint(SILVER_STREAM))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return _progress_summary(query)


# --------------------------------------------------------------------- utilidades demo
def _maybe_inject_failure(lh: Lakehouse, batch_id: int) -> None:
    """Falla controlada para la demostración: ocurre después de escribir plata y antes de cuarentena."""
    target = os.environ.get("OW_FALLA_PLATA_EN_LOTE")
    if target is None or int(target) != batch_id:
        return
    marker = Path(lh.checkpoint(SILVER_STREAM)) / f"_falla_inyectada_lote_{batch_id}"
    if marker.exists():
        return
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("falla inyectada una sola vez para demostrar el reproceso idempotente\n")
    raise RuntimeError(f"FALLA INYECTADA en plata, lote {batch_id}, después de escribir plata")


def _install_crash_listener(lh: Lakehouse, query_name: str, crash_after_batch: int) -> None:
    from pyspark.sql.streaming import StreamingQueryListener

    class CrashListener(StreamingQueryListener):
        def onQueryStarted(self, event):
            pass

        def onQueryProgress(self, event):
            if event.progress.name == query_name and event.progress.batchId >= crash_after_batch:
                print(f"[demo] caída abrupta del proceso tras confirmar el lote {event.progress.batchId}", flush=True)
                os._exit(137)

        def onQueryIdle(self, event):
            pass

        def onQueryTerminated(self, event):
            pass

    lh.spark.streams.addListener(CrashListener())


def _progress_summary(query) -> dict:
    progress = [p if isinstance(p, dict) else json.loads(p.json) for p in (query.recentProgress or [])]
    batches = [p for p in progress if (p.get("numInputRows") or 0) > 0]  # serverless puede devolver None
    return {
        "query": query.name,
        "lotes_con_datos": len(batches),
        "filas_entrada": int(sum(b.get("numInputRows") or 0 for b in batches)),
        "ids_lote": [b.get("batchId") for b in batches],
    }
