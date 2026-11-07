"""Abstracción mínima del lakehouse: nombres de tablas, rutas y escritura Delta.

En Databricks las tablas son administradas por Unity Catalog (`catalogo.esquema.tabla`)
y los archivos viven en Volumes. En local cada tabla es un directorio Delta bajo
`local_root/<esquema>/<tabla>` y se referencia en SQL como ``delta.`ruta` ``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

# En serverless, el worker de foreachBatch no hereda DATABRICKS_RUNTIME_VERSION: se detecta también
# por el directorio de instalación del runtime, que no existe en el contenedor local.
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ or os.path.isdir("/databricks/spark")

# Esquemas (capas) del lakehouse de la Entrega 2. `landing`, `reference` y `analytics`
# vienen de la Entrega 1 y se conservan.
SCHEMAS = {
    "landing": "Archivos AIS que llegan del feed diario NOAA (Volume). No es una capa analítica.",
    "reference": "Datos de referencia: puertos, catálogo VesselType, máscara de tierra H3.",
    "bronze": "Capa bronce: AIS crudo inmutable (append-only), tal como llegó, con metadatos de origen.",
    "silver": "Capa plata: AIS validado por compuertas de calidad, deduplicado por MMSI+timestamp; cuarentena y métricas.",
    "gold": "Capa oro: modelo de negocio (trayectorias, presencia en puertos, tráfico por celda/hora, anomalías).",
    "ml": "Características, modelos registrados y predicciones de ML.",
    "ops": "Operación del pipeline: ejecuciones, checkpoints y calendario del feed.",
}


@dataclass
class Lakehouse:
    """Resuelve nombres y operaciones de E/S según la plataforma."""

    spark: SparkSession
    catalog: str = "oceanwatch_g07"
    local_root: str | None = None
    schema_names: dict = field(default_factory=lambda: {k: k for k in SCHEMAS})

    # ------------------------------------------------------------------ nombres
    @property
    def is_local(self) -> bool:
        return self.local_root is not None

    def path(self, schema: str, table: str) -> str:
        if not self.is_local:
            raise ValueError("Las tablas administradas de Unity Catalog no se leen por ruta.")
        return str(Path(self.local_root) / schema / table)

    def name(self, schema: str, table: str) -> str:
        """Identificador SQL de la tabla."""
        if self.is_local:
            return f"delta.`{self.path(schema, table)}`"
        return f"{self.catalog}.{self.schema_names[schema]}.{table}"

    def volume_dir(self, schema: str, volume: str) -> str:
        if self.is_local:
            return str(Path(self.local_root) / "_volumes" / schema / volume)
        return f"/Volumes/{self.catalog}/{self.schema_names[schema]}/{volume}"

    def landing_dir(self) -> str:
        """Directorio monitoreado por el stream bronce: un subdirectorio por fecha."""
        return f"{self.volume_dir('landing', 'raw_ais')}/feed"

    def checkpoint(self, stream_name: str) -> str:
        return f"{self.volume_dir('ops', 'checkpoints')}/{stream_name}"

    def reference_dir(self) -> str:
        return self.volume_dir("reference", "files")

    # --------------------------------------------------------------------- E/S
    def exists(self, schema: str, table: str) -> bool:
        if self.is_local:
            return (Path(self.path(schema, table)) / "_delta_log").is_dir()
        return self.spark.catalog.tableExists(self.name(schema, table))

    def read(self, schema: str, table: str) -> DataFrame:
        if self.is_local:
            return self.spark.read.format("delta").load(self.path(schema, table))
        return self.spark.read.table(self.name(schema, table))

    def read_stream(self, schema: str, table: str, **options) -> DataFrame:
        reader = self.spark.readStream.format("delta")
        for key, value in options.items():
            reader = reader.option(key, value)
        if self.is_local:
            return reader.load(self.path(schema, table))
        return reader.table(self.name(schema, table))

    def _writer(self, df: DataFrame, mode: str, options: dict | None = None):
        writer = df.write.format("delta").mode(mode)
        for key, value in (options or {}).items():
            writer = writer.option(key, value)
        return writer

    def _save(self, writer, schema: str, table: str) -> None:
        if self.is_local:
            writer.save(self.path(schema, table))
        else:
            writer.saveAsTable(self.name(schema, table))

    def append(self, df: DataFrame, schema: str, table: str, txn: tuple[str, int] | None = None) -> None:
        """Append; con `txn=(app_id, version)` la escritura es idempotente ante reintentos.

        Delta registra (txnAppId, txnVersion) en el log y descarta una segunda
        escritura con la misma pareja: así un micro-lote reprocesado tras una falla
        no duplica filas (garantía exactly-once en `foreachBatch`).
        """
        options = {}
        if txn is not None:
            options = {"txnAppId": txn[0], "txnVersion": str(txn[1])}
        self._save(self._writer(df, "append", options), schema, table)

    def replace_partition(self, df: DataFrame, schema: str, table: str, predicate: str) -> None:
        """Sobrescribe solo las filas que cumplen `predicate` (idempotente por fecha)."""
        writer = self._writer(df, "overwrite", {"replaceWhere": predicate})
        self._save(writer, schema, table)

    def overwrite(self, df: DataFrame, schema: str, table: str) -> None:
        self._save(self._writer(df, "overwrite", {"overwriteSchema": "true"}), schema, table)

    def sql(self, statement: str) -> DataFrame:
        return self.spark.sql(statement)


def build_local_spark(app_name: str = "oceanwatch-local", shuffle_partitions: int = 8,
                      driver_memory: str = "6g") -> SparkSession:
    """SparkSession local con Delta Lake OSS (equivalente funcional del runtime Databricks)."""
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.jars.ivy", os.environ.get("IVY_HOME", "/workspace/.ivy2"))
        # Sistema de archivos local sin archivos .crc (como el almacenamiento de objetos de los Volumes):
        # con ChecksumFileSystem un kill -9 deja `.N.crc` huérfanos en el checkpoint y el reinicio
        # los confunde con un stream concurrente (SparkConcurrentModificationException).
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.RawLocalFs")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def get_lakehouse(spark: SparkSession | None = None, catalog: str | None = None,
                  local_root: str | None = None) -> Lakehouse:
    """Punto de entrada único: en Databricks usa la sesión activa; en local construye una."""
    catalog = catalog or os.environ.get("OW_CATALOG", "oceanwatch_g07")
    if IS_DATABRICKS:
        spark = spark or SparkSession.getActiveSession()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        return Lakehouse(spark=spark, catalog=catalog, local_root=None)
    local_root = local_root or os.environ.get("OW_LOCAL_ROOT", "/workspace/local_lakehouse")
    spark = spark or build_local_spark()
    return Lakehouse(spark=spark, catalog=catalog, local_root=local_root)
