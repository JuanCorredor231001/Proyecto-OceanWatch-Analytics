"""ML con MLflow: detección de anomalías de velocidad por buque y día.

Flujo reproducible dentro del pipeline:
  1. `build_features(fecha)`: variables desde el lakehouse (plata + oro) -> `ml.features_velocidad_buque_dia`.
     La misma función alimenta entrenamiento y puntuación (sin desalineación training/serving).
  2. `train(fechas)`: IsolationForest (scikit-learn) en un Pipeline imputación + escalado; experimento
     trackeado en MLflow (parámetros, métricas, artefactos, firma) y modelo registrado con alias
     `champion` (Unity Catalog en Databricks; registro local SQLite en pruebas).
  3. `score(fecha)`: carga `models:/<modelo>@champion` y escribe `gold.anomalias_velocidad_diarias`.

El objetivo es la integración reproducible, no la máxima precisión (enunciado, sección 5).
"""

from __future__ import annotations

import json
import os
import tempfile

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from .lakehouse import IS_DATABRICKS, Lakehouse

FEATURES = [
    "n_posiciones", "sog_media_kn", "sog_std_kn", "sog_p95_kn", "sog_max_kn", "frac_sog_mayor_25kn",
    "velocidad_implicita_max_kn", "distancia_km", "frac_pares_excluidos_d04", "length_m",
]
EXPERIMENT_LOCAL = "oceanwatch_anomalias_velocidad"
MODEL_SHORT_NAME = "oceanwatch_anomalias_velocidad"
ALIAS = "champion"
SKOPS_TRUSTED_TYPES = ["numpy.dtype", "sklearn.tree._tree.Tree"]


def model_name(lh: Lakehouse) -> str:
    return f"{lh.catalog}.{lh.schema_names['ml']}.{MODEL_SHORT_NAME}" if not lh.is_local else MODEL_SHORT_NAME


def configure_mlflow(lh: Lakehouse) -> None:
    import mlflow

    if IS_DATABRICKS:
        mlflow.set_tracking_uri("databricks")
        mlflow.set_registry_uri("databricks-uc")
        experiment = os.environ.get("OW_MLFLOW_EXPERIMENT", "/Shared/oceanwatch/anomalias_velocidad")
        try:  # la carpeta padre del experimento debe existir en el workspace
            from databricks.sdk import WorkspaceClient

            WorkspaceClient().workspace.mkdirs(os.path.dirname(experiment))
        except Exception:  # noqa: BLE001
            pass
        mlflow.set_experiment(experiment)
    else:
        base = os.path.join(lh.local_root, "_mlflow")
        os.makedirs(base, exist_ok=True)
        uri = f"sqlite:///{base}/mlflow.db"
        mlflow.set_tracking_uri(uri)
        mlflow.set_registry_uri(uri)
        if mlflow.get_experiment_by_name(EXPERIMENT_LOCAL) is None:
            mlflow.create_experiment(EXPERIMENT_LOCAL, artifact_location=f"file://{base}/artifacts")
        mlflow.set_experiment(EXPERIMENT_LOCAL)


def build_features(lh: Lakehouse, fecha: str) -> int:
    """Ingeniería de características por buque-día desde plata (SOG) y oro (trayectorias D-04)."""
    day = F.lit(fecha).cast("date")
    pos = lh.read("silver", "ais_posiciones").filter(F.col("event_date") == day)
    sog_stats = pos.groupBy("mmsi").agg(
        F.round(F.stddev_samp("sog"), 4).alias("sog_std_kn"),
        F.percentile_approx("sog", 0.95).alias("sog_p95_kn"),
        F.round(F.avg((F.col("sog") > 25).cast("double")), 4).alias("frac_sog_mayor_25kn"),
    )
    traj = lh.read("gold", "trayectorias_diarias_buque").filter(F.col("event_date") == day)
    feats = (
        traj.join(sog_stats, "mmsi", "left")
        .withColumn("frac_pares_excluidos_d04",
                    F.round(F.col("pares_excluidos_d04") / F.greatest(F.lit(1), F.col("pares_validos_d04") + F.col("pares_excluidos_d04")), 4))
        .select("event_date", "mmsi", "vessel_name", "vessel_type_group",
                F.col("lat_fin").alias("lat_ultima"), F.col("lon_fin").alias("lon_ultima"),
                *[F.col(c).cast("double").alias(c) for c in FEATURES])
        .withColumn("_feature_processed_at", F.current_timestamp())
    )
    lh.replace_partition(feats, "ml", "features_velocidad_buque_dia", f"event_date = '{fecha}'")
    return lh.read("ml", "features_velocidad_buque_dia").filter(F.col("event_date") == day).count()


def _pipeline(contamination: float, n_estimators: int, random_state: int):
    from sklearn.ensemble import IsolationForest
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    return Pipeline([
        ("imputar", SimpleImputer(strategy="median")),
        ("escalar", StandardScaler()),
        ("modelo", IsolationForest(n_estimators=n_estimators, contamination=contamination,
                                    random_state=random_state, n_jobs=-1)),
    ])


def train(lh: Lakehouse, fechas: list[str], contamination: float = 0.01, n_estimators: int = 200,
          random_state: int = 42, min_positions: int = 10) -> dict:
    """Entrena, registra en MLflow y asigna el alias `champion` a la nueva versión."""
    import mlflow
    from mlflow.models import infer_signature

    configure_mlflow(lh)
    feats = (lh.read("ml", "features_velocidad_buque_dia")
             .filter(F.col("event_date").cast("string").isin(list(fechas)))
             .filter(F.col("n_posiciones") >= min_positions))
    pdf = feats.select("event_date", "mmsi", "vessel_name", "vessel_type_group", *FEATURES).toPandas()
    if pdf.empty:
        raise RuntimeError(f"Sin características para entrenar en {fechas}")
    X = pdf[FEATURES].astype("float64")

    with mlflow.start_run(run_name=f"isoforest_{min(fechas)}_{max(fechas)}") as run:
        pipe = _pipeline(contamination, n_estimators, random_state).fit(X)
        scores = -pipe.score_samples(X)  # mayor = más anómalo
        preds = pipe.predict(X) == -1
        mlflow.log_params({
            "algoritmo": "IsolationForest", "contamination": contamination, "n_estimators": n_estimators,
            "random_state": random_state, "min_posiciones": min_positions, "fechas_entrenamiento": ",".join(fechas),
            "features": ",".join(FEATURES),
        })
        mlflow.log_metrics({
            "n_entrenamiento": float(len(X)), "tasa_anomalias": float(preds.mean()),
            "score_p50": float(np.percentile(scores, 50)), "score_p99": float(np.percentile(scores, 99)),
            "score_max": float(scores.max()),
        })
        stats = {f: {"media": float(X[f].mean()), "std": float(X[f].std(ddof=0) or 1.0)} for f in FEATURES}
        with tempfile.TemporaryDirectory() as tmp:
            top = pdf.assign(anomaly_score=scores).sort_values("anomaly_score", ascending=False).head(25)
            top.to_csv(os.path.join(tmp, "top25_anomalias_entrenamiento.csv"), index=False)
            with open(os.path.join(tmp, "estadisticas_features.json"), "w", encoding="utf-8") as fh:
                json.dump(stats, fh, indent=2)
            mlflow.log_artifacts(tmp, artifact_path="diagnostico")
        signature = infer_signature(X, pipe.predict(X))
        # MLflow 3 serializa con skops: se declaran explícitamente los tipos internos del árbol.
        info = mlflow.sklearn.log_model(pipe, name="modelo", signature=signature, input_example=X.head(5),
                                        registered_model_name=model_name(lh),
                                        skops_trusted_types=SKOPS_TRUSTED_TYPES)
        version = str(info.registered_model_version)
        client = mlflow.MlflowClient()
        client.set_registered_model_alias(model_name(lh), ALIAS, version)
        # Las estadísticas de entrenamiento viajan como artefacto del run (UC limita las etiquetas a 256 caracteres).
        return {"run_id": run.info.run_id, "modelo": model_name(lh), "version": version,
                "n_entrenamiento": int(len(X)), "tasa_anomalias": float(preds.mean())}


def score(lh: Lakehouse, fecha: str) -> dict:
    """Puntúa un día con la versión `champion` y escribe la tabla oro de anomalías."""
    import mlflow

    from .contract import conform

    configure_mlflow(lh)
    client = mlflow.MlflowClient()
    mv = client.get_model_version_by_alias(model_name(lh), ALIAS)
    pipe = mlflow.sklearn.load_model(f"models:/{model_name(lh)}@{ALIAS}")
    try:
        stats_path = mlflow.artifacts.download_artifacts(run_id=mv.run_id,
                                                         artifact_path="diagnostico/estadisticas_features.json")
        with open(stats_path, encoding="utf-8") as fh:
            stats = json.load(fh)
    except Exception:  # noqa: BLE001
        stats = {}

    day = F.lit(fecha).cast("date")
    feats = lh.read("ml", "features_velocidad_buque_dia").filter(F.col("event_date") == day)
    pdf = feats.toPandas()
    X = pdf[FEATURES].astype("float64")
    pdf["anomaly_score"] = -pipe.score_samples(X)
    pdf["es_anomalia"] = pipe.predict(X) == -1
    if stats:
        z = pd.DataFrame({f: (X[f] - stats[f]["media"]).abs() / (stats[f]["std"] or 1.0) for f in FEATURES})
        pdf["motivo_principal"] = z.fillna(0).idxmax(axis=1)
    else:
        pdf["motivo_principal"] = None
    pdf["modelo"] = model_name(lh)
    pdf["modelo_version"] = str(mv.version)
    keep = ["event_date", "mmsi", "vessel_name", "vessel_type_group", "n_posiciones", "sog_media_kn", "sog_p95_kn",
            "sog_max_kn", "velocidad_implicita_max_kn", "distancia_km", "anomaly_score", "es_anomalia",
            "motivo_principal", "lat_ultima", "lon_ultima", "modelo", "modelo_version"]
    out = lh.spark.createDataFrame(pdf[keep]).withColumn("_gold_processed_at", F.current_timestamp())
    lh.replace_partition(conform(out, "anomalias_velocidad_diarias"), "gold", "anomalias_velocidad_diarias",
                         f"event_date = '{fecha}'")
    return {"modelo": model_name(lh), "version": str(mv.version), "buques_puntuados": int(len(pdf)),
            "anomalias": int(pdf["es_anomalia"].sum())}
