-- Anomalías del día: buques marcados por el modelo MLflow (alias champion).
SELECT event_date, mmsi, COALESCE(vessel_name, '(sin nombre)') AS vessel_name, vessel_type_group,
       ROUND(anomaly_score, 4) AS anomaly_score, motivo_principal,
       sog_max_kn, velocidad_implicita_max_kn, ROUND(distancia_km, 1) AS distancia_km, n_posiciones,
       lat_ultima, lon_ultima, modelo_version
FROM __CATALOGO__.gold.anomalias_velocidad_diarias
WHERE es_anomalia
ORDER BY event_date, anomaly_score DESC
