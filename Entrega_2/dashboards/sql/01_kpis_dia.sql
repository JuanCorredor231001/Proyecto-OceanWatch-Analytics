-- KPIs del día: buques activos, posiciones, distancia, puertos con actividad y anomalías.
WITH t AS (
  SELECT event_date, COUNT(*) AS buques_activos, SUM(n_posiciones) AS posiciones,
         ROUND(SUM(distancia_km), 0) AS distancia_total_km
  FROM __CATALOGO__.gold.trayectorias_diarias_buque GROUP BY event_date),
p AS (
  SELECT event_date, COUNT(*) AS puertos_con_actividad, SUM(buques_en_puerto_al_cierre) AS buques_en_puerto_al_cierre
  FROM __CATALOGO__.gold.puertos_resumen_diario GROUP BY event_date),
a AS (
  SELECT event_date, COUNT_IF(es_anomalia) AS anomalias
  FROM __CATALOGO__.gold.anomalias_velocidad_diarias GROUP BY event_date)
SELECT t.event_date, t.buques_activos, t.posiciones, t.distancia_total_km,
       COALESCE(p.puertos_con_actividad, 0) AS puertos_con_actividad,
       COALESCE(p.buques_en_puerto_al_cierre, 0) AS buques_en_puerto_al_cierre,
       COALESCE(a.anomalias, 0) AS anomalias
FROM t LEFT JOIN p USING (event_date) LEFT JOIN a USING (event_date)
ORDER BY t.event_date
