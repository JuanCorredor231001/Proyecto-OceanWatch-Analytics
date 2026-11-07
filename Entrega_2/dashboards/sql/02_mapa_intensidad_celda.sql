-- Mapa de intensidad de tráfico: posiciones del día agregadas desde las celdas H3 r8 de oro a una
-- rejilla regular de 0.05° (~5.5 km). Un día tiene hasta ~445 mil celdas r8, más de lo que dibuja el
-- widget de mapa sin truncar; la rejilla deja <= ~60 mil puntos por día y usa SQL portable (local y Databricks).
SELECT event_date,
       FLOOR(centro_lat / 0.05) * 0.05 + 0.025 AS celda_lat,
       FLOOR(centro_lon / 0.05) * 0.05 + 0.025 AS celda_lon,
       SUM(posiciones)          AS posiciones,
       COUNT(DISTINCT h3_r8)    AS celdas_h3_r8,
       MAX(buques_distintos)    AS buques_hora_pico,
       ROUND(LOG10(SUM(posiciones)), 3) AS intensidad_log10
FROM __CATALOGO__.gold.trafico_celda_hora
GROUP BY event_date, FLOOR(centro_lat / 0.05), FLOOR(centro_lon / 0.05)
