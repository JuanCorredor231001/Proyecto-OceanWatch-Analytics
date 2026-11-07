-- Ranking de puertos por buques distintos con estadía en el día.
SELECT event_date, ranking, port_name, buques_distintos, arribos, buques_atracados,
       buques_en_puerto_al_cierre, ROUND(duracion_media_horas, 2) AS duracion_media_horas,
       buques_carga, buques_tanquero, buques_pasajeros, port_lat, port_lon
FROM __CATALOGO__.gold.puertos_resumen_diario
WHERE ranking <= 20
ORDER BY event_date, ranking
