-- Mapa de intensidad de tráfico: posiciones por celda H3 r8 en el día (centroide de la celda).
SELECT event_date, h3_r8, centro_lat, centro_lon,
       SUM(posiciones)        AS posiciones,
       MAX(buques_distintos)  AS buques_hora_pico,
       ROUND(LOG10(SUM(posiciones)), 3) AS intensidad_log10
FROM __CATALOGO__.gold.trafico_celda_hora
GROUP BY event_date, h3_r8, centro_lat, centro_lon
