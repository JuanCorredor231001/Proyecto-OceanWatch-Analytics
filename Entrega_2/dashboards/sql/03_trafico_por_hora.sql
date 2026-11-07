-- Perfil horario del tráfico (UTC) por grupo de buque.
SELECT event_date, hora,
       SUM(posiciones) AS posiciones,
       SUM(buques_carga) AS carga, SUM(buques_tanquero) AS tanquero, SUM(buques_pasajeros) AS pasajeros,
       SUM(buques_pesca) AS pesca, SUM(buques_remolque) AS remolque, SUM(buques_recreo) AS recreo
FROM __CATALOGO__.gold.trafico_celda_hora
GROUP BY event_date, hora
ORDER BY event_date, hora
