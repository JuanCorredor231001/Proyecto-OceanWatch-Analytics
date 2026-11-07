-- Buques en puerto: estadías del día (al cierre del día o atracados), con duración.
SELECT event_date, port_name, mmsi, COALESCE(vessel_name, '(sin nombre)') AS vessel_name, vessel_type_group,
       arribo_ts, salida_ts, duracion_horas, estado, en_puerto_al_cierre
FROM __CATALOGO__.gold.estadias_puerto
WHERE en_puerto_al_cierre OR estado = 'ATRACADO_O_FONDEADO'
ORDER BY event_date, port_name, duracion_horas DESC
