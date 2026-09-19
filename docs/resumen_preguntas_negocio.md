# Resumen — cinco preguntas de negocio

1. **Buques distintos por día:** se observaron entre 19,597 y 21,086 MMSI válidos diarios. Se eligió `countDistinct` porque `approx_count_distinct` tuvo errores de 1.10% a 9.21%.
2. **Tipos con mayor tráfico:** Towing (31) lidera con 16,557,760 posiciones y Pleasure craft (37) le sigue con 14,476,457. `SOG=102.3` se excluye de los promedios por ser un sentinela AIS.
3. **Mayor distancia semanal:** JUSTIN PAUL ECKSTEIN (367638030, tipo 31) recorrió 5,770.92 km. D-04 conservó 60,369,977 pares válidos y el control físico fue consistente.
4. **Concentración espacial:** la celda H3 r8 `8828d555a1fffff` concentra 235,260 posiciones. D-03 cerró con un buffer conservador de 2 km: 2 de las 10 celdas líderes se asocian a WPI; a 5 km serían 5, por lo que la sensibilidad queda declarada.
5. **Permanencia y visitantes:** 12,656 de 31,780 MMSI válidos (39.8238%) transmitieron los siete días. Hubo 5,964 visitantes de un día, de los cuales 65.5265% fueron Pleasure craft o Sailing.

Las evidencias completas están en `docs/evidencia_pregunta_03a.md` a `docs/evidencia_pregunta_03e.md`.
