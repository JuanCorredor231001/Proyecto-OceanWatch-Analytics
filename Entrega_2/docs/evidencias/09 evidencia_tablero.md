# Evidencia requisito 4 - Consola de visualización

**Herramienta:** Databricks AI/BI Dashboards, la opción recomendada en el enunciado. Definición: [`dashboards/oceanwatch_consola.lvdash.json`](../../dashboards/oceanwatch_consola.lvdash.json), generada por `scripts/generar_tablero.py` a partir de las consultas SQL de [`dashboards/sql/`](../../dashboards/sql/). Se despliega con el bundle (`resources/oceanwatch_consola.dashboard.yml`) y la tarea `refrescar_tablero` del job la refresca después de cada día.

**Alimentado solo por la capa oro:** los seis datasets consultan únicamente `<catálogo>.gold.*`.

| Panel pedido | Widget | Dataset / tabla oro |
|---|---|---|
| Mapa de intensidad de tráfico por celda | Mapa de puntos sobre el centroide H3 r8, color = log10(posiciones) | `02_mapa_intensidad_celda` ← `trafico_celda_hora` |
| Ranking de puertos | Barras top 20 por buques distintos | `04_ranking_puertos` ← `puertos_resumen_diario` |
| Buques en puerto | Tabla de estadías (atracados o en puerto al cierre) y contador | `05_buques_en_puerto` ← `estadias_puerto`; `01_kpis_dia` |
| Anomalías del día | Tabla con puntaje, variable principal y velocidades, más contador | `06_anomalias_dia` ← `anomalias_velocidad_diarias` |
| Contexto | Filtro de fecha, KPIs y perfil horario por grupo de buque | `01_kpis_dia`, `03_trafico_por_hora` |

## Vista previa local (mismas consultas sobre oro local, 2023-06-09)

![Consola](local/tablero/consola_vista_previa.png)

| KPI | 01-jun | 08-jun | 09-jun |
|---|---:|---:|---:|
| Buques activos | 1,996 | 2,054 | 2,150 |
| Puertos con actividad | 78 | 81 | 86 |
| Buques en puerto al cierre | 168 | 172 | 183 |
| Anomalías del día | 21 | 25 | 24 |

Top 5 del ranking del 09-jun: New York (29 buques), Newport (23), Chicago (18), Annapolis (14), Miami (13). Los resultados de cada consulta están en [`local/tablero/*.csv`](local/tablero/).

> La vista previa PNG se generó con matplotlib solo para verificar en local que las consultas devuelven datos coherentes. **No es el entregable.** El entregable es el tablero AI/BI publicado en el workspace institucional.

## Pendiente en el workspace institucional

1. Configurar `warehouse_id` en `databricks.yml` y ejecutar `databricks bundle deploy`.
2. Abrir el tablero y verificar los widgets. El JSON sigue el formato Lakeview, pero no se importó desde este entorno. Si algún widget (p. ej. el mapa de puntos) no carga, se recrea en la UI sobre el mismo dataset.
3. Publicar el tablero y capturar pantalla para la sustentación.
