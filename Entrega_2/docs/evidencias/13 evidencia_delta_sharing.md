# Evidencia bono - Delta Sharing (Open Sharing)

**Objetivo del bono:** publicar las tablas oro desde el workspace institucional con Delta Sharing (Open Sharing) y consumirlas desde Databricks Free Edition para reconstruir la consola, sin exportar archivos a mano ni duplicar el dataset.

**Estado (2026-11-07): proveedor listo a medias, bloqueado por la configuración del metastore.** El share existe con las 5 tablas oro. El destinatario no se puede crear porque el metastore no permite sharing externo. Ese ajuste solo lo puede cambiar un administrador del metastore o de la cuenta, y el equipo no tiene ese rol.

## Diseño

| Pieza | Dónde | Qué hace |
|---|---|---|
| Proveedor | [`notebooks/13_delta_sharing_proveedor.py`](../../notebooks/13_delta_sharing_proveedor.py) | `CREATE SHARE oceanwatch_oro`, agrega las 5 tablas del contrato `WITH HISTORY`, crea el destinatario de Open Sharing `oceanwatch_free_edition` (token) y le otorga `SELECT` sobre el share. Nunca imprime `activation_link` |
| Consumidor | [`notebooks/14_delta_sharing_consumidor_free.py`](../../notebooks/14_delta_sharing_consumidor_free.py) | En Free Edition lee `config.share` desde un Volume privado y lista las tablas con `delta_sharing.SharingClient`. Luego lee cada tabla con el conector `deltaSharing` y refresca en `workspace.oceanwatch_shared` solo los agregados oro que usa la consola (MB, no los ~60 M de posiciones) |
| Consola | `dashboards/oceanwatch_consola.lvdash.json` | Se importa en Free Edition cambiando el catálogo de los datasets (`scripts/generar_tablero.py --catalogo ...`) |

La credencial (`config.share` / enlace de activación) se entrega por canal privado y está excluida de Git (`.gitignore`: `*.share`, `config.share`).

## Ejecución en el workspace institucional

**Privilegios del usuario en el metastore `metastore_aws_us_east_2`.** El equipo los recibe como administrador del workspace (`_workspace_admins_workspace_7474654323473441`): `CREATE_SHARE`, `CREATE_RECIPIENT`, `CREATE_PROVIDER`, entre otros. El metastore, en cambio, tiene:

```json
"delta_sharing_scope": "INTERNAL",
"owner": "System user"
```

**Corrida única `650951907184292`** del notebook 13, en serverless:

1. `CREATE SHARE oceanwatch_oro` → OK.
2. `ALTER SHARE ... ADD TABLE ... WITH HISTORY` × 5 → OK:

| Objeto compartido | Tipo | Historial | Estado |
|---|---|---|---|
| `gold.trayectorias_diarias_buque` | TABLE | ENABLED | ACTIVE |
| `gold.estadias_puerto` | TABLE | ENABLED | ACTIVE |
| `gold.puertos_resumen_diario` | TABLE | ENABLED | ACTIVE |
| `gold.trafico_celda_hora` | TABLE | ENABLED | ACTIVE |
| `gold.anomalias_velocidad_diarias` | TABLE | ENABLED | ACTIVE |

3. `CREATE RECIPIENT oceanwatch_free_edition` → **falló**:

```text
[UNAUTHORIZED_ACCESS] PERMISSION_DENIED:
External Delta Sharing is not enabled on the metastore.
Please contact the metastore admin to enable External Delta Sharing. SQLSTATE: 42501
```

No se creó ningún destinatario (`recipients list` vacío) y no se emitió ninguna credencial.

## Para completar el bono

1. Un administrador de la cuenta habilita el sharing externo en el metastore (*Account console → Catalog → metastore → External Delta Sharing*, que pasa `delta_sharing_scope` a `INTERNAL_AND_EXTERNAL`). El equipo no puede hacerlo.
2. Volver a ejecutar el notebook 13. Es idempotente: el share ya existe y las tablas no se duplican.
3. Abrir `DESCRIBE RECIPIENT oceanwatch_free_edition` en el workspace, copiar el enlace de activación a mano y descargar `config.share`.
4. En Free Edition: subir `config.share` a un Volume privado, ejecutar el notebook 14 e importar el tablero con el catálogo de Free Edition.
5. Capturar el tablero en Free Edition para esta evidencia.
