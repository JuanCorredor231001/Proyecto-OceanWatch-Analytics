# Ejecuta un comando dentro del contenedor local de OceanWatch (Spark 4.2 + Delta + MLflow).
# Uso: .\scripts\docker_run.ps1 python scripts/run_pipeline_local.py --preparar
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Comando)
$raiz = Split-Path -Parent $PSScriptRoot
docker run --rm -v "${raiz}:/workspace" -e OW_LOCAL_ROOT=/workspace/local_lakehouse -e GIT_PYTHON_REFRESH=quiet oceanwatch-local:e2 @Comando
