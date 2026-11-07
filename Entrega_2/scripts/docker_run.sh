#!/usr/bin/env bash
# Ejecuta un comando dentro del contenedor local de OceanWatch (Spark 4.2 + Delta + MLflow).
# Uso: scripts/docker_run.sh python scripts/run_pipeline_local.py --preparar
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && (pwd -W 2>/dev/null || pwd))"
MSYS_NO_PATHCONV=1 docker run --rm -v "${ROOT}:/workspace" -e OW_LOCAL_ROOT=/workspace/local_lakehouse -e GIT_PYTHON_REFRESH=quiet oceanwatch-local:e2 "$@"
