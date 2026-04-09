#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

BRANCH="${BRANCH:-main}"
REMOTE="${REMOTE:-origin}"
LIVE_URL="${LIVE_URL:-}"
DEFAULT_MESSAGE="panel pro + vgc persistido + sort por columnas"
COMMIT_MESSAGE="${1:-$DEFAULT_MESSAGE}"

echo "==> Repo: $ROOT_DIR"
echo "==> Revisando cambios..."
git status --short --branch

if ! git diff --quiet -- monitor.py ventas.py skus.csv deploy_live.sh || ! git diff --cached --quiet -- monitor.py ventas.py skus.csv deploy_live.sh; then
  echo "==> Agregando archivos del deploy..."
  git add monitor.py ventas.py skus.csv deploy_live.sh
else
  echo "==> No hay cambios nuevos en monitor.py, ventas.py, skus.csv o deploy_live.sh"
fi

if git diff --cached --quiet; then
  echo "==> No hay nada staged para commit. Saliendo."
  exit 0
fi

echo "==> Commit: $COMMIT_MESSAGE"
git commit -m "$COMMIT_MESSAGE"

echo "==> Push a $REMOTE/$BRANCH..."
git push "$REMOTE" "$BRANCH"

echo
echo "Deploy enviado."
echo "Si tu hosting esta conectado al repo de GitHub, el redeploy ya debe estar corriendo."

if [[ -n "$LIVE_URL" ]]; then
  echo "Abriendo URL en vivo: $LIVE_URL"
  open "$LIVE_URL" >/dev/null 2>&1 || true
  echo "$LIVE_URL"
else
  echo "Tip: ejecuta asi para abrir la app al terminar:"
  echo "LIVE_URL=\"https://tu-app-en-vivo\" ./deploy_live.sh"
fi
