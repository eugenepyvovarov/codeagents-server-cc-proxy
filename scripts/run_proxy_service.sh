#!/bin/sh
set -eu

ENV_FILE=/etc/claude-proxy.env
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
INSTALL_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

exec "$INSTALL_DIR/.venv/bin/python" -m uvicorn app:app --host 127.0.0.1 --port 8787
