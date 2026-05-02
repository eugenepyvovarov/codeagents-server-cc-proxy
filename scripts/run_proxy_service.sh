#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
INSTALL_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

for ENV_FILE in "${CODEAGENTS_DAEMON_ENV_FILE:-}" /etc/codeagents-daemon.env /etc/claude-proxy.env; do
  if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE"
    set +a
    break
  fi
done

exec "$INSTALL_DIR/.venv/bin/python" -m uvicorn app:app --host 127.0.0.1 --port 8787
