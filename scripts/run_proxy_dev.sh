#!/bin/sh
set -eu

# Dev helper to create/activate a local venv, install deps, then run the proxy.

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROXY_DIR=$(cd "$SCRIPT_DIR/.." && pwd)

cd "$PROXY_DIR"

ENV_FILE="${ENV_FILE:-.env.local}"
if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
  set -a
  . "$ENV_FILE"
  set +a
fi

PORT="${PORT:-8787}"
HOST="${HOST:-127.0.0.1}"

if [ ! -d ".venv" ] || [ ! -x ".venv/bin/python" ]; then
  python3 -m venv .venv
fi

. .venv/bin/activate

if [ "${SKIP_PIP_INSTALL:-}" = "1" ]; then
  if ! python -c "import fastapi, uvicorn, claude_agent_sdk" >/dev/null 2>&1; then
    echo "Missing Python deps in .venv. Re-run without SKIP_PIP_INSTALL=1." >&2
    exit 1
  fi
else
  req_hash=$(
    python - <<'PY'
import hashlib
from pathlib import Path

h = hashlib.sha256()
for name in ("requirements.txt", "requirements-dev.txt"):
    p = Path(name)
    if p.exists():
        h.update(p.read_bytes())
    h.update(b"\0")
print(h.hexdigest())
PY
  )

  stamp_path=".venv/.requirements.sha256"
  needs_install=0
  if [ ! -f "$stamp_path" ] || [ "$(cat "$stamp_path")" != "$req_hash" ]; then
    needs_install=1
  fi
  if ! python -c "import fastapi, uvicorn, claude_agent_sdk" >/dev/null 2>&1; then
    needs_install=1
  fi

  if [ "$needs_install" -eq 1 ]; then
    python -m pip install -r requirements.txt -r requirements-dev.txt
    printf '%s\n' "$req_hash" > "$stamp_path"
  fi
fi

if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
  echo "Warning: ANTHROPIC_API_KEY is not set; agent runs may fail unless auth is configured." >&2
fi

exec python -m uvicorn app:app --host "$HOST" --port "$PORT" --reload
