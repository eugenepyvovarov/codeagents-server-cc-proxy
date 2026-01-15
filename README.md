# Claude Agent SSE Proxy

FastAPI service that runs `claude-agent-sdk` (Claude Code CLI) locally on a server and exposes:

- SSE stream for live events
- NDJSON replay for resumptions (`since=<event_id>`)

Designed to be bound to `127.0.0.1` and accessed over an SSH tunnel.

## Quick start (dev)

```bash
cd server/claude-proxy
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
uvicorn app:app --host 127.0.0.1 --port 8787 --reload
```

Or use the helper script:

```bash
./scripts/run_proxy_dev.sh
```

If you create `server/claude-proxy/.env.local`, `run_proxy_dev.sh` auto-loads it.

Health check:

```bash
curl -s http://127.0.0.1:8787/healthz
```

## API

### Start/attach stream

`POST /v1/agent/stream` returns `text/event-stream`.

Body:

```json
{
  "conversation_id": "uuid-string",
  "text": "Hello",
  "cwd": "/root/projects/my-project",
  "allowed_tools": ["Bash", "Read", "Write"],
  "system_prompt": "optional system prompt",
  "max_turns": 3
}
```

Notes:

- `conversation_id` is the stable chat/thread id owned by the client (iOS).
- `cwd` is the agent folder on the server. A conversation is bound to its first `cwd` and mismatches are rejected.
- Only 1 active run per `cwd` is allowed. If the folder is busy, the proxy returns:
  `409 { "error": "agent_folder_busy", "cwd": "...", "retry_after_ms": 2000 }`.

Resume from a specific event id by sending `Last-Event-ID: <event_id>` header.

### Replay missed events

`GET /v1/conversations/{conversation_id}/events?since=<event_id>`

Returns `application/x-ndjson` where each line is the same JSON payload that was sent via SSE
(Claude stream-json objects).

## Manual testing (no iOS app)

Run the demo script (streams 2 conversations in parallel using different `cwd` folders):

```bash
./scripts/demo_parallel_conversations.sh
```

Stream with curl (replace `conversation_id` and `cwd`):

```bash
curl -N \
  -H 'Accept: text/event-stream' \
  -H 'Content-Type: application/json' \
  -X POST \
  http://127.0.0.1:8787/v1/agent/stream \
  -d '{"conversation_id":"c1","cwd":"/tmp","text":"Hello from curl"}'
```

If you interrupt the stream (Ctrl+C), you can replay everything after an event id:

```bash
curl -s "http://127.0.0.1:8787/v1/conversations/c1/events?since=0"
```

## Tests

```bash
cd server/claude-proxy
python -m pytest -q
```

## Deployment (installer)

Run the installer on the target server (passwordless sudo required):

```bash
curl -fsSL https://raw.githubusercontent.com/eugenepyvovarov/codeagents-server-cc-proxy/HEAD/install.sh | sudo bash
```

Optional overrides:

```bash
REPO_URL=https://github.com/eugenepyvovarov/codeagents-server-cc-proxy.git \
INSTALL_DIR=/opt/claude-proxy \
DATA_DIR=/opt/claude-proxy/data \
LOG_DIR=/var/log/claude-proxy \
curl -fsSL https://raw.githubusercontent.com/eugenepyvovarov/codeagents-server-cc-proxy/HEAD/install.sh | sudo bash
```

What it does:

- Installs Python 3.10+, Node/npm, and `@anthropic-ai/claude-code`.
- Clones or updates the proxy repo in `/opt/claude-proxy`.
- Creates a venv and installs Python deps.
- Sets up a daemon:
  - Linux: supervisord program (`deploy/supervisord/claude-proxy.conf`)
  - macOS: launchd plist (`deploy/launchd/com.codeagents.claude-proxy.plist`)
- Verifies `/healthz` on `127.0.0.1:8787`.

Logs:

- Linux: `/var/log/claude-proxy/claude-proxy.log`
- macOS: `/var/log/claude-proxy/claude-proxy.log`
