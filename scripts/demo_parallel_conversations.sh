#!/bin/sh
set -eu

BASE_URL="${BASE_URL:-http://127.0.0.1:8787}"

CONV_A="${CONV_A:-demo-conv-a}"
CONV_B="${CONV_B:-demo-conv-b}"

CWD_A="${CWD_A:-/tmp/codeagents-demo-agentA}"
CWD_B="${CWD_B:-/tmp/codeagents-demo-agentB}"

if [ -z "${TOKEN_A:-}" ]; then
  TOKEN_A=$(python3 - <<'PY'
import uuid
print(f"tokenA-{uuid.uuid4().hex[:8]}")
PY
  )
fi
if [ -z "${TOKEN_B:-}" ]; then
  TOKEN_B=$(python3 - <<'PY'
import uuid
print(f"tokenB-{uuid.uuid4().hex[:8]}")
PY
  )
fi

TEXT_A1="${TEXT_A1:-Remember this token exactly: $TOKEN_A. Reply ONLY: OK.}"
TEXT_B1="${TEXT_B1:-Remember this token exactly: $TOKEN_B. Reply ONLY: OK.}"
TEXT_A2="${TEXT_A2:-What token did I ask you to remember? Reply ONLY with the token.}"
TEXT_B2="${TEXT_B2:-What token did I ask you to remember? Reply ONLY with the token.}"

mkdir -p "$CWD_A" "$CWD_B"

make_json() {
  python3 - "$@" <<'PY'
import json
import sys

conversation_id, cwd, text = sys.argv[1], sys.argv[2], sys.argv[3]
print(json.dumps({"conversation_id": conversation_id, "cwd": cwd, "text": text}))
PY
}

stream() {
  label="$1"
  conversation_id="$2"
  cwd="$3"
  text="$4"
  since="$5"
  out_file="$6"
  body=$(make_json "$conversation_id" "$cwd" "$text")

  curl -sS -N \
    -H 'Accept: text/event-stream' \
    -H "Last-Event-ID: ${since}" \
    -H 'Content-Type: application/json' \
    -X POST \
    "$BASE_URL/v1/agent/stream" \
    -d "$body" | tee "$out_file" | awk -v label="$label" '
      BEGIN { id=""; data=""; }
      /^:/ { next }
      /^id:[[:space:]]*/ { id=substr($0, index($0, ":")+2); next }
      /^data:[[:space:]]*/ { data=substr($0, index($0, ":")+2); next }
      /^[[:space:]]*$/ {
        if (id != "" && data != "") {
          printf("[%s] id=%s data=%s\n", label, id, data);
          id=""; data="";
        }
        next
      }
      { printf("[%s] %s\n", label, $0); }
    '
}

last_event_id() {
  awk '/^id:/{id=$2} END{print id+0}' "$1"
}

verify_token() {
  conversation_id="$1"
  expected="$2"
  python3 - "$conversation_id" "$expected" <<'PY'
import json
import sys

conversation_id = sys.argv[1]
expected = sys.argv[2]

last_result = None
for raw in sys.stdin:
    raw = raw.strip()
    if not raw:
        continue
    try:
        obj = json.loads(raw)
    except Exception:
        continue
    if obj.get("type") == "result" and isinstance(obj.get("result"), str):
        last_result = obj["result"]

if last_result is None:
    print(f"[{conversation_id}] WARN: no result event found")
    sys.exit(1)

if expected in last_result:
    print(f"[{conversation_id}] OK: continued (token found in last result)")
    sys.exit(0)

print(f"[{conversation_id}] WARN: token not found in last result. last_result={last_result!r}")
sys.exit(1)
PY
}

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/claude-proxy-demo.XXXXXX")
trap 'rm -rf "$TMP_DIR"' EXIT

echo "BASE_URL=$BASE_URL"
echo "A: conversation_id=$CONV_A cwd=$CWD_A"
echo "B: conversation_id=$CONV_B cwd=$CWD_B"
echo "A token: $TOKEN_A"
echo "B token: $TOKEN_B"
echo ""

echo "== Turn 1 (parallel) =="
f_a1="$TMP_DIR/a1.sse"
f_b1="$TMP_DIR/b1.sse"
f_a2="$TMP_DIR/a2.sse"
f_b2="$TMP_DIR/b2.sse"

stream "A" "$CONV_A" "$CWD_A" "$TEXT_A1" "0" "$f_a1" &
pid_a=$!
stream "B" "$CONV_B" "$CWD_B" "$TEXT_B1" "0" "$f_b1" &
pid_b=$!
wait "$pid_a" "$pid_b"

last_a1=$(last_event_id "$f_a1")
last_b1=$(last_event_id "$f_b1")
echo ""
echo "Turn 1 last event ids: A=$last_a1 B=$last_b1"
echo ""

echo "== Turn 2 (parallel, same conversation_ids; should auto-resume) =="
stream "A" "$CONV_A" "$CWD_A" "$TEXT_A2" "$last_a1" "$f_a2" &
pid_a=$!
stream "B" "$CONV_B" "$CWD_B" "$TEXT_B2" "$last_b1" "$f_b2" &
pid_b=$!
wait "$pid_a" "$pid_b"
echo ""

echo "== Verify continuation (token should appear in turn 2 result) =="
curl -s "$BASE_URL/v1/conversations/$CONV_A/events?since=0" | verify_token "$CONV_A" "$TOKEN_A" || true
curl -s "$BASE_URL/v1/conversations/$CONV_B/events?since=0" | verify_token "$CONV_B" "$TOKEN_B" || true
echo ""

echo "Replay NDJSON:"
echo "  curl -s \"$BASE_URL/v1/conversations/$CONV_A/events?since=0\" | head"
echo "  curl -s \"$BASE_URL/v1/conversations/$CONV_B/events?since=0\" | head"
