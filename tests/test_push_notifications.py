from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from claude_proxy.push_notifications import trigger_reply_finished


class CapturingHandler(BaseHTTPRequestHandler):
    captured: dict[str, Any] = {}

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8")
        CapturingHandler.captured = {
            "path": self.path,
            "authorization": self.headers.get("Authorization"),
            "content_type": self.headers.get("Content-Type"),
            "body": json.loads(body),
        }
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, format: str, *args: Any) -> None:
        _ = format
        _ = args


@pytest.mark.asyncio
async def test_trigger_reply_finished_posts_to_mocked_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    CapturingHandler.captured = {}
    server = HTTPServer(("127.0.0.1", 0), CapturingHandler)
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{server.server_port}"
    monkeypatch.setenv("CODEAGENTS_PUSH_SECRET", "push-secret")
    monkeypatch.setenv("CODEAGENTS_PUSH_GATEWAY_BASE_URL", base_url)

    await trigger_reply_finished(
        cwd="/tmp/project",
        conversation_id="ses_fixture",
        message_preview=" scheduled\n\n task   complete ",
        renderable_assistant_count=3,
        assistant_message_cursor=2,
    )

    thread.join(timeout=2)
    server.server_close()

    assert CapturingHandler.captured == {
        "path": "/triggerReplyFinished",
        "authorization": "Bearer push-secret",
        "content_type": "application/json",
        "body": {
            "cwd": "/tmp/project",
            "conversation_id": "ses_fixture",
            "renderable_assistant_count": 3,
            "assistant_message_cursor": 2,
            "cursor_version": 2,
            "message_preview": "scheduled task complete",
            "include_preview": True,
        },
    }


@pytest.mark.asyncio
async def test_trigger_reply_finished_omits_v2_marker_without_v2_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_post_json(url: str, *, secret: str, payload: dict[str, Any]) -> dict[str, Any]:
        captured.update({"url": url, "secret": secret, "payload": payload})
        return {"ok": True}

    monkeypatch.setenv("CODEAGENTS_PUSH_SECRET", "push-secret")
    monkeypatch.setenv("CODEAGENTS_PUSH_GATEWAY_BASE_URL", "https://push.example")
    monkeypatch.setattr("claude_proxy.push_notifications._post_json", fake_post_json)

    await trigger_reply_finished(cwd="/tmp/project", renderable_assistant_count=3)

    assert captured["payload"] == {
        "cwd": "/tmp/project",
        "renderable_assistant_count": 3,
    }
