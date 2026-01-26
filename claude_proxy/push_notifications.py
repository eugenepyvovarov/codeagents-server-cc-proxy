from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


def _env(name: str) -> str | None:
    value = os.environ.get(name)
    if not value:
        return None
    trimmed = value.strip()
    return trimmed or None


def _build_url(base: str, path: str) -> str:
    return base.rstrip("/") + "/" + path.lstrip("/")


def _post_json(url: str, *, secret: str, payload: dict[str, Any]) -> None:
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url=url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {secret}",
        },
    )
    with urllib.request.urlopen(request, timeout=2.5) as response:
        _ = response.read()


async def trigger_reply_finished(*, cwd: str, conversation_id: str | None = None) -> None:
    secret = _env("CODEAGENTS_PUSH_SECRET")
    base_url = _env("CODEAGENTS_PUSH_GATEWAY_BASE_URL")
    if not secret or not base_url:
        return

    url = _build_url(base_url, "triggerReplyFinished")
    payload: dict[str, Any] = {"cwd": cwd}
    if conversation_id:
        payload["conversation_id"] = conversation_id

    try:
        await asyncio.to_thread(_post_json, url, secret=secret, payload=payload)
    except urllib.error.HTTPError as exc:
        logger.warning("Push trigger HTTP error: %s", getattr(exc, "code", "unknown"))
    except Exception:
        logger.exception("Push trigger failed")

