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


def _normalize_preview(text: str, max_len: int = 600) -> str | None:
    trimmed = text.strip()
    if not trimmed:
        return None

    collapsed = " ".join(trimmed.split())
    if not collapsed:
        return None

    if len(collapsed) <= max_len:
        return collapsed

    slice_len = max(0, max_len - 1)
    return collapsed[:slice_len].rstrip() + "…"


# Cloud Functions cold-start + FCM multicast routinely exceeds 2–3s.
# Keep this high enough that scheduled-task completions are not dropped.
_PUSH_GATEWAY_TIMEOUT_SECONDS = 15.0


def _post_json(url: str, *, secret: str, payload: dict[str, Any]) -> dict[str, Any]:
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
    with urllib.request.urlopen(request, timeout=_PUSH_GATEWAY_TIMEOUT_SECONDS) as response:
        raw = response.read()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


async def trigger_reply_finished(
    *,
    cwd: str,
    conversation_id: str | None = None,
    message_preview: str | None = None,
    renderable_assistant_count: int | None = None,
) -> None:
    secret = _env("CODEAGENTS_PUSH_SECRET")
    base_url = _env("CODEAGENTS_PUSH_GATEWAY_BASE_URL")
    if not secret or not base_url:
        missing = [
            name
            for name, value in (
                ("CODEAGENTS_PUSH_SECRET", secret),
                ("CODEAGENTS_PUSH_GATEWAY_BASE_URL", base_url),
            )
            if not value
        ]
        logger.info("Push trigger skipped: missing %s", ", ".join(missing))
        return

    url = _build_url(base_url, "triggerReplyFinished")
    payload: dict[str, Any] = {"cwd": cwd}
    if conversation_id:
        payload["conversation_id"] = conversation_id
    if renderable_assistant_count is not None:
        payload["renderable_assistant_count"] = renderable_assistant_count
    if message_preview:
        normalized = _normalize_preview(message_preview)
        if normalized:
            payload["message_preview"] = normalized

    try:
        result = await asyncio.to_thread(_post_json, url, secret=secret, payload=payload)
        attempted = result.get("attempted")
        sent = result.get("sent")
        pruned = result.get("pruned")
        errors = result.get("errors")
        if attempted == 0:
            logger.info("Push trigger completed with no registered devices for cwd=%s", cwd)
        elif isinstance(attempted, int) and isinstance(sent, int) and sent <= 0:
            extra = ""
            if isinstance(errors, dict) and errors:
                extra = f" errors={errors}"
            if isinstance(pruned, int) and pruned > 0:
                extra += f" pruned={pruned}"
            logger.warning(
                "Push trigger sent 0/%s notifications for cwd=%s%s",
                attempted,
                cwd,
                extra,
            )
        elif isinstance(attempted, int) and isinstance(sent, int):
            logger.info("Push trigger sent %s/%s notifications for cwd=%s", sent, attempted, cwd)
    except urllib.error.HTTPError as exc:
        logger.warning("Push trigger HTTP error: %s", getattr(exc, "code", "unknown"))
    except Exception:
        logger.exception("Push trigger failed")
