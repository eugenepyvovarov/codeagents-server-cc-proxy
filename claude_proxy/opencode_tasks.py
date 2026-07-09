from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

import httpx

from claude_proxy.conversation_manager import AgentFolderBusyError
from claude_proxy.opencode_client import OpenCodeClient
from claude_proxy.push_notifications import trigger_reply_finished
from claude_proxy.util import normalize_agent_id

logger = logging.getLogger(__name__)

# Real OpenCode ids look like ses_<long token>. Reject placeholders (e.g. ses_diag).
_OPENCODE_SESSION_ID_RE = re.compile(r"^ses_[A-Za-z0-9]{12,}$")


class OpenCodeTaskRunner:
    start_run_waits_for_completion = True

    def __init__(
        self,
        *,
        client: OpenCodeClient,
        store: Any,
        poll_interval_seconds: float = 1.0,
        max_wait_seconds: float = 60 * 60,
    ) -> None:
        self._client = client
        self._store = store
        self._poll_interval_seconds = poll_interval_seconds
        self._max_wait_seconds = max_wait_seconds
        self._active_by_cwd: dict[str, str] = {}
        self._active_lock = asyncio.Lock()

    async def resolve_conversation_id(
        self,
        *,
        conversation_id: str,
        cwd: str | None,
        conversation_group: str | None = None,
    ) -> str:
        _ = cwd
        _ = conversation_group
        return conversation_id

    async def start_run(self, *, conversation_id: str, prompt: str, request_body: dict[str, Any]) -> bool:
        prompt = prompt.strip()
        if not prompt:
            raise ValueError("text required to start a new run.")

        cwd_value = request_body.get("cwd")
        if not isinstance(cwd_value, str) or not cwd_value.strip():
            raise ValueError("cwd is required to start a scheduled OpenCode run.")
        cwd = cwd_value.strip()

        async with self._active_lock:
            active = self._active_by_cwd.get(cwd)
            if active is not None:
                raise AgentFolderBusyError(cwd=cwd)
            self._active_by_cwd[cwd] = conversation_id

        try:
            session_id = await self._resolve_session_id(
                conversation_id=conversation_id,
                cwd=cwd,
                request_body=request_body,
            )
            if await self._is_session_busy(session_id=session_id, cwd=cwd):
                return False

            try:
                await self._client.prompt_async(session_id=session_id, prompt=prompt, directory=cwd)
            except httpx.HTTPStatusError as exc:
                if exc.response is None or exc.response.status_code != 404:
                    raise
                logger.warning(
                    "OpenCode session %s missing (404); clearing pin and recreating for cwd=%s",
                    session_id,
                    cwd,
                )
                session_id = await self._recreate_session_after_missing(
                    conversation_id=conversation_id,
                    cwd=cwd,
                    request_body=request_body,
                    dead_session_id=session_id,
                )
                await self._client.prompt_async(session_id=session_id, prompt=prompt, directory=cwd)

            await self._wait_until_idle(session_id=session_id, cwd=cwd)
            await self._trigger_push_if_configured(session_id=session_id, cwd=cwd)
            return True
        finally:
            async with self._active_lock:
                if self._active_by_cwd.get(cwd) == conversation_id:
                    self._active_by_cwd.pop(cwd, None)

    async def has_active_runs(self) -> bool:
        async with self._active_lock:
            return bool(self._active_by_cwd)

    async def _resolve_session_id(
        self,
        *,
        conversation_id: str,
        cwd: str,
        request_body: dict[str, Any],
    ) -> str:
        agent_id, conversation_group = self._identity_from_request(request_body)

        active_session_id = await self._store.get_active_opencode_session(
            agent_id=agent_id,
            conversation_group=conversation_group,
            cwd=cwd,
        )
        if active_session_id and not looks_like_opencode_session_id(active_session_id):
            logger.warning(
                "Ignoring invalid active OpenCode pin %r for agent=%s cwd=%s",
                active_session_id,
                agent_id,
                cwd,
            )
            await self._store.clear_active_opencode_session(
                agent_id=agent_id,
                conversation_group=conversation_group,
                cwd=cwd,
            )
            active_session_id = None

        if active_session_id:
            await self._store.save_opencode_session(
                agent_id=agent_id,
                conversation_id=conversation_id,
                conversation_group=conversation_group,
                cwd=cwd,
                session_id=active_session_id,
            )
            # Re-pin canonical lowercase key so mixed-case legacy rows collapse.
            await self._store.save_active_opencode_session(
                agent_id=agent_id,
                conversation_group=conversation_group,
                cwd=cwd,
                session_id=active_session_id,
            )
            return active_session_id

        explicit_session_id = request_body.get("open_code_session_id") or request_body.get("session_id")
        if isinstance(explicit_session_id, str) and explicit_session_id.strip():
            session_id = explicit_session_id.strip()
            if looks_like_opencode_session_id(session_id):
                await self._store.save_active_opencode_session(
                    agent_id=agent_id,
                    conversation_group=conversation_group,
                    cwd=cwd,
                    session_id=session_id,
                )
                await self._store.save_opencode_session(
                    agent_id=agent_id,
                    conversation_id=conversation_id,
                    conversation_group=conversation_group,
                    cwd=cwd,
                    session_id=session_id,
                )
                return session_id
            logger.warning(
                "Ignoring invalid explicit OpenCode session id %r for agent=%s cwd=%s",
                session_id,
                agent_id,
                cwd,
            )

        stored_session_id = await self._store.get_opencode_session(
            agent_id=agent_id,
            conversation_id=conversation_id,
            conversation_group=conversation_group,
            cwd=cwd,
        )
        if stored_session_id and looks_like_opencode_session_id(stored_session_id):
            return stored_session_id

        return await self._create_and_store_session(
            agent_id=agent_id,
            conversation_id=conversation_id,
            conversation_group=conversation_group,
            cwd=cwd,
            pin_as_active=False,
        )

    async def _recreate_session_after_missing(
        self,
        *,
        conversation_id: str,
        cwd: str,
        request_body: dict[str, Any],
        dead_session_id: str,
    ) -> str:
        agent_id, conversation_group = self._identity_from_request(request_body)
        try:
            await self._store.clear_active_opencode_session(
                agent_id=agent_id,
                conversation_group=conversation_group,
                cwd=cwd,
            )
        except Exception:
            logger.exception(
                "Failed clearing dead OpenCode pin %s for agent=%s cwd=%s",
                dead_session_id,
                agent_id,
                cwd,
            )
        # Create a replacement and pin it so chat + scheduler reconverge on a live session.
        return await self._create_and_store_session(
            agent_id=agent_id,
            conversation_id=conversation_id,
            conversation_group=conversation_group,
            cwd=cwd,
            pin_as_active=True,
        )

    async def _create_and_store_session(
        self,
        *,
        agent_id: str,
        conversation_id: str,
        conversation_group: str | None,
        cwd: str,
        pin_as_active: bool,
    ) -> str:
        title = self._session_title(agent_id=agent_id, cwd=cwd, conversation_group=conversation_group)
        created = await self._client.create_session(title=title, directory=cwd)
        session_id = created.get("id")
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("OpenCode did not return a session id.")

        session_id = session_id.strip()
        await self._store.save_opencode_session(
            agent_id=agent_id,
            conversation_id=conversation_id,
            conversation_group=conversation_group,
            cwd=cwd,
            session_id=session_id,
        )
        if pin_as_active:
            await self._store.save_active_opencode_session(
                agent_id=agent_id,
                conversation_group=conversation_group,
                cwd=cwd,
                session_id=session_id,
            )
        return session_id

    def _identity_from_request(self, request_body: dict[str, Any]) -> tuple[str, str | None]:
        raw_agent_id = request_body.get("agent_id")
        if isinstance(raw_agent_id, str) and raw_agent_id.strip():
            try:
                agent_id = normalize_agent_id(raw_agent_id)
            except ValueError:
                agent_id = raw_agent_id.strip().lower()
        else:
            agent_id = "default"

        conversation_group = request_body.get("conversation_group")
        conversation_group = (
            conversation_group.strip()
            if isinstance(conversation_group, str) and conversation_group.strip()
            else None
        )
        return agent_id, conversation_group

    async def _is_session_busy(self, *, session_id: str, cwd: str) -> bool:
        statuses = await self._client.session_status(directory=cwd)
        return _status_type(statuses.get(session_id)) in {"busy", "running", "retry", "retrying"}

    async def _wait_until_idle(self, *, session_id: str, cwd: str) -> None:
        deadline = asyncio.get_running_loop().time() + self._max_wait_seconds

        while True:
            statuses = await self._client.session_status(directory=cwd)
            status_type = _status_type(statuses.get(session_id))
            if status_type in {None, "idle"}:
                return
            if status_type == "error":
                raise RuntimeError(f"OpenCode session {session_id} failed.")
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(f"OpenCode session {session_id} did not become idle.")
            await asyncio.sleep(self._poll_interval_seconds)

    async def _trigger_push_if_configured(self, *, session_id: str, cwd: str) -> None:
        try:
            messages = await self._client.session_messages(session_id=session_id, directory=cwd, limit=100)
            preview, renderable_count = summarize_open_code_messages(messages)
            await trigger_reply_finished(
                cwd=cwd,
                conversation_id=session_id,
                message_preview=preview,
                renderable_assistant_count=renderable_count,
            )
        except Exception:
            logger.exception("Failed to trigger OpenCode scheduled task push")

    def _session_title(self, *, agent_id: str, cwd: str, conversation_group: str | None) -> str:
        project_name = cwd.rstrip("/").split("/")[-1] or cwd
        if conversation_group:
            return f"Scheduled task: {project_name} ({conversation_group})"
        return f"Scheduled task: {project_name} ({agent_id})"


def looks_like_opencode_session_id(value: str | None) -> bool:
    if not isinstance(value, str):
        return False
    trimmed = value.strip()
    return bool(_OPENCODE_SESSION_ID_RE.fullmatch(trimmed))


def _status_type(value: Any) -> str | None:
    if isinstance(value, str):
        return value.strip().lower() or None
    if isinstance(value, dict):
        for key in ("type", "status"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip().lower()
    return None


def summarize_open_code_messages(messages: list[Any]) -> tuple[str | None, int]:
    renderable_count = 0
    last_text: str | None = None

    for message in messages:
        if not isinstance(message, dict):
            continue
        info = message.get("info")
        role = info.get("role") if isinstance(info, dict) else None
        if role != "assistant":
            continue

        parts = message.get("parts")
        if not isinstance(parts, list):
            continue

        for part in parts:
            if not isinstance(part, dict):
                continue
            part_type = part.get("type")
            if part_type in {"text", "reasoning"}:
                text = part.get("text")
                if isinstance(text, str) and text.strip():
                    renderable_count += 1
                    last_text = text
            elif part_type in {
                "tool",
                "file",
                "patch",
                "snapshot",
                "agent",
                "subtask",
                "retry",
                "compaction",
            }:
                renderable_count += 1

    return last_text, renderable_count
