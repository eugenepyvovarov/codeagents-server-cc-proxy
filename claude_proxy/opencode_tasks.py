from __future__ import annotations

import asyncio
import logging
import re
import uuid
from typing import Any

import httpx

from claude_proxy.conversation_manager import AgentFolderBusyError
from claude_proxy.opencode_client import OpenCodeClient
from claude_proxy.push_notifications import trigger_reply_finished
from claude_proxy.util import normalize_agent_id

logger = logging.getLogger(__name__)

# Real OpenCode ids look like ses_<long token>. Reject placeholders (e.g. ses_diag).
_OPENCODE_SESSION_ID_RE = re.compile(r"^ses_[A-Za-z0-9]{12,}$")
_RECENT_MESSAGE_LIMIT = 100
_SYNTHETIC_TOOL_NARRATION_RE = re.compile(
    r"^\s*Called the \S+ tool with the following input\b",
    re.IGNORECASE | re.MULTILINE,
)


def _new_task_message_id() -> str:
    return f"msg_{uuid.uuid4().hex}"


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

            task_message_id = _new_task_message_id()
            try:
                await self._client.session_messages(
                    session_id=session_id,
                    directory=cwd,
                    limit=_RECENT_MESSAGE_LIMIT,
                )
            except httpx.HTTPStatusError as exc:
                if not _is_not_found(exc):
                    raise
                logger.warning(
                    "OpenCode session %s missing during baseline fetch (404); recreating for cwd=%s",
                    session_id,
                    cwd,
                )
                session_id = await self._recreate_session_after_missing(
                    conversation_id=conversation_id,
                    cwd=cwd,
                    request_body=request_body,
                    dead_session_id=session_id,
                )
                await self._client.session_messages(
                    session_id=session_id,
                    directory=cwd,
                    limit=_RECENT_MESSAGE_LIMIT,
                )

            try:
                await self._client.prompt_async(
                    session_id=session_id,
                    prompt=prompt,
                    directory=cwd,
                    message_id=task_message_id,
                )
            except httpx.HTTPStatusError as exc:
                if not _is_not_found(exc):
                    raise
                logger.warning(
                    "OpenCode session %s missing during prompt (404); recreating for cwd=%s",
                    session_id,
                    cwd,
                )
                session_id = await self._recreate_session_after_missing(
                    conversation_id=conversation_id,
                    cwd=cwd,
                    request_body=request_body,
                    dead_session_id=session_id,
                )
                await self._client.session_messages(
                    session_id=session_id,
                    directory=cwd,
                    limit=_RECENT_MESSAGE_LIMIT,
                )
                await self._client.prompt_async(
                    session_id=session_id,
                    prompt=prompt,
                    directory=cwd,
                    message_id=task_message_id,
                )

            await self._wait_for_correlated_assistant_and_idle(
                session_id=session_id,
                cwd=cwd,
                task_message_id=task_message_id,
            )
            await self._trigger_push_if_configured(
                session_id=session_id,
                cwd=cwd,
                task_message_id=task_message_id,
            )
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

    async def _wait_for_correlated_assistant_and_idle(
        self,
        *,
        session_id: str,
        cwd: str,
        task_message_id: str,
    ) -> None:
        deadline = asyncio.get_running_loop().time() + self._max_wait_seconds

        while True:
            # Take the message snapshot first. An idle status sampled before the
            # snapshot can belong to the pre-prompt state and must not complete a task.
            messages = await self._client.session_messages(
                session_id=session_id,
                directory=cwd,
                limit=_RECENT_MESSAGE_LIMIT,
            )
            if _has_correlated_assistant_error(messages, parent_message_id=task_message_id):
                raise RuntimeError(f"OpenCode assistant reply for task {task_message_id} failed.")
            has_correlated_assistant = bool(
                finalized_renderable_open_code_assistant_message_ids(
                    messages,
                    parent_message_id=task_message_id,
                )
            )

            statuses = await self._client.session_status(directory=cwd)
            status_type = _status_type(statuses.get(session_id))
            if status_type == "error":
                raise RuntimeError(f"OpenCode session {session_id} failed.")
            if has_correlated_assistant and status_type in {None, "idle"}:
                return
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"OpenCode session {session_id} did not produce its finalized assistant reply and become idle."
                )
            await asyncio.sleep(self._poll_interval_seconds)

    async def _trigger_push_if_configured(
        self,
        *,
        session_id: str,
        cwd: str,
        task_message_id: str | None = None,
    ) -> None:
        try:
            # Refresh after the idle confirmation. The snapshot used to decide
            # completion was captured before the status sample and may be stale.
            recent_messages = await self._client.session_messages(
                session_id=session_id,
                directory=cwd,
                limit=_RECENT_MESSAGE_LIMIT,
            )
            preview, legacy_renderable_count, _ = summarize_open_code_messages(
                recent_messages,
                parent_message_id=task_message_id,
            )
            # The old part-based cursor was always a bounded recent snapshot.
            # Fetch the full session only once, after exact completion + idle,
            # to produce the v2 absolute finalized-assistant cursor.
            full_messages = await self._client.session_messages(
                session_id=session_id,
                directory=cwd,
                limit=None,
            )
            _, _, assistant_message_cursor = summarize_open_code_messages(full_messages)
            await trigger_reply_finished(
                cwd=cwd,
                conversation_id=session_id,
                message_preview=preview,
                renderable_assistant_count=legacy_renderable_count,
                assistant_message_cursor=assistant_message_cursor,
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


def _is_not_found(exc: httpx.HTTPStatusError) -> bool:
    return exc.response is not None and exc.response.status_code == 404


def summarize_open_code_messages(
    messages: list[Any],
    *,
    parent_message_id: str | None = None,
) -> tuple[str | None, int, int]:
    legacy_renderable_count = _legacy_renderable_assistant_count(messages)
    assistant_message_ids: set[str] = set()
    last_text: str | None = None

    for message in messages:
        if not isinstance(message, dict):
            continue
        info = message.get("info")
        role = info.get("role") if isinstance(info, dict) else None
        if role != "assistant":
            continue
        if not _has_completed_time(info):
            continue
        message_id = info.get("id") if isinstance(info, dict) else None
        if not isinstance(message_id, str) or not message_id.strip():
            continue
        message_id = message_id.strip()

        rendered_text = _rendered_open_code_text(message.get("parts"))
        if rendered_text:
            assistant_message_ids.add(message_id)
            if parent_message_id is None or info.get("parentID") == parent_message_id:
                last_text = rendered_text

    return last_text, legacy_renderable_count, len(assistant_message_ids)


def finalized_renderable_open_code_assistant_message_ids(
    messages: list[Any],
    *,
    parent_message_id: str | None = None,
) -> set[str]:
    ids: set[str] = set()
    for message in messages:
        if not isinstance(message, dict):
            continue
        info = message.get("info")
        if not isinstance(info, dict) or info.get("role") != "assistant":
            continue
        if not _has_completed_time(info):
            continue
        if parent_message_id is not None and info.get("parentID") != parent_message_id:
            continue
        message_id = info.get("id")
        if not isinstance(message_id, str) or not message_id.strip():
            continue
        if _rendered_open_code_text(message.get("parts")):
            ids.add(message_id.strip())
    return ids


def _has_correlated_assistant_error(messages: list[Any], *, parent_message_id: str) -> bool:
    for message in messages:
        if not isinstance(message, dict):
            continue
        info = message.get("info")
        if not isinstance(info, dict):
            continue
        if info.get("role") != "assistant" or info.get("parentID") != parent_message_id:
            continue
        if info.get("error") is not None:
            return True
    return False


def _has_completed_time(info: dict[str, Any]) -> bool:
    time = info.get("time")
    return isinstance(time, dict) and time.get("completed") is not None


def _legacy_renderable_assistant_count(messages: list[Any]) -> int:
    """Preserve the pre-v2 bubble/part cursor for installed app versions."""
    renderable_count = 0
    for message in messages:
        if not isinstance(message, dict):
            continue
        info = message.get("info")
        if not isinstance(info, dict) or info.get("role") != "assistant":
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
    return renderable_count


def _rendered_open_code_text(parts: Any) -> str | None:
    if not isinstance(parts, list):
        return None
    rendered: list[str] = []
    for part in parts:
        if not isinstance(part, dict) or part.get("type") != "text":
            continue
        if part.get("synthetic") is True or part.get("ignored") is True:
            continue
        text = part.get("text")
        if not isinstance(text, str):
            continue
        if _SYNTHETIC_TOOL_NARRATION_RE.search(text):
            # OpenCode may omit the synthetic flag. Discard the whole generated
            # narration part so nested tool input is never previewed as a reply.
            continue
        cleaned = text.strip()
        if cleaned:
            rendered.append(cleaned)
    combined = "\n".join(rendered).strip()
    return combined or None
