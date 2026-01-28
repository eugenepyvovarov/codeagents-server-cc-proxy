from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from collections import deque
from collections.abc import AsyncIterator, Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk.types import HookMatcher, PermissionResultAllow, PermissionResultDeny, ToolPermissionContext

from claude_proxy.serialization import serialize_message
from claude_proxy.push_notifications import trigger_reply_finished

logger = logging.getLogger(__name__)

TOOL_PERMISSION_TIMEOUT_SECONDS = 300


class AgentFolderBusyError(RuntimeError):
    def __init__(self, *, cwd: str) -> None:
        super().__init__(f"Agent folder busy: {cwd}")
        self.cwd = cwd


class ConversationCwdMismatchError(RuntimeError):
    def __init__(self, *, conversation_id: str, expected_cwd: str, got_cwd: str) -> None:
        super().__init__(f"Conversation {conversation_id} is bound to cwd={expected_cwd!r}, got {got_cwd!r}")
        self.conversation_id = conversation_id
        self.expected_cwd = expected_cwd
        self.got_cwd = got_cwd


class ConversationGroupMismatchError(RuntimeError):
    def __init__(self, *, conversation_id: str, expected_group: str, got_group: str) -> None:
        super().__init__(
            f"Conversation {conversation_id} is bound to group={expected_group!r}, got {got_group!r}"
        )
        self.conversation_id = conversation_id
        self.expected_group = expected_group
        self.got_group = got_group


@dataclass
class _Conversation:
    conversation_id: str
    conversation_dir: Path
    meta_path: Path
    events_path: Path

    created_at: float = field(default_factory=time.time)
    prompt: str | None = None
    cwd: str | None = None
    conversation_group: str | None = None
    claude_session_id: str | None = None

    last_event_id: int = 0
    is_running: bool = False
    is_done: bool = True

    buffer: deque[tuple[int, str]] = field(default_factory=lambda: deque(maxlen=512))
    subscribers: set[asyncio.Queue[tuple[int, str] | None]] = field(default_factory=set)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    runner_task: asyncio.Task[None] | None = None


class ConversationManager:
    def __init__(
        self,
        *,
        store_dir: Path | None,
        backend: Callable[..., AsyncIterator[Any]],
        buffer_size: int = 512,
        heartbeat_seconds: float = 15.0,
    ) -> None:
        self._store_dir = (store_dir or Path("data")).resolve()
        self._conversations_dir = self._store_dir / "conversations"
        self._conversations_dir.mkdir(parents=True, exist_ok=True)

        self._backend = backend
        self._buffer_size = buffer_size
        self._heartbeat_seconds = heartbeat_seconds

        self._conversations: dict[str, _Conversation] = {}
        self._conversations_lock = asyncio.Lock()

        self._active_run_by_cwd: dict[str, str] = {}
        self._cwd_lock = asyncio.Lock()

        self._cwd_index: dict[str, str] = {}
        self._group_index: dict[tuple[str, str], str] = {}
        self._alias_index: dict[str, str] = {}
        self._cwd_index_lock = asyncio.Lock()
        self._prime_cwd_index()
        self._on_run_finished: Callable[[str], Any] | None = None

        self._pending_tool_permissions: dict[
            str, tuple[str, asyncio.Future[PermissionResultAllow | PermissionResultDeny]]
        ] = {}
        self._tool_permission_lock = asyncio.Lock()

    def set_run_finished_callback(
        self,
        callback: Callable[[str], asyncio.Future[None] | asyncio.Task[None] | None] | None,
    ) -> None:
        self._on_run_finished = callback

    def new_conversation_id(self) -> str:
        return uuid.uuid4().hex

    async def has_active_runs(self) -> bool:
        async with self._cwd_lock:
            if self._active_run_by_cwd:
                return True
        async with self._conversations_lock:
            return any(conv.is_running for conv in self._conversations.values())

    def _dir_for(self, conversation_id: str) -> Path:
        return self._conversations_dir / conversation_id

    def _paths_for(self, conversation_id: str) -> tuple[Path, Path, Path]:
        conversation_dir = self._dir_for(conversation_id)
        return conversation_dir, conversation_dir / "meta.json", conversation_dir / "events.ndjson"

    def _normalize_cwd(self, cwd: str) -> str:
        trimmed = cwd.strip()
        if not trimmed:
            return ""
        try:
            return str(Path(trimmed).expanduser().resolve(strict=False))
        except Exception:
            return trimmed

    def _normalize_conversation_group(self, group: str | None) -> str | None:
        if group is None:
            return None
        normalized = str(group).strip()
        return normalized or None

    def _group_key(self, *, cwd: str, group: str) -> tuple[str, str]:
        return (self._normalize_cwd(cwd), group)

    def _prime_cwd_index(self) -> None:
        if not self._conversations_dir.exists():
            return

        cwd_candidates: dict[str, tuple[str, float]] = {}
        cwd_conversations: dict[str, list[str]] = {}
        group_keys: set[tuple[str, str]] = set()

        for meta_path in self._conversations_dir.glob("*/meta.json"):
            try:
                payload = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            conversation_id = payload.get("conversation_id")
            if not isinstance(conversation_id, str) or not conversation_id.strip():
                continue

            cwd_value = payload.get("cwd")
            group_value = payload.get("conversation_group")
            group = self._normalize_conversation_group(group_value) if isinstance(group_value, str) else None
            if isinstance(cwd_value, str) and cwd_value.strip():
                cwd = self._normalize_cwd(cwd_value)
                try:
                    mtime = meta_path.stat().st_mtime
                except OSError:
                    mtime = 0.0

                cwd_conversations.setdefault(cwd, []).append(conversation_id)
                existing = cwd_candidates.get(cwd)
                if existing is None or mtime > existing[1]:
                    cwd_candidates[cwd] = (conversation_id, mtime)

                if group:
                    group_keys.add((cwd, group))
            else:
                self._alias_index[conversation_id] = conversation_id

        for cwd, (canonical_id, _) in cwd_candidates.items():
            self._cwd_index[cwd] = canonical_id
            for conv_id in cwd_conversations.get(cwd, []):
                self._alias_index[conv_id] = canonical_id
            self._alias_index[canonical_id] = canonical_id

        for key in group_keys:
            cwd, _group = key
            canonical_id = self._cwd_index.get(cwd)
            if canonical_id:
                self._group_index[key] = canonical_id

    async def conversation_exists(self, conversation_id: str) -> bool:
        async with self._conversations_lock:
            if conversation_id in self._conversations:
                return True
        conversation_dir = self._dir_for(conversation_id)
        return conversation_dir.exists()

    async def get_or_create_conversation(self, conversation_id: str) -> _Conversation:
        conv: _Conversation
        async with self._conversations_lock:
            existing = self._conversations.get(conversation_id)
            if existing is not None:
                return existing

            conversation_dir, meta_path, events_path = self._paths_for(conversation_id)
            conv = _Conversation(
                conversation_id=conversation_id,
                conversation_dir=conversation_dir,
                meta_path=meta_path,
                events_path=events_path,
            )
            conv.buffer = deque(maxlen=self._buffer_size)

            await asyncio.to_thread(self._load_from_disk, conv)
            self._conversations[conversation_id] = conv

        await self._register_conversation_id(
            conversation_id=conv.conversation_id,
            cwd=conv.cwd,
            conversation_group=conv.conversation_group,
        )
        return conv

    async def resolve_conversation_id(
        self,
        *,
        conversation_id: str,
        cwd: str | None,
        conversation_group: str | None = None,
    ) -> str:
        conversation_id = conversation_id.strip()
        if not conversation_id:
            raise ValueError("conversation_id must be a non-empty string.")

        normalized_group = self._normalize_conversation_group(conversation_group)
        normalized_cwd = self._normalize_cwd(cwd) if cwd and cwd.strip() else None

        if normalized_cwd and await self.conversation_exists(conversation_id):
            conv = await self.get_or_create_conversation(conversation_id)
            async with conv.lock:
                existing_cwd = conv.cwd
            if existing_cwd and self._normalize_cwd(existing_cwd) != normalized_cwd:
                raise ConversationCwdMismatchError(
                    conversation_id=conversation_id,
                    expected_cwd=existing_cwd,
                    got_cwd=cwd,
                )

        if normalized_cwd:
            async with self._cwd_index_lock:
                canonical = self._cwd_index.get(normalized_cwd)
                if canonical:
                    self._alias_index[conversation_id] = canonical
                    self._alias_index[canonical] = canonical
                    if normalized_group:
                        self._group_index[(normalized_cwd, normalized_group)] = canonical
                    return canonical

        async with self._cwd_index_lock:
            canonical = self._alias_index.get(conversation_id)
        if canonical:
            return canonical

        if normalized_cwd is None:
            return conversation_id

        canonical = await self._register_conversation_id(
            conversation_id=conversation_id,
            cwd=normalized_cwd,
            conversation_group=normalized_group,
        )
        return canonical or conversation_id

    async def resolve_existing_conversation_id(
        self,
        *,
        conversation_id: str,
        cwd: str | None = None,
        conversation_group: str | None = None,
    ) -> str | None:
        conversation_id = conversation_id.strip()
        if not conversation_id:
            return None

        normalized_group = self._normalize_conversation_group(conversation_group)
        normalized_cwd = self._normalize_cwd(cwd) if cwd and cwd.strip() else None

        if normalized_cwd and await self.conversation_exists(conversation_id):
            conv = await self.get_or_create_conversation(conversation_id)
            async with conv.lock:
                existing_cwd = conv.cwd
            if existing_cwd and self._normalize_cwd(existing_cwd) != normalized_cwd:
                return None

        async with self._cwd_index_lock:
            if normalized_cwd:
                cwd_match = self._cwd_index.get(normalized_cwd)
                if cwd_match:
                    self._alias_index[conversation_id] = cwd_match
                    self._alias_index[cwd_match] = cwd_match
                    if normalized_group:
                        self._group_index[(normalized_cwd, normalized_group)] = cwd_match
                    return cwd_match

            canonical = self._alias_index.get(conversation_id)
            if canonical:
                return canonical

        if await self.conversation_exists(conversation_id):
            conv = await self.get_or_create_conversation(conversation_id)
            async with conv.lock:
                cwd_value = conv.cwd
                conv_group = conv.conversation_group
            canonical = await self._register_conversation_id(
                conversation_id=conv.conversation_id,
                cwd=cwd_value,
                conversation_group=conv_group,
            )
            return canonical or conversation_id

        return None

    async def ensure_cwd_binding(
        self,
        *,
        conversation_id: str,
        cwd: str,
        conversation_group: str | None = None,
    ) -> None:
        cwd = self._normalize_cwd(cwd)
        if not cwd:
            raise ValueError("cwd must be a non-empty string.")

        normalized_group = self._normalize_conversation_group(conversation_group)
        conv = await self.get_or_create_conversation(conversation_id)
        should_write = False
        async with conv.lock:
            if conv.cwd is None:
                conv.cwd = cwd
                should_write = True
            elif conv.cwd != cwd:
                raise ConversationCwdMismatchError(
                    conversation_id=conversation_id,
                    expected_cwd=conv.cwd,
                    got_cwd=cwd,
                )
            if normalized_group:
                if conv.conversation_group is None:
                    conv.conversation_group = normalized_group
                    should_write = True
                elif conv.conversation_group != normalized_group:
                    conv.conversation_group = normalized_group
                    should_write = True

        if should_write:
            await asyncio.to_thread(self._write_meta, conv)
            await self._register_conversation_id(
                conversation_id=conv.conversation_id,
                cwd=conv.cwd,
                conversation_group=conv.conversation_group,
            )

    async def start_run(self, *, conversation_id: str, prompt: str, request_body: dict[str, Any]) -> bool:
        prompt = prompt.strip()
        if not prompt:
            raise ValueError("text required to start a new run.")

        conv = await self.get_or_create_conversation(conversation_id)

        cwd_value = request_body.get("cwd")
        if isinstance(cwd_value, str):
            cwd = cwd_value
        else:
            cwd = conv.cwd or ""
        group_value = request_body.get("conversation_group")
        group = group_value.strip() if isinstance(group_value, str) and group_value.strip() else None
        await self.ensure_cwd_binding(
            conversation_id=conversation_id,
            cwd=cwd,
            conversation_group=group,
        )

        options: ClaudeAgentOptions | None = None
        tool_approvals: tuple[set[str], set[str]] | None = None
        cwd_value: str | None = None
        conv_group: str | None = None

        async with conv.lock:
            if conv.is_running:
                return False

            if conv.cwd is None:
                raise ValueError("cwd is required to start a new run.")

            async with self._cwd_lock:
                active = self._active_run_by_cwd.get(conv.cwd)
                if active is not None:
                    raise AgentFolderBusyError(cwd=conv.cwd)
                self._active_run_by_cwd[conv.cwd] = conversation_id

            conv.prompt = prompt
            conv.is_running = True
            conv.is_done = False
            cwd_value = conv.cwd
            conv_group = conv.conversation_group

            user_payload = self._build_user_prompt_event(prompt)
            if user_payload:
                json_line = json.dumps(user_payload, separators=(",", ":"), ensure_ascii=False)
                self._append_event(conv, json_line=json_line)

            options = self._build_options(request_body)
            tool_approvals = self._parse_tool_approvals(request_body)
            if getattr(options, "cwd", None) in (None, "") and conv.cwd:
                options.cwd = conv.cwd
            if request_body.get("resume") is None and conv.claude_session_id:
                options.resume = conv.claude_session_id

        if cwd_value:
            old_canonical = await self._promote_canonical(
                conversation_id=conversation_id,
                cwd=cwd_value,
                conversation_group=conv_group,
            )
            if old_canonical and old_canonical != conversation_id:
                await self._emit_session_switch(
                    previous_id=old_canonical,
                    new_id=conversation_id,
                    cwd=cwd_value,
                )

        try:
            async with conv.lock:
                conv.runner_task = asyncio.create_task(
                    self._run_agent(
                        conv=conv,
                        prompt=prompt,
                        options=options,
                        tool_approvals=tool_approvals,
                    )
                )
        except Exception:
            async with self._cwd_lock:
                if self._active_run_by_cwd.get(conv.cwd) == conversation_id:
                    self._active_run_by_cwd.pop(conv.cwd, None)
            raise
        return True

    async def activate_conversation(
        self,
        *,
        conversation_id: str,
        cwd: str,
        conversation_group: str | None = None,
    ) -> str | None:
        await self.ensure_cwd_binding(
            conversation_id=conversation_id,
            cwd=cwd,
            conversation_group=conversation_group,
        )

        conv = await self.get_or_create_conversation(conversation_id)
        async with conv.lock:
            cwd_value = conv.cwd
            conv_group = conv.conversation_group

        if not cwd_value:
            raise ValueError("cwd is required to activate a conversation.")

        async with self._cwd_lock:
            active = self._active_run_by_cwd.get(cwd_value)
            if active is not None and active != conversation_id:
                raise AgentFolderBusyError(cwd=cwd_value)

        old_canonical = await self._promote_canonical(
            conversation_id=conversation_id,
            cwd=cwd_value,
            conversation_group=conv_group,
        )
        if old_canonical and old_canonical != conversation_id:
            await self._emit_session_switch(
                previous_id=old_canonical,
                new_id=conversation_id,
                cwd=cwd_value,
            )

        return old_canonical

    async def sse_stream(self, *, conversation_id: str, since: int, request: Any) -> AsyncIterator[bytes]:
        conv = await self.get_or_create_conversation(conversation_id)
        queue: asyncio.Queue[tuple[int, str] | None] = asyncio.Queue()

        async with conv.lock:
            conv.subscribers.add(queue)

        try:
            async for eid, line in self._iter_events(conv=conv, since=since):
                yield _format_sse(eid=eid, json_line=line)
                since = eid

            while True:
                if await request.is_disconnected():
                    break

                async with conv.lock:
                    done = conv.is_done
                    last_eid = conv.last_event_id

                if done and since >= last_eid:
                    break

                try:
                    item = await asyncio.wait_for(queue.get(), timeout=self._heartbeat_seconds)
                except TimeoutError:
                    yield b": ping\n\n"
                    continue

                if item is None:
                    break

                eid, line = item
                if eid <= since:
                    continue
                yield _format_sse(eid=eid, json_line=line)
                since = eid
        finally:
            async with conv.lock:
                conv.subscribers.discard(queue)

    async def iter_ndjson(self, *, conversation_id: str, since: int) -> AsyncIterator[str]:
        conv = await self.get_or_create_conversation(conversation_id)
        async for _, line in self._iter_events(conv=conv, since=since):
            yield line + "\n"

    def _load_from_disk(self, conv: _Conversation) -> None:
        self._load_meta(conv)
        self._load_existing_events(conv)

    def _load_meta(self, conv: _Conversation) -> None:
        if not conv.meta_path.exists():
            return
        try:
            payload = json.loads(conv.meta_path.read_text(encoding="utf-8"))
        except Exception:
            return

        cwd = payload.get("cwd")
        if isinstance(cwd, str) and cwd.strip():
            conv.cwd = self._normalize_cwd(cwd)

        group = payload.get("conversation_group")
        if isinstance(group, str) and group.strip():
            conv.conversation_group = group.strip()

        claude_session_id = payload.get("claude_session_id")
        if isinstance(claude_session_id, str) and claude_session_id.strip():
            conv.claude_session_id = claude_session_id

    def _load_existing_events(self, conv: _Conversation) -> None:
        if not conv.events_path.exists():
            return

        try:
            eid = 0
            with conv.events_path.open("r", encoding="utf-8") as handle:
                for raw in handle:
                    raw = raw.strip()
                    if not raw:
                        continue
                    eid += 1
                    conv.last_event_id = eid
                    conv.buffer.append((eid, raw))
        except Exception:
            return

        conv.is_running = False
        conv.is_done = True

    async def _iter_events(self, *, conv: _Conversation, since: int) -> AsyncIterator[tuple[int, str]]:
        async with conv.lock:
            buffered = list(conv.buffer)
            last_eid = conv.last_event_id

        if buffered and since >= buffered[0][0] - 1:
            for eid, line in buffered:
                if eid > since:
                    yield eid, line
            return

        path = conv.events_path
        if not path.exists():
            return

        def read_lines() -> list[tuple[int, str]]:
            out: list[tuple[int, str]] = []
            eid = 0
            with path.open("r", encoding="utf-8") as handle:
                for raw in handle:
                    raw = raw.strip()
                    if not raw:
                        continue
                    eid += 1
                    if eid > since:
                        out.append((eid, raw))
            return out

        for eid, line in await asyncio.to_thread(read_lines):
            yield eid, line

        async with conv.lock:
            conv.is_done = conv.is_done or (not conv.is_running and conv.last_event_id == last_eid)

    def _append_event(self, conv: _Conversation, *, json_line: str) -> tuple[int, str]:
        conv.last_event_id += 1
        eid = conv.last_event_id

        conv.buffer.append((eid, json_line))

        conv.conversation_dir.mkdir(parents=True, exist_ok=True)
        with conv.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json_line + "\n")
            handle.flush()

        for q in list(conv.subscribers):
            q.put_nowait((eid, json_line))

        return eid, json_line

    def _build_user_prompt_event(self, prompt: str) -> dict[str, Any] | None:
        if not prompt:
            return None
        return {
            "type": "user",
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            },
        }

    async def _promote_canonical(
        self,
        *,
        conversation_id: str,
        cwd: str,
        conversation_group: str | None = None,
    ) -> str | None:
        normalized_cwd = self._normalize_cwd(cwd)
        if not normalized_cwd:
            return None

        normalized_group = self._normalize_conversation_group(conversation_group)

        async with self._cwd_index_lock:
            old_canonical = self._cwd_index.get(normalized_cwd)
            self._cwd_index[normalized_cwd] = conversation_id

            for key in list(self._group_index.keys()):
                if key[0] == normalized_cwd:
                    self._group_index[key] = conversation_id
            if normalized_group:
                self._group_index[(normalized_cwd, normalized_group)] = conversation_id

            self._alias_index[conversation_id] = conversation_id
            if old_canonical:
                self._alias_index[old_canonical] = conversation_id

        return old_canonical

    async def _emit_session_switch(self, *, previous_id: str, new_id: str, cwd: str) -> None:
        conv = await self.get_or_create_conversation(previous_id)
        payload = {
            "type": "proxy_session",
            "event": "switched",
            "canonical_id": new_id,
            "previous_id": previous_id,
            "cwd": cwd,
        }
        json_line = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        async with conv.lock:
            self._append_event(conv, json_line=json_line)

    def _coerce_json(self, value: Any) -> Any:
        try:
            return json.loads(json.dumps(value, default=str))
        except Exception:
            return str(value)

    def _format_permission_suggestions(self, context: ToolPermissionContext) -> list[str]:
        suggestions: list[str] = []
        for suggestion in getattr(context, "suggestions", []) or []:
            try:
                suggestions.append(
                    json.dumps(asdict(suggestion), separators=(",", ":"), ensure_ascii=False)
                )
            except Exception:
                suggestions.append(str(suggestion))
        return suggestions

    def _normalize_tool_name(self, tool_name: str) -> str:
        return tool_name.strip().lower()

    def _parse_tool_approvals(self, body: dict[str, Any]) -> tuple[set[str], set[str]]:
        approvals = body.get("tool_approvals")
        if not isinstance(approvals, dict):
            return set(), set()

        def collect(key: str) -> set[str]:
            values = approvals.get(key)
            if not isinstance(values, list):
                return set()
            out: set[str] = set()
            for value in values:
                if isinstance(value, str):
                    normalized = self._normalize_tool_name(value)
                    if normalized:
                        out.add(normalized)
            return out

        allow = collect("allow")
        deny = collect("deny")
        return allow, deny

    async def _await_tool_permission(
        self,
        *,
        conv: _Conversation,
        tool_name: str,
        tool_input: dict[str, Any],
        context: ToolPermissionContext,
    ) -> PermissionResultAllow | PermissionResultDeny:
        permission_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future: asyncio.Future[PermissionResultAllow | PermissionResultDeny] = loop.create_future()

        async with self._tool_permission_lock:
            self._pending_tool_permissions[permission_id] = (conv.conversation_id, future)

        payload = {
            "type": "tool_permission",
            "permission_id": permission_id,
            "tool_name": tool_name,
            "input": self._coerce_json(tool_input),
            "permission_suggestions": self._format_permission_suggestions(context),
            "blocked_path": None,
        }
        json_line = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        async with conv.lock:
            self._append_event(conv, json_line=json_line)

        await self.log_cwd_event(
            cwd=conv.cwd,
            event="tool_permission_request",
            payload={
                "conversation_id": conv.conversation_id,
                "permission_id": permission_id,
                "tool_name": tool_name,
            },
        )

        try:
            return await asyncio.wait_for(future, timeout=TOOL_PERMISSION_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            return PermissionResultDeny(message="Permission request timed out.", interrupt=False)
        finally:
            async with self._tool_permission_lock:
                self._pending_tool_permissions.pop(permission_id, None)

    async def resolve_tool_permission(
        self,
        *,
        permission_id: str,
        behavior: str,
        message: str | None = None,
        conversation_id: str | None = None,
    ) -> bool:
        async with self._tool_permission_lock:
            entry = self._pending_tool_permissions.get(permission_id)
            if entry is None:
                return False
            pending_conversation_id, future = entry
            if conversation_id and pending_conversation_id != conversation_id:
                return False
            self._pending_tool_permissions.pop(permission_id, None)

        if future.done():
            return True

        if behavior == "allow":
            future.set_result(PermissionResultAllow())
        else:
            future.set_result(PermissionResultDeny(message=message or "Permission denied by user."))
        return True

    async def _cancel_pending_permissions(self, conversation_id: str) -> None:
        pending: list[tuple[str, asyncio.Future[PermissionResultAllow | PermissionResultDeny]]] = []
        async with self._tool_permission_lock:
            for permission_id, (conv_id, future) in list(self._pending_tool_permissions.items()):
                if conv_id == conversation_id:
                    pending.append((permission_id, future))
                    self._pending_tool_permissions.pop(permission_id, None)

        for _, future in pending:
            if not future.done():
                future.set_result(PermissionResultDeny(message="Permission request cancelled.", interrupt=False))

    async def _run_agent(
        self,
        *,
        conv: _Conversation,
        prompt: str,
        options: ClaudeAgentOptions,
        tool_approvals: tuple[set[str], set[str]] | None = None,
    ) -> None:
        saw_result = False
        should_trigger_push = False
        message_preview: str | None = None
        allow_set, deny_set = tool_approvals or (set(), set())

        async def pre_tool_use_hook(
            _input: Any, _tool_use_id: str | None, _context: dict[str, Any]
        ) -> dict[str, Any]:
            return {"continue_": True}

        async def can_use_tool(
            tool_name: str, tool_input: dict[str, Any], context: ToolPermissionContext
        ) -> PermissionResultAllow | PermissionResultDeny:
            normalized = self._normalize_tool_name(tool_name)
            if normalized in deny_set:
                return PermissionResultDeny(message="Permission denied by user.", interrupt=False)
            if normalized in allow_set:
                return PermissionResultAllow()
            return await self._await_tool_permission(
                conv=conv,
                tool_name=tool_name,
                tool_input=tool_input,
                context=context,
            )

        options.can_use_tool = can_use_tool
        if options.hooks is None:
            options.hooks = {"PreToolUse": [HookMatcher(matcher=None, hooks=[pre_tool_use_hook])]}
        elif "PreToolUse" not in options.hooks:
            options.hooks["PreToolUse"] = [HookMatcher(matcher=None, hooks=[pre_tool_use_hook])]

        try:
            async for message in self._backend(prompt=prompt, options=options):
                payload = serialize_message(message)
                if not payload:
                    continue

                if payload.get("type") == "stream_event":
                    continue

                if payload.get("type") == "result":
                    saw_result = True
                    is_error_value = payload.get("is_error")
                    if is_error_value is False:
                        should_trigger_push = True
                        result_value = payload.get("result")
                        if isinstance(result_value, str) and result_value.strip():
                            message_preview = result_value

                session_id_value = payload.get("session_id")
                if isinstance(session_id_value, str) and session_id_value.strip():
                    await self._maybe_store_claude_session_id(conv=conv, session_id=session_id_value)

                json_line = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
                async with conv.lock:
                    self._append_event(conv, json_line=json_line)
        except Exception as exc:
            error_payload = {
                "type": "result",
                "subtype": "proxy_error",
                "is_error": True,
                "duration_ms": 0,
                "duration_api_ms": 0,
                "num_turns": 0,
                "result": f"ERROR: {type(exc).__name__}: {exc}",
            }
            json_line = json.dumps(error_payload, separators=(",", ":"), ensure_ascii=False)
            async with conv.lock:
                self._append_event(conv, json_line=json_line)
        finally:
            if not saw_result:
                final_payload = {
                    "type": "result",
                    "subtype": "proxy_finished",
                    "is_error": False,
                    "duration_ms": 0,
                    "duration_api_ms": 0,
                    "num_turns": 0,
                }
                json_line = json.dumps(final_payload, separators=(",", ":"), ensure_ascii=False)
                async with conv.lock:
                    self._append_event(conv, json_line=json_line)

            async with conv.lock:
                conv.is_running = False
                conv.is_done = True
                for q in list(conv.subscribers):
                    q.put_nowait(None)

            await self._cancel_pending_permissions(conv.conversation_id)

            cwd = conv.cwd
            if cwd:
                async with self._cwd_lock:
                    if self._active_run_by_cwd.get(cwd) == conv.conversation_id:
                        self._active_run_by_cwd.pop(cwd, None)

                if should_trigger_push:
                    try:
                        asyncio.create_task(
                            trigger_reply_finished(
                                cwd=cwd,
                                conversation_id=conv.conversation_id,
                                message_preview=message_preview,
                            )
                        )
                    except Exception:
                        logger.exception("Failed to schedule push trigger")

                if self._on_run_finished:
                    try:
                        result = self._on_run_finished(cwd)
                        if asyncio.iscoroutine(result) or asyncio.isfuture(result):
                            await result
                    except Exception:
                        logger.exception("Run finished callback failed")

    def _build_options(self, body: dict[str, Any]) -> ClaudeAgentOptions:
        kwargs: dict[str, Any] = {}

        project_claude_dir: Path | None = None
        project_settings_path: Path | None = None
        project_mcp_path: Path | None = None
        project_skills_dir: Path | None = None
        if isinstance(body.get("cwd"), str):
            kwargs["cwd"] = body["cwd"]
            cwd_path = Path(body["cwd"]).expanduser().resolve()
            project_claude_dir = cwd_path / ".claude"
            project_settings_path = project_claude_dir / "settings.json"
            project_mcp_path = project_claude_dir / "mcp.json"
            project_skills_dir = project_claude_dir / "skills"

        allowed_tools = None
        if isinstance(body.get("allowed_tools"), list):
            allowed_tools = body["allowed_tools"]
            kwargs["allowed_tools"] = allowed_tools
        if body.get("system_prompt") is not None:
            kwargs["system_prompt"] = body["system_prompt"]
        if body.get("max_turns") is not None:
            kwargs["max_turns"] = body["max_turns"]
        if body.get("mcp_servers") is not None:
            kwargs["mcp_servers"] = body["mcp_servers"]
        if body.get("resume") is not None:
            kwargs["resume"] = body["resume"]
        if body.get("continue_conversation") is not None:
            kwargs["continue_conversation"] = body["continue_conversation"]
        if body.get("model") is not None:
            kwargs["model"] = body["model"]
        if body.get("permission_mode") is not None:
            kwargs["permission_mode"] = body["permission_mode"]

        if body.get("settings") is not None:
            kwargs["settings"] = body["settings"]
        elif project_settings_path and project_settings_path.is_file():
            kwargs["settings"] = str(project_settings_path)

        if body.get("setting_sources") is not None:
            kwargs["setting_sources"] = body["setting_sources"]
        else:
            user_skills_dir = Path.home() / ".claude" / "skills"
            should_load_settings = False
            if project_settings_path and project_settings_path.is_file():
                should_load_settings = True
            if project_mcp_path and project_mcp_path.is_file():
                should_load_settings = True
            if project_skills_dir and project_skills_dir.is_dir():
                should_load_settings = True
            if user_skills_dir.is_dir():
                should_load_settings = True
            if should_load_settings:
                kwargs["setting_sources"] = ["user", "project"]

        if body.get("mcp_servers") is None and project_mcp_path and project_mcp_path.is_file():
            kwargs["mcp_servers"] = str(project_mcp_path)

        if allowed_tools is not None:
            has_skill_tool = "Skill" in allowed_tools
            if not has_skill_tool:
                if project_skills_dir and project_skills_dir.is_dir():
                    allowed_tools.append("Skill")
                    kwargs["allowed_tools"] = allowed_tools
                else:
                    user_skills_dir = Path.home() / ".claude" / "skills"
                    if user_skills_dir.is_dir():
                        allowed_tools.append("Skill")
                        kwargs["allowed_tools"] = allowed_tools

        return ClaudeAgentOptions(**kwargs)

    async def _maybe_store_claude_session_id(self, *, conv: _Conversation, session_id: str) -> None:
        session_id = session_id.strip()
        if not session_id:
            return

        should_write = False
        async with conv.lock:
            if conv.claude_session_id != session_id:
                conv.claude_session_id = session_id
                should_write = True

        if should_write:
            await asyncio.to_thread(self._write_meta, conv)

    def _write_meta(self, conv: _Conversation) -> None:
        payload: dict[str, Any] = {
            "conversation_id": conv.conversation_id,
            "cwd": conv.cwd,
            "conversation_group": conv.conversation_group,
            "claude_session_id": conv.claude_session_id,
            "updated_at": _now_iso(),
        }
        if not conv.meta_path.exists():
            payload["created_at"] = payload["updated_at"]

        conv.conversation_dir.mkdir(parents=True, exist_ok=True)

        tmp_path = conv.meta_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(conv.meta_path)

    async def log_cwd_event(
        self,
        *,
        cwd: str | None,
        event: str,
        payload: dict[str, Any],
        version: str | None = None,
        started_at: str | None = None,
    ) -> None:
        if cwd is None:
            return

        normalized_cwd = self._normalize_cwd(cwd)
        if not normalized_cwd:
            return

        digest = hashlib.sha1(normalized_cwd.encode("utf-8")).hexdigest()
        log_dir = self._store_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        path = log_dir / f"{digest}.log"

        record = {
            "timestamp": _now_iso(),
            "event": event,
            "cwd": normalized_cwd,
            "cwd_hash": digest,
            **payload,
        }
        if version:
            record["version"] = version
        if started_at:
            record["started_at"] = started_at

        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        await asyncio.to_thread(self._append_log_line, path, line)

    @staticmethod
    def _append_log_line(path: Path, line: str) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    async def _register_conversation_id(
        self,
        *,
        conversation_id: str,
        cwd: str | None,
        conversation_group: str | None = None,
    ) -> str | None:
        conversation_id = conversation_id.strip()
        if not conversation_id:
            return None

        normalized_group = self._normalize_conversation_group(conversation_group)
        if cwd is None or not str(cwd).strip():
            async with self._cwd_index_lock:
                self._alias_index[conversation_id] = conversation_id
            return conversation_id

        normalized_cwd = self._normalize_cwd(cwd)
        async with self._cwd_index_lock:
            canonical = self._cwd_index.get(normalized_cwd)
            if canonical is None:
                canonical = conversation_id
                self._cwd_index[normalized_cwd] = canonical

            if normalized_group:
                self._group_index[(normalized_cwd, normalized_group)] = canonical

            self._alias_index[conversation_id] = canonical
            self._alias_index[canonical] = canonical

        return canonical


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _format_sse(*, eid: int, json_line: str) -> bytes:
    return f"id: {eid}\ndata: {json_line}\n\n".encode("utf-8")
