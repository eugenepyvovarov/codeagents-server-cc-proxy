from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import deque
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from claude_agent_sdk import ClaudeAgentOptions

from claude_proxy.serialization import serialize_message

logger = logging.getLogger(__name__)


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


@dataclass
class _Conversation:
    conversation_id: str
    conversation_dir: Path
    meta_path: Path
    events_path: Path

    created_at: float = field(default_factory=time.time)
    prompt: str | None = None
    cwd: str | None = None
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

    def new_conversation_id(self) -> str:
        return uuid.uuid4().hex

    def _dir_for(self, conversation_id: str) -> Path:
        return self._conversations_dir / conversation_id

    def _paths_for(self, conversation_id: str) -> tuple[Path, Path, Path]:
        conversation_dir = self._dir_for(conversation_id)
        return conversation_dir, conversation_dir / "meta.json", conversation_dir / "events.ndjson"

    async def conversation_exists(self, conversation_id: str) -> bool:
        async with self._conversations_lock:
            if conversation_id in self._conversations:
                return True
        conversation_dir = self._dir_for(conversation_id)
        return conversation_dir.exists()

    async def get_or_create_conversation(self, conversation_id: str) -> _Conversation:
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
            return conv

    async def ensure_cwd_binding(self, *, conversation_id: str, cwd: str) -> None:
        cwd = cwd.strip()
        if not cwd:
            raise ValueError("cwd must be a non-empty string.")

        conv = await self.get_or_create_conversation(conversation_id)
        async with conv.lock:
            if conv.cwd is None:
                conv.cwd = cwd
                await asyncio.to_thread(self._write_meta, conv)
            elif conv.cwd != cwd:
                raise ConversationCwdMismatchError(
                    conversation_id=conversation_id,
                    expected_cwd=conv.cwd,
                    got_cwd=cwd,
                )

    async def start_run(self, *, conversation_id: str, prompt: str, request_body: dict[str, Any]) -> None:
        prompt = prompt.strip()
        if not prompt:
            raise ValueError("text required to start a new run.")

        conv = await self.get_or_create_conversation(conversation_id)

        cwd_value = request_body.get("cwd")
        if isinstance(cwd_value, str):
            cwd = cwd_value
        else:
            cwd = conv.cwd or ""
        await self.ensure_cwd_binding(conversation_id=conversation_id, cwd=cwd)

        async with conv.lock:
            if conv.is_running:
                return

            if conv.cwd is None:
                raise ValueError("cwd is required to start a new run.")

            async with self._cwd_lock:
                active = self._active_run_by_cwd.get(conv.cwd)
                if active is not None and active != conversation_id:
                    raise AgentFolderBusyError(cwd=conv.cwd)
                self._active_run_by_cwd[conv.cwd] = conversation_id

            conv.prompt = prompt
            conv.is_running = True
            conv.is_done = False

            options = self._build_options(request_body)
            if getattr(options, "cwd", None) in (None, "") and conv.cwd:
                options.cwd = conv.cwd
            if request_body.get("resume") is None and conv.claude_session_id:
                options.resume = conv.claude_session_id

            try:
                conv.runner_task = asyncio.create_task(self._run_agent(conv=conv, prompt=prompt, options=options))
            except Exception:
                async with self._cwd_lock:
                    if self._active_run_by_cwd.get(conv.cwd) == conversation_id:
                        self._active_run_by_cwd.pop(conv.cwd, None)
                raise

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
            conv.cwd = cwd

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

    async def _run_agent(self, *, conv: _Conversation, prompt: str, options: ClaudeAgentOptions) -> None:
        saw_result = False

        try:
            async for message in self._backend(prompt=prompt, options=options):
                payload = serialize_message(message)
                if not payload:
                    continue

                if payload.get("type") == "stream_event":
                    continue

                if payload.get("type") == "result":
                    saw_result = True

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

            cwd = conv.cwd
            if cwd:
                async with self._cwd_lock:
                    if self._active_run_by_cwd.get(cwd) == conv.conversation_id:
                        self._active_run_by_cwd.pop(cwd, None)

    def _build_options(self, body: dict[str, Any]) -> ClaudeAgentOptions:
        kwargs: dict[str, Any] = {}

        if isinstance(body.get("cwd"), str):
            kwargs["cwd"] = body["cwd"]
        if isinstance(body.get("allowed_tools"), list):
            kwargs["allowed_tools"] = body["allowed_tools"]
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
            "claude_session_id": conv.claude_session_id,
            "updated_at": _now_iso(),
        }
        if not conv.meta_path.exists():
            payload["created_at"] = payload["updated_at"]

        conv.conversation_dir.mkdir(parents=True, exist_ok=True)

        tmp_path = conv.meta_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(conv.meta_path)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _format_sse(*, eid: int, json_line: str) -> bytes:
    return f"id: {eid}\ndata: {json_line}\n\n".encode("utf-8")
