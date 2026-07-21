from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

import claude_proxy.opencode_tasks as opencode_tasks
from claude_proxy.opencode_tasks import OpenCodeTaskRunner, summarize_open_code_messages
from claude_proxy.task_scheduler import TaskScheduler, TaskStore, parse_task_payload


TASK_MESSAGE_ID = "msg_12345678123456781234567812345678"


@pytest.fixture(autouse=True)
def fixed_task_message_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(opencode_tasks, "_new_task_message_id", lambda: TASK_MESSAGE_ID)


class FakeOpenCodeClient:
    def __init__(
        self,
        *,
        statuses: list[dict[str, Any]] | None = None,
        prompt_errors: list[Exception] | None = None,
        message_errors: list[Exception] | None = None,
        created_session_id: str = "ses_fixture00001",
        message_batches: list[list[Any]] | None = None,
    ) -> None:
        self.created_sessions: list[dict[str, Any]] = []
        self.prompts: list[dict[str, Any]] = []
        self.created_session_id = created_session_id
        self.statuses = statuses or [{created_session_id: {"type": "idle"}}]
        self.prompt_errors = list(prompt_errors or [])
        self.message_errors = list(message_errors or [])
        self.calls: list[str] = []
        self.message_limits: list[int | None] = []
        completed_message = {
            "info": {
                "role": "assistant",
                "id": "msg_assistant_1",
                "parentID": TASK_MESSAGE_ID,
                "time": {"completed": 1},
            },
            "parts": [{"type": "text", "text": "scheduled task complete"}],
        }
        self.message_batches = message_batches or [[], [completed_message]]

    async def create_session(
        self,
        *,
        title: str | None = None,
        parent_id: str | None = None,
        directory: str | None = None,
    ) -> dict[str, Any]:
        self.created_sessions.append(
            {
                "title": title,
                "parent_id": parent_id,
                "directory": directory,
            }
        )
        return {"id": self.created_session_id}

    async def session_status(self, *, directory: str | None = None) -> dict[str, Any]:
        _ = directory
        self.calls.append("status")
        if len(self.statuses) > 1:
            return self.statuses.pop(0)
        return self.statuses[0]

    async def prompt_async(
        self,
        *,
        session_id: str,
        prompt: str,
        directory: str | None = None,
        message_id: str | None = None,
    ) -> None:
        self.calls.append("prompt")
        if self.prompt_errors:
            raise self.prompt_errors.pop(0)
        self.prompts.append(
            {
                "session_id": session_id,
                "prompt": prompt,
                "directory": directory,
                "message_id": message_id,
            }
        )

    async def session_messages(
        self,
        *,
        session_id: str,
        directory: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        _ = session_id
        _ = directory
        self.calls.append("messages")
        self.message_limits.append(limit)
        if self.message_errors:
            raise self.message_errors.pop(0)
        if len(self.message_batches) > 1:
            return self.message_batches.pop(0)
        return self.message_batches[0]


@pytest.mark.asyncio
async def test_opencode_task_runner_creates_session_runs_prompt_and_triggers_push(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pushed: list[dict[str, Any]] = []

    async def fake_push(**payload: Any) -> None:
        pushed.append(payload)

    monkeypatch.setattr(opencode_tasks, "trigger_reply_finished", fake_push)

    store = TaskStore(tmp_path / "tasks.db")
    client = FakeOpenCodeClient()
    runner = OpenCodeTaskRunner(client=client, store=store, poll_interval_seconds=0)

    started = await runner.start_run(
        conversation_id="task-conv",
        prompt=" scheduled ping ",
        request_body={
            "agent_id": "agent-1",
            "conversation_group": "main",
            "cwd": "/tmp/project",
        },
    )

    assert started is True
    assert client.created_sessions == [
        {
            "title": "Scheduled task: project (main)",
            "parent_id": None,
            "directory": "/tmp/project",
        }
    ]
    assert client.prompts == [
        {
            "session_id": "ses_fixture00001",
            "prompt": "scheduled ping",
            "directory": "/tmp/project",
            "message_id": TASK_MESSAGE_ID,
        }
    ]
    assert (
        await store.get_opencode_session(
            agent_id="agent-1",
            conversation_id="task-conv",
            conversation_group="main",
            cwd="/tmp/project",
        )
        == "ses_fixture00001"
    )
    assert pushed == [
        {
            "cwd": "/tmp/project",
            "conversation_id": "ses_fixture00001",
            "message_preview": "scheduled task complete",
            "renderable_assistant_count": 1,
            "assistant_message_cursor": 1,
        }
    ]
    assert client.message_limits == [100, 100, 100, None]


@pytest.mark.asyncio
async def test_opencode_task_runner_does_not_accept_pre_start_idle_as_completion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pushed: list[dict[str, Any]] = []

    async def fake_push(**payload: Any) -> None:
        pushed.append(payload)

    monkeypatch.setattr(opencode_tasks, "trigger_reply_finished", fake_push)
    old = {
        "info": {
            "role": "assistant",
            "id": "msg_old",
            "parentID": "msg_old_user",
            "time": {"completed": 1},
        },
        "parts": [{"type": "text", "text": "old reply"}],
    }
    unrelated = {
        "info": {
            "role": "assistant",
            "id": "msg_unrelated",
            "parentID": "msg_other_user",
            "time": {"completed": 2},
        },
        "parts": [{"type": "text", "text": "unrelated reply"}],
    }
    new = {
        "info": {
            "role": "assistant",
            "id": "msg_new",
            "parentID": TASK_MESSAGE_ID,
            "time": {"completed": 3},
        },
        "parts": [{"type": "text", "text": "new reply"}],
    }
    client = FakeOpenCodeClient(
        statuses=[
            {"ses_fixture00001": {"type": "idle"}},
            {"ses_fixture00001": {"type": "idle"}},
            {"ses_fixture00001": {"type": "busy"}},
            {"ses_fixture00001": {"type": "idle"}},
        ],
        message_batches=[[old], [old], [old, unrelated], [old, unrelated, new]],
    )
    runner = OpenCodeTaskRunner(
        client=client,
        store=TaskStore(tmp_path / "tasks.db"),
        poll_interval_seconds=0,
    )

    started = await runner.start_run(
        conversation_id="task-conv",
        prompt="scheduled ping",
        request_body={"agent_id": "agent-1", "cwd": "/tmp/project"},
    )

    assert started is True
    assert pushed[0]["message_preview"] == "new reply"
    assert pushed[0]["renderable_assistant_count"] == 3
    assert pushed[0]["assistant_message_cursor"] == 3
    assert client.calls == [
        "status",
        "messages",
        "prompt",
        "messages",
        "status",
        "messages",
        "status",
        "messages",
        "status",
        "messages",
        "messages",
    ]


@pytest.mark.asyncio
async def test_opencode_task_runner_waits_for_correlated_reply_to_be_finalized(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pushed: list[dict[str, Any]] = []

    async def fake_push(**payload: Any) -> None:
        pushed.append(payload)

    monkeypatch.setattr(opencode_tasks, "trigger_reply_finished", fake_push)
    incomplete = {
        "info": {
            "role": "assistant",
            "id": "msg_reply",
            "parentID": TASK_MESSAGE_ID,
            "time": {"created": 1},
        },
        "parts": [{"type": "text", "text": "partial reply"}],
    }
    completed = {
        "info": {
            "role": "assistant",
            "id": "msg_reply",
            "parentID": TASK_MESSAGE_ID,
            "time": {"created": 1, "completed": 2},
        },
        "parts": [{"type": "text", "text": "final reply"}],
    }
    client = FakeOpenCodeClient(
        statuses=[
            {"ses_fixture00001": {"type": "idle"}},
            {"ses_fixture00001": {"type": "idle"}},
            {"ses_fixture00001": {"type": "idle"}},
        ],
        message_batches=[[], [incomplete], [completed]],
    )
    runner = OpenCodeTaskRunner(
        client=client,
        store=TaskStore(tmp_path / "tasks.db"),
        poll_interval_seconds=0,
    )

    started = await runner.start_run(
        conversation_id="task-conv",
        prompt="scheduled ping",
        request_body={"agent_id": "agent-1", "cwd": "/tmp/project"},
    )

    assert started is True
    assert pushed[0]["message_preview"] == "final reply"
    assert pushed[0]["assistant_message_cursor"] == 1
    assert client.calls == [
        "status",
        "messages",
        "prompt",
        "messages",
        "status",
        "messages",
        "status",
        "messages",
        "messages",
    ]


@pytest.mark.asyncio
async def test_opencode_task_runner_fails_immediately_for_correlated_assistant_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pushed: list[dict[str, Any]] = []

    async def fake_push(**payload: Any) -> None:
        pushed.append(payload)

    monkeypatch.setattr(opencode_tasks, "trigger_reply_finished", fake_push)
    failed = {
        "info": {
            "role": "assistant",
            "id": "msg_failed_reply",
            "parentID": TASK_MESSAGE_ID,
            "error": {"name": "ProviderError", "message": "request failed"},
        },
        "parts": [],
    }
    client = FakeOpenCodeClient(
        statuses=[{"ses_fixture00001": {"type": "idle"}}],
        message_batches=[[], [failed]],
    )
    runner = OpenCodeTaskRunner(
        client=client,
        store=TaskStore(tmp_path / "tasks.db"),
        poll_interval_seconds=0,
    )

    with pytest.raises(RuntimeError, match="assistant reply.*failed"):
        await runner.start_run(
            conversation_id="task-conv",
            prompt="scheduled ping",
            request_body={"agent_id": "agent-1", "cwd": "/tmp/project"},
        )

    assert pushed == []
    assert client.calls == ["status", "messages", "prompt", "messages"]
    assert await runner.has_active_runs() is False


@pytest.mark.asyncio
async def test_opencode_task_runner_refreshes_bounded_snapshot_after_idle_before_full_v2(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pushed: list[dict[str, Any]] = []

    async def fake_push(**payload: Any) -> None:
        pushed.append(payload)

    monkeypatch.setattr(opencode_tasks, "trigger_reply_finished", fake_push)
    old = {
        "info": {
            "role": "assistant",
            "id": "msg_old",
            "parentID": "msg_old_user",
            "time": {"completed": 1},
        },
        "parts": [{"type": "text", "text": "old reply"}],
    }
    pre_idle = {
        "info": {
            "role": "assistant",
            "id": "msg_recent",
            "parentID": TASK_MESSAGE_ID,
            "time": {"completed": 2},
        },
        "parts": [{"type": "text", "text": "pre-idle reply"}],
    }
    post_idle = {
        "info": {
            "role": "assistant",
            "id": "msg_recent",
            "parentID": TASK_MESSAGE_ID,
            "time": {"completed": 2},
        },
        "parts": [
            {"type": "text", "text": "final reply"},
            {"type": "text", "text": "post-idle detail"},
        ],
    }
    client = FakeOpenCodeClient(
        statuses=[
            {"ses_fixture00001": {"type": "idle"}},
            {"ses_fixture00001": {"type": "idle"}},
        ],
        message_batches=[[], [pre_idle], [post_idle], [old, post_idle]],
    )
    runner = OpenCodeTaskRunner(
        client=client,
        store=TaskStore(tmp_path / "tasks.db"),
        poll_interval_seconds=0,
    )

    started = await runner.start_run(
        conversation_id="task-conv",
        prompt="scheduled ping",
        request_body={"agent_id": "agent-1", "cwd": "/tmp/project"},
    )

    assert started is True
    assert pushed == [
        {
            "cwd": "/tmp/project",
            "conversation_id": "ses_fixture00001",
            "message_preview": "final reply\npost-idle detail",
            "renderable_assistant_count": 2,
            "assistant_message_cursor": 2,
        }
    ]
    assert client.calls == [
        "status",
        "messages",
        "prompt",
        "messages",
        "status",
        "messages",
        "messages",
    ]
    assert client.message_limits == [100, 100, 100, None]


@pytest.mark.asyncio
async def test_opencode_task_runner_defers_when_session_is_busy(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "tasks.db")
    client = FakeOpenCodeClient(statuses=[{"ses_existing0001": {"type": "busy"}}])
    runner = OpenCodeTaskRunner(client=client, store=store, poll_interval_seconds=0)

    started = await runner.start_run(
        conversation_id="ses_existing0001",
        prompt="scheduled ping",
        request_body={
            "agent_id": "agent-1",
            "cwd": "/tmp/project",
            "session_id": "ses_existing0001",
        },
    )

    assert started is False
    assert client.prompts == []
    assert await runner.has_active_runs() is False


@pytest.mark.asyncio
async def test_opencode_task_runner_prefers_active_session_over_legacy_explicit_session(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "tasks.db")
    await store.save_active_opencode_session(
        agent_id="agent-1",
        conversation_group=None,
        cwd="/tmp/project",
        session_id="ses_active000001",
    )
    client = FakeOpenCodeClient(statuses=[{"ses_active000001": {"type": "idle"}}])
    runner = OpenCodeTaskRunner(client=client, store=store, poll_interval_seconds=0)

    started = await runner.start_run(
        conversation_id="task-conv",
        prompt="scheduled ping",
        request_body={
            "agent_id": "agent-1",
            "cwd": "/tmp/project",
            "open_code_session_id": "ses_legacy000001",
        },
    )

    assert started is True
    assert client.created_sessions == []
    assert client.prompts == [
        {
            "session_id": "ses_active000001",
            "prompt": "scheduled ping",
            "directory": "/tmp/project",
            "message_id": TASK_MESSAGE_ID,
        }
    ]


@pytest.mark.asyncio
async def test_opencode_task_runner_matches_active_session_despite_agent_id_case(
    tmp_path: Path,
) -> None:
    """iOS may pin active chat with uppercase UUID while tasks store lowercase."""
    store = TaskStore(tmp_path / "tasks.db")
    # Simulate legacy mixed-case pin written before normalize_agent_id.
    import hashlib
    import json
    import sqlite3

    identity = json.dumps(
        {
            "agent_id": "A027C2D3-79AA-416D-8349-7DDFEE4E9A46",
            "conversation_group": "",
            "cwd": "/home/codeagent/projects/X",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    session_key = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    with sqlite3.connect(tmp_path / "tasks.db") as conn:
        conn.execute(
            """
            INSERT INTO opencode_active_sessions (
                session_key, agent_id, conversation_group, cwd, session_id, created_at, updated_at
            ) VALUES (?, ?, NULL, ?, ?, ?, ?)
            """,
            (
                session_key,
                "A027C2D3-79AA-416D-8349-7DDFEE4E9A46",
                "/home/codeagent/projects/X",
                "ses_realchat00001",
                "2026-07-09T07:00:00Z",
                "2026-07-09T07:18:00Z",
            ),
        )
        # Newer side-session pin under lowercase must not win over older real chat.
        identity_lower = json.dumps(
            {
                "agent_id": "a027c2d3-79aa-416d-8349-7ddfee4e9a46",
                "conversation_group": "",
                "cwd": "/home/codeagent/projects/X",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        session_key_lower = hashlib.sha256(identity_lower.encode("utf-8")).hexdigest()
        conn.execute(
            """
            INSERT INTO opencode_active_sessions (
                session_key, agent_id, conversation_group, cwd, session_id, created_at, updated_at
            ) VALUES (?, ?, NULL, ?, ?, ?, ?)
            """,
            (
                session_key_lower,
                "a027c2d3-79aa-416d-8349-7ddfee4e9a46",
                "/home/codeagent/projects/X",
                "ses_sidesched0001",
                "2026-07-09T07:30:00Z",
                "2026-07-09T07:30:00Z",
            ),
        )

    client = FakeOpenCodeClient(statuses=[{"ses_realchat00001": {"type": "idle"}}])
    runner = OpenCodeTaskRunner(client=client, store=store, poll_interval_seconds=0)

    started = await runner.start_run(
        conversation_id="scheduler-a027c2d3-79aa-416d-8349-7ddfee4e9a46",
        prompt="Ask me how I am doing today.",
        request_body={
            "agent_id": "a027c2d3-79aa-416d-8349-7ddfee4e9a46",
            "cwd": "/home/codeagent/projects/X",
        },
    )

    assert started is True
    assert client.created_sessions == []
    assert client.prompts == [
        {
            "session_id": "ses_realchat00001",
            "prompt": "Ask me how I am doing today.",
            "directory": "/home/codeagent/projects/X",
            "message_id": TASK_MESSAGE_ID,
        }
    ]


@pytest.mark.asyncio
async def test_opencode_task_runner_does_not_pin_created_side_session_as_active(
    tmp_path: Path,
) -> None:
    store = TaskStore(tmp_path / "tasks.db")
    client = FakeOpenCodeClient()
    runner = OpenCodeTaskRunner(client=client, store=store, poll_interval_seconds=0)

    started = await runner.start_run(
        conversation_id="scheduler-conv",
        prompt="scheduled ping",
        request_body={
            "agent_id": "agent-1",
            "cwd": "/tmp/project",
        },
    )

    assert started is True
    assert await store.get_active_opencode_session(
        agent_id="agent-1",
        conversation_group=None,
        cwd="/tmp/project",
    ) is None
    assert (
        await store.get_opencode_session(
            agent_id="agent-1",
            conversation_id="scheduler-conv",
            conversation_group=None,
            cwd="/tmp/project",
        )
        == "ses_fixture00001"
    )


@pytest.mark.asyncio
async def test_opencode_task_runner_skips_invalid_active_pin_and_creates_session(
    tmp_path: Path,
) -> None:
    store = TaskStore(tmp_path / "tasks.db")
    await store.save_active_opencode_session(
        agent_id="agent-1",
        conversation_group=None,
        cwd="/tmp/project",
        session_id="ses_diag",
    )
    client = FakeOpenCodeClient(created_session_id="ses_healed0000001")
    runner = OpenCodeTaskRunner(client=client, store=store, poll_interval_seconds=0)

    started = await runner.start_run(
        conversation_id="task-conv",
        prompt="scheduled ping",
        request_body={
            "agent_id": "agent-1",
            "cwd": "/tmp/project",
        },
    )

    assert started is True
    assert client.created_sessions
    assert client.prompts == [
        {
            "session_id": "ses_healed0000001",
            "prompt": "scheduled ping",
            "directory": "/tmp/project",
            "message_id": TASK_MESSAGE_ID,
        }
    ]
    assert (
        await store.get_active_opencode_session(
            agent_id="agent-1",
            conversation_group=None,
            cwd="/tmp/project",
        )
        is None
    )


@pytest.mark.asyncio
async def test_opencode_task_runner_recovers_from_missing_session_404(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import httpx

    async def fake_push(**payload: Any) -> None:
        _ = payload

    monkeypatch.setattr(opencode_tasks, "trigger_reply_finished", fake_push)

    store = TaskStore(tmp_path / "tasks.db")
    await store.save_active_opencode_session(
        agent_id="agent-1",
        conversation_group=None,
        cwd="/tmp/project",
        session_id="ses_dead00000001",
    )
    request = httpx.Request("POST", "http://127.0.0.1:4096/session/ses_dead00000001/prompt_async")
    response = httpx.Response(404, request=request)
    client = FakeOpenCodeClient(
        statuses=[
            {"ses_dead00000001": {"type": "idle"}},
            {"ses_healed0000001": {"type": "idle"}},
        ],
        prompt_errors=[httpx.HTTPStatusError("missing", request=request, response=response)],
        created_session_id="ses_healed0000001",
        message_batches=[
            [],
            [],
            [
                {
                    "info": {
                        "role": "assistant",
                        "id": "msg_assistant_1",
                        "parentID": TASK_MESSAGE_ID,
                        "time": {"completed": 1},
                    },
                    "parts": [{"type": "text", "text": "scheduled task complete"}],
                }
            ],
        ],
    )
    runner = OpenCodeTaskRunner(client=client, store=store, poll_interval_seconds=0)

    started = await runner.start_run(
        conversation_id="task-conv",
        prompt="scheduled ping",
        request_body={
            "agent_id": "agent-1",
            "cwd": "/tmp/project",
        },
    )

    assert started is True
    assert client.created_sessions
    assert client.prompts == [
        {
            "session_id": "ses_healed0000001",
            "prompt": "scheduled ping",
            "directory": "/tmp/project",
            "message_id": TASK_MESSAGE_ID,
        }
    ]
    assert (
        await store.get_active_opencode_session(
            agent_id="agent-1",
            conversation_group=None,
            cwd="/tmp/project",
        )
        == "ses_healed0000001"
    )


@pytest.mark.asyncio
async def test_opencode_task_runner_recovers_when_baseline_fetch_returns_404(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import httpx

    async def fake_push(**payload: Any) -> None:
        _ = payload

    monkeypatch.setattr(opencode_tasks, "trigger_reply_finished", fake_push)

    store = TaskStore(tmp_path / "tasks.db")
    await store.save_active_opencode_session(
        agent_id="agent-1",
        conversation_group=None,
        cwd="/tmp/project",
        session_id="ses_dead00000001",
    )
    request = httpx.Request("GET", "http://127.0.0.1:4096/session/ses_dead00000001/message")
    response = httpx.Response(404, request=request)
    client = FakeOpenCodeClient(
        statuses=[
            {"ses_dead00000001": {"type": "idle"}},
            {"ses_healed0000001": {"type": "idle"}},
        ],
        message_errors=[httpx.HTTPStatusError("missing", request=request, response=response)],
        created_session_id="ses_healed0000001",
    )
    runner = OpenCodeTaskRunner(client=client, store=store, poll_interval_seconds=0)

    started = await runner.start_run(
        conversation_id="task-conv",
        prompt="scheduled ping",
        request_body={"agent_id": "agent-1", "cwd": "/tmp/project"},
    )

    assert started is True
    assert client.created_sessions
    assert client.prompts == [
        {
            "session_id": "ses_healed0000001",
            "prompt": "scheduled ping",
            "directory": "/tmp/project",
            "message_id": TASK_MESSAGE_ID,
        }
    ]
    assert (
        await store.get_active_opencode_session(
            agent_id="agent-1",
            conversation_group=None,
            cwd="/tmp/project",
        )
        == "ses_healed0000001"
    )


@pytest.mark.asyncio
async def test_clearing_active_session_strips_legacy_session_pins(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "tasks.db")
    record = parse_task_payload(
        {
            "agent_id": "agent-1",
            "conversation_id": "task-conv",
            "cwd": "/tmp/project",
            "prompt": "scheduled ping",
            "open_code_session_id": "ses_legacy000001",
            "enabled": True,
            "time_zone": "UTC",
            "schedule": {"frequency": "daily"},
        }
    )
    await store.create_task(record)
    await store.save_opencode_session(
        agent_id="agent-1",
        conversation_id="task-conv",
        conversation_group=None,
        cwd="/tmp/project",
        session_id="ses_legacy000001",
    )
    await store.save_active_opencode_session(
        agent_id="agent-1",
        conversation_group=None,
        cwd="/tmp/project",
        session_id="ses_legacy000001",
    )

    await store.clear_active_opencode_session(
        agent_id="agent-1",
        conversation_group=None,
        cwd="/tmp/project",
    )

    updated = await store.get_task(record.id)
    assert updated is not None
    assert "open_code_session_id" not in updated.request_body
    assert (
        await store.get_opencode_session(
            agent_id="agent-1",
            conversation_id="task-conv",
            conversation_group=None,
            cwd="/tmp/project",
        )
        is None
    )


@pytest.mark.asyncio
async def test_task_scheduler_runs_scheduled_task_through_opencode_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def fake_push(**payload: Any) -> None:
        _ = payload

    monkeypatch.setattr(opencode_tasks, "trigger_reply_finished", fake_push)

    store = TaskStore(tmp_path / "tasks.db")
    client = FakeOpenCodeClient()
    runner = OpenCodeTaskRunner(client=client, store=store, poll_interval_seconds=0)
    scheduler = TaskScheduler(store=store, task_runner=runner)
    await scheduler.start()
    try:
        record = parse_task_payload(
            {
                "agent_id": "agent-1",
                "conversation_id": "task-conv",
                "conversation_group": "main",
                "cwd": "/tmp/project",
                "prompt": "scheduled ping",
                "enabled": True,
                "time_zone": "UTC",
                "schedule": {"frequency": "daily", "interval": 1, "time_minutes": 0},
            }
        )
        stored = await scheduler.create_task(record)

        await scheduler._run_task_job(stored.id, scheduled_at=datetime.now(timezone.utc))

        updated = await store.get_task(stored.id)
        assert updated is not None
        assert updated.last_run_at is not None
        assert client.prompts == [
            {
                "session_id": "ses_fixture00001",
                "prompt": "scheduled ping",
                "directory": "/tmp/project",
                "message_id": TASK_MESSAGE_ID,
            }
        ]
    finally:
        await scheduler.shutdown()


def test_parse_task_payload_uses_opencode_session_id_when_conversation_id_is_absent() -> None:
    record = parse_task_payload(
        {
            "agent_id": "agent-1",
            "open_code_session_id": "ses_fixture00001",
            "cwd": "/tmp/project",
            "prompt": "scheduled ping",
            "enabled": True,
            "time_zone": "UTC",
            "schedule": {"frequency": "daily"},
        }
    )

    assert record.conversation_id == "ses_fixture00001"
    assert record.request_body["open_code_session_id"] == "ses_fixture00001"


def test_summarize_open_code_messages_counts_unique_renderable_assistant_messages() -> None:
    preview, legacy_count, assistant_message_cursor = summarize_open_code_messages(
        [
            {
                "info": {
                    "role": "assistant",
                    "id": "msg_assistant_1",
                    "parentID": TASK_MESSAGE_ID,
                    "time": {"completed": 1},
                },
                "parts": [
                    {"type": "reasoning", "text": "thinking"},
                    {"type": "text", "text": "final text"},
                    {"type": "tool", "text": "tool"},
                ],
            },
            {
                "info": {"role": "user", "id": "msg_user_1"},
                "parts": [{"type": "text", "text": "ignored"}],
            },
            {
                "info": {
                    "role": "assistant",
                    "id": "msg_tool_only",
                    "time": {"completed": 2},
                },
                "parts": [{"type": "tool", "text": "not rendered by iOS hydration"}],
            },
            {
                "info": {
                    "role": "assistant",
                    "id": "msg_synthetic",
                    "time": {"completed": 3},
                },
                "parts": [
                    {
                        "type": "text",
                        "synthetic": True,
                        "text": "Called the Read tool with the following input: {}",
                    }
                ],
            },
            {
                "info": {"role": "assistant", "id": "msg_incomplete"},
                "parts": [{"type": "text", "text": "still streaming"}],
            },
        ],
        parent_message_id=TASK_MESSAGE_ID,
    )

    assert preview == "final text"
    assert legacy_count == 6
    assert assistant_message_cursor == 1


def test_summarize_open_code_messages_discards_entire_unflagged_tool_narration_part() -> None:
    preview, legacy_count, assistant_message_cursor = summarize_open_code_messages(
        [
            {
                "info": {
                    "role": "assistant",
                    "id": "msg_synthetic_narration",
                    "parentID": TASK_MESSAGE_ID,
                    "time": {"completed": 1},
                },
                "parts": [
                    {
                        "type": "text",
                        "text": (
                            "Called the Bash tool with the following input:\n"
                            "{\n"
                            '  "token": "must-not-leak",\n'
                            '  "nested": {"value": 1}\n'
                            "}\n"
                            "This trailing line is part of the synthetic narration."
                        ),
                    }
                ],
            }
        ],
        parent_message_id=TASK_MESSAGE_ID,
    )

    assert preview is None
    # The legacy cursor deliberately preserves its historical part-count unit.
    assert legacy_count == 1
    assert assistant_message_cursor == 0
