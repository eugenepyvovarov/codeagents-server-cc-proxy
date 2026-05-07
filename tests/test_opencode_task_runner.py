from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

import claude_proxy.opencode_tasks as opencode_tasks
from claude_proxy.opencode_tasks import OpenCodeTaskRunner, summarize_open_code_messages
from claude_proxy.task_scheduler import TaskScheduler, TaskStore, parse_task_payload


class FakeOpenCodeClient:
    def __init__(self, *, statuses: list[dict[str, Any]] | None = None) -> None:
        self.created_sessions: list[dict[str, Any]] = []
        self.prompts: list[dict[str, Any]] = []
        self.statuses = statuses or [{"ses_fixture": {"type": "idle"}}]

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
        return {"id": "ses_fixture"}

    async def session_status(self, *, directory: str | None = None) -> dict[str, Any]:
        _ = directory
        if len(self.statuses) > 1:
            return self.statuses.pop(0)
        return self.statuses[0]

    async def prompt_async(self, *, session_id: str, prompt: str, directory: str | None = None) -> None:
        self.prompts.append(
            {
                "session_id": session_id,
                "prompt": prompt,
                "directory": directory,
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
        _ = limit
        return [
            {
                "info": {"role": "assistant"},
                "parts": [{"type": "text", "text": "scheduled task complete"}],
            }
        ]


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
            "session_id": "ses_fixture",
            "prompt": "scheduled ping",
            "directory": "/tmp/project",
        }
    ]
    assert (
        await store.get_opencode_session(
            agent_id="agent-1",
            conversation_id="task-conv",
            conversation_group="main",
            cwd="/tmp/project",
        )
        == "ses_fixture"
    )
    assert pushed == [
        {
            "cwd": "/tmp/project",
            "conversation_id": "ses_fixture",
            "message_preview": "scheduled task complete",
            "renderable_assistant_count": 1,
        }
    ]


@pytest.mark.asyncio
async def test_opencode_task_runner_defers_when_session_is_busy(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "tasks.db")
    client = FakeOpenCodeClient(statuses=[{"ses_existing": {"type": "busy"}}])
    runner = OpenCodeTaskRunner(client=client, store=store, poll_interval_seconds=0)

    started = await runner.start_run(
        conversation_id="ses_existing",
        prompt="scheduled ping",
        request_body={
            "agent_id": "agent-1",
            "cwd": "/tmp/project",
            "session_id": "ses_existing",
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
        session_id="ses_active",
    )
    client = FakeOpenCodeClient(statuses=[{"ses_active": {"type": "idle"}}])
    runner = OpenCodeTaskRunner(client=client, store=store, poll_interval_seconds=0)

    started = await runner.start_run(
        conversation_id="task-conv",
        prompt="scheduled ping",
        request_body={
            "agent_id": "agent-1",
            "cwd": "/tmp/project",
            "open_code_session_id": "ses_legacy",
        },
    )

    assert started is True
    assert client.created_sessions == []
    assert client.prompts == [
        {
            "session_id": "ses_active",
            "prompt": "scheduled ping",
            "directory": "/tmp/project",
        }
    ]


@pytest.mark.asyncio
async def test_clearing_active_session_strips_legacy_session_pins(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "tasks.db")
    record = parse_task_payload(
        {
            "agent_id": "agent-1",
            "conversation_id": "task-conv",
            "cwd": "/tmp/project",
            "prompt": "scheduled ping",
            "open_code_session_id": "ses_legacy",
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
        session_id="ses_legacy",
    )
    await store.save_active_opencode_session(
        agent_id="agent-1",
        conversation_group=None,
        cwd="/tmp/project",
        session_id="ses_legacy",
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
                "session_id": "ses_fixture",
                "prompt": "scheduled ping",
                "directory": "/tmp/project",
            }
        ]
    finally:
        await scheduler.shutdown()


def test_parse_task_payload_uses_opencode_session_id_when_conversation_id_is_absent() -> None:
    record = parse_task_payload(
        {
            "agent_id": "agent-1",
            "open_code_session_id": "ses_fixture",
            "cwd": "/tmp/project",
            "prompt": "scheduled ping",
            "enabled": True,
            "time_zone": "UTC",
            "schedule": {"frequency": "daily"},
        }
    )

    assert record.conversation_id == "ses_fixture"
    assert record.request_body["open_code_session_id"] == "ses_fixture"


def test_summarize_open_code_messages_counts_renderable_assistant_parts() -> None:
    preview, count = summarize_open_code_messages(
        [
            {
                "info": {"role": "assistant"},
                "parts": [
                    {"type": "reasoning", "text": "thinking"},
                    {"type": "text", "text": "final text"},
                    {"type": "tool", "text": "tool"},
                ],
            },
            {
                "info": {"role": "user"},
                "parts": [{"type": "text", "text": "ignored"}],
            },
        ]
    )

    assert preview == "final text"
    assert count == 3
