import asyncio
from datetime import datetime, timezone

import pytest
from claude_agent_sdk.types import AssistantMessage, ResultMessage, SystemMessage, TextBlock

from claude_proxy.conversation_manager import ConversationManager
from claude_proxy.task_scheduler import TaskScheduler, TaskStore, parse_task_payload


async def _scheduler_backend(*, prompt: str, options):
    cwd = getattr(options, "cwd", None) or "/tmp/project"
    yield SystemMessage(
        subtype="init",
        data={
            "type": "system",
            "subtype": "init",
            "session_id": "upstream-session",
            "tools": ["Bash"],
            "cwd": cwd,
        },
    )
    await asyncio.sleep(0.01)
    yield AssistantMessage(content=[TextBlock(text=f"Echo: {prompt}")], model="claude-test")
    await asyncio.sleep(0.01)
    yield ResultMessage(
        subtype="success",
        duration_ms=10,
        duration_api_ms=10,
        is_error=False,
        num_turns=1,
        session_id="upstream-session",
        total_cost_usd=0.01,
        usage={"input_tokens": 1, "output_tokens": 1},
        result="Done",
    )


@pytest.mark.asyncio
async def test_scheduled_task_aliases_to_canonical(tmp_path):
    manager = ConversationManager(store_dir=tmp_path, backend=_scheduler_backend)
    store = TaskStore(tmp_path / "tasks.db")
    scheduler = TaskScheduler(store=store, manager=manager)
    await scheduler.start()
    try:
        canonical_id = "canonical-conv"
        await manager.resolve_conversation_id(
            conversation_id=canonical_id,
            cwd="/tmp/project",
            conversation_group="g1",
        )

        payload = {
            "agent_id": "agent-123",
            "conversation_id": "task-conv",
            "conversation_group": "g1",
            "cwd": "/tmp/project",
            "prompt": "scheduled ping",
            "enabled": True,
            "time_zone": "UTC",
            "schedule": {"frequency": "daily", "interval": 1, "time_minutes": 0},
        }
        record = parse_task_payload(payload)
        stored = await scheduler.create_task(record)

        await scheduler._run_task_job(stored.id, scheduled_at=datetime.now(timezone.utc))

        output_lines = []
        for _ in range(20):
            output_lines = [
                line async for line in manager.iter_ndjson(conversation_id=canonical_id, since=0)
            ]
            if any("\"type\":\"result\"" in line for line in output_lines):
                break
            await asyncio.sleep(0.05)

        output = "".join(output_lines)
        assert "Echo: scheduled ping" in output
        assert not (tmp_path / "conversations" / stored.conversation_id).exists()
    finally:
        await scheduler.shutdown()
