import asyncio
import json

import pytest
from claude_agent_sdk.types import (
    AssistantMessage,
    ResultMessage,
    SystemMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
import httpx

from app import create_app
from claude_proxy.conversation_manager import AgentFolderBusyError, ConversationManager


async def _fake_backend(*, prompt: str, options):
    _ = options
    yield SystemMessage(
        subtype="init",
        data={
            "type": "system",
            "subtype": "init",
            "session_id": "upstream-session",
            "tools": ["Bash"],
            "cwd": "/tmp",
        },
    )
    await asyncio.sleep(0.05)
    yield AssistantMessage(content=[TextBlock(text=f"Echo: {prompt}")], model="claude-test")
    await asyncio.sleep(0.05)
    yield AssistantMessage(
        content=[ToolUseBlock(id="toolu_1", name="bash", input={"cmd": "echo hi"})],
        model="claude-test",
    )
    await asyncio.sleep(0.05)
    yield UserMessage(content=[ToolResultBlock(tool_use_id="toolu_1", content="hi", is_error=False)])
    await asyncio.sleep(0.05)
    yield AssistantMessage(content=[TextBlock(text="Done")], model="claude-test")
    await asyncio.sleep(0.05)
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


async def _slow_backend(*, prompt: str, options):
    _ = prompt
    _ = options
    yield SystemMessage(
        subtype="init",
        data={
            "type": "system",
            "subtype": "init",
            "session_id": "upstream-session",
            "tools": ["Bash"],
            "cwd": "/tmp",
        },
    )
    await asyncio.sleep(0.5)
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


async def _collect_sse_events(resp: httpx.Response, *, limit: int | None = None):
    events = []
    current_id = None
    current_data = None

    async for line in resp.aiter_lines():
        if line.startswith(":"):
            continue
        if line.startswith("id:"):
            current_id = int(line[len("id:") :].strip())
            continue
        if line.startswith("data:"):
            current_data = json.loads(line[len("data:") :].strip())
            continue
        if line.strip() == "":
            if current_id is not None and current_data is not None:
                events.append((current_id, current_data))
                if limit is not None and len(events) >= limit:
                    return events
            current_id = None
            current_data = None

    return events


@pytest.mark.asyncio
async def test_disconnect_then_replay(tmp_path):
    app = create_app(store_dir=tmp_path, backend=_fake_backend)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        received = []
        async with client.stream(
            "POST",
            "/v1/agent/stream",
            json={"text": "hello", "conversation_id": "c1", "cwd": "/tmp"},
        ) as resp:
            assert resp.status_code == 200
            received = await _collect_sse_events(resp, limit=2)

        assert received
        first_eid = received[-1][0]
        session_payload = next(
            (payload for _, payload in received if payload.get("session_id") == "upstream-session"),
            None,
        )
        assert session_payload is not None

        # Ensure the background run continues after the client disconnects.
        await asyncio.sleep(0.3)

        replay = await client.get("/v1/conversations/c1/events", params={"since": first_eid})
        assert replay.status_code == 200

        replay_events = [json.loads(line) for line in replay.text.splitlines() if line.strip()]
        assert replay_events, "Expected replay events after disconnect"
        assert all(e.get("session_id") in (None, "upstream-session") for e in replay_events)
        assert any(e.get("type") == "result" for e in replay_events)


@pytest.mark.asyncio
async def test_agent_folder_lock(tmp_path):
    manager = ConversationManager(store_dir=tmp_path, backend=_slow_backend)
    await manager.start_run(conversation_id="c1", prompt="hello", request_body={"cwd": "/tmp"})
    with pytest.raises(AgentFolderBusyError):
        await manager.start_run(conversation_id="c2", prompt="hello2", request_body={"cwd": "/tmp"})
    conv = await manager.get_or_create_conversation("c1")
    if conv.runner_task:
        await conv.runner_task


@pytest.mark.asyncio
async def test_auto_resume_uses_stored_session_id(tmp_path):
    seen_resume = []

    async def _backend(*, prompt: str, options):
        _ = prompt
        seen_resume.append(getattr(options, "resume", None))
        yield SystemMessage(
            subtype="init",
            data={
                "type": "system",
                "subtype": "init",
                "session_id": "upstream-session",
                "tools": ["Bash"],
                "cwd": "/tmp",
            },
        )
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

    app = create_app(store_dir=tmp_path, backend=_backend)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
            "POST",
            "/v1/agent/stream",
            json={"conversation_id": "c1", "cwd": "/tmp", "text": "first"},
        ) as resp:
            assert resp.status_code == 200
            _ = await _collect_sse_events(resp)

        async with client.stream(
            "POST",
            "/v1/agent/stream",
            json={"conversation_id": "c1", "cwd": "/tmp", "text": "second"},
        ) as resp:
            assert resp.status_code == 200
            _ = await _collect_sse_events(resp)

    assert seen_resume[0] in (None, "")
    assert seen_resume[1] == "upstream-session"
