import asyncio

import httpx
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

from app import create_app


async def _renderable_count_backend(*, prompt: str, options):
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
    yield AssistantMessage(
        content=[TextBlock(text="First"), TextBlock(text="Second")],
        model="claude-test",
    )
    yield AssistantMessage(content=[TextBlock(text="   ")], model="claude-test")
    yield AssistantMessage(
        content=[ToolUseBlock(id="toolu_1", name="bash", input={"cmd": "echo hi"})],
        model="claude-test",
    )
    yield UserMessage(content=[ToolResultBlock(tool_use_id="toolu_1", content="hi", is_error=False)])
    yield AssistantMessage(content=[TextBlock(text="Done")], model="claude-test")
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


async def _whitespace_only_backend(*, prompt: str, options):
    _ = prompt
    _ = options
    yield SystemMessage(
        subtype="init",
        data={
            "type": "system",
            "subtype": "init",
            "session_id": "upstream-session",
            "tools": [],
            "cwd": "/tmp",
        },
    )
    yield AssistantMessage(content=[TextBlock(text="  \n  ")], model="claude-test")
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


async def _drain_stream(resp: httpx.Response) -> None:
    async for _ in resp.aiter_lines():
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_replay_header_exposes_renderable_bubble_count(tmp_path):
    app = create_app(store_dir=tmp_path, backend=_renderable_count_backend)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
            "POST",
            "/v1/agent/stream",
            json={"text": "hello", "conversation_id": "c1", "cwd": "/tmp"},
        ) as resp:
            assert resp.status_code == 200
            assert resp.headers.get("X-Proxy-Renderable-Assistant-Count") is not None
            await _drain_stream(resp)

        replay = await client.get("/v1/conversations/c1/events", params={"since": 0})

    assert replay.status_code == 200
    # 2 text blocks + 1 tool_use + 1 tool_result + 1 text ("Done"); whitespace-only text is ignored.
    assert replay.headers.get("X-Proxy-Renderable-Assistant-Count") == "5"


@pytest.mark.asyncio
async def test_renderable_bubble_count_survives_restart(tmp_path):
    first_app = create_app(store_dir=tmp_path, backend=_renderable_count_backend)

    transport = httpx.ASGITransport(app=first_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
            "POST",
            "/v1/agent/stream",
            json={"text": "hello", "conversation_id": "c1", "cwd": "/tmp"},
        ) as resp:
            assert resp.status_code == 200
            await _drain_stream(resp)

    restarted_app = create_app(store_dir=tmp_path, backend=_renderable_count_backend)
    transport = httpx.ASGITransport(app=restarted_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        replay = await client.get("/v1/conversations/c1/events", params={"since": 0})

    assert replay.status_code == 200
    assert replay.headers.get("X-Proxy-Renderable-Assistant-Count") == "5"


@pytest.mark.asyncio
async def test_renderable_bubble_count_ignores_whitespace_text(tmp_path):
    app = create_app(store_dir=tmp_path, backend=_whitespace_only_backend)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
            "POST",
            "/v1/agent/stream",
            json={"text": "hello", "conversation_id": "c1", "cwd": "/tmp"},
        ) as resp:
            assert resp.status_code == 200
            await _drain_stream(resp)

        replay = await client.get("/v1/conversations/c1/events", params={"since": 0})

    assert replay.status_code == 200
    assert replay.headers.get("X-Proxy-Renderable-Assistant-Count") == "0"

