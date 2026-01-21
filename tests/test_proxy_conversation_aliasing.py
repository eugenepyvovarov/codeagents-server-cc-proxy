import asyncio
import json

import httpx
import pytest
from claude_agent_sdk.types import AssistantMessage, ResultMessage, SystemMessage, TextBlock

from app import create_app


async def _alias_backend(*, prompt: str, options):
    _ = options
    yield SystemMessage(
        subtype="init",
        data={
            "type": "system",
            "subtype": "init",
            "session_id": "upstream-session",
            "tools": ["Bash"],
            "cwd": "/tmp/project",
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


async def _collect_sse_events(resp: httpx.Response):
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
            current_id = None
            current_data = None

    return events


@pytest.mark.asyncio
async def test_replay_aliases_by_cwd_when_conversation_id_differs(tmp_path):
    app = create_app(store_dir=tmp_path, backend=_alias_backend)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
            "POST",
            "/v1/agent/stream",
            json={"text": "hello", "conversation_id": "c1", "cwd": "/tmp/project"},
        ) as resp:
            assert resp.status_code == 200
            _ = await _collect_sse_events(resp)

        aliased = await client.get(
            "/v1/conversations/c2/events",
            params={"since": 0, "cwd": "/tmp/project"},
        )

    assert aliased.status_code == 200
    aliased_lines = [json.loads(line) for line in aliased.text.splitlines() if line.strip()]
    assert aliased_lines, "Expected replay events when cwd matches existing conversation."
    assert any(line.get("type") == "assistant" for line in aliased_lines)
    assert any(line.get("type") == "result" for line in aliased_lines)


@pytest.mark.asyncio
async def test_replay_aliases_after_restart(tmp_path):
    first_app = create_app(store_dir=tmp_path, backend=_alias_backend)

    transport = httpx.ASGITransport(app=first_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
            "POST",
            "/v1/agent/stream",
            json={"text": "hello", "conversation_id": "c1", "cwd": "/tmp/project"},
        ) as resp:
            assert resp.status_code == 200
            _ = await _collect_sse_events(resp)

    restarted_app = create_app(store_dir=tmp_path, backend=_alias_backend)
    transport = httpx.ASGITransport(app=restarted_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        aliased = await client.get(
            "/v1/conversations/c2/events",
            params={"since": 0, "cwd": "/tmp/project"},
        )

    assert aliased.status_code == 200
    aliased_lines = [json.loads(line) for line in aliased.text.splitlines() if line.strip()]
    assert aliased_lines, "Expected replay events after restart when cwd matches existing conversation."


@pytest.mark.asyncio
async def test_replay_without_cwd_returns_unknown(tmp_path):
    app = create_app(store_dir=tmp_path, backend=_alias_backend)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
            "POST",
            "/v1/agent/stream",
            json={"text": "hello", "conversation_id": "c1", "cwd": "/tmp/project"},
        ) as resp:
            assert resp.status_code == 200
            _ = await _collect_sse_events(resp)

        missing_cwd = await client.get("/v1/conversations/c2/events", params={"since": 0})

    assert missing_cwd.status_code == 404
    assert missing_cwd.json().get("error") == "conversation_unknown"


@pytest.mark.asyncio
async def test_conversation_group_isolates_cwd_aliasing(tmp_path):
    app = create_app(store_dir=tmp_path, backend=_alias_backend)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        async with client.stream(
            "POST",
            "/v1/agent/stream",
            json={
                "text": "first",
                "conversation_id": "c1",
                "conversation_group": "g1",
                "cwd": "/tmp/project",
            },
        ) as resp:
            assert resp.status_code == 200
            _ = await _collect_sse_events(resp)

        async with client.stream(
            "POST",
            "/v1/agent/stream",
            json={
                "text": "second",
                "conversation_id": "c2",
                "conversation_group": "g2",
                "cwd": "/tmp/project",
            },
        ) as resp:
            assert resp.status_code == 200
            _ = await _collect_sse_events(resp)

        replay_g2 = await client.get(
            "/v1/conversations/c2/events",
            params={"since": 0, "cwd": "/tmp/project", "conversation_group": "g2"},
        )
        replay_g1 = await client.get(
            "/v1/conversations/c2/events",
            params={"since": 0, "cwd": "/tmp/project", "conversation_group": "g1"},
        )

    assert replay_g2.status_code == 200
    assert replay_g1.status_code == 200
    g2_text = json.dumps([json.loads(line) for line in replay_g2.text.splitlines() if line.strip()])
    g1_text = json.dumps([json.loads(line) for line in replay_g1.text.splitlines() if line.strip()])
    assert "Echo: second" in g2_text
    assert "Echo: first" in g1_text
