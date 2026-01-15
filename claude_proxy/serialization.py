from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

from claude_agent_sdk.types import (
    AssistantMessage,
    ContentBlock,
    ResultMessage,
    StreamEvent,
    SystemMessage,
    TextBlock,
    ThinkingBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)


def serialize_message(message: Any) -> dict[str, Any] | None:
    if isinstance(message, SystemMessage):
        payload = dict(message.data)
        payload.setdefault("type", "system")
        payload.setdefault("subtype", message.subtype)
        return payload

    if isinstance(message, AssistantMessage):
        return {
            "type": "assistant",
            "message": {
                "role": "assistant",
                "model": message.model,
                "content": _serialize_blocks(message.content),
            },
        }

    if isinstance(message, UserMessage):
        content_blocks: list[ContentBlock]
        if isinstance(message.content, str):
            content_blocks = [TextBlock(text=message.content)]
        else:
            content_blocks = message.content
        return {
            "type": "user",
            "message": {
                "role": "user",
                "content": _serialize_blocks(content_blocks),
            },
        }

    if isinstance(message, ResultMessage):
        payload: dict[str, Any] = {
            "type": "result",
            "subtype": message.subtype,
            "duration_ms": message.duration_ms,
            "duration_api_ms": message.duration_api_ms,
            "is_error": message.is_error,
            "num_turns": message.num_turns,
            "session_id": message.session_id,
        }
        if message.total_cost_usd is not None:
            payload["total_cost_usd"] = message.total_cost_usd
        if message.usage is not None:
            payload["usage"] = message.usage
        if message.result is not None:
            payload["result"] = message.result
        if message.structured_output is not None:
            payload["structured_output"] = message.structured_output
        return payload

    if isinstance(message, StreamEvent):
        return {
            "type": "stream_event",
            "uuid": message.uuid,
            "session_id": message.session_id,
            "event": message.event,
            "parent_tool_use_id": message.parent_tool_use_id,
        }

    if is_dataclass(message):
        return asdict(message)

    if isinstance(message, dict):
        return message

    return None


def _serialize_blocks(blocks: list[ContentBlock]) -> list[dict[str, Any]]:
    encoded: list[dict[str, Any]] = []
    for block in blocks:
        if isinstance(block, TextBlock):
            encoded.append({"type": "text", "text": block.text})
        elif isinstance(block, ThinkingBlock):
            encoded.append({"type": "thinking", "thinking": block.thinking, "signature": block.signature})
        elif isinstance(block, ToolUseBlock):
            encoded.append({"type": "tool_use", "id": block.id, "name": block.name, "input": block.input})
        elif isinstance(block, ToolResultBlock):
            payload: dict[str, Any] = {"type": "tool_result", "tool_use_id": block.tool_use_id}
            if block.content is not None:
                payload["content"] = block.content
            if block.is_error is not None:
                payload["is_error"] = block.is_error
            encoded.append(payload)
    return encoded

