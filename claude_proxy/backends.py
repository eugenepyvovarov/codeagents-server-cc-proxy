from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from claude_agent_sdk import ClaudeAgentOptions, query


async def _single_prompt(prompt: str) -> AsyncIterator[dict[str, Any]]:
    yield {"role": "user", "content": [{"type": "text", "text": prompt}]}


async def default_backend(*, prompt: str, options: ClaudeAgentOptions) -> AsyncIterator[Any]:
    async for message in query(prompt=_single_prompt(prompt), options=options):
        yield message
