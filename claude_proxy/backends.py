from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from claude_agent_sdk import ClaudeAgentOptions, query


async def default_backend(*, prompt: str, options: ClaudeAgentOptions) -> AsyncIterator[Any]:
    async for message in query(prompt=prompt, options=options):
        yield message

