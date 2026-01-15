from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

from claude_proxy.backends import default_backend
from claude_proxy.conversation_manager import (
    AgentFolderBusyError,
    ConversationCwdMismatchError,
    ConversationManager,
)
from claude_proxy.util import parse_int, sanitize_id


def create_app(*, store_dir: Path | None = None, backend=default_backend) -> FastAPI:
    app = FastAPI()
    manager = ConversationManager(store_dir=store_dir, backend=backend)

    def json_error(status_code: int, *, error: str, **extra: Any) -> JSONResponse:
        return JSONResponse(status_code=status_code, content={"error": error, **extra})

    @app.get("/healthz")
    async def healthz() -> PlainTextResponse:
        return PlainTextResponse("ok")

    @app.post("/v1/agent/stream")
    async def agent_stream(request: Request) -> StreamingResponse:
        body: dict[str, Any] = await request.json()

        prompt = body.get("text") or body.get("prompt")
        if prompt is not None and not isinstance(prompt, str):
            return json_error(400, error="bad_request", message="text must be a string.")
        if isinstance(prompt, str):
            prompt = prompt.strip()

        conversation_id = body.get("conversation_id") or body.get("session_id")
        if conversation_id is None:
            return json_error(400, error="bad_request", message="conversation_id is required.")
        if not isinstance(conversation_id, str):
            return json_error(400, error="bad_request", message="conversation_id must be a string.")
        try:
            conversation_id = sanitize_id(conversation_id)
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        since = parse_int(request.headers.get("Last-Event-ID"), default=0)
        if since is None:
            return json_error(400, error="bad_request", message="Invalid Last-Event-ID header.")

        conversation = await manager.get_or_create_conversation(conversation_id)

        if conversation.is_running:
            if prompt is not None and prompt != conversation.prompt:
                return json_error(409, error="conversation_already_running", conversation_id=conversation_id)
            cwd_value = body.get("cwd")
            if not isinstance(cwd_value, str):
                return json_error(400, error="bad_request", message="cwd is required to attach.")
            try:
                await manager.ensure_cwd_binding(conversation_id=conversation_id, cwd=cwd_value)
            except ConversationCwdMismatchError as exc:
                return json_error(
                    409,
                    error="conversation_cwd_mismatch",
                    conversation_id=conversation_id,
                    expected_cwd=exc.expected_cwd,
                    got_cwd=exc.got_cwd,
                )
            except ValueError as exc:
                return json_error(400, error="bad_request", message=str(exc))
        else:
            if not prompt:
                return json_error(400, error="bad_request", message="text required to start a new run.")
            try:
                await manager.start_run(conversation_id=conversation_id, prompt=prompt, request_body=body)
            except AgentFolderBusyError as exc:
                return json_error(409, error="agent_folder_busy", cwd=exc.cwd, retry_after_ms=2000)
            except ConversationCwdMismatchError as exc:
                return json_error(
                    409,
                    error="conversation_cwd_mismatch",
                    conversation_id=conversation_id,
                    expected_cwd=exc.expected_cwd,
                    got_cwd=exc.got_cwd,
                )
            except ValueError as exc:
                return json_error(400, error="bad_request", message=str(exc))

        stream = manager.sse_stream(conversation_id=conversation_id, since=since, request=request)
        return StreamingResponse(
            stream,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/v1/conversations/{conversation_id}/events")
    async def replay(conversation_id: str, request: Request) -> StreamingResponse:
        try:
            conversation_id = sanitize_id(conversation_id)
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))
        since = parse_int(request.query_params.get("since"), default=0)
        if since is None:
            return json_error(400, error="bad_request", message="Invalid since parameter.")

        if not await manager.conversation_exists(conversation_id):
            return json_error(404, error="conversation_unknown", conversation_id=conversation_id)

        async def iter_ndjson():
            async for line in manager.iter_ndjson(conversation_id=conversation_id, since=since):
                yield line.encode("utf-8")

        return StreamingResponse(
            iter_ndjson(),
            media_type="application/x-ndjson",
            headers={
                "Cache-Control": "no-cache",
            },
        )

    return app


app = create_app()
