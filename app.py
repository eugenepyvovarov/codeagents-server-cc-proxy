from __future__ import annotations

import asyncio
import logging
import os
import subprocess
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

logger = logging.getLogger(__name__)


def _get_update_interval_seconds() -> int:
    raw = os.environ.get("CLAUDE_PROXY_UPDATE_INTERVAL_SECONDS", "600")
    try:
        return int(raw)
    except ValueError:
        return 21600


def _run_command(args: list[str], *, cwd: Path) -> str:
    return subprocess.check_output(args, cwd=str(cwd), text=True).strip()


def _default_branch(repo_dir: Path) -> str:
    try:
        output = _run_command(
            ["git", "-C", str(repo_dir), "ls-remote", "--symref", "origin", "HEAD"],
            cwd=repo_dir,
        )
        for line in output.splitlines():
            if line.startswith("ref:"):
                ref = line.split()[1]
                if ref.startswith("refs/heads/"):
                    return ref.replace("refs/heads/", "")
    except Exception:
        logger.warning("Auto-update: failed to detect default branch")
    return "main"


def _install_requirements(repo_dir: Path) -> None:
    venv_python = repo_dir / ".venv" / "bin" / "python"
    requirements = repo_dir / "requirements.txt"
    if not venv_python.exists() or not requirements.is_file():
        logger.warning("Auto-update: missing venv or requirements.txt, skipping pip install")
        return
    subprocess.check_call(
        [str(venv_python), "-m", "pip", "install", "-r", str(requirements)],
        cwd=str(repo_dir),
    )


def _apply_repo_update(repo_dir: Path) -> bool:
    if not (repo_dir / ".git").exists():
        logger.warning("Auto-update: repo not found, skipping")
        return False

    branch = _default_branch(repo_dir)
    subprocess.check_call(["git", "-C", str(repo_dir), "fetch", "--prune", "origin"])
    local_sha = _run_command(["git", "-C", str(repo_dir), "rev-parse", "HEAD"], cwd=repo_dir)
    remote_ref = f"origin/{branch}"
    remote_sha = _run_command(["git", "-C", str(repo_dir), "rev-parse", remote_ref], cwd=repo_dir)

    if local_sha == remote_sha:
        return False

    logger.info("Auto-update: applying %s -> %s", local_sha, remote_sha)
    subprocess.check_call(["git", "-C", str(repo_dir), "reset", "--hard", remote_ref])
    _install_requirements(repo_dir)
    return True


def create_app(*, store_dir: Path | None = None, backend=default_backend) -> FastAPI:
    app = FastAPI()
    manager = ConversationManager(store_dir=store_dir, backend=backend)
    update_lock = asyncio.Lock()
    update_task: asyncio.Task[None] | None = None

    async def _maybe_update() -> None:
        if update_lock.locked():
            return
        async with update_lock:
            if await manager.has_active_runs():
                logger.info("Auto-update: skipping because a run is active")
                return
            repo_dir = Path(__file__).resolve().parent
            try:
                updated = await asyncio.to_thread(_apply_repo_update, repo_dir)
            except Exception:
                logger.exception("Auto-update: failed")
                return
            if updated:
                logger.info("Auto-update: applied; restarting process")
                os._exit(0)

    async def _auto_update_loop() -> None:
        interval = _get_update_interval_seconds()
        if interval <= 0:
            logger.info("Auto-update: disabled")
            return
        while True:
            await asyncio.sleep(interval)
            await _maybe_update()

    @app.on_event("startup")
    async def _startup() -> None:
        nonlocal update_task
        update_task = asyncio.create_task(_auto_update_loop())

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        if update_task:
            update_task.cancel()

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

        cwd_value = body.get("cwd")
        cwd = cwd_value if isinstance(cwd_value, str) else None
        try:
            conversation_id = await manager.resolve_conversation_id(conversation_id=conversation_id, cwd=cwd)
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

        conversation = await manager.get_or_create_conversation(conversation_id)

        if conversation.is_running:
            if prompt is not None and prompt != conversation.prompt:
                return json_error(409, error="conversation_already_running", conversation_id=conversation_id)
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

        cwd = request.query_params.get("cwd")
        resolved = await manager.resolve_existing_conversation_id(conversation_id=conversation_id, cwd=cwd)
        if resolved is None:
            return json_error(404, error="conversation_unknown", conversation_id=conversation_id)
        conversation_id = resolved

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
