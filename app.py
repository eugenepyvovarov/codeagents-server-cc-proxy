from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from claude_proxy.backends import default_backend
from claude_proxy.conversation_manager import (
    AgentFolderBusyError,
    ConversationCwdMismatchError,
    ConversationGroupMismatchError,
    ConversationManager,
)
from claude_proxy.opencode_client import OpenCodeClient
from claude_proxy.opencode_tasks import OpenCodeTaskRunner
from claude_proxy.task_scheduler import (
    TaskScheduler,
    TaskStore,
    TaskValidationError,
    parse_task_payload,
    serialize_task,
    update_task_from_payload,
)
from claude_proxy.util import normalize_agent_id, parse_int, sanitize_id

logger = logging.getLogger(__name__)

_ENV_KEY_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_ENV_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_env_key(value: str) -> str:
    normalized = _ENV_WHITESPACE_RE.sub("_", value.strip()).upper()
    if not _ENV_KEY_RE.match(normalized):
        raise ValueError("Invalid env key format.")
    return normalized


def _get_update_interval_seconds() -> int:
    # Default 0 = disabled. Blind auto-follow of origin is a supply-chain risk;
    # upgrades are driven by the app's pinned install revision.
    raw = os.environ.get("CLAUDE_PROXY_UPDATE_INTERVAL_SECONDS", "0")
    try:
        return int(raw)
    except ValueError:
        return 0


def _daemon_auth_token() -> str:
    return (
        os.environ.get("CODEAGENTS_DAEMON_TOKEN", "").strip()
        or os.environ.get("CLAUDE_PROXY_AUTH_TOKEN", "").strip()
    )


def _mask_secret_value(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "****"
    return f"{value[:2]}…{value[-2:]} ({len(value)} chars)"


def hmac_compare(presented: str, expected: str) -> bool:
    """Constant-time compare for bearer tokens."""
    if not presented or not expected or len(presented) != len(expected):
        return False
    # Use hmac.compare_digest — hashlib has no compare_digest on some runtimes.
    return hmac.compare_digest(presented.encode("utf-8"), expected.encode("utf-8"))


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


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _git_sha(repo_dir: Path) -> str:
    try:
        return _run_command(["git", "-C", str(repo_dir), "rev-parse", "--short", "HEAD"], cwd=repo_dir)
    except Exception:
        logger.warning("Failed to read git SHA for proxy version")
    return "unknown"


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
    store_root = (store_dir or Path("data")).resolve()
    task_store = TaskStore(store_root / "tasks.db")
    manager = ConversationManager(store_dir=store_dir, backend=backend, env_store=task_store)
    opencode_client = OpenCodeClient.from_environment()
    opencode_task_runner = OpenCodeTaskRunner(client=opencode_client, store=task_store)
    task_scheduler = TaskScheduler(store=task_store, manager=manager, task_runner=opencode_task_runner)
    manager.set_run_finished_callback(task_scheduler.on_run_finished)
    update_lock = asyncio.Lock()
    update_task: asyncio.Task[None] | None = None
    repo_dir = Path(__file__).resolve().parent
    version = _git_sha(repo_dir)
    started_at = _now_iso()
    auth_token = _daemon_auth_token()
    if not auth_token:
        logger.warning(
            "Daemon auth token not set (CODEAGENTS_DAEMON_TOKEN); "
            "non-health endpoints remain open to local processes"
        )

    def proxy_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {
            "X-Proxy-Version": version,
            "X-Proxy-Started-At": started_at,
        }
        if extra:
            headers.update(extra)
        return headers

    def _extract_bearer(request: Request) -> str | None:
        auth = request.headers.get("authorization") or request.headers.get("Authorization") or ""
        if auth.lower().startswith("bearer "):
            return auth[7:].strip()
        token_header = request.headers.get("x-codeagents-token") or request.headers.get("X-CodeAgents-Token")
        if token_header:
            return token_header.strip()
        return None

    @app.middleware("http")
    async def require_daemon_auth(request: Request, call_next):
        path = request.url.path or ""
        # Health: soft probes / install verification.
        if path == "/healthz" or path.startswith("/healthz/"):
            return await call_next(request)
        # Loopback MCP for OpenCode (managed scheduler). OpenCode cannot attach
        # arbitrary Bearer headers to MCP transports; keep MCP on 127.0.0.1 only.
        if path == "/mcp" or path.startswith("/mcp/"):
            return await call_next(request)
        # OpenCode probes these after MCP 401; avoid auth noise / OAuth discovery loops.
        if path.startswith("/.well-known/") or path == "/register":
            return await call_next(request)
        expected = _daemon_auth_token() or auth_token
        if not expected:
            # Fail open only when no token is configured (legacy installs).
            return await call_next(request)
        presented = _extract_bearer(request)
        if not presented or not hmac_compare(presented, expected):
            # Do not log token values — only presence/length for support diagnosis.
            logger.warning(
                "daemon auth 401 path=%s method=%s presented=%s presented_len=%s expected_len=%s",
                path,
                request.method,
                "yes" if presented else "no",
                len(presented) if presented else 0,
                len(expected),
            )
            return JSONResponse(
                status_code=401,
                content={"error": "unauthorized", "message": "Valid bearer token required."},
                headers=proxy_headers(),
            )
        return await call_next(request)

    async def _maybe_update() -> None:
        if update_lock.locked():
            return
        async with update_lock:
            if await manager.has_active_runs() or await opencode_task_runner.has_active_runs():
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
        await task_scheduler.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        if update_task:
            update_task.cancel()
        await task_scheduler.shutdown()

    def json_error(status_code: int, *, error: str, reset: bool = False, **extra: Any) -> JSONResponse:
        content: dict[str, Any] = {"error": error, **extra}
        headers = proxy_headers()
        if reset:
            content["reset"] = True
            headers["X-Proxy-Reset"] = "true"
        return JSONResponse(
            status_code=status_code,
            content=content,
            headers=headers,
        )

    MCP_TOOL_NAME_LIST = "list_scheduled_tasks"
    MCP_TOOL_NAME_CREATE = "create_scheduled_task"
    MCP_TOOL_NAME_UPDATE = "update_scheduled_task"
    MCP_TOOL_NAME_DELETE = "delete_scheduled_task"
    MCP_PROTOCOL_VERSION = "2025-03-26"
    MCP_SERVER_NAME = "codeagents-scheduled-tasks"
    MCP_SERVER_VERSION = "0.1.0"
    MCP_CAPABILITY_VERSION = "2024-11-05"

    def _mcp_result(payload_id: Any, result: dict[str, Any]) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": payload_id, "result": result}

    def _mcp_error(payload_id: Any, code: int, message: str, data: Any | None = None) -> dict[str, Any]:
        error_payload: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            error_payload["data"] = data
        return {"jsonrpc": "2.0", "id": payload_id, "error": error_payload}

    def _first_non_empty(*values: Any) -> str | None:
        for value in values:
            if isinstance(value, str):
                normalized = value.strip()
                if normalized:
                    return normalized
        return None

    def _to_bool(value: Any) -> bool | None:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "no", "n", "off"}:
                return False
        return None

    def _to_str_array(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        references: list[str] = []
        for item in value:
            if not isinstance(item, str):
                continue
            normalized = item.strip().lstrip("@")
            if normalized:
                references.append(normalized)
        return references

    def _header_value(headers: dict[str, str], *names: str) -> str | None:
        lowered = {key.lower(): value for key, value in headers.items()}
        for name in names:
            value = lowered.get(name.lower())
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _resolve_mcp_scope(request: Request, args: dict[str, Any] | None) -> dict[str, str]:
        raw_args = args or {}
        headers = dict(request.headers)
        return {
            "agent_id": (
                _first_non_empty(
                    _header_value(headers, "x-codeagents-agent-id", "x-codeagents-project-id"),
                    raw_args.get("agent_id"),
                    raw_args.get("project_id"),
                )
                or ""
            ),
            "conversation_id": (
                _first_non_empty(
                    _header_value(headers, "x-codeagents-conversation-id", "x-codeagents-session-id"),
                    raw_args.get("conversation_id"),
                )
                or ""
            ),
            "conversation_group": (
                _first_non_empty(
                    _header_value(headers, "x-codeagents-conversation-group-id"),
                    raw_args.get("conversation_group"),
                )
                or ""
            ),
            "cwd": (
                _first_non_empty(
                    _header_value(headers, "x-codeagents-project-path", "x-codeagents-cwd"),
                    raw_args.get("cwd"),
                    raw_args.get("project_path"),
                )
                or ""
            ),
            "time_zone": (
                _first_non_empty(
                    _header_value(headers, "x-codeagents-time-zone", "x-codeagents-timezone"),
                    raw_args.get("time_zone"),
                    raw_args.get("timeZone"),
                    raw_args.get("timeZoneId"),
                    raw_args.get("time_zone_id"),
                )
                or ""
            ),
        }

    def _normalized_cwd(value: str) -> str:
        normalized = value.strip()
        if not normalized:
            return ""
        normalized = normalized.rstrip("/")
        return normalized or "/"

    def _project_scope_key(cwd: str) -> str:
        normalized = _normalized_cwd(cwd)
        if not normalized:
            return ""
        marker = "/projects/"
        if marker in normalized:
            return normalized.split(marker, 1)[1].strip("/")
        return normalized.split("/")[-1]

    def _filter_tasks_by_cwd(tasks: list[Any], cwd: str) -> list[Any]:
        normalized = _normalized_cwd(cwd)
        if not normalized:
            return tasks

        exact = [task for task in tasks if _normalized_cwd(getattr(task, "cwd", "")) == normalized]
        if exact:
            return exact

        # Path roots can differ across environments (for example /root/projects vs /home/<user>/projects).
        # Fall back to a project-scope key so list does not silently drop valid tasks.
        target_key = _project_scope_key(normalized)
        if not target_key:
            return []
        return [task for task in tasks if _project_scope_key(getattr(task, "cwd", "")) == target_key]

    def _normalize_slug(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip().lower()
        if not text:
            return None
        normalized: list[str] = []
        wrote_dash = False
        for char in text:
            if char.isalnum():
                normalized.append(char)
                wrote_dash = False
            elif char.isspace() or char in {"-", "_"}:
                if normalized and not wrote_dash:
                    normalized.append("-")
                    wrote_dash = True
        if not normalized:
            return None
        result = "".join(normalized).strip("-")
        return result or None

    def _compose_prompt(args: dict[str, Any]) -> str | None:
        prompt = _first_non_empty(args.get("prompt"), args.get("text"))
        if prompt is not None:
            return prompt

        message = _first_non_empty(args.get("message")) or ""
        skill_slug = _first_non_empty(args.get("skill_slug"), args.get("skillSlug"), args.get("slug"))
        skill_name = _first_non_empty(args.get("skill_name"), args.get("skillName"))
        file_references = _to_str_array(args.get("file_references") or args.get("fileReferences"))
        skill_command = skill_slug or _normalize_slug(skill_name)

        if skill_command is not None:
            first = f"/{skill_command}" if not message else f"/{skill_command} {message}"
            if file_references:
                references = "\n".join(f"@{ref}" for ref in file_references)
                return f"{first}\n\n{references}"
            return first

        if file_references:
            references = "\n".join(f"@{ref}" for ref in file_references)
            if message:
                return f"{references}\n\n{message}"
            return references

        return message or None

    def _extract_schedule_payload(args: dict[str, Any]) -> dict[str, Any]:
        nested = args.get("schedule")
        if isinstance(nested, dict):
            raw = dict(nested)
        else:
            raw = {}

        for source in ("frequency", "interval", "weekday_mask", "monthly_mode", "day_of_month", "weekday_ordinal", "weekday", "month", "time_minutes"):
            if source in raw:
                continue
            camel = "".join(part.capitalize() if idx else part for idx, part in enumerate(source.split("_")))
            if source in args:
                raw[source] = args[source]
            elif camel in args:
                raw[source] = args[camel]

        accepted = {
            "frequency",
            "interval",
            "weekday_mask",
            "monthly_mode",
            "day_of_month",
            "weekday_ordinal",
            "weekday",
            "month",
            "time_minutes",
        }
        return {key: value for key, value in raw.items() if key in accepted}

    def _build_payload_from_mcp_tool_args(args: dict[str, Any], context: dict[str, str]) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        title = _first_non_empty(args.get("title"))
        if title is not None:
            payload["title"] = title

        prompt = _compose_prompt(args)
        if prompt is not None:
            payload["prompt"] = prompt

        enabled = _to_bool(_first_non_empty(args.get("enabled"), args.get("isEnabled"), args.get("is_enabled")))
        if enabled is not None:
            payload["enabled"] = enabled

        time_zone = _first_non_empty(
            args.get("time_zone"),
            args.get("timeZone"),
            args.get("timeZoneId"),
            args.get("time_zone_id"),
        )
        if time_zone is None and context.get("time_zone"):
            time_zone = context["time_zone"]
        if time_zone is not None:
            payload["time_zone"] = time_zone

        if context.get("agent_id"):
            payload["agent_id"] = context["agent_id"]
        if context.get("conversation_id"):
            payload["conversation_id"] = context["conversation_id"]
        if context.get("conversation_group"):
            payload["conversation_group"] = context["conversation_group"]
        if context.get("cwd"):
            payload["cwd"] = context["cwd"]

        schedule_payload = _extract_schedule_payload(args)
        if schedule_payload:
            payload["schedule"] = schedule_payload
        return payload

    def _format_time_minutes(value: Any) -> str | None:
        try:
            minutes = int(value)
        except (TypeError, ValueError):
            return None
        minutes = max(0, min(minutes, 1_439))
        return f"{minutes // 60:02d}:{minutes % 60:02d}"

    def _task_summary_line(task: dict[str, Any]) -> str:
        task_id = str(task.get("id", "")).strip()
        title = str(task.get("title", "")).strip() or "(untitled)"
        status = "enabled" if task.get("enabled") else "disabled"
        time_zone = str(task.get("time_zone") or "UTC").strip() or "UTC"
        schedule = task.get("schedule") if isinstance(task.get("schedule"), dict) else {}
        frequency = str(schedule.get("frequency") or "").strip() or "unknown"
        clock = _format_time_minutes(schedule.get("time_minutes"))
        next_run = str(task.get("next_run_at") or "").strip() or "n/a"
        last_run = str(task.get("last_run_at") or "").strip() or "never"
        last_error = str(task.get("last_error") or "").strip()

        parts = [f"{title} [{task_id}] ({status})", f"freq={frequency}", f"tz={time_zone}"]
        if clock is not None:
            parts.append(f"at={clock}")
        parts.append(f"next={next_run}")
        parts.append(f"last={last_run}")
        if last_error:
            parts.append(f"error={last_error}")
        return " ".join(parts)

    def _task_tools_schema() -> list[dict[str, Any]]:
        return [
            {
                "name": MCP_TOOL_NAME_LIST,
                "description": "List scheduled tasks for the active project.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "enabled": {"type": "boolean", "description": "Optional filter by enabled state."},
                        "limit": {"type": "integer", "description": "Optional max tasks to return (1-200)."},
                    },
                },
            },
            {
                "name": MCP_TOOL_NAME_CREATE,
                "description": (
                    "Create a scheduled task for the active project. "
                    "frequency may be minutely, hourly, daily, weekly, monthly, yearly, or once. "
                    "For once, pass day_of_month, month, time_minutes, and optional next_run_at (ISO); "
                    "the task disables and deletes itself after a successful run. "
                    "time_minutes is wall-clock minutes from midnight in time_zone "
                    "(e.g. 540 = 09:00). Always pass time_zone as an IANA id "
                    "(e.g. Europe/Berlin) matching the user's local timezone unless they specify otherwise. "
                    "After create, confirm next_run_at and time_zone in the tool result."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "prompt": {"type": "string", "description": "Direct prompt to schedule."},
                        "message": {"type": "string", "description": "Prompt text if direct prompt is not provided."},
                        "isEnabled": {"type": "boolean"},
                        "timeZoneId": {
                            "type": "string",
                            "description": "IANA timezone (alias of time_zone), e.g. Europe/Berlin.",
                        },
                        "time_zone": {
                            "type": "string",
                            "description": (
                                "IANA timezone for time_minutes (e.g. Europe/Berlin). "
                                "Required unless the client sends x-codeagents-time-zone. "
                                "Do not omit this when the user gives a local clock time."
                            ),
                        },
                        "frequency": {"type": "string"},
                        "interval": {"type": "integer"},
                        "weekday_mask": {"type": "integer"},
                        "monthly_mode": {"type": "string"},
                        "day_of_month": {"type": "integer"},
                        "weekday_ordinal": {"type": "string"},
                        "weekday": {"type": "integer"},
                        "month": {"type": "integer"},
                        "time_minutes": {
                            "type": "integer",
                            "description": "Minutes from midnight in time_zone (0-1439). Example: 540 = 09:00.",
                        },
                        "skill_slug": {"type": "string"},
                        "skill_name": {"type": "string"},
                        "file_references": {"type": "array", "items": {"type": "string"}},
                        "schedule": {"type": "object"},
                    },
                },
            },
            {
                "name": MCP_TOOL_NAME_UPDATE,
                "description": (
                    "Update an existing scheduled task for the active project. "
                    "When changing the clock time, also pass time_zone (IANA) so the schedule stays in the user's local zone. "
                    "Confirm next_run_at after update."
                ),
                "inputSchema": {
                    "type": "object",
                    "required": ["task_id"],
                    "properties": {
                        "task_id": {"type": "string"},
                        "title": {"type": "string"},
                        "prompt": {"type": "string"},
                        "message": {"type": "string"},
                        "isEnabled": {"type": "boolean"},
                        "timeZoneId": {
                            "type": "string",
                            "description": "IANA timezone (alias of time_zone), e.g. Europe/Berlin.",
                        },
                        "time_zone": {
                            "type": "string",
                            "description": "IANA timezone for schedule wall-clock times (e.g. Europe/Berlin).",
                        },
                        "frequency": {"type": "string"},
                        "interval": {"type": "integer"},
                        "weekday_mask": {"type": "integer"},
                        "monthly_mode": {"type": "string"},
                        "day_of_month": {"type": "integer"},
                        "weekday_ordinal": {"type": "string"},
                        "weekday": {"type": "integer"},
                        "month": {"type": "integer"},
                        "time_minutes": {
                            "type": "integer",
                            "description": "Minutes from midnight in time_zone (0-1439). Example: 540 = 09:00.",
                        },
                        "skill_slug": {"type": "string"},
                        "skill_name": {"type": "string"},
                        "file_references": {"type": "array", "items": {"type": "string"}},
                        "schedule": {"type": "object"},
                    },
                },
            },
            {
                "name": MCP_TOOL_NAME_DELETE,
                "description": "Delete a scheduled task by id.",
                "inputSchema": {
                    "type": "object",
                    "required": ["task_id"],
                    "properties": {"task_id": {"type": "string"}},
                },
            },
        ]

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "version": version,
                "started_at": started_at,
                "opencode": await opencode_client.health_status(),
            },
            headers=proxy_headers(),
        )

    @app.post("/mcp")
    async def mcp_router(request: Request) -> JSONResponse:
        try:
            request_data = await request.json()
        except Exception:
            return JSONResponse(
                status_code=200,
                content=_mcp_error(None, -32700, "Parse error"),
                headers=proxy_headers(),
            )

        if not isinstance(request_data, dict):
            return JSONResponse(
                status_code=200,
                content=_mcp_error(None, -32600, "Invalid request"),
                headers=proxy_headers(),
            )

        payload_id = request_data.get("id")
        method = request_data.get("method")
        if not isinstance(method, str):
            return JSONResponse(
                status_code=200,
                content=_mcp_error(payload_id, -32600, "Missing or invalid method"),
                headers=proxy_headers(),
            )

        if method == "initialize":
            return JSONResponse(
                status_code=200,
                content=_mcp_result(
                    payload_id,
                    {
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {
                            "tools": {
                                "listChanged": False,
                            }
                        },
                        "serverInfo": {
                            "name": MCP_SERVER_NAME,
                            "version": MCP_SERVER_VERSION,
                        },
                    },
                ),
                headers=proxy_headers(),
            )

        if method == "tools/list":
            return JSONResponse(
                status_code=200,
                content=_mcp_result(
                    payload_id,
                    {"tools": _task_tools_schema()},
                ),
                headers=proxy_headers(),
            )

        if method != "tools/call":
            return JSONResponse(
                status_code=200,
                content=_mcp_error(payload_id, -32601, f"Method '{method}' not found"),
                headers=proxy_headers(),
            )

        params = request_data.get("params")
        if not isinstance(params, dict):
            return JSONResponse(
                status_code=200,
                content=_mcp_error(payload_id, -32602, "Invalid params"),
                headers=proxy_headers(),
            )

        tool_name = params.get("name")
        arguments = params.get("arguments", {})
        if not isinstance(tool_name, str):
            return JSONResponse(
                status_code=200,
                content=_mcp_error(payload_id, -32602, "Missing tool name"),
                headers=proxy_headers(),
            )
        if not isinstance(arguments, dict):
            return JSONResponse(
                status_code=200,
                content=_mcp_error(payload_id, -32602, "Tool arguments must be an object"),
                headers=proxy_headers(),
            )

        context = _resolve_mcp_scope(request, arguments)
        scoped_payload = _build_payload_from_mcp_tool_args(arguments, context)

        if tool_name == MCP_TOOL_NAME_LIST:
            raw_agent_id = context.get("agent_id", "")
            cwd = (context.get("cwd", "") or "").strip()
            agent_id: str | None = None

            if raw_agent_id:
                try:
                    agent_id = normalize_agent_id(raw_agent_id)
                except ValueError as exc:
                    return JSONResponse(
                        status_code=200,
                        content=_mcp_error(payload_id, -32602, str(exc)),
                        headers=proxy_headers(),
                    )

            enabled_filter = _to_bool(_first_non_empty(arguments.get("enabled"), arguments.get("isEnabled"), arguments.get("is_enabled")))

            limit_value = arguments.get("limit")
            limit: int | None = None
            if limit_value is not None:
                if isinstance(limit_value, bool):
                    return JSONResponse(
                        status_code=200,
                        content=_mcp_error(payload_id, -32602, "limit must be an integer between 1 and 200"),
                        headers=proxy_headers(),
                    )
                if isinstance(limit_value, int):
                    limit = limit_value
                elif isinstance(limit_value, float) and limit_value.is_integer():
                    limit = int(limit_value)
                elif isinstance(limit_value, str):
                    normalized = limit_value.strip()
                    if normalized.isdigit():
                        limit = int(normalized)
                if limit is None or limit < 1 or limit > 200:
                    return JSONResponse(
                        status_code=200,
                        content=_mcp_error(payload_id, -32602, "limit must be an integer between 1 and 200"),
                        headers=proxy_headers(),
                    )

            if agent_id:
                tasks = await task_store.list_tasks(agent_id=agent_id)
                if cwd:
                    tasks = _filter_tasks_by_cwd(tasks, cwd)
                    if not tasks:
                        # Identity can drift between legacy and current clients.
                        # Fall back to project-scope matching across all tasks.
                        all_tasks = await task_store.list_tasks()
                        tasks = _filter_tasks_by_cwd(all_tasks, cwd)
            else:
                all_tasks = await task_store.list_tasks()
                tasks = _filter_tasks_by_cwd(all_tasks, cwd)

            if enabled_filter is not None:
                tasks = [task for task in tasks if task.enabled is enabled_filter]
            if limit is not None:
                tasks = tasks[:limit]

            serialized = [serialize_task(task) for task in tasks]
            if serialized:
                lines = [f"Found {len(serialized)} scheduled task(s):"]
                for task in serialized:
                    lines.append(f"- {_task_summary_line(task)}")
                text = "\n".join(lines)
            else:
                text = "No scheduled tasks found."

            return JSONResponse(
                status_code=200,
                content=_mcp_result(
                    payload_id,
                    {
                        "tasks": serialized,
                        "count": len(serialized),
                        "structuredContent": {
                            "tasks": serialized,
                            "count": len(serialized),
                        },
                        "content": [
                            {
                                "type": "text",
                                "text": text,
                            }
                        ],
                    },
                ),
                headers=proxy_headers(),
            )

        if tool_name == MCP_TOOL_NAME_CREATE:
            if not scoped_payload.get("time_zone"):
                return JSONResponse(
                    status_code=200,
                    content=_mcp_error(
                        payload_id,
                        -32602,
                        (
                            "time_zone is required when creating a scheduled task. "
                            "Pass an IANA timezone such as Europe/Berlin that matches the user's local clock "
                            "(or ensure the client sends x-codeagents-time-zone). "
                            "Without it, wall-clock times like 09:00 would be mis-scheduled."
                        ),
                    ),
                    headers=proxy_headers(),
                )
            try:
                record = parse_task_payload(scoped_payload)
            except (TaskValidationError, ValueError) as exc:
                return JSONResponse(
                    status_code=200,
                    content=_mcp_error(payload_id, -32602, str(exc)),
                    headers=proxy_headers(),
                )

            stored = await task_scheduler.create_task(record)
            stored_payload = serialize_task(stored)
            return JSONResponse(
                status_code=200,
                content=_mcp_result(
                    payload_id,
                    {
                        "task": stored_payload,
                        "content": [
                            {
                                "type": "text",
                                "text": f"Created scheduled task: {_task_summary_line(stored_payload)}",
                            }
                        ],
                    },
                ),
                headers=proxy_headers(),
            )

        if tool_name == MCP_TOOL_NAME_UPDATE:
            task_id = arguments.get("task_id")
            if not isinstance(task_id, str) or not task_id.strip():
                return JSONResponse(
                    status_code=200,
                    content=_mcp_error(payload_id, -32602, "task_id is required"),
                    headers=proxy_headers(),
                )

            try:
                task_id = sanitize_id(task_id)
            except ValueError as exc:
                return JSONResponse(
                    status_code=200,
                    content=_mcp_error(payload_id, -32602, str(exc)),
                    headers=proxy_headers(),
                )

            existing = await task_store.get_task(task_id)
            if existing is None:
                return JSONResponse(
                    status_code=200,
                    content=_mcp_error(payload_id, -32000, "Task not found"),
                    headers=proxy_headers(),
                )

            try:
                updated = update_task_from_payload(existing, scoped_payload)
            except (TaskValidationError, ValueError) as exc:
                return JSONResponse(
                    status_code=200,
                    content=_mcp_error(payload_id, -32602, str(exc)),
                    headers=proxy_headers(),
                )

            stored = await task_scheduler.update_task(task_id, updated)
            stored_payload = serialize_task(stored)
            return JSONResponse(
                status_code=200,
                content=_mcp_result(
                    payload_id,
                    {
                        "task": stored_payload,
                        "content": [
                            {
                                "type": "text",
                                "text": f"Updated scheduled task: {_task_summary_line(stored_payload)}",
                            }
                        ],
                    },
                ),
                headers=proxy_headers(),
            )

        if tool_name == MCP_TOOL_NAME_DELETE:
            task_id = arguments.get("task_id")
            if not isinstance(task_id, str) or not task_id.strip():
                return JSONResponse(
                    status_code=200,
                    content=_mcp_error(payload_id, -32602, "task_id is required"),
                    headers=proxy_headers(),
                )

            try:
                task_id = sanitize_id(task_id)
            except ValueError as exc:
                return JSONResponse(
                    status_code=200,
                    content=_mcp_error(payload_id, -32602, str(exc)),
                    headers=proxy_headers(),
                )

            existing = await task_store.get_task(task_id)
            if existing is None:
                return JSONResponse(
                    status_code=200,
                    content=_mcp_error(payload_id, -32000, "Task not found"),
                    headers=proxy_headers(),
                )

            await task_scheduler.delete_task(task_id)
            return JSONResponse(
                status_code=200,
                content=_mcp_result(
                    payload_id,
                    {
                        "ok": True,
                    },
                ),
                headers=proxy_headers(),
            )

    @app.get("/v1/conversations/canonical")
    async def canonical_conversation(request: Request) -> JSONResponse:
        cwd_value = request.query_params.get("cwd")
        if not isinstance(cwd_value, str) or not cwd_value.strip():
            return json_error(400, error="bad_request", message="cwd is required.")

        candidate_id = manager.new_conversation_id()
        try:
            canonical_id = await manager.resolve_conversation_id(
                conversation_id=candidate_id,
                cwd=cwd_value,
                conversation_group=None,
            )
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        created = canonical_id == candidate_id
        await manager.ensure_cwd_binding(
            conversation_id=canonical_id,
            cwd=cwd_value,
            conversation_group=None,
        )

        return JSONResponse(
            status_code=200,
            content={"canonical_id": canonical_id, "cwd": cwd_value, "created": created},
            headers=proxy_headers(),
        )

    @app.post("/v1/conversations/activate")
    async def activate_conversation(request: Request) -> JSONResponse:
        body: dict[str, Any] = await request.json()

        conversation_id = body.get("conversation_id") or body.get("session_id")
        if conversation_id is None:
            return json_error(400, error="bad_request", message="conversation_id is required.")
        if not isinstance(conversation_id, str):
            return json_error(400, error="bad_request", message="conversation_id must be a string.")
        try:
            conversation_id = sanitize_id(conversation_id)
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        group_value = body.get("conversation_group")
        conversation_group: str | None = None
        if group_value is not None:
            if not isinstance(group_value, str):
                return json_error(400, error="bad_request", message="conversation_group must be a string.")
            group_value = group_value.strip()
            if group_value:
                try:
                    conversation_group = sanitize_id(group_value)
                except ValueError as exc:
                    return json_error(400, error="bad_request", message=str(exc))

        cwd_value = body.get("cwd")
        if not isinstance(cwd_value, str) or not cwd_value.strip():
            return json_error(400, error="bad_request", message="cwd is required.")

        try:
            previous_id = await manager.activate_conversation(
                conversation_id=conversation_id,
                cwd=cwd_value,
                conversation_group=conversation_group,
            )
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
        except ConversationGroupMismatchError as exc:
            return json_error(
                409,
                error="conversation_group_mismatch",
                conversation_id=conversation_id,
                expected_group=exc.expected_group,
                got_group=exc.got_group,
            )
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        await manager.log_cwd_event(
            cwd=cwd_value,
            event="activate",
            payload={
                "conversation_id": conversation_id,
                "previous_id": previous_id,
                "conversation_group": conversation_group,
            },
            version=version,
            started_at=started_at,
        )

        return JSONResponse(
            status_code=200,
            content={
                "conversation_id": conversation_id,
                "canonical_id": conversation_id,
                "previous_id": previous_id,
            },
            headers=proxy_headers(),
        )

    @app.post("/v1/agent/stream")
    async def agent_stream(request: Request) -> StreamingResponse:
        body: dict[str, Any] = await request.json()

        agent_id = body.get("agent_id")
        if agent_id is not None:
            if not isinstance(agent_id, str) or not agent_id.strip():
                return json_error(400, error="bad_request", message="agent_id must be a non-empty string.")
            try:
                body["agent_id"] = normalize_agent_id(agent_id)
            except ValueError as exc:
                return json_error(400, error="bad_request", message=str(exc))

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
        incoming_conversation_id = conversation_id

        group_value = body.get("conversation_group")
        conversation_group: str | None = None
        if group_value is not None:
            if not isinstance(group_value, str):
                return json_error(400, error="bad_request", message="conversation_group must be a string.")
            group_value = group_value.strip()
            if group_value:
                try:
                    conversation_group = sanitize_id(group_value)
                except ValueError as exc:
                    return json_error(400, error="bad_request", message=str(exc))

        since = parse_int(request.headers.get("Last-Event-ID"), default=0)
        if since is None:
            return json_error(400, error="bad_request", message="Invalid Last-Event-ID header.")

        cwd_value = body.get("cwd")
        cwd = cwd_value if isinstance(cwd_value, str) else None
        try:
            conversation_id = await manager.resolve_conversation_id(
                conversation_id=conversation_id,
                cwd=cwd,
                conversation_group=conversation_group,
            )
        except ConversationCwdMismatchError as exc:
            return json_error(
                409,
                error="conversation_cwd_mismatch",
                conversation_id=conversation_id,
                expected_cwd=exc.expected_cwd,
                got_cwd=exc.got_cwd,
            )
        except ConversationGroupMismatchError as exc:
            return json_error(
                409,
                error="conversation_group_mismatch",
                conversation_id=conversation_id,
                expected_group=exc.expected_group,
                got_group=exc.got_group,
            )
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        conversation = await manager.get_or_create_conversation(conversation_id)
        async with conversation.lock:
            last_eid = conversation.last_event_id
            renderable_count = conversation.renderable_bubble_count
        await manager.log_cwd_event(
            cwd=cwd or conversation.cwd,
            event="stream",
            payload={
                "incoming_conversation_id": incoming_conversation_id,
                "resolved_conversation_id": conversation_id,
                "alias_used": incoming_conversation_id != conversation_id,
                "since": since,
                "has_prompt": bool(prompt),
                "is_running": conversation.is_running,
                "conversation_group": conversation_group,
            },
            version=version,
            started_at=started_at,
        )

        if conversation.is_running:
            if prompt is not None and prompt != conversation.prompt:
                return json_error(409, error="conversation_already_running", conversation_id=conversation_id)
            if not isinstance(cwd_value, str):
                return json_error(400, error="bad_request", message="cwd is required to attach.")
            try:
                await manager.ensure_cwd_binding(
                    conversation_id=conversation_id,
                    cwd=cwd_value,
                    conversation_group=conversation_group,
                )
            except ConversationCwdMismatchError as exc:
                return json_error(
                    409,
                    error="conversation_cwd_mismatch",
                    conversation_id=conversation_id,
                    expected_cwd=exc.expected_cwd,
                    got_cwd=exc.got_cwd,
                )
            except ConversationGroupMismatchError as exc:
                return json_error(
                    409,
                    error="conversation_group_mismatch",
                    conversation_id=conversation_id,
                    expected_group=exc.expected_group,
                    got_group=exc.got_group,
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
            except ConversationGroupMismatchError as exc:
                return json_error(
                    409,
                    error="conversation_group_mismatch",
                    conversation_id=conversation_id,
                    expected_group=exc.expected_group,
                    got_group=exc.got_group,
                )
            except ValueError as exc:
                return json_error(400, error="bad_request", message=str(exc))

        stream = manager.sse_stream(conversation_id=conversation_id, since=since, request=request)
        return StreamingResponse(
            stream,
            media_type="text/event-stream",
            headers=proxy_headers(
                {
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                    "X-Proxy-Last-Event-Id": str(last_eid),
                    "X-Proxy-Renderable-Assistant-Count": str(renderable_count),
                }
            ),
        )

    @app.post("/v1/agent/tool_permission")
    async def tool_permission(request: Request) -> JSONResponse:
        body: dict[str, Any] = await request.json()

        permission_id = body.get("permission_id")
        if not isinstance(permission_id, str) or not permission_id.strip():
            return json_error(400, error="bad_request", message="permission_id is required.")
        try:
            permission_id = sanitize_id(permission_id)
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        behavior = body.get("behavior")
        if behavior not in ("allow", "deny"):
            return json_error(400, error="bad_request", message="behavior must be allow or deny.")

        message = body.get("message")
        if message is not None and not isinstance(message, str):
            return json_error(400, error="bad_request", message="message must be a string.")

        conversation_id = body.get("conversation_id")
        if conversation_id is not None:
            if not isinstance(conversation_id, str):
                return json_error(400, error="bad_request", message="conversation_id must be a string.")
            try:
                conversation_id = sanitize_id(conversation_id)
            except ValueError as exc:
                return json_error(400, error="bad_request", message=str(exc))

        resolved = await manager.resolve_tool_permission(
            permission_id=permission_id,
            behavior=behavior,
            message=message,
            conversation_id=conversation_id,
        )
        if not resolved:
            return json_error(
                404,
                error="permission_not_found",
                permission_id=permission_id,
                message="Permission request expired or was already handled.",
            )

        cwd_value = body.get("cwd")
        if isinstance(cwd_value, str) and cwd_value.strip():
            await manager.log_cwd_event(
                cwd=cwd_value,
                event="tool_permission_decision",
                payload={
                    "permission_id": permission_id,
                    "behavior": behavior,
                    "conversation_id": conversation_id,
                },
                version=version,
                started_at=started_at,
            )

        return JSONResponse(status_code=200, content={"ok": True}, headers=proxy_headers())

    @app.get("/v1/conversations/{conversation_id}/events")
    async def replay(conversation_id: str, request: Request) -> StreamingResponse:
        try:
            conversation_id = sanitize_id(conversation_id)
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))
        incoming_conversation_id = conversation_id
        since = parse_int(request.query_params.get("since"), default=0)
        if since is None:
            return json_error(400, error="bad_request", message="Invalid since parameter.")

        cwd = request.query_params.get("cwd")
        group_value = request.query_params.get("conversation_group")
        conversation_group: str | None = None
        if group_value is not None:
            group_value = group_value.strip()
            if group_value:
                try:
                    conversation_group = sanitize_id(group_value)
                except ValueError as exc:
                    return json_error(400, error="bad_request", message=str(exc))

        resolved = await manager.resolve_existing_conversation_id(
            conversation_id=conversation_id,
            cwd=cwd,
            conversation_group=conversation_group,
        )
        if resolved is None:
            return json_error(404, error="conversation_unknown", reset=True, conversation_id=conversation_id)
        conversation_id = resolved
        conversation = await manager.get_or_create_conversation(conversation_id)
        async with conversation.lock:
            last_eid = conversation.last_event_id
            renderable_count = conversation.renderable_bubble_count
        alias_used = incoming_conversation_id != resolved
        await manager.log_cwd_event(
            cwd=cwd or conversation.cwd,
            event="replay",
            payload={
                "incoming_conversation_id": incoming_conversation_id,
                "resolved_conversation_id": resolved,
                "alias_used": alias_used,
                "since": since,
                "cwd_param_provided": bool(cwd),
                "conversation_group": conversation_group,
            },
            version=version,
            started_at=started_at,
        )

        if alias_used and since > 0:
            payload = {
                "type": "proxy_session",
                "event": "switched",
                "canonical_id": resolved,
                "previous_id": incoming_conversation_id,
                "cwd": cwd or conversation.cwd or "",
            }
            json_line = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

            async def iter_ndjson():
                yield (json_line + "\n").encode("utf-8")

            return StreamingResponse(
                iter_ndjson(),
                media_type="application/x-ndjson",
                headers=proxy_headers(
                    {
                        "X-Proxy-Last-Event-Id": str(last_eid),
                        "X-Proxy-Renderable-Assistant-Count": str(renderable_count),
                    }
                ),
            )

        async def iter_ndjson():
            async for line in manager.iter_ndjson(conversation_id=conversation_id, since=since):
                yield line.encode("utf-8")

        return StreamingResponse(
            iter_ndjson(),
            media_type="application/x-ndjson",
            headers=proxy_headers(
                {
                    "Cache-Control": "no-cache",
                    "X-Proxy-Last-Event-Id": str(last_eid),
                    "X-Proxy-Renderable-Assistant-Count": str(renderable_count),
                }
            ),
        )

    @app.get("/v1/agent/tasks")
    async def list_tasks(
        agent_id: str | None = None,
        conversation_id: str | None = None,
        conversation_group: str | None = None,
    ) -> JSONResponse:
        try:
            agent_value = normalize_agent_id(agent_id) if agent_id else None
            conversation_value = sanitize_id(conversation_id) if conversation_id else None
            group_value = sanitize_id(conversation_group) if conversation_group else None
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        tasks = await task_store.list_tasks(
            agent_id=agent_value,
            conversation_id=conversation_value,
            conversation_group=group_value,
        )
        return JSONResponse(
            status_code=200,
            content={"tasks": [serialize_task(task) for task in tasks]},
            headers=proxy_headers(),
        )

    def _active_opencode_payload_values(payload: dict[str, Any]) -> tuple[str, str | None, str, str | None]:
        agent_id = payload.get("agent_id")
        if not isinstance(agent_id, str) or not agent_id.strip():
            raise ValueError("agent_id is required.")
        agent_id = normalize_agent_id(agent_id)

        conversation_group = payload.get("conversation_group")
        if isinstance(conversation_group, str) and conversation_group.strip():
            group_value = sanitize_id(conversation_group)
        else:
            group_value = None

        cwd = payload.get("cwd")
        if not isinstance(cwd, str) or not cwd.strip():
            raise ValueError("cwd is required.")
        cwd_value = cwd.strip()

        session_id = payload.get("session_id") or payload.get("open_code_session_id")
        session_value = session_id.strip() if isinstance(session_id, str) and session_id.strip() else None
        return agent_id, group_value, cwd_value, session_value

    @app.get("/v1/opencode/active-session")
    async def get_active_opencode_session(
        agent_id: str,
        cwd: str,
        conversation_group: str | None = None,
    ) -> JSONResponse:
        try:
            agent_value = normalize_agent_id(agent_id)
            group_value = sanitize_id(conversation_group) if conversation_group else None
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))
        cwd_value = cwd.strip()
        if not cwd_value:
            return json_error(400, error="bad_request", message="cwd is required.")

        session_id = await task_store.get_active_opencode_session(
            agent_id=agent_value,
            conversation_group=group_value,
            cwd=cwd_value,
        )
        return JSONResponse(
            status_code=200,
            content={
                "agent_id": agent_value,
                "conversation_group": group_value,
                "cwd": cwd_value,
                "session_id": session_id,
            },
            headers=proxy_headers(),
        )

    @app.post("/v1/opencode/active-session")
    async def set_active_opencode_session(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
        except Exception:
            return json_error(400, error="bad_request", message="Invalid JSON payload.")
        if not isinstance(payload, dict):
            return json_error(400, error="bad_request", message="Payload must be an object.")

        try:
            agent_id, conversation_group, cwd, session_id = _active_opencode_payload_values(payload)
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        if session_id:
            await task_store.save_active_opencode_session(
                agent_id=agent_id,
                conversation_group=conversation_group,
                cwd=cwd,
                session_id=session_id,
            )
        else:
            await task_store.clear_active_opencode_session(
                agent_id=agent_id,
                conversation_group=conversation_group,
                cwd=cwd,
            )

        return JSONResponse(
            status_code=200,
            content={
                "ok": True,
                "agent_id": agent_id,
                "conversation_group": conversation_group,
                "cwd": cwd,
                "session_id": session_id,
            },
            headers=proxy_headers(),
        )

    @app.get("/v1/push/status")
    async def push_status() -> JSONResponse:
        secret = os.environ.get("CODEAGENTS_PUSH_SECRET", "").strip()
        gateway_url = os.environ.get("CODEAGENTS_PUSH_GATEWAY_BASE_URL", "").strip()
        return JSONResponse(
            status_code=200,
            content={
                "configured": bool(secret and gateway_url),
                "has_secret": bool(secret),
                "server_key": hashlib.sha256(secret.encode("utf-8")).hexdigest() if secret else None,
                "has_gateway_url": bool(gateway_url),
                "gateway_url": gateway_url or None,
            },
            headers=proxy_headers(),
        )

    @app.post("/v1/agent/tasks")
    async def create_task(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
        except Exception:
            return json_error(400, error="bad_request", message="Invalid JSON payload.")

        try:
            record = parse_task_payload(payload)
        except (TaskValidationError, ValueError) as exc:
            return json_error(400, error="bad_request", message=str(exc))

        stored = await task_scheduler.create_task(record)
        return JSONResponse(
            status_code=201,
            content={"task": serialize_task(stored)},
            headers=proxy_headers(),
        )

    @app.patch("/v1/agent/tasks/{task_id}")
    async def update_task(task_id: str, request: Request) -> JSONResponse:
        try:
            task_id = sanitize_id(task_id)
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        existing = await task_store.get_task(task_id)
        if existing is None:
            return json_error(404, error="task_not_found", task_id=task_id)

        try:
            payload = await request.json()
        except Exception:
            return json_error(400, error="bad_request", message="Invalid JSON payload.")

        try:
            updated = update_task_from_payload(existing, payload)
        except (TaskValidationError, ValueError) as exc:
            return json_error(400, error="bad_request", message=str(exc))

        stored = await task_scheduler.update_task(task_id, updated)
        return JSONResponse(
            status_code=200,
            content={"task": serialize_task(stored)},
            headers=proxy_headers(),
        )

    @app.delete("/v1/agent/tasks/{task_id}")
    async def delete_task(task_id: str) -> JSONResponse:
        try:
            task_id = sanitize_id(task_id)
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        existing = await task_store.get_task(task_id)
        if existing is None:
            return json_error(404, error="task_not_found", task_id=task_id)

        await task_scheduler.delete_task(task_id)
        return JSONResponse(
            status_code=200,
            content={"ok": True},
            headers=proxy_headers(),
        )

    @app.post("/v1/agent/tasks/{task_id}/run")
    async def run_task_now(task_id: str) -> JSONResponse:
        """Start a manual run immediately without advancing the schedule."""
        try:
            task_id = sanitize_id(task_id)
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        existing = await task_store.get_task(task_id)
        if existing is None:
            return json_error(404, error="task_not_found", task_id=task_id)

        try:
            stored = await task_scheduler.run_now(task_id)
        except KeyError:
            return json_error(404, error="task_not_found", task_id=task_id)
        except Exception as exc:
            logger.exception("Failed to start manual task run: %s", task_id)
            return json_error(500, error="task_run_failed", message=str(exc))

        return JSONResponse(
            status_code=202,
            content={
                "ok": True,
                "started": True,
                "task": serialize_task(stored),
            },
            headers=proxy_headers(),
        )

    @app.get("/v1/agent/env")
    async def list_env(agent_id: str | None = None) -> JSONResponse:
        if not agent_id:
            return json_error(400, error="bad_request", message="agent_id is required.")
        try:
            agent_value = normalize_agent_id(agent_id)
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        env = await task_store.list_env(agent_id=agent_value)
        # Never return plaintext secret values after creation — mask for listing.
        masked: list[dict[str, Any]] = []
        for item in env:
            if not isinstance(item, dict):
                continue
            entry = dict(item)
            raw_value = entry.get("value")
            if isinstance(raw_value, str):
                entry["value"] = _mask_secret_value(raw_value)
                entry["has_value"] = bool(raw_value)
            masked.append(entry)
        return JSONResponse(
            status_code=200,
            content={"env": masked},
            headers=proxy_headers(),
        )

    @app.put("/v1/agent/env")
    async def replace_env(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
        except Exception:
            return json_error(400, error="bad_request", message="Invalid JSON payload.")

        agent_id = payload.get("agent_id")
        if not isinstance(agent_id, str) or not agent_id.strip():
            return json_error(400, error="bad_request", message="agent_id is required.")
        try:
            agent_value = normalize_agent_id(agent_id)
        except ValueError as exc:
            return json_error(400, error="bad_request", message=str(exc))

        env_payload = payload.get("env")
        if env_payload is None:
            env_payload = []
        if not isinstance(env_payload, list):
            return json_error(400, error="bad_request", message="env must be a list.")

        env: list[dict[str, Any]] = []
        seen = set()
        for item in env_payload:
            if not isinstance(item, dict):
                return json_error(400, error="bad_request", message="env entries must be objects.")

            raw_key = item.get("key")
            raw_value = item.get("value")
            enabled_value = item.get("enabled", True)

            if not isinstance(raw_key, str) or not raw_key.strip():
                return json_error(400, error="bad_request", message="env.key must be a string.")
            if not isinstance(raw_value, str):
                return json_error(400, error="bad_request", message="env.value must be a string.")

            try:
                key = _normalize_env_key(raw_key)
            except ValueError as exc:
                return json_error(400, error="bad_request", message=str(exc))

            if key in seen:
                return json_error(400, error="bad_request", message="Duplicate env key.")
            seen.add(key)

            enabled = bool(enabled_value)
            env.append({"key": key, "value": raw_value, "enabled": enabled})

        await task_store.replace_env(agent_id=agent_value, env=env)
        return JSONResponse(status_code=200, content={"ok": True}, headers=proxy_headers())

    return app


app = create_app()
