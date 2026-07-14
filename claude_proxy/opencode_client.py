from __future__ import annotations

import os
import shlex
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote

import httpx


DEFAULT_OPENCODE_BASE_URL = "http://127.0.0.1:4096"
DEFAULT_OPENCODE_USERNAME = "opencode"
DEFAULT_OPENCODE_ENV_FILE = Path("/etc/opencode-server.env")


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        value = raw_value.strip()
        try:
            parsed = shlex.split(value, comments=False, posix=True)
        except ValueError:
            parsed = []
        values[key] = parsed[0] if len(parsed) == 1 else value.strip("\"'")

    return values


class OpenCodeClient:
    def __init__(
        self,
        *,
        base_url: str = DEFAULT_OPENCODE_BASE_URL,
        username: str = DEFAULT_OPENCODE_USERNAME,
        password: str | None = None,
        timeout: float = 2.0,
        transport: httpx.AsyncBaseTransport | None = None,
        environment: Mapping[str, str] | None = None,
        env_file_paths: tuple[Path, ...] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self._transport = transport
        self._environment = environment
        self._env_file_paths = env_file_paths

    @classmethod
    def from_environment(
        cls,
        *,
        env: Mapping[str, str] | None = None,
        env_file_paths: tuple[Path, ...] = (DEFAULT_OPENCODE_ENV_FILE,),
        timeout: float = 2.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> "OpenCodeClient":
        resolved_env = os.environ if env is None else env
        env_file_values: dict[str, str] = {}
        for path in env_file_paths:
            env_file_values.update(parse_env_file(path))

        base_url = (
            resolved_env.get("OPENCODE_BASE_URL")
            or env_file_values.get("OPENCODE_BASE_URL")
            or DEFAULT_OPENCODE_BASE_URL
        )
        username = (
            resolved_env.get("OPENCODE_SERVER_USERNAME")
            or env_file_values.get("OPENCODE_SERVER_USERNAME")
            or DEFAULT_OPENCODE_USERNAME
        )
        password = resolved_env.get("OPENCODE_SERVER_PASSWORD") or env_file_values.get("OPENCODE_SERVER_PASSWORD")

        return cls(
            base_url=base_url,
            username=username,
            password=password,
            timeout=timeout,
            transport=transport,
            environment=resolved_env,
            env_file_paths=env_file_paths,
        )

    def _reload_environment(self) -> tuple[str, str, str | None]:
        if self._environment is None or self._env_file_paths is None:
            return self.base_url, self.username, self.password

        env_file_values: dict[str, str] = {}
        for path in self._env_file_paths:
            env_file_values.update(parse_env_file(path))

        self.base_url = (
            self._environment.get("OPENCODE_BASE_URL")
            or env_file_values.get("OPENCODE_BASE_URL")
            or DEFAULT_OPENCODE_BASE_URL
        ).rstrip("/")
        self.username = (
            self._environment.get("OPENCODE_SERVER_USERNAME")
            or env_file_values.get("OPENCODE_SERVER_USERNAME")
            or DEFAULT_OPENCODE_USERNAME
        )
        self.password = (
            self._environment.get("OPENCODE_SERVER_PASSWORD")
            or env_file_values.get("OPENCODE_SERVER_PASSWORD")
        )
        return self.base_url, self.username, self.password

    async def _request_once(
        self,
        method: str,
        path: str,
        *,
        json: Mapping[str, Any] | None,
        params: Mapping[str, Any] | None,
        base_url: str,
        username: str,
        password: str | None,
    ) -> httpx.Response:
        auth: tuple[str, str] | None = None
        if password:
            auth = (username, password)

        async with httpx.AsyncClient(
            base_url=base_url,
            timeout=self.timeout,
            auth=auth,
            transport=self._transport,
        ) as client:
            return await client.request(method, path, json=json, params=params)

    async def request(
        self,
        method: str,
        path: str,
        *,
        json: Mapping[str, Any] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        if not path.startswith("/"):
            raise ValueError("OpenCode path must start with /")

        initial_config = (self.base_url, self.username, self.password)
        response = await self._request_once(
            method,
            path,
            json=json,
            params=params,
            base_url=initial_config[0],
            username=initial_config[1],
            password=initial_config[2],
        )

        # OpenCode credentials can be rotated independently of this long-lived
        # daemon. A 401 is rejected before OpenCode executes the request, so it
        # is safe to reload the managed env file and retry once when auth changed.
        if response.status_code == 401 and self._env_file_paths is not None:
            refreshed_config = self._reload_environment()
            if refreshed_config != initial_config:
                response = await self._request_once(
                    method,
                    path,
                    json=json,
                    params=params,
                    base_url=refreshed_config[0],
                    username=refreshed_config[1],
                    password=refreshed_config[2],
                )

        response.raise_for_status()
        if not response.content:
            return None
        return response.json()

    async def health(self) -> dict[str, Any]:
        response = await self.request("GET", "/global/health")
        if not isinstance(response, dict):
            raise ValueError("OpenCode health response must be an object")
        return response

    async def health_status(self) -> dict[str, Any]:
        try:
            health = await self.health()
        except Exception as exc:
            return {"healthy": False, "error": str(exc)}

        return {
            "healthy": bool(health.get("healthy")),
            "version": health.get("version"),
        }

    async def create_session(
        self,
        *,
        title: str | None = None,
        parent_id: str | None = None,
        directory: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if title:
            payload["title"] = title
        if parent_id:
            payload["parentID"] = parent_id

        response = await self.request(
            "POST",
            "/session",
            json=payload,
            params={"directory": directory} if directory else None,
        )
        if not isinstance(response, dict):
            raise ValueError("OpenCode create session response must be an object")
        return response

    async def session_status(self, *, directory: str | None = None) -> dict[str, Any]:
        response = await self.request(
            "GET",
            "/session/status",
            params={"directory": directory} if directory else None,
        )
        if not isinstance(response, dict):
            raise ValueError("OpenCode session status response must be an object")
        return response

    async def prompt_async(self, *, session_id: str, prompt: str, directory: str | None = None) -> None:
        await self.request(
            "POST",
            f"/session/{quote(session_id, safe='')}/prompt_async",
            json={"parts": [{"type": "text", "text": prompt}]},
            params={"directory": directory} if directory else None,
        )

    async def session_messages(
        self,
        *,
        session_id: str,
        directory: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        params: dict[str, Any] = {}
        if directory:
            params["directory"] = directory
        if limit is not None:
            params["limit"] = limit

        response = await self.request(
            "GET",
            f"/session/{quote(session_id, safe='')}/message",
            params=params or None,
        )
        if not isinstance(response, list):
            raise ValueError("OpenCode session messages response must be an array")
        return response
