from pathlib import Path

import httpx
import pytest

import app as app_module
from claude_proxy.opencode_client import OpenCodeClient, parse_env_file


def test_parse_env_file_reads_quoted_opencode_password(tmp_path: Path):
    env_path = tmp_path / "opencode.env"
    env_path.write_text(
        """
        # comment
        OPENCODE_SERVER_USERNAME="opencode"
        OPENCODE_SERVER_PASSWORD="fixture password"
        OPENCODE_BASE_URL=http://127.0.0.1:4096
        """,
        encoding="utf-8",
    )

    values = parse_env_file(env_path)

    assert values["OPENCODE_SERVER_USERNAME"] == "opencode"
    assert values["OPENCODE_SERVER_PASSWORD"] == "fixture password"
    assert values["OPENCODE_BASE_URL"] == "http://127.0.0.1:4096"


def test_client_from_environment_prefers_process_env_over_file(tmp_path: Path):
    env_path = tmp_path / "opencode.env"
    env_path.write_text(
        """
        OPENCODE_SERVER_USERNAME="file-user"
        OPENCODE_SERVER_PASSWORD="file-password"
        OPENCODE_BASE_URL=http://127.0.0.1:4096
        """,
        encoding="utf-8",
    )

    client = OpenCodeClient.from_environment(
        env={
            "OPENCODE_SERVER_PASSWORD": "env-password",
            "OPENCODE_BASE_URL": "http://127.0.0.1:4097",
        },
        env_file_paths=(env_path,),
    )

    assert client.base_url == "http://127.0.0.1:4097"
    assert client.username == "file-user"
    assert client.password == "env-password"


@pytest.mark.asyncio
async def test_health_uses_basic_auth_when_password_configured():
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Basic b3BlbmNvZGU6Zml4dHVyZV9wYXNzd29yZA=="
        assert request.url.path == "/global/health"
        return httpx.Response(200, json={"healthy": True, "version": "1.14.21"})

    client = OpenCodeClient(
        username="opencode",
        password="fixture_password",
        transport=httpx.MockTransport(handler),
    )

    assert await client.health_status() == {"healthy": True, "version": "1.14.21"}


@pytest.mark.asyncio
async def test_health_status_reports_unavailable_on_error():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "not ready"})

    client = OpenCodeClient(transport=httpx.MockTransport(handler))

    status = await client.health_status()

    assert status["healthy"] is False
    assert "503" in status["error"]


@pytest.mark.asyncio
async def test_healthz_includes_opencode_status(monkeypatch, tmp_path: Path):
    class FakeOpenCodeClient:
        async def health_status(self):
            return {"healthy": True, "version": "fixture"}

    monkeypatch.setattr(
        app_module.OpenCodeClient,
        "from_environment",
        staticmethod(lambda: FakeOpenCodeClient()),
    )
    app = app_module.create_app(store_dir=tmp_path)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["opencode"] == {"healthy": True, "version": "fixture"}
