import json
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
async def test_client_reloads_rotated_environment_credentials_after_401(tmp_path: Path):
    env_path = tmp_path / "opencode.env"
    env_path.write_text(
        "OPENCODE_SERVER_USERNAME=opencode\nOPENCODE_SERVER_PASSWORD=old-password\n",
        encoding="utf-8",
    )
    authorizations: list[str | None] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        authorizations.append(request.headers.get("authorization"))
        if len(authorizations) == 1:
            return httpx.Response(401, json={"error": "unauthorized"})
        return httpx.Response(200, json={"healthy": True, "version": "rotated"})

    client = OpenCodeClient.from_environment(
        env={},
        env_file_paths=(env_path,),
        transport=httpx.MockTransport(handler),
    )
    env_path.write_text(
        "OPENCODE_SERVER_USERNAME=opencode\nOPENCODE_SERVER_PASSWORD=new-password\n",
        encoding="utf-8",
    )

    assert await client.health_status() == {"healthy": True, "version": "rotated"}
    assert len(authorizations) == 2
    assert authorizations[0] != authorizations[1]


@pytest.mark.asyncio
async def test_health_status_reports_unavailable_on_error():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "not ready"})

    client = OpenCodeClient(transport=httpx.MockTransport(handler))

    status = await client.health_status()

    assert status["healthy"] is False
    assert "503" in status["error"]


@pytest.mark.asyncio
async def test_session_endpoints_include_directory_and_encode_session_id():
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raw_path = request.url.raw_path.decode("utf-8").split("?", 1)[0]
        if raw_path == "/session":
            assert request.url.params["directory"] == "/tmp/project"
            return httpx.Response(200, json={"id": "ses_fixture"})
        if raw_path == "/session/status":
            assert request.url.params["directory"] == "/tmp/project"
            return httpx.Response(200, json={"ses_fixture": {"type": "idle"}})
        if raw_path == "/session/ses%2Ffixture/prompt_async":
            assert request.url.params["directory"] == "/tmp/project"
            assert json.loads(request.content.decode("utf-8")) == {
                "parts": [{"type": "text", "text": "hello"}]
            }
            return httpx.Response(200)
        if raw_path == "/session/ses%2Ffixture/message":
            assert request.url.params["directory"] == "/tmp/project"
            assert request.url.params["limit"] == "10"
            return httpx.Response(200, json=[])
        return httpx.Response(404, json={"error": "unexpected"})

    client = OpenCodeClient(transport=httpx.MockTransport(handler))

    assert await client.create_session(title="Fixture", directory="/tmp/project") == {"id": "ses_fixture"}
    assert await client.session_status(directory="/tmp/project") == {"ses_fixture": {"type": "idle"}}
    await client.prompt_async(session_id="ses/fixture", prompt="hello", directory="/tmp/project")
    assert await client.session_messages(session_id="ses/fixture", directory="/tmp/project", limit=10) == []
    assert [request.method for request in requests] == ["POST", "GET", "POST", "GET"]


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
