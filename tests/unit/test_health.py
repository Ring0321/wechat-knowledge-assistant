import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from redis.exceptions import ConnectionError as RedisConnectionError

from app.core.config import Settings
from app.core.health import check_readiness
from app.main import create_app


class FakeProbe:
    def __init__(self, failure: str | None = None, delay: float = 0) -> None:
        self.failure = failure
        self.delay = delay

    async def database(self) -> None:
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.failure == "database":
            raise OSError("postgresql://secret:password@host")

    async def redis(self) -> None:
        if self.failure == "redis":
            raise RedisConnectionError("redis://secret:password@host")


@asynccontextmanager
async def client_for(probe: FakeProbe) -> AsyncIterator[AsyncClient]:
    config = Settings(
        database_url=SecretStr("postgresql+asyncpg://app:secret@localhost/db"),
        redis_url=SecretStr("redis://localhost/0"),
    )
    app = create_app(config, probe)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client,
    ):
        yield client


async def test_liveness_readiness_and_request_id() -> None:
    async with client_for(FakeProbe()) as client:
        live = await client.get("/health/live", headers={"X-Request-ID": "untrusted-input"})
        assert live.status_code == 200
        assert live.json() == {"status": "ok"}
        UUID(live.headers["X-Request-ID"])
        ready = await client.get("/health/ready")
        assert ready.json() == {
            "status": "ready",
            "dependencies": {"database": "up", "redis": "up"},
        }
        assert (await client.post("/wecom/callback")).status_code == 404


@pytest.mark.parametrize("dependency", ["database", "redis"])
async def test_unavailable_dependency_is_503_without_secret_leak(dependency: str) -> None:
    async with client_for(FakeProbe(dependency)) as client:
        assert (await client.get("/health/live")).status_code == 200
        ready = await client.get("/health/ready")
        assert ready.status_code == 503
        assert ready.json()["dependencies"][dependency] == "down"
        assert "secret" not in ready.text
        assert "password" not in ready.text


async def test_hanging_dependency_is_bounded() -> None:
    result = await check_readiness(FakeProbe(delay=1), 0.01)
    assert result == {"database": "down", "redis": "up"}


async def test_unexpected_exception_is_generic_500() -> None:
    class BrokenProbe(FakeProbe):
        async def database(self) -> None:
            raise ValueError("secret unexpected error")

    async with client_for(BrokenProbe()) as client:
        response = await client.get("/health/ready")
        assert response.status_code == 500
        assert response.json() == {"detail": "Internal server error"}
        assert response.headers["X-Request-ID"]
