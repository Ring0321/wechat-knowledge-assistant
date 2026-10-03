import os
from uuid import uuid4

import httpx
import pytest
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.health import InfrastructureHealthProbe, check_readiness

pytestmark = pytest.mark.integration


async def test_real_redis_round_trip_and_namespaced_cleanup(urls: dict[str, str]) -> None:
    client = Redis.from_url(urls["TEST_REDIS_URL"], decode_responses=True)
    key = f"m1-test:{uuid4().hex}"
    try:
        assert await client.ping()
        assert await client.set(key, "round-trip", ex=60, nx=True)
        assert not await client.set(key, "duplicate", ex=60, nx=True)
        assert await client.get(key) == "round-trip"
        assert 0 < await client.ttl(key) <= 60
    finally:
        await client.delete(key)
        await client.aclose()


async def test_real_readiness_accepts_runtime_and_rejects_admin(
    runtime_engine: AsyncEngine, admin_engine: AsyncEngine, urls: dict[str, str]
) -> None:
    client = Redis.from_url(urls["TEST_REDIS_URL"])
    try:
        assert await check_readiness(InfrastructureHealthProbe(runtime_engine, client), 5) == {
            "database": "up",
            "redis": "up",
        }
        assert await check_readiness(InfrastructureHealthProbe(admin_engine, client), 5) == {
            "database": "down",
            "redis": "up",
        }
    finally:
        await client.aclose()


def test_running_container_health_and_no_customer_endpoints(urls: dict[str, str]) -> None:
    api_url = os.environ.get("TEST_API_URL")
    if not api_url:
        pytest.skip("TEST_API_URL absent; use scripts/verify.py for container smoke")
    with httpx.Client(base_url=api_url, timeout=10) as client:
        assert client.get("/health/live").json() == {"status": "ok"}
        response = client.get("/health/ready")
        assert response.status_code == 200
        assert response.json()["dependencies"] == {"database": "up", "redis": "up"}
        assert "x-request-id" in response.headers
        assert client.post("/wecom/callback").status_code == 404
