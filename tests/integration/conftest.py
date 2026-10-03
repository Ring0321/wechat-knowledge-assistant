"""Real services only. Provisioned by the disposable Compose verification project."""

import os
import subprocess
import sys
from collections.abc import AsyncIterator

import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


@pytest.fixture(scope="session")
def urls() -> dict[str, str]:
    names = (
        "TEST_DATABASE_ADMIN_URL",
        "TEST_DATABASE_URL",
        "TEST_REDIS_URL",
        "TEST_CONNECTOR_DATABASE_URL",
    )
    if not all(os.environ.get(name) for name in names):
        pytest.skip("Real-service URLs absent; run scripts/verify.py for the full gate")
    values = {name: os.environ[name] for name in names}
    admin, runtime = (make_url(values[name]) for name in names[:2])
    if not admin.database or not admin.database.endswith("_test"):
        pytest.fail("Refusing integration tests outside an explicitly named _test database")
    if (admin.host, admin.port, admin.database) != (runtime.host, runtime.port, runtime.database):
        pytest.fail("Admin and runtime URLs must refer to the same disposable database")
    connector = make_url(values["TEST_CONNECTOR_DATABASE_URL"])
    if (admin.host, admin.port, admin.database) != (
        connector.host,
        connector.port,
        connector.database,
    ):
        pytest.fail("Connector URL must refer to the same disposable database")
    environment = dict(os.environ, DATABASE_ADMIN_URL=values["TEST_DATABASE_ADMIN_URL"])
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"], env=environment, check=True
    )
    return values


@pytest.fixture
async def admin_engine(urls: dict[str, str]) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(urls["TEST_DATABASE_ADMIN_URL"], hide_parameters=True)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def runtime_engine(urls: dict[str, str]) -> AsyncIterator[AsyncEngine]:
    # One pooled connection makes transaction-local identity leakage deterministic to test.
    engine = create_async_engine(
        urls["TEST_DATABASE_URL"], pool_size=1, max_overflow=0, hide_parameters=True
    )
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def connector_engine(urls: dict[str, str]) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(
        urls["TEST_CONNECTOR_DATABASE_URL"], pool_size=3, max_overflow=0, hide_parameters=True
    )
    try:
        yield engine
    finally:
        await engine.dispose()
