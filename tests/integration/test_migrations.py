import asyncio
import os
import subprocess
import sys
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.health import InfrastructureHealthProbe, check_readiness

pytestmark = pytest.mark.integration


def test_migrations_round_trip_model_drift_and_owner_guard(urls: dict[str, str]) -> None:
    """Round-trip a separate database so the API's live database remains intact."""
    admin_url = make_url(urls["TEST_DATABASE_ADMIN_URL"])
    db_name = f"migration_{uuid4().hex}_test"
    migration_url = admin_url.set(database=db_name).render_as_string(hide_password=False)

    async def database_ddl(statement: str) -> None:
        engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT", hide_parameters=True)
        try:
            async with engine.connect() as connection:
                await connection.execute(text(statement))
        finally:
            await engine.dispose()

    async def table_count() -> int:
        engine = create_async_engine(migration_url, hide_parameters=True)
        try:
            async with engine.connect() as connection:
                return int(
                    await connection.scalar(
                        text("SELECT count(*) FROM pg_tables WHERE schemaname='public'")
                    )
                )
        finally:
            await engine.dispose()

    async def check_runtime_ready(expected: str, *, make_owner: bool = False) -> None:
        if make_owner:
            admin = create_async_engine(migration_url, hide_parameters=True)
            try:
                async with admin.begin() as connection:
                    await connection.execute(text("ALTER TABLE users OWNER TO pkb_app"))
            finally:
                await admin.dispose()
        runtime_url = make_url(urls["TEST_DATABASE_URL"]).set(database=db_name)
        engine = create_async_engine(runtime_url, hide_parameters=True)
        client = Redis.from_url(urls["TEST_REDIS_URL"])
        try:
            status = await check_readiness(InfrastructureHealthProbe(engine, client), 5)
            assert status["database"] == expected
        finally:
            await client.aclose()
            await engine.dispose()

    environment = dict(os.environ, DATABASE_ADMIN_URL=migration_url)
    asyncio.run(database_ddl(f'CREATE DATABASE "{db_name}"'))
    try:
        asyncio.run(check_runtime_ready("down"))  # Missing migration is not ready.
        for arguments in (["upgrade", "head"], ["check"], ["downgrade", "base"]):
            subprocess.run(
                [sys.executable, "-m", "alembic", *arguments], env=environment, check=True
            )
        assert asyncio.run(table_count()) == 1  # only alembic_version remains
        subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"], env=environment, check=True
        )
        assert asyncio.run(table_count()) == 15  # includes questions, dispatches and confirmations
        subprocess.run([sys.executable, "-m", "alembic", "check"], env=environment, check=True)
        asyncio.run(check_runtime_ready("up"))
        asyncio.run(check_runtime_ready("down", make_owner=True))
    finally:
        asyncio.run(database_ddl(f'DROP DATABASE "{db_name}" WITH (FORCE)'))
