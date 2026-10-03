from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import Settings
from app.core.health import InfrastructureHealthProbe


@asynccontextmanager
async def infrastructure(settings: Settings) -> AsyncIterator[InfrastructureHealthProbe]:
    engine = create_async_engine(
        settings.database_url.get_secret_value(),
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.health_timeout_seconds,
        connect_args={"timeout": settings.health_timeout_seconds},
        hide_parameters=True,
    )
    redis_client = Redis.from_url(
        settings.redis_url.get_secret_value(),
        decode_responses=True,
        socket_connect_timeout=settings.health_timeout_seconds,
        socket_timeout=settings.health_timeout_seconds,
    )
    try:
        yield InfrastructureHealthProbe(
            engine, redis_client, require_durability=settings.wecom_enabled
        )
    finally:
        try:
            await redis_client.aclose()
        finally:
            await engine.dispose()
