"""Small health port plus concrete PostgreSQL/Redis adapter."""

import asyncio
import logging
from typing import Protocol

from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)
SCHEMA_REVISION = "0005_agent"


class HealthProbe(Protocol):
    async def database(self) -> None: ...

    async def redis(self) -> None: ...


class InfrastructureHealthProbe:
    def __init__(
        self, engine: AsyncEngine, redis_client: Redis, *, require_durability: bool = False
    ) -> None:
        self.engine = engine
        self.redis_client = redis_client
        self.require_durability = require_durability

    async def database(self) -> None:
        async with self.engine.connect() as connection:
            revision = await connection.scalar(text("SELECT version_num FROM alembic_version"))
            if revision != SCHEMA_REVISION:
                raise RuntimeError("Schema revision is not current")
            unsafe_role = await connection.scalar(
                text(
                    "SELECT EXISTS (SELECT 1 FROM pg_roles r "
                    "WHERE (r.rolsuper OR r.rolbypassrls) "
                    "AND pg_has_role(current_user, r.oid, 'MEMBER')) "
                    "OR EXISTS (SELECT 1 FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname='public' AND c.relname IN "
                    "('users','sources','assets','conversations','messages','ingestion_jobs',"
                    "'wecom_sync_states','wecom_outbox','message_sources','ingestion_dispatches',"
                    "'knowledge_files','question_jobs','question_dispatches','agent_actions') "
                    "AND pg_has_role(current_user, c.relowner, 'MEMBER'))"
                )
            )
            if unsafe_role:
                raise RuntimeError("Runtime database role must not bypass row level security")

    async def redis(self) -> None:
        if not await self.redis_client.ping():
            raise RuntimeError("Redis ping failed")
        if self.require_durability:
            persistence = await self.redis_client.info("persistence")
            if (
                persistence.get("aof_enabled") != 1
                or persistence.get("aof_last_write_status") != "ok"
            ):
                raise RuntimeError("Redis AOF must be enabled and writable")


async def check_readiness(probe: HealthProbe, timeout_seconds: float) -> dict[str, str]:
    async def check(name: str) -> str:
        try:
            async with asyncio.timeout(timeout_seconds):
                if name == "database":
                    await probe.database()
                else:
                    await probe.redis()
        except (SQLAlchemyError, RedisError, OSError, TimeoutError, RuntimeError):
            logger.warning("dependency_unavailable", extra={"dependency": name})
            return "down"
        return "up"

    database, redis_status = await asyncio.gather(check("database"), check("redis"))
    return {"database": database, "redis": redis_status}
