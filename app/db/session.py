"""A tenant context is always transaction local, including with pooled connections."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@asynccontextmanager
async def tenant_session(
    factory: async_sessionmaker[AsyncSession], user_id: UUID
) -> AsyncIterator[AsyncSession]:
    if not isinstance(user_id, UUID):
        raise TypeError("Tenant identity must be a verified UUID")
    async with factory() as session, session.begin():
        await session.execute(
            text("SELECT set_config('app.user_id', :user_id, true)"), {"user_id": str(user_id)}
        )
        yield session


@asynccontextmanager
async def corporate_session(
    factory: async_sessionmaker[AsyncSession], corp_id: str
) -> AsyncIterator[AsyncSession]:
    async with factory() as session, session.begin():
        await session.execute(
            text("SELECT set_config('app.wecom_corp_id', :corp_id, true)"), {"corp_id": corp_id}
        )
        yield session
