"""Migrations use an admin URL that is never injected into the runtime API."""

import asyncio
import os

from alembic import context
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.agent import models as agent_models  # noqa: F401
from app.connectors.wecom import persistence  # noqa: F401
from app.db import models  # noqa: F401
from app.db.base import Base
from app.ingestion import models as ingestion_models  # noqa: F401

target_metadata = Base.metadata


def database_url() -> str:
    value = os.environ.get("DATABASE_ADMIN_URL")
    if not value:
        raise RuntimeError("DATABASE_ADMIN_URL is required for migrations")
    return value


def run_migrations_offline() -> None:
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = create_async_engine(database_url(), poolclass=NullPool, hide_parameters=True)
    try:
        async with engine.connect() as connection:

            def migrate(sync_connection: Connection) -> None:
                context.configure(connection=sync_connection, target_metadata=target_metadata)
                with context.begin_transaction():
                    context.run_migrations()

            await connection.run_sync(migrate)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
