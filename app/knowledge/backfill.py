"""Bounded historical indexing from verified corporate dispatches, never re-downloads."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.db.models import Source, User
from app.db.session import corporate_session, tenant_session
from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceStatus
from app.ingestion.models import IngestionDispatch
from app.knowledge.jobs import enqueue


async def backfill(
    tenant_factory: async_sessionmaker[AsyncSession],
    connector_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    limit: int = 1000,
) -> int:
    if not settings.knowledge_enabled or not 1 <= limit <= 10000:
        raise ArtifactError("knowledge_backfill_configuration_invalid")
    count, after = 0, UUID(int=0)
    while count < limit:
        async with corporate_session(connector_factory, settings.wecom_corp_id) as session:
            users = list(
                (
                    await session.scalars(
                        select(IngestionDispatch.user_id)
                        .distinct()
                        .where(IngestionDispatch.user_id > after)
                        .order_by(IngestionDispatch.user_id)
                        .limit(100)
                    )
                ).all()
            )
        if not users:
            break
        for user_id in users:
            async with tenant_session(tenant_factory, user_id) as session:
                user = await session.get(User, user_id)
                if (
                    user is None
                    or not user.is_active
                    or user.wecom_corp_id != settings.wecom_corp_id
                    or not settings.allows_wecom_user(user.wecom_external_user_id)
                ):
                    continue
                sources = await session.scalars(
                    select(Source)
                    .where(
                        Source.status == SourceStatus.STORED,
                        Source.metadata_["index_status"].astext.in_(("deferred_m6", "disabled")),
                    )
                    .order_by(Source.created_at, Source.id)
                    .limit(limit - count)
                    .with_for_update()
                )
                for source in sources:
                    if source.text and source.text.strip():
                        await enqueue(session, source, settings)
                        count += 1
            if count >= limit:
                break
        after = users[-1]
    return count
