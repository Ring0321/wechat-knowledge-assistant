"""Rules for supplementing an existing card without replacing its identity."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import IngestionJob, KnowledgeFile, Source
from app.domain.enums import SourceStatus, SourceType

SUPPLEMENT = "channel_supplement"


async def can_supplement(session: AsyncSession, source: Source) -> bool:
    return (
        source.source_type == SourceType.WECHAT_CHANNEL
        and source.status == SourceStatus.METADATA_ONLY
        and source.vector_file_id is None
        and not source.metadata_.get("original_video_available")
        and not source.metadata_.get("supplement_original_key")
        and SUPPLEMENT not in source.metadata_
        and await session.get(KnowledgeFile, (source.user_id, source.id)) is None
        and await session.scalar(
            select(IngestionJob.id)
            .where(
                IngestionJob.source_id == source.id,
                IngestionJob.input_data["kind"].astext == "knowledge_index",
            )
            .limit(1)
        )
        is None
    )
