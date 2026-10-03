"""Transactional task creation shared with ingestion; no provider calls."""

from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.db.models import IngestionJob, Source
from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceStatus
from app.ingestion.models import IngestionDispatch

INDEX = "knowledge_index"
DELETE = "knowledge_delete"


async def enqueue(
    session: AsyncSession,
    source: Source,
    settings: Settings,
    *,
    operation: str = INDEX,
    message_id: UUID | None = None,
) -> UUID | None:
    """Caller locks the source. Replays return the existing durable dispatch."""
    if not settings.knowledge_enabled:
        raise ArtifactError("knowledge_disabled")
    if operation not in (INDEX, DELETE):
        raise ArtifactError("knowledge_operation_invalid")
    prior = await session.scalar(
        select(IngestionDispatch)
        .join(IngestionJob, IngestionJob.id == IngestionDispatch.job_id)
        .where(
            IngestionJob.source_id == source.id, IngestionJob.input_data["kind"].astext == operation
        )
    )
    if prior is not None:
        return prior.id
    if source.status == SourceStatus.DELETED:
        return None
    if operation == INDEX:
        if source.status == SourceStatus.READY:
            return None
        if source.status != SourceStatus.STORED or not source.text or not source.text.strip():
            raise ArtifactError("knowledge_source_not_indexable")
        source.metadata_ = {**source.metadata_, "index_status": "queued"}
    else:
        source.status = SourceStatus.DELETING
        source.metadata_ = {**source.metadata_, "index_status": "deleting"}
    job_id, dispatch_id = uuid4(), uuid4()
    session.add(
        IngestionJob(
            id=job_id,
            user_id=source.user_id,
            source_id=source.id,
            message_id=message_id,
            operation_key=f"{operation}:{source.id}",
            input_data={"kind": operation},
            max_attempts=settings.ingestion_max_attempts,
        )
    )
    await session.flush()
    session.add(
        IngestionDispatch(
            id=dispatch_id,
            corp_id=settings.wecom_corp_id,
            user_id=source.user_id,
            job_id=job_id,
        )
    )
    return dispatch_id
