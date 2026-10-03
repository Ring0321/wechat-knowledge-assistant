"""Explicit operator recovery for a crash between a pending checkpoint and HTTP.

All ingestion workers must be stopped and remote absence confirmed by the operator.
Normal automatic/manual retry never clears an ambiguous mutation checkpoint.
"""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select

from app.db.models import IngestionJob, KnowledgeFile, Source, User
from app.db.session import corporate_session, tenant_session
from app.domain.artifacts import ArtifactError
from app.domain.enums import JobStatus
from app.domain.knowledge import VectorProvider
from app.ingestion.models import IngestionDispatch
from app.ingestion.repository import JobRepository
from app.knowledge.jobs import DELETE, INDEX
from app.knowledge.processor import filename


async def resolve_unknown(
    repository: JobRepository,
    provider: VectorProvider,
    dispatch_id: UUID,
    *,
    confirmed_remote_absence: bool = False,
) -> bool:
    if not confirmed_remote_absence or not repository.settings.knowledge_enabled:
        raise ArtifactError("knowledge_recovery_confirmation_required")
    async with corporate_session(
        repository.connector_factory, repository.settings.wecom_corp_id
    ) as session:
        dispatch = await session.get(IngestionDispatch, dispatch_id)
        if dispatch is None:
            return False
        user_id, job_id = dispatch.user_id, dispatch.job_id
    async with tenant_session(repository.tenant_factory, user_id) as session:
        job = await session.get(IngestionJob, job_id, with_for_update=True)
        if (
            job is None
            or job.status != JobStatus.FAILED
            or job.lease_token is not None
            or job.input_data.get("kind") not in (INDEX, DELETE)
        ):
            raise ArtifactError("knowledge_recovery_requires_failed_job")
        active = await session.scalar(
            select(IngestionJob.id)
            .where(IngestionJob.lease_expires_at > datetime.now(UTC))
            .limit(1)
        )
        if active is not None:
            raise ArtifactError("knowledge_recovery_workers_active")
        source = await session.get(Source, job.source_id, with_for_update=True)
        user = await session.get(User, user_id, with_for_update=True)
        assert source is not None and user is not None
        if user.vector_store_pending and user.vector_store_id is None:
            user.vector_store_id = await provider.find_store(user_id)
            user.vector_store_pending = False
        record = await session.get(KnowledgeFile, (user_id, source.id), with_for_update=True)
        if record is not None and record.upload_started and record.vector_file_id is None:
            record.vector_file_id = await provider.find_file(filename(record))
            if record.vector_file_id is None:
                record.upload_started = False
        # Keep the task failed until the normal retry path issues a fresh lease.
    return await repository.retry(dispatch_id)
