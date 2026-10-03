"""Tenant-scoped job leases; Redis IDs resolve through the corporate dispatch ledger."""

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.core.retry import retry_delay
from app.db.models import IngestionJob, KnowledgeFile, Source, User
from app.db.session import corporate_session, tenant_session
from app.domain.artifacts import ArtifactError
from app.domain.enums import JobStatus, SourceStatus
from app.ingestion.channels import SUPPLEMENT, can_supplement
from app.ingestion.contracts import CompletionNotifier, WorkItem
from app.ingestion.models import IngestionDispatch
from app.ingestion.queue import IngestionQueue
from app.knowledge.jobs import DELETE, INDEX


class JobRepository:
    def __init__(
        self,
        tenant_factory: async_sessionmaker[AsyncSession],
        connector_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        notifier: CompletionNotifier,
    ) -> None:
        self.tenant_factory, self.connector_factory = tenant_factory, connector_factory
        self.settings, self.notifier = settings, notifier

    async def resolve(self, dispatch_id: UUID) -> tuple[UUID, UUID] | None:
        async with corporate_session(
            self.connector_factory, self.settings.wecom_corp_id
        ) as session:
            row = await session.get(IngestionDispatch, dispatch_id)
            if row is None or row.finished_at is not None:
                return None
            return row.user_id, row.job_id

    async def claim(self, dispatch_id: UUID) -> WorkItem | None:
        identity = await self.resolve(dispatch_id)
        if identity is None:
            return None
        user_id, job_id = identity
        async with tenant_session(self.tenant_factory, user_id) as session:
            job = await session.get(IngestionJob, job_id, with_for_update=True)
            # Waiting for another transaction must not consume the lease we are issuing.
            now = datetime.now(UTC)
            if job is None or job.status == JobStatus.COMPLETED:
                return None
            if job.lease_expires_at is not None and job.lease_expires_at > now:
                return None
            if (job.status == JobStatus.FAILED and job.next_retry_at is None) or (
                job.next_retry_at is not None and job.next_retry_at > now
            ):
                return None
            source = await session.get(Source, job.source_id, with_for_update=True)
            user = await session.get(User, user_id)
            if source is None or user is None:
                raise ArtifactError("job_relationship_missing")
            error = None
            operation = job.input_data.get("kind")
            if user.wecom_corp_id != self.settings.wecom_corp_id or (
                operation != DELETE
                and (
                    not user.is_active
                    or not self.settings.allows_wecom_user(user.wecom_external_user_id)
                )
            ):
                error = "allowlist_revoked"
            elif source.status == SourceStatus.DELETED or (
                source.status == SourceStatus.DELETING and operation != DELETE
            ):
                error = "source_unavailable"
            elif job.attempts >= job.max_attempts:
                error = "attempts_exhausted"
            elif operation == SUPPLEMENT and not await can_supplement(session, source):
                error = "channel_supplement_target_invalid"
            if error is not None:
                job.status, job.error_message = JobStatus.FAILED, error
                job.next_retry_at = job.lease_token = job.lease_expires_at = None
                if operation not in (INDEX, DELETE, SUPPLEMENT) and source.status not in (
                    SourceStatus.DELETING,
                    SourceStatus.DELETED,
                ):
                    source.status = SourceStatus.FAILED
                dispatch = await session.get(IngestionDispatch, dispatch_id)
                assert dispatch is not None
                dispatch.finished_at = now
                await self.notifier.finished(session, job, source, outcome="failed")
                return None
            job.attempts += 1
            job.status, job.started_at = (
                (JobStatus.INDEXING if operation in (INDEX, DELETE) else JobStatus.DOWNLOADING),
                now,
            )
            job.lease_token = uuid4()
            job.lease_expires_at = now + timedelta(seconds=self.settings.ingestion_lease_seconds)
            job.next_retry_at = None
            if operation not in (INDEX, DELETE, SUPPLEMENT):
                source.status = SourceStatus.PROCESSING
            return WorkItem(
                dispatch_id,
                user_id,
                job.id,
                source.id,
                job.lease_token,
                source.title,
                source.source_type,
                source.original_url,
                source.created_at,
                dict(job.input_data),
            )

    async def locked_job(self, session: AsyncSession, work: WorkItem) -> IngestionJob:
        job = await session.get(IngestionJob, work.job_id, with_for_update=True)
        if (
            job is None
            or job.lease_token != work.lease_token
            or job.lease_expires_at is None
            or job.lease_expires_at <= datetime.now(UTC)
        ):
            raise ArtifactError("job_lease_lost", retryable=True)
        return job

    async def stage(self, work: WorkItem, status: JobStatus) -> None:
        async with tenant_session(self.tenant_factory, work.user_id) as session:
            job = await self.locked_job(session, work)
            job.status = status

    async def fail(self, work: WorkItem, error: ArtifactError) -> None:
        async with tenant_session(self.tenant_factory, work.user_id) as session:
            job = await session.get(IngestionJob, work.job_id, with_for_update=True)
            if job is None or job.lease_token != work.lease_token:
                return
            source = await session.get(Source, job.source_id, with_for_update=True)
            dispatch = await session.get(IngestionDispatch, work.dispatch_id)
            assert source is not None and dispatch is not None
            job.status, job.error_message = JobStatus.FAILED, error.code[:128]
            job.lease_token = job.lease_expires_at = None
            operation = job.input_data.get("kind")
            if source.status not in (SourceStatus.DELETING, SourceStatus.DELETED):
                if operation == INDEX:
                    source.metadata_ = {**source.metadata_, "index_status": "failed"}
                elif operation not in (DELETE, SUPPLEMENT):
                    source.status = SourceStatus.FAILED
            if error.retryable and job.attempts < job.max_attempts:
                job.next_retry_at = datetime.now(UTC) + timedelta(
                    seconds=retry_delay(5, job.attempts)
                )
                dispatch.available_at, dispatch.published_at = job.next_retry_at, None
            else:
                job.next_retry_at = None
                dispatch.finished_at = datetime.now(UTC)
                await self.notifier.finished(session, job, source, outcome="failed")

    async def retry(self, dispatch_id: UUID) -> bool:
        # Include finished records: operator explicitly requests a terminal job retry.
        async with corporate_session(
            self.connector_factory, self.settings.wecom_corp_id
        ) as session:
            dispatch = await session.get(IngestionDispatch, dispatch_id)
            if dispatch is None:
                return False
            user_id, job_id = dispatch.user_id, dispatch.job_id
        async with tenant_session(self.tenant_factory, user_id) as session:
            job = await session.get(IngestionJob, job_id, with_for_update=True)
            if job is None or job.status != JobStatus.FAILED or job.lease_token is not None:
                return False
            job.status, job.attempts, job.next_retry_at = JobStatus.QUEUED, 0, None
            source = await session.get(Source, job.source_id, with_for_update=True)
            operation = job.input_data.get("kind")
            if (
                source is None
                or source.status == SourceStatus.DELETED
                or (source.status == SourceStatus.DELETING and operation != DELETE)
            ):
                raise ArtifactError("source_unavailable")
            if operation == INDEX:
                job.input_data = {**job.input_data, "reattach": True}
                source.metadata_ = {**source.metadata_, "index_status": "queued"}
                record = await session.get(KnowledgeFile, (user_id, source.id))
                if record is not None:
                    record.index_started_at = datetime.now(UTC)
            elif operation == SUPPLEMENT:
                if not await can_supplement(session, source):
                    raise ArtifactError("channel_supplement_target_invalid")
            elif operation != DELETE:
                source.status = SourceStatus.RECEIVED
            dispatch = await session.get(IngestionDispatch, dispatch_id)
            assert dispatch is not None
            dispatch.finished_at = dispatch.published_at = None
            dispatch.available_at = datetime.now(UTC)
            return True

    async def publish_due(self, queue: IngestionQueue) -> int:
        now = datetime.now(UTC)
        async with corporate_session(
            self.connector_factory, self.settings.wecom_corp_id
        ) as session:
            rows = (
                await session.scalars(
                    select(IngestionDispatch)
                    .where(
                        IngestionDispatch.finished_at.is_(None),
                        IngestionDispatch.available_at <= now,
                        or_(
                            IngestionDispatch.published_at.is_(None),
                            IngestionDispatch.published_at <= now - timedelta(seconds=60),
                        ),
                    )
                    .order_by(IngestionDispatch.available_at)
                    .limit(20)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            for dispatch in rows:
                await queue.enqueue(dispatch.id)
                dispatch.published_at = now
            return len(rows)
