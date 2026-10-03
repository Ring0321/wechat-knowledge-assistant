"""Resumable remote operations. Ambiguous creates reconcile without blind replay."""

import hashlib
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from app.db.models import Asset, IngestionJob, KnowledgeFile, Source, User
from app.db.session import tenant_session
from app.domain.artifacts import ArtifactError
from app.domain.enums import JobStatus, SourceStatus
from app.domain.knowledge import ObjectDeleter, TenantContext, VectorProvider, VectorRequestRejected
from app.ingestion.contracts import WorkItem
from app.ingestion.models import IngestionDispatch
from app.ingestion.repository import JobRepository
from app.knowledge.jobs import DELETE, INDEX
from app.knowledge.rendering import document_bytes
from app.knowledge.service import authorize


def filename(record: KnowledgeFile) -> str:
    return f"pkb-{record.user_id}-{record.source_id}-{record.document_sha256}.md"


class KnowledgeProcessor:
    def __init__(
        self, repository: JobRepository, provider: VectorProvider, objects: ObjectDeleter
    ) -> None:
        self.repository, self.provider, self.objects = repository, provider, objects

    async def process(self, work: WorkItem) -> None:
        if not self.repository.settings.knowledge_enabled:
            raise ArtifactError("knowledge_disabled")
        if work.input_data.get("kind") == INDEX:
            await self._index(work)
        elif work.input_data.get("kind") == DELETE:
            await self._delete(work)
        else:
            raise ArtifactError("knowledge_operation_invalid")

    async def _live_source(self, work: WorkItem) -> Source:
        async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
            await self.repository.locked_job(session, work)
            await authorize(session, TenantContext(work.user_id), self.repository.settings)
            source = await session.get(Source, work.source_id, with_for_update=True)
            if source is None or source.status not in (SourceStatus.STORED, SourceStatus.READY):
                raise ArtifactError("knowledge_source_unavailable")
            return source

    async def _store(self, work: WorkItem) -> str:
        # A persisted pending bit is the right to attempt ONE create. It survives crashes.
        async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
            await self.repository.locked_job(session, work)
            user = await session.get(User, work.user_id, with_for_update=True)
            assert user is not None
            if user.vector_store_id:
                return user.vector_store_id
            pending = user.vector_store_pending
            user.vector_store_pending = True
        if pending:
            store_id = await self.provider.find_store(work.user_id)
            if store_id is None:
                raise ArtifactError("knowledge_store_outcome_unknown", retryable=True)
        else:
            try:
                store_id = await self.provider.create_store(work.user_id)
            except VectorRequestRejected:
                async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
                    user = await session.get(User, work.user_id, with_for_update=True)
                    assert user is not None
                    if user.vector_store_id is None:
                        user.vector_store_pending = False
                raise
        async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
            user = await session.get(User, work.user_id, with_for_update=True)
            assert user is not None
            if user.vector_store_id is not None and user.vector_store_id != store_id:
                raise ArtifactError("knowledge_store_conflict")
            user.vector_store_id, user.vector_store_pending = store_id, False
        return store_id

    async def _index(self, work: WorkItem) -> None:
        source: Source | None = await self._live_source(work)
        assert source is not None
        content = document_bytes(source)
        if len(content) > self.repository.settings.knowledge_max_document_bytes:
            raise ArtifactError("knowledge_document_too_large")
        digest = hashlib.sha256(content).hexdigest()
        async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
            await self.repository.locked_job(session, work)
            record = await session.get(KnowledgeFile, (work.user_id, work.source_id))
            if record is None:
                record = KnowledgeFile(
                    user_id=work.user_id, source_id=work.source_id, document_sha256=digest
                )
                session.add(record)
                await session.flush()
            elif record.document_sha256 != digest:
                raise ArtifactError("knowledge_document_changed")
        store_id = await self._store(work)
        async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
            await self.repository.locked_job(session, work)
            source = await session.get(Source, work.source_id, with_for_update=True)
            if source is None or source.status not in (SourceStatus.STORED, SourceStatus.READY):
                raise ArtifactError("knowledge_source_unavailable")
            record = await session.get(
                KnowledgeFile, (work.user_id, work.source_id), with_for_update=True
            )
            assert record is not None
            record.vector_store_id = store_id
            file_id, pending = record.vector_file_id, record.upload_started
            record.upload_started = True
        if file_id is None:
            if pending:
                file_id = await self.provider.find_file(filename(record))
                if file_id is None:
                    raise ArtifactError("knowledge_upload_outcome_unknown", retryable=True)
            else:
                try:
                    file_id = await self.provider.upload(filename(record), content)
                except VectorRequestRejected:
                    async with tenant_session(
                        self.repository.tenant_factory, work.user_id
                    ) as session:
                        current = await session.get(
                            KnowledgeFile, (work.user_id, work.source_id), with_for_update=True
                        )
                        assert current is not None
                        if current.vector_file_id is None:
                            current.upload_started = False
                    raise
            # Persist cleanup obligation even if the source was tombstoned during upload.
            async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
                current = await session.get(
                    KnowledgeFile, (work.user_id, work.source_id), with_for_update=True
                )
                assert current is not None
                if current.vector_file_id is not None and current.vector_file_id != file_id:
                    raise ArtifactError("knowledge_file_conflict")
                current.vector_file_id = file_id
        await self._live_source(work)
        status = await self.provider.file_status(store_id, file_id)
        if status in ("failed", "cancelled") and work.input_data.get("reattach") is True:
            await self.provider.detach(store_id, file_id)
            status = None
        if status is None:
            status = await self.provider.attach(
                store_id,
                file_id,
                {
                    "user_id": str(work.user_id),
                    "source_id": str(work.source_id),
                    "document_sha256": digest,
                    "source_type": source.source_type.value,
                    "created_at": int(source.created_at.timestamp()),
                },
            )
        if status in ("failed", "cancelled"):
            raise ArtifactError("knowledge_index_failed")
        if status != "completed":
            if (
                datetime.now(UTC) - record.index_started_at
            ).total_seconds() > self.repository.settings.knowledge_index_timeout_seconds:
                raise ArtifactError("knowledge_index_timeout")
            await self._defer(work)
            return
        async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
            job = await self.repository.locked_job(session, work)
            await authorize(session, TenantContext(work.user_id), self.repository.settings)
            source = await session.get(Source, work.source_id, with_for_update=True)
            if source is None or source.status not in (SourceStatus.STORED, SourceStatus.READY):
                raise ArtifactError("knowledge_source_unavailable")
            if hashlib.sha256(document_bytes(source)).hexdigest() != digest:
                raise ArtifactError("knowledge_document_changed")
            source.status, source.vector_file_id = SourceStatus.READY, file_id
            source.metadata_ = {
                **source.metadata_,
                "index_status": "ready",
                "document_sha256": digest,
            }
            self._complete(job)
            dispatch = await session.get(IngestionDispatch, work.dispatch_id)
            assert dispatch is not None
            dispatch.finished_at = datetime.now(UTC)
            await self.repository.notifier.finished(session, job, source, outcome="indexed")

    async def _defer(self, work: WorkItem) -> None:
        async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
            job = await self.repository.locked_job(session, work)
            job.status, job.attempts = JobStatus.QUEUED, max(0, job.attempts - 1)
            job.lease_token = job.lease_expires_at = None
            job.next_retry_at = datetime.now(UTC) + timedelta(
                seconds=self.repository.settings.knowledge_poll_seconds
            )
            dispatch = await session.get(IngestionDispatch, work.dispatch_id)
            assert dispatch is not None
            dispatch.available_at, dispatch.published_at = job.next_retry_at, None

    async def _delete(self, work: WorkItem) -> None:
        async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
            await self.repository.locked_job(session, work)
            source = await session.get(Source, work.source_id, with_for_update=True)
            if source is None or source.status != SourceStatus.DELETING:
                raise ArtifactError("knowledge_delete_state_invalid")
            record = await session.get(KnowledgeFile, (work.user_id, work.source_id))
            assets = list(
                (await session.scalars(select(Asset).where(Asset.source_id == source.id))).all()
            )
        if record is not None:
            file_id = record.vector_file_id
            if file_id is None and record.upload_started:
                file_id = await self.provider.find_file(filename(record))
                if file_id is None:
                    # Could still be an in-flight upload. Do not claim cleanup completed.
                    raise ArtifactError("knowledge_upload_outcome_unknown", retryable=True)
                async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
                    current = await session.get(
                        KnowledgeFile, (work.user_id, work.source_id), with_for_update=True
                    )
                    assert current is not None
                    current.vector_file_id = file_id
            if file_id is not None:
                if record.vector_store_id is not None:
                    await self.provider.detach(record.vector_store_id, file_id)
                await self.provider.delete_file(file_id)
        # Rolled-back ingestion may have uploaded deterministic objects before its
        # Asset rows committed. Enumerate only the exact trusted tenant/source prefix.
        orphan_candidates = await self.objects.list_keys(
            user_id=work.user_id, source_id=work.source_id
        )
        for key in sorted({asset.storage_key for asset in assets} | set(orphan_candidates)):
            await self.objects.delete(user_id=work.user_id, source_id=work.source_id, key=key)
        async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
            job = await self.repository.locked_job(session, work)
            source = await session.get(Source, work.source_id, with_for_update=True)
            assert source is not None and source.status == SourceStatus.DELETING
            source.status, source.vector_file_id = SourceStatus.DELETED, None
            source.text, source.summary, source.tags = None, None, []
            source.metadata_ = {"index_status": "deleted"}
            self._complete(job)
            dispatch = await session.get(IngestionDispatch, work.dispatch_id)
            assert dispatch is not None
            dispatch.finished_at = datetime.now(UTC)

    @staticmethod
    def _complete(job: IngestionJob) -> None:
        job.status, job.completed_at = JobStatus.COMPLETED, datetime.now(UTC)
        job.lease_token = job.lease_expires_at = job.next_retry_at = job.error_message = None
