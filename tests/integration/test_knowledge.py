"""M6 real PostgreSQL/Redis/S3 acceptance with a deterministic remote provider."""

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from botocore.exceptions import ClientError
from pydantic import SecretStr
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from app.db.models import Asset, IngestionJob, KnowledgeFile, Source, User
from app.db.session import corporate_session, tenant_session
from app.domain.artifacts import ArtifactError
from app.domain.enums import JobStatus, SourceStatus
from app.domain.knowledge import TenantContext, VectorHit
from app.domain.parsing import ParseLimits
from app.ingestion.models import IngestionDispatch, MessageSource
from app.ingestion.pipeline import IngestionPipeline
from app.ingestion.worker import IngestionWorker
from app.knowledge.backfill import backfill
from app.knowledge.jobs import INDEX
from app.knowledge.processor import KnowledgeProcessor
from app.knowledge.recovery import resolve_unknown
from app.knowledge.service import KnowledgeService
from app.parsers.local import LocalFileParser
from tests.integration.test_ingestion import Harness, Media, s3_client
from tests.integration.test_ingestion import harness as harness
from tests.integration.test_ingestion import s3_config as s3_config
from tests.knowledge_helpers import FakeVector
from tests.parser_helpers import text_pdf
from tests.wecom_helpers import TEST_USER

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("operation", ["claim", "fail", "retry"])
async def test_ingestion_status_changes_cannot_overwrite_a_concurrent_tombstone(
    harness: Harness,
    urls: dict[str, str],
    admin_engine: AsyncEngine,
    operation: str,
) -> None:
    context = TenantContext(await harness.admit("保存：并发删除"))
    dispatch = (await harness.dispatches())[0]
    work = None
    if operation in ("fail", "retry"):
        work = await harness.repository.claim(dispatch.id)
        assert work is not None
    if operation == "retry":
        assert work is not None
        await harness.repository.fail(work, ArtifactError("synthetic_failed"))
    app_name = "m6-race-" + uuid4().hex
    engine = create_async_engine(
        urls["TEST_DATABASE_URL"],
        pool_size=3,
        connect_args={"server_settings": {"application_name": app_name}},
        hide_parameters=True,
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    harness.repository.tenant_factory = factory
    job = await job_for(harness, context, dispatch.id)
    task: asyncio.Task[object] | None = None
    try:
        async with tenant_session(factory, context.user_id) as session:
            row = await session.get(Source, job.source_id, with_for_update=True)
            assert row is not None
            row.status = SourceStatus.DELETING
            await session.flush()
            if operation == "claim":
                task = asyncio.create_task(harness.repository.claim(dispatch.id))
            elif operation == "fail":
                assert work is not None
                task = asyncio.create_task(
                    harness.repository.fail(work, ArtifactError("synthetic_failed"))
                )
            else:
                task = asyncio.create_task(harness.repository.retry(dispatch.id))
            async with asyncio.timeout(5):
                while True:
                    async with admin_engine.connect() as connection:
                        blocked = await connection.scalar(
                            text(
                                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                                "WHERE application_name=:name "
                                "AND cardinality(pg_blocking_pids(pid)) > 0)"
                            ),
                            {"name": app_name},
                        )
                    if blocked:
                        break
                    await asyncio.sleep(0.01)
        if operation == "retry":
            with pytest.raises(ArtifactError, match="source_unavailable"):
                await task
        else:
            await task
        assert (await source_for(harness, context, job.source_id)).status == SourceStatus.DELETING
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await engine.dispose()


async def test_manual_retry_resets_index_wait_budget_but_normal_poll_does_not(
    harness: Harness,
) -> None:
    vector = FakeVector()
    _, processor = enable(harness, vector)
    vector.attach_status = "in_progress"
    context, source_id, dispatch_id = await ingest(harness)
    assert await execute(harness, processor, dispatch_id) is None
    old = datetime.now(UTC) - timedelta(hours=2)
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        record = await session.get(KnowledgeFile, (context.user_id, source_id))
        assert record is not None
        record.index_started_at = old
    await make_due(harness, context, dispatch_id)
    error = await execute(harness, processor, dispatch_id)
    assert error and error.code == "knowledge_index_timeout"
    assert await harness.repository.retry(dispatch_id)
    assert await execute(harness, processor, dispatch_id) is None
    assert (await job_for(harness, context, dispatch_id)).status == JobStatus.QUEUED
    assert vector.calls["upload"] == 1


async def test_unknown_checkpoint_has_explicit_guarded_operator_recovery(harness: Harness) -> None:
    vector = FakeVector()
    _, processor = enable(harness, vector)
    context, _, dispatch_id = await ingest(harness)
    # Crash after pending commit, before provider accepted any request.
    vector.failures["create_store"] = 1
    assert await execute(harness, processor, dispatch_id)
    with pytest.raises(ArtifactError, match="confirmation_required"):
        await resolve_unknown(harness.repository, vector, dispatch_id)
    assert await resolve_unknown(
        harness.repository, vector, dispatch_id, confirmed_remote_absence=True
    )
    assert await execute(harness, processor, dispatch_id) is None
    assert (await job_for(harness, context, dispatch_id)).status == JobStatus.COMPLETED


@pytest.mark.parametrize("operation", ["create_store", "upload"])
async def test_operator_recovery_reuses_remote_objects_and_rolls_back_on_scan_failure(
    harness: Harness,
    operation: str,
) -> None:
    vector = FakeVector()
    _, processor = enable(harness, vector)
    context, source_id, dispatch_id = await ingest(harness)
    vector.uncertain_after.add(operation)
    assert await execute(harness, processor, dispatch_id)
    find_operation = "find_store" if operation == "create_store" else "find_file"
    vector.failures[find_operation] = 1
    with pytest.raises(ArtifactError, match="synthetic_vector_unavailable"):
        await resolve_unknown(
            harness.repository, vector, dispatch_id, confirmed_remote_absence=True
        )
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        user = await session.get(User, context.user_id)
        record = await session.get(KnowledgeFile, (context.user_id, source_id))
        assert user and record
        if operation == "create_store":
            assert user.vector_store_pending and user.vector_store_id is None
        else:
            assert record.upload_started and record.vector_file_id is None
    assert await resolve_unknown(
        harness.repository, vector, dispatch_id, confirmed_remote_absence=True
    )
    assert await execute(harness, processor, dispatch_id) is None
    assert vector.calls["create_store"] == vector.calls["upload"] == 1


async def test_operator_recovery_refuses_active_tenant_lease_without_mutating_pending(
    harness: Harness,
) -> None:
    vector = FakeVector()
    _, processor = enable(harness, vector)
    context, _, first = await ingest(harness)
    _, _, second = await ingest(harness, content="保存：第二条")
    vector.failures["create_store"] = 1
    assert await execute(harness, processor, first)
    assert await harness.repository.claim(second) is not None
    with pytest.raises(ArtifactError, match="workers_active"):
        await resolve_unknown(harness.repository, vector, first, confirmed_remote_absence=True)
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        user = await session.get(User, context.user_id)
        assert user and user.vector_store_pending
    assert vector.calls["find_store"] == 0


async def test_delete_cleans_orphan_objects_after_ingestion_transaction_rollback(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    vector = FakeVector()
    service, processor = enable(harness, vector)
    context = TenantContext(await harness.admit("保存：回滚后的原件"))
    admission = (await harness.dispatches())[0]
    work = await harness.repository.claim(admission.id)
    assert work is not None
    real_put = harness.objects.put_file

    async def failing_put(**kwargs: object) -> object:
        if kwargs["kind"] == "canonical":
            raise ArtifactError("synthetic_s3_failure", retryable=True)
        return await real_put(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(harness.objects, "put_file", failing_put)
    with pytest.raises(ArtifactError, match="synthetic_s3_failure") as caught:
        await harness.worker().pipeline.process(work)
    await harness.repository.fail(work, caught.value)
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        assert await session.scalar(select(func.count()).select_from(Asset)) == 0
    assert (
        len(await harness.objects.list_keys(user_id=context.user_id, source_id=work.source_id)) == 1
    )
    # An unregistered neighboring source object must survive this exact source deletion.
    neighbor_id, body = uuid4(), b"neighbor synthetic original"
    path = tmp_path / "neighbor"
    path.write_bytes(body)
    await real_put(
        user_id=context.user_id,
        source_id=neighbor_id,
        kind="original",
        path=path,
        sha256=hashlib.sha256(body).hexdigest(),
        content_type="text/plain",
    )
    deletion = await service.delete_document(context, work.source_id)
    assert deletion is not None
    assert await execute(harness, processor, deletion) is None
    assert await harness.objects.list_keys(user_id=context.user_id, source_id=work.source_id) == ()
    assert len(await harness.objects.list_keys(user_id=context.user_id, source_id=neighbor_id)) == 1


def enable(h: Harness, vector: FakeVector) -> tuple[KnowledgeService, KnowledgeProcessor]:
    # These references are also held by the authenticated bridge and repository.
    h.settings.openai_api_key = SecretStr("synthetic-test-key")
    h.settings.knowledge_enabled = True
    return (
        KnowledgeService(h.store.tenant_factory, h.settings, vector),
        KnowledgeProcessor(h.repository, vector, h.objects),
    )


async def dispatch_for(h: Harness, context: TenantContext, source_id: UUID, kind: str) -> UUID:
    async with tenant_session(h.store.tenant_factory, context.user_id) as session:
        return (
            await session.scalars(
                select(IngestionDispatch.id)
                .join(IngestionJob, IngestionJob.id == IngestionDispatch.job_id)
                .where(
                    IngestionJob.source_id == source_id,
                    IngestionJob.input_data["kind"].astext == kind,
                )
            )
        ).one()


async def ingest(
    h: Harness, *, content: str = "保存：测试知识", user: str = TEST_USER, file: bool = False
) -> tuple[TenantContext, UUID, UUID]:
    before = {row.id for row in await h.dispatches()}
    context = TenantContext(await h.admit(content, user=user, file=file))
    admission = next(row for row in await h.dispatches() if row.id not in before)
    work = await h.repository.claim(admission.id)
    assert work is not None
    await h.worker().pipeline.process(work)
    index_id = await dispatch_for(h, context, work.source_id, INDEX)
    return context, work.source_id, index_id


async def execute(
    h: Harness, processor: KnowledgeProcessor, dispatch_id: UUID
) -> ArtifactError | None:
    work = await h.repository.claim(dispatch_id)
    assert work is not None, "expected a due durable task"
    try:
        await processor.process(work)
    except ArtifactError as error:
        await h.repository.fail(work, error)
        return error
    return None


async def job_for(h: Harness, context: TenantContext, dispatch_id: UUID) -> IngestionJob:
    async with tenant_session(h.store.tenant_factory, context.user_id) as session:
        dispatch = await session.get(IngestionDispatch, dispatch_id)
        assert dispatch is not None
        job = await session.get(IngestionJob, dispatch.job_id)
        assert job is not None
        return job


async def source_for(h: Harness, context: TenantContext, source_id: UUID) -> Source:
    async with tenant_session(h.store.tenant_factory, context.user_id) as session:
        source = await session.get(Source, source_id)
        assert source is not None
        return source


async def make_due(h: Harness, context: TenantContext, dispatch_id: UUID) -> None:
    due = datetime.now(UTC) - timedelta(seconds=1)
    async with tenant_session(h.store.tenant_factory, context.user_id) as session:
        dispatch = await session.get(IngestionDispatch, dispatch_id)
        assert dispatch is not None
        await session.execute(
            update(IngestionJob).where(IngestionJob.id == dispatch.job_id).values(next_retry_at=due)
        )
        dispatch.available_at, dispatch.published_at = due, None
    # Expire just this synthetic dispatch's 60-second Redis publication deduplication.
    digest = hashlib.sha256(str(dispatch_id).encode()).hexdigest()
    await h.queue.redis.delete(h.queue.stream + ":dedup:" + digest)


async def test_ingestion_enqueues_index_redis_worker_returns_ready_citation(
    harness: Harness,
) -> None:
    vector = FakeVector()
    service, processor = enable(harness, vector)
    context, source_id, dispatch_id = await ingest(harness)
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        source = await session.get(Source, source_id)
        assert source is not None
        source.metadata_ = {
            **source.metadata_,
            "segments": [{"text": "保存：测试知识", "locator": {"page": 2}, "method": "native"}],
        }
    assert (await source_for(harness, context, source_id)).status == SourceStatus.STORED
    assert await harness.repository.publish_due(harness.queue) == 1
    worker = IngestionWorker(
        harness.queue, harness.repository, harness.worker().pipeline, knowledge=processor
    )
    assert await worker.once()
    assert (await job_for(harness, context, dispatch_id)).status == JobStatus.COMPLETED
    source = await service.get_source(context, source_id)
    assert source is not None and source.status == SourceStatus.READY
    hits = await service.search(context, "测试知识")
    assert len(hits) == 1 and hits[0].source == source
    assert hits[0].locators == ({"page": 2},)
    assert str(source_id) in next(iter(vector.contents.values())).decode()
    assert await service.index_document(context, source_id) == dispatch_id
    assert await harness.repository.claim(dispatch_id) is None
    assert vector.calls["create_store"] == vector.calls["upload"] == vector.calls["attach"] == 1


async def test_two_users_keep_independent_store_files_and_reject_forged_hits(
    harness: Harness,
) -> None:
    vector = FakeVector()
    service, processor = enable(harness, vector)
    first, first_source, first_dispatch = await ingest(harness)
    second, second_source, second_dispatch = await ingest(harness, user="second-customer")
    assert await execute(harness, processor, first_dispatch) is None
    assert await execute(harness, processor, second_dispatch) is None
    assert vector.stores[first.user_id] != vector.stores[second.user_id]
    first_row = await source_for(harness, first, first_source)
    second_row = await source_for(harness, second, second_source)
    assert first_row.vector_file_id != second_row.vector_file_id
    assert first_row.sha256 == second_row.sha256
    second_hit = (await service.search(second, "测试"))[0]
    second_file = second_row.vector_file_id
    assert second_file is not None
    foreign_attributes = dict(vector.attributes[vector.stores[second.user_id], second_file])
    vector.extra_hits = (
        VectorHit(second_file, 1.0, second_hit.text, foreign_attributes),
        VectorHit(
            second_file, 1.0, second_hit.text, {**foreign_attributes, "user_id": str(first.user_id)}
        ),
        VectorHit("unknown-file", 1.0, "forged evidence", {"user_id": str(first.user_id)}),
    )
    assert [hit.source.source_id for hit in await service.search(first, "测试")] == [first_source]
    assert vector.search_calls[-1] == (vector.stores[first.user_id], first.user_id)
    assert await service.get_source(first, second_source) is None
    assert [source.source_id for source in await service.list_sources(first)] == [first_source]
    with pytest.raises(ArtifactError, match="knowledge_source_not_found"):
        await service.index_document(first, second_source)
    with pytest.raises(ArtifactError, match="knowledge_source_not_found"):
        await service.delete_document(first, second_source)


async def test_polling_requeues_without_reupload_or_consuming_failure_attempt(
    harness: Harness,
) -> None:
    vector = FakeVector()
    vector.attach_status = "in_progress"
    service, processor = enable(harness, vector)
    context, source_id, dispatch_id = await ingest(harness)
    worker = IngestionWorker(
        harness.queue, harness.repository, harness.worker().pipeline, knowledge=processor
    )
    assert await harness.repository.publish_due(harness.queue) == 1
    assert await worker.once()
    job = await job_for(harness, context, dispatch_id)
    assert job.status == JobStatus.QUEUED and job.attempts == 0 and job.next_retry_at is not None
    assert await harness.repository.claim(dispatch_id) is None
    assert await service.search(context, "测试") == ()
    vector.statuses = {key: "completed" for key in vector.statuses}
    await make_due(harness, context, dispatch_id)
    assert await harness.repository.publish_due(harness.queue) == 1
    assert await worker.once()
    assert (await source_for(harness, context, source_id)).status == SourceStatus.READY
    assert vector.calls["upload"] == vector.calls["attach"] == 1


@pytest.mark.parametrize("operation", ["create_store", "upload", "attach"])
async def test_successful_remote_operation_with_lost_ack_reconciles(
    harness: Harness, operation: str
) -> None:
    vector = FakeVector()
    vector.uncertain_after.add(operation)
    _, processor = enable(harness, vector)
    context, source_id, dispatch_id = await ingest(harness)
    error = await execute(harness, processor, dispatch_id)
    assert error is not None and error.retryable
    assert (await source_for(harness, context, source_id)).status == SourceStatus.STORED
    await make_due(harness, context, dispatch_id)
    assert await execute(harness, processor, dispatch_id) is None
    assert (await source_for(harness, context, source_id)).status == SourceStatus.READY
    assert vector.calls["create_store"] == vector.calls["upload"] == vector.calls["attach"] == 1
    if operation == "create_store":
        assert vector.calls["find_store"] == 1
    if operation == "upload":
        assert vector.calls["find_file"] == 1


@pytest.mark.parametrize(
    "operation,lookup", [("create_store", "find_store"), ("upload", "find_file")]
)
async def test_ambiguous_remote_outcome_not_found_never_blindly_recreates(
    harness: Harness, operation: str, lookup: str
) -> None:
    vector = FakeVector()
    vector.uncertain_after.add(operation)
    vector.hidden.add(lookup)
    _, processor = enable(harness, vector)
    context, source_id, dispatch_id = await ingest(harness)
    assert await execute(harness, processor, dispatch_id) is not None
    for _ in range(2):
        await make_due(harness, context, dispatch_id)
        error = await execute(harness, processor, dispatch_id)
        assert error is not None and error.code.endswith("outcome_unknown")
    job = await job_for(harness, context, dispatch_id)
    assert job.status == JobStatus.FAILED and job.next_retry_at is None
    assert vector.calls[operation] == 1
    assert (await source_for(harness, context, source_id)).status == SourceStatus.STORED
    vector.hidden.clear()
    assert await harness.repository.retry(dispatch_id)
    assert await execute(harness, processor, dispatch_id) is None
    assert vector.calls[operation] == 1


async def test_terminal_index_failure_retains_content_and_manual_retry_never_reparses(
    harness: Harness,
) -> None:
    vector = FakeVector()
    vector.attach_status = "failed"
    service, processor = enable(harness, vector)
    context, source_id, dispatch_id = await ingest(harness, file=True)
    original = await source_for(harness, context, source_id)
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        asset_ids = set(await session.scalars(select(Asset.id)))
    error = await execute(harness, processor, dispatch_id)
    assert error is not None and error.code == "knowledge_index_failed"
    job = await job_for(harness, context, dispatch_id)
    assert job.status == JobStatus.FAILED and job.next_retry_at is None
    retained = await service.get_source(context, source_id)
    assert retained is not None and retained.text == original.text
    assert retained.status == SourceStatus.STORED
    assert await service.search(context, "synthetic") == ()
    assert await harness.repository.retry(dispatch_id)
    vector.attach_status = "completed"
    assert await execute(harness, processor, dispatch_id) is None
    assert (
        vector.calls["upload"] == 1 and vector.calls["detach"] == 1 and vector.calls["attach"] == 2
    )
    assert harness.media.calls == 1
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        assert set(await session.scalars(select(Asset.id))) == asset_ids
    assert (await source_for(harness, context, source_id)).storage_key == original.storage_key


@pytest.mark.parametrize("operation", ["create_store", "upload"])
async def test_conclusive_rejection_can_retry_without_an_unknown_outcome_dead_end(
    harness: Harness, operation: str
) -> None:
    vector = FakeVector()
    vector.rejections[operation] = 1
    _, processor = enable(harness, vector)
    context, source_id, dispatch_id = await ingest(harness)
    error = await execute(harness, processor, dispatch_id)
    assert error is not None and error.code == "synthetic_vector_rejected"
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        if operation == "create_store":
            user = await session.get(User, context.user_id)
            assert user is not None and not user.vector_store_pending
        else:
            journal = await session.get(KnowledgeFile, (context.user_id, source_id))
            assert journal is not None and not journal.upload_started
    await make_due(harness, context, dispatch_id)
    assert await execute(harness, processor, dispatch_id) is None
    assert (await source_for(harness, context, source_id)).status == SourceStatus.READY
    assert vector.calls[operation] == 2 and len(vector.contents) == 1


async def test_delete_hides_immediately_filters_stale_hits_and_removes_real_s3_assets(
    harness: Harness, s3_config: dict[str, str]
) -> None:
    vector = FakeVector()
    service, processor = enable(harness, vector)
    context, source_id, index_id = await ingest(harness)
    assert await execute(harness, processor, index_id) is None
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        keys = list(await session.scalars(select(Asset.storage_key)))
    assert len(keys) == 3
    remote = (
        await vector.search(
            vector.stores[context.user_id], "测试", user_id=context.user_id, limit=10
        )
    )[0]
    vector.extra_hits = (remote,)
    deletion = await service.delete_document(context, source_id)
    assert deletion is not None
    assert await service.delete_document(context, source_id) == deletion
    assert await service.get_source(context, source_id) is None
    assert await service.list_sources(context) == ()
    assert await service.search(context, "测试") == ()
    assert await execute(harness, processor, deletion) is None
    assert (await source_for(harness, context, source_id)).status == SourceStatus.DELETED
    assert vector.calls["detach"] == vector.calls["delete_file"] == 1
    assert not vector.contents and not vector.statuses
    assert await service.search(context, "测试") == ()
    assert await service.delete_document(context, source_id) in (None, deletion)
    assert await harness.repository.claim(deletion) is None
    client = s3_client(s3_config)
    try:
        for key in keys:
            with pytest.raises(ClientError) as caught:
                client.head_object(Bucket=s3_config["bucket"], Key=key)
            assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    finally:
        client.close()


@pytest.mark.parametrize("operation", ["detach", "delete_file"])
async def test_delete_failure_retry_preserves_tombstone_and_remote_cleanup_is_idempotent(
    harness: Harness, operation: str
) -> None:
    vector = FakeVector()
    service, processor = enable(harness, vector)
    context, source_id, index_id = await ingest(harness)
    assert await execute(harness, processor, index_id) is None
    vector.uncertain_after.add(operation)
    deletion = await service.delete_document(context, source_id)
    assert deletion is not None
    assert await execute(harness, processor, deletion) is not None
    assert (await source_for(harness, context, source_id)).status == SourceStatus.DELETING
    assert await service.get_source(context, source_id) is None
    assert await service.search(context, "测试") == ()
    await make_due(harness, context, deletion)
    assert await execute(harness, processor, deletion) is None
    assert (await source_for(harness, context, source_id)).status == SourceStatus.DELETED
    assert vector.calls[operation] == 2 and not vector.contents and not vector.statuses


async def test_partial_real_s3_delete_failure_retries_without_resurrecting_content(
    harness: Harness, s3_config: dict[str, str]
) -> None:
    vector = FakeVector()
    service, processor = enable(harness, vector)
    context, source_id, index_id = await ingest(harness)
    assert await execute(harness, processor, index_id) is None

    class PartiallyFailingObjects:
        async def list_keys(self, *, user_id: UUID, source_id: UUID) -> tuple[str, ...]:
            return await harness.objects.list_keys(user_id=user_id, source_id=source_id)

        calls = 0

        async def delete(self, *, user_id: UUID, source_id: UUID, key: str) -> None:
            self.calls += 1
            if self.calls == 2:
                raise ArtifactError("synthetic_s3_delete_failure", retryable=True)
            await harness.objects.delete(user_id=user_id, source_id=source_id, key=key)

    processor = KnowledgeProcessor(harness.repository, vector, PartiallyFailingObjects())
    deletion = await service.delete_document(context, source_id)
    assert deletion is not None
    error = await execute(harness, processor, deletion)
    assert error is not None and error.code == "synthetic_s3_delete_failure"
    assert (await source_for(harness, context, source_id)).status == SourceStatus.DELETING
    assert await service.search(context, "测试") == ()
    client = s3_client(s3_config)
    prefix = f"users/{context.user_id}/"
    try:
        assert client.list_objects_v2(Bucket=s3_config["bucket"], Prefix=prefix)["KeyCount"] == 2
        await make_due(harness, context, deletion)
        assert await execute(harness, processor, deletion) is None
        assert client.list_objects_v2(Bucket=s3_config["bucket"], Prefix=prefix)["KeyCount"] == 0
    finally:
        client.close()
    assert (await source_for(harness, context, source_id)).status == SourceStatus.DELETED
    assert vector.calls["detach"] == vector.calls["delete_file"] == 2


async def test_upload_delete_race_keeps_cleanup_pending_until_upload_resolves(
    harness: Harness,
) -> None:
    vector = FakeVector()
    vector.pause("upload")
    service, processor = enable(harness, vector)
    context, source_id, index_id = await ingest(harness)
    indexing = asyncio.create_task(execute(harness, processor, index_id))
    try:
        await asyncio.wait_for(vector.entered["upload"].wait(), timeout=5)
        deletion = await service.delete_document(context, source_id)
        assert deletion is not None
        error = await execute(harness, processor, deletion)
        assert error is not None and error.code == "knowledge_upload_outcome_unknown"
        assert (await source_for(harness, context, source_id)).status == SourceStatus.DELETING
        vector.release["upload"].set()
        index_error = await asyncio.wait_for(indexing, timeout=5)
        assert index_error is not None and index_error.code == "knowledge_source_unavailable"
        assert vector.calls["attach"] == 0
        await make_due(harness, context, deletion)
        assert await execute(harness, processor, deletion) is None
        assert not vector.contents
        assert (await source_for(harness, context, source_id)).status == SourceStatus.DELETED
        assert await service.search(context, "测试") == ()
    finally:
        vector.release["upload"].set()
        if not indexing.done():
            indexing.cancel()
        await asyncio.gather(indexing, return_exceptions=True)


async def test_concurrent_first_documents_create_exactly_one_tenant_store(
    harness: Harness, urls: dict[str, str]
) -> None:
    vector = FakeVector()
    vector.pause("create_store")
    _, processor = enable(harness, vector)
    context, first_source, first_dispatch = await ingest(harness, content="保存：第一条独立内容")
    _, second_source, second_dispatch = await ingest(harness, content="保存：第二条独立内容")
    engine = create_async_engine(urls["TEST_DATABASE_URL"], pool_size=3, hide_parameters=True)
    original_factory = harness.repository.tenant_factory
    harness.repository.tenant_factory = async_sessionmaker(engine, expire_on_commit=False)
    first = asyncio.create_task(execute(harness, processor, first_dispatch))
    try:
        await asyncio.wait_for(vector.entered["create_store"].wait(), timeout=5)
        second_error = await execute(harness, processor, second_dispatch)
        assert second_error is not None and second_error.code == "knowledge_store_outcome_unknown"
        assert vector.calls["create_store"] == 1
        vector.release["create_store"].set()
        assert await asyncio.wait_for(first, timeout=5) is None
        await make_due(harness, context, second_dispatch)
        assert await execute(harness, processor, second_dispatch) is None
        assert vector.calls["create_store"] == 1 and vector.calls["upload"] == 2
        for source_id in (first_source, second_source):
            assert (await source_for(harness, context, source_id)).status == SourceStatus.READY
        async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
            assert set(await session.scalars(select(KnowledgeFile.vector_store_id))) == {
                vector.stores[context.user_id]
            }
    finally:
        vector.release["create_store"].set()
        if not first.done():
            first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        harness.repository.tenant_factory = original_factory
        await engine.dispose()


async def test_knowledge_journal_rls_denies_missing_foreign_identity_and_connector(
    harness: Harness, runtime_engine: AsyncEngine
) -> None:
    vector = FakeVector()
    _, processor = enable(harness, vector)
    first, _, first_dispatch = await ingest(harness)
    second, second_source, _ = await ingest(harness, user="second-customer")
    assert await execute(harness, processor, first_dispatch) is None
    async with runtime_engine.connect() as connection:
        assert await connection.scalar(select(func.count()).select_from(KnowledgeFile)) == 0
    with pytest.raises(DBAPIError) as missing:
        async with harness.store.tenant_factory() as session, session.begin():
            session.add(
                KnowledgeFile(
                    user_id=second.user_id, source_id=second_source, document_sha256="a" * 64
                )
            )
            await session.flush()
    assert missing.value.orig.sqlstate == "42501"
    with pytest.raises(DBAPIError) as foreign:
        async with tenant_session(harness.store.tenant_factory, first.user_id) as session:
            session.add(
                KnowledgeFile(
                    user_id=second.user_id, source_id=second_source, document_sha256="b" * 64
                )
            )
            await session.flush()
    assert foreign.value.orig.sqlstate == "42501"
    async with tenant_session(harness.store.tenant_factory, second.user_id) as session:
        assert await session.scalar(select(func.count()).select_from(KnowledgeFile)) == 0
        result = await session.execute(
            update(KnowledgeFile)
            .where(KnowledgeFile.user_id == first.user_id)
            .values(vector_file_id="forged-file")
        )
        assert result.rowcount == 0
    for statement in (
        select(KnowledgeFile),
        update(KnowledgeFile).values(vector_file_id="forged-file"),
    ):
        with pytest.raises(DBAPIError) as connector:
            async with corporate_session(
                harness.store.connector_factory, harness.store.corp_id
            ) as session:
                await session.execute(statement)
        assert connector.value.orig.sqlstate == "42501"


async def test_metadata_upgrade_indexes_original_source_identity(
    harness: Harness, tmp_path: Path
) -> None:
    path = tmp_path / "metadata-upgrade.pdf"
    text_pdf(path, pages=2)
    harness.media = Media(path.read_bytes(), path.name)
    context = TenantContext(await harness.admit(file=True))
    await harness.drain()
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        original = (await session.scalars(select(Source))).one()
        assert original.status == SourceStatus.METADATA_ONLY
        original_source, original_key = original.id, original.storage_key
    vector = FakeVector()
    service, processor = enable(harness, vector)
    before = {row.id for row in await harness.dispatches()}
    await harness.admit(file=True)
    incoming = next(row for row in await harness.dispatches() if row.id not in before)
    work = await harness.repository.claim(incoming.id)
    assert work is not None and work.source_id != original_source
    pipeline = IngestionPipeline(
        harness.repository, harness.media, harness.objects, parser=LocalFileParser(ParseLimits())
    )
    await pipeline.process(work)
    index_id = await dispatch_for(harness, context, original_source, INDEX)
    assert await execute(harness, processor, index_id) is None
    source = await service.get_source(context, original_source)
    assert source is not None and source.status == SourceStatus.READY and "page 2" in source.text
    assert await service.get_source(context, work.source_id) is None
    assert (await source_for(harness, context, original_source)).storage_key == original_key
    assert [hit.source.source_id for hit in await service.search(context, "page 2")] == [
        original_source
    ]
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        assert set(await session.scalars(select(MessageSource.source_id))) == {original_source}


async def test_historical_backfill_is_bounded_idempotent_and_reuses_stored_assets(
    harness: Harness,
) -> None:
    context = TenantContext(await harness.admit("保存：历史内容一"))
    await harness.admit("保存：历史内容二")
    await harness.admit("保存：未授权历史内容", user="second-customer")
    await harness.drain()
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        source_ids = set(await session.scalars(select(Source.id)))
        asset_ids = set(await session.scalars(select(Asset.id)))
    vector = FakeVector()
    _, processor = enable(harness, vector)
    harness.settings.wecom_allowed_user_ids = frozenset({TEST_USER})
    arguments = (harness.store.tenant_factory, harness.store.connector_factory, harness.settings)
    assert await backfill(*arguments, limit=1) == 1
    assert await backfill(*arguments, limit=1) == 1
    assert await backfill(*arguments, limit=10) == 0
    for source_id in source_ids:
        dispatch_id = await dispatch_for(harness, context, source_id, INDEX)
        assert await execute(harness, processor, dispatch_id) is None
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        assert set(await session.scalars(select(Asset.id))) == asset_ids
        assert set(await session.scalars(select(Source.status))) == {SourceStatus.READY}
    assert not vector.calls["find_file"] and vector.calls["upload"] == 2
    assert harness.media.calls == 0


async def test_revoked_tenant_cannot_search_or_index_and_queued_index_makes_no_remote_calls(
    harness: Harness,
) -> None:
    vector = FakeVector()
    service, _ = enable(harness, vector)
    context, source_id, dispatch_id = await ingest(harness)
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        user = await session.get(User, context.user_id)
        assert user is not None
        user.is_active = False
    with pytest.raises(ArtifactError, match="knowledge_access_denied"):
        await service.search(context, "测试")
    with pytest.raises(ArtifactError, match="knowledge_access_denied"):
        await service.index_document(context, source_id)
    assert await harness.repository.claim(dispatch_id) is None
    assert not vector.calls
    assert (await source_for(harness, context, source_id)).status == SourceStatus.STORED
