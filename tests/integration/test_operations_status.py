"""Status uses real PostgreSQL RLS, immutable reads, and no privileged credentials."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr
from sqlalchemy import event, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.agent.models import QuestionDispatch, QuestionJob
from app.connectors.wecom.persistence import WeComOutbox, WeComSyncState
from app.core.config import Settings
from app.db.models import Conversation, IngestionJob, Message, Source, User
from app.db.session import corporate_session, tenant_session
from app.domain.enums import JobStatus, MessageRole, SourceStatus, SourceType
from app.ingestion.models import IngestionDispatch
from app.operations.status import _readonly, run, snapshot

pytestmark = pytest.mark.integration


@dataclass
class StatusFixture:
    settings: Settings
    tenant: async_sessionmaker[AsyncSession]
    connector: async_sessionmaker[AsyncSession]
    user_id: UUID
    source_id: UUID
    ingestion_id: UUID
    question_id: UUID
    outbox_id: UUID


async def seed(
    urls: dict[str, str],
    runtime: AsyncEngine,
    connector: AsyncEngine,
) -> StatusFixture:
    corp = "private-corp-" + uuid4().hex
    tenant_factory = async_sessionmaker(runtime, expire_on_commit=False)
    connector_factory = async_sessionmaker(connector, expire_on_commit=False)
    user_id, source_id = uuid4(), uuid4()
    now = datetime.now(UTC)
    old = now - timedelta(minutes=20)
    config = Settings(
        database_url=SecretStr(urls["TEST_DATABASE_URL"]),
        connector_database_url=SecretStr(urls["TEST_CONNECTOR_DATABASE_URL"]),
        redis_url=SecretStr(urls["TEST_REDIS_URL"]),
        wecom_corp_id=corp,
        wecom_open_kfids=frozenset({"private-present-account", "private-missing-account"}),
    )
    async with tenant_session(tenant_factory, user_id) as session:
        session.add(
            User(id=user_id, wecom_corp_id=corp, wecom_external_user_id="private-external-identity")
        )
        await session.flush()
        conversation = Conversation(user_id=user_id, open_kfid="private-present-account")
        source = Source(
            id=source_id,
            user_id=user_id,
            source_type=SourceType.NOTE,
            title="private-source-title",
            text="private-source-body",
            status=SourceStatus.STORED,
        )
        session.add_all([conversation, source])
        await session.flush()
        message = Message(
            user_id=user_id,
            conversation_id=conversation.id,
            role=MessageRole.USER,
            message_type="text",
            content="private-message-body",
            wechat_msg_id="private-message-id",
        )
        session.add(message)
        await session.flush()
        ingestion = IngestionJob(
            user_id=user_id,
            source_id=source_id,
            message_id=message.id,
            status=JobStatus.FAILED,
            error_message="s3_upload_failed",
            attempts=2,
            next_retry_at=now + timedelta(minutes=1),
            input_data={"media_id": "private-media-identity", "query": "private-query"},
        )
        question = QuestionJob(
            user_id=user_id,
            message_id=message.id,
            status="processing",
            attempts=1,
            lease_token=uuid4(),
            lease_expires_at=now - timedelta(seconds=10),
            result_parts=["private-result-content"],
            source_snapshots={"private": "private"},
        )
        session.add_all([ingestion, question])
        await session.flush()
        first = IngestionDispatch(
            corp_id=corp,
            user_id=user_id,
            job_id=ingestion.id,
            created_at=old,
            available_at=old,
            published_at=old,
        )
        second = QuestionDispatch(
            corp_id=corp,
            user_id=user_id,
            job_id=question.id,
            created_at=old,
            available_at=now + timedelta(minutes=1),
        )
        outbox = WeComOutbox(
            corp_id=corp,
            user_id=user_id,
            inbound_message_id=message.id,
            open_kfid="private-present-account",
            external_userid="private-external-identity",
            reply_msgid=uuid4().hex,
            content="private-reply-content",
            received_at=old,
            created_at=old,
            status="uncertain",
            error_message="wecom_transport_failed",
            attempts=1,
        )
        session.add_all([first, second, outbox])
        await session.flush()
    async with corporate_session(connector_factory, corp) as session:
        session.add(
            WeComSyncState(
                corp_id=corp,
                open_kfid="private-present-account",
                cursor="private-cursor",
                status="retry",
                attempts=1,
                last_synced_at=old,
                error_message="wecom_95007",
            )
        )
    return StatusFixture(
        config,
        tenant_factory,
        connector_factory,
        user_id,
        source_id,
        first.id,
        second.id,
        outbox.id,
    )


@pytest.fixture
async def monitor(
    urls: dict[str, str],
    runtime_engine: AsyncEngine,
    connector_engine: AsyncEngine,
) -> StatusFixture:
    return await seed(urls, runtime_engine, connector_engine)


async def test_snapshot_counts_lags_errors_without_private_values(monitor: StatusFixture) -> None:
    report = await snapshot(monitor.tenant, monitor.connector, monitor.settings)
    assert report["business_health"] == "not_assessed"
    assert report["sync"]["account_count"] == 1
    assert report["sync"]["states"]["retry"] == 1
    assert report["sync"]["configured_account_count"] == 2
    assert report["sync"]["missing_configured_account_count"] == 1
    assert report["sync"]["oldest_last_sync_age_seconds"] >= 1200
    assert report["ingestion"]["unfinished_count"] == 1
    assert report["ingestion"]["due_count"] == 1
    assert report["questions"]["unfinished_count"] == 1
    assert report["questions"]["due_count"] == 0
    assert report["outbox"]["states"]["uncertain"] == 1
    assert report["outbox"]["unresolved_records"][0]["error"] == "wecom_transport_failed"
    encoded = json.dumps(report)
    assert "private" not in encoded
    assert str(monitor.user_id) not in encoded
    assert str(monitor.source_id) not in encoded


@pytest.mark.parametrize("kind", ["ingestion", "question"])
async def test_dispatch_diagnosis_binds_verified_tenant(
    monitor: StatusFixture,
    kind: str,
) -> None:
    dispatch_id = monitor.ingestion_id if kind == "ingestion" else monitor.question_id
    report = await snapshot(
        monitor.tenant, monitor.connector, monitor.settings, dispatch_id=dispatch_id
    )
    job = report["dispatch"]["job"]
    assert job["kind"] == kind
    if kind == "ingestion":
        assert job["status"] == "failed"
        assert job["error"] == "s3_upload_failed"
        assert job["attempts"] == 2
        assert job["source_status"] == "stored"
        assert job["next_retry_at"] is not None
    else:
        assert job["status"] == "processing"
        assert job["lease_expired"] is True
    assert "private" not in json.dumps(report)


async def test_foreign_dispatch_and_corporate_totals_are_hidden(
    monitor: StatusFixture,
    urls: dict[str, str],
    runtime_engine: AsyncEngine,
    connector_engine: AsyncEngine,
) -> None:
    other = await seed(urls, runtime_engine, connector_engine)
    for dispatch_id in (other.ingestion_id, other.question_id, uuid4()):
        with pytest.raises(RuntimeError, match="dispatch_unavailable"):
            await snapshot(
                monitor.tenant, monitor.connector, monitor.settings, dispatch_id=dispatch_id
            )
    own = await snapshot(monitor.tenant, monitor.connector, monitor.settings)
    assert own["sync"]["account_count"] == 1
    assert own["outbox"]["states"]["uncertain"] == 1


async def test_unknown_stored_errors_are_redacted(monitor: StatusFixture) -> None:
    async with corporate_session(monitor.connector, monitor.settings.wecom_corp_id) as session:
        await session.execute(
            update(WeComOutbox)
            .where(WeComOutbox.id == monitor.outbox_id)
            .values(error_message="private_lowercase_secret")
        )
    async with tenant_session(monitor.tenant, monitor.user_id) as session:
        await session.execute(update(IngestionJob).values(error_message="private_secret"))
    report = await snapshot(
        monitor.tenant, monitor.connector, monitor.settings, dispatch_id=monitor.ingestion_id
    )
    assert report["outbox"]["unresolved_records"][0]["error"] == "unrecognized_error"
    assert report["dispatch"]["job"]["error"] == "unrecognized_error"
    assert "private" not in json.dumps(report)


async def test_finished_dispatches_are_not_counted_as_backlog(monitor: StatusFixture) -> None:
    async with corporate_session(monitor.connector, monitor.settings.wecom_corp_id) as session:
        await session.execute(update(IngestionDispatch).values(finished_at=datetime.now(UTC)))
        await session.execute(update(QuestionDispatch).values(finished_at=datetime.now(UTC)))
    report = await snapshot(
        monitor.tenant, monitor.connector, monitor.settings, dispatch_id=monitor.question_id
    )
    assert report["ingestion"]["unfinished_count"] == 0
    assert report["ingestion"]["oldest_due_age_seconds"] is None
    assert report["questions"]["unfinished_records"] == []
    assert report["dispatch"]["job"]["lease_expired"] is True
    assert report["dispatch"]["finished_at"] is not None


async def test_record_bound_preserves_total_counts_and_never_synced_accounts(
    monitor: StatusFixture,
) -> None:
    async with tenant_session(monitor.tenant, monitor.user_id) as session:
        original = await session.get(WeComOutbox, monitor.outbox_id)
        assert original is not None
        session.add(
            WeComOutbox(
                corp_id=monitor.settings.wecom_corp_id,
                user_id=monitor.user_id,
                inbound_message_id=original.inbound_message_id,
                open_kfid="private-present-account",
                external_userid="private-external-identity",
                reply_msgid=uuid4().hex,
                purpose="synthetic-second-reply",
                content="private-second-reply-content",
                received_at=datetime.now(UTC),
            )
        )
    async with corporate_session(monitor.connector, monitor.settings.wecom_corp_id) as session:
        session.add(
            WeComSyncState(
                corp_id=monitor.settings.wecom_corp_id,
                open_kfid="private-missing-account",
            )
        )
    report = await snapshot(monitor.tenant, monitor.connector, monitor.settings, limit=1)
    assert report["sync"]["never_synced_count"] == 1
    assert report["sync"]["missing_configured_account_count"] == 0
    assert report["outbox"]["states"]["queued"] == 1
    assert report["outbox"]["states"]["uncertain"] == 1
    assert len(report["outbox"]["unresolved_records"]) == 1


async def test_snapshot_does_not_issue_data_mutations(
    monitor: StatusFixture,
    runtime_engine: AsyncEngine,
    connector_engine: AsyncEngine,
) -> None:
    statements: list[str] = []

    def record(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    for engine in (runtime_engine, connector_engine):
        event.listen(engine.sync_engine, "before_cursor_execute", record)
    try:
        await snapshot(
            monitor.tenant, monitor.connector, monitor.settings, dispatch_id=monitor.ingestion_id
        )
    finally:
        for engine in (runtime_engine, connector_engine):
            event.remove(engine.sync_engine, "before_cursor_execute", record)
    assert statements
    assert all(
        not command.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "ALTER"))
        for command in statements
    )
    assert all("FOR UPDATE" not in command.upper() for command in statements)
    assert all(
        "input_data" not in command
        and "result_parts" not in command
        and "wecom_outbox.content" not in command
        for command in statements
    )


async def test_postgresql_enforces_read_only_and_transaction_local_identity(
    monitor: StatusFixture,
) -> None:
    with pytest.raises(DBAPIError):
        async with corporate_session(monitor.connector, monitor.settings.wecom_corp_id) as session:
            await _readonly(session)
            assert await session.scalar(text("SHOW transaction_read_only")) == "on"
            await session.execute(update(WeComOutbox).values(status="queued"))
    async with monitor.connector() as session:
        assert await session.scalar(text("SHOW transaction_read_only")) == "off"
        assert (await session.scalars(select(WeComOutbox.id))).all() == []


@pytest.mark.parametrize("field", ["database_url", "connector_database_url"])
async def test_privileged_database_urls_rejected_before_reporting(
    monitor: StatusFixture,
    urls: dict[str, str],
    field: str,
) -> None:
    configuration = monitor.settings.model_copy(
        update={field: SecretStr(urls["TEST_DATABASE_ADMIN_URL"])}
    )
    with pytest.raises(RuntimeError, match="must not bypass row level security"):
        await run(configuration)


async def test_runtime_role_cannot_be_used_as_connector(monitor: StatusFixture) -> None:
    configuration = monitor.settings.model_copy(
        update={
            "connector_database_url": monitor.settings.database_url,
        }
    )
    with pytest.raises(RuntimeError, match="unsafe_database_role"):
        await run(configuration)


async def test_run_with_actual_restricted_roles_produces_read_only_snapshot(
    monitor: StatusFixture,
) -> None:
    report = await run(monitor.settings, limit=1, dispatch_id=monitor.question_id)
    assert report["sync"]["account_count"] == 1
    assert report["questions"]["record_limit"] == 1
    assert report["dispatch"]["job"]["lease_expired"] is True
