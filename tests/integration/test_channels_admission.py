"""Channels supplement admission against real PostgreSQL and the non-owner tenant role."""

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

import app.connectors.wecom.channels as channels_module
from app.connectors.wecom.channels import WeComChannelSupplementBridge
from app.connectors.wecom.contracts import NormalizedMessage, WeComError
from app.connectors.wecom.ingestion import WeComIngestionBridge
from app.connectors.wecom.persistence import WeComOutbox
from app.connectors.wecom.store import WeComStore, user_identity
from app.core.config import Settings
from app.db.models import Conversation, IngestionJob, KnowledgeFile, Message, Source, User
from app.db.session import tenant_session
from app.domain.artifacts import ArtifactError
from app.domain.enums import JobStatus, MessageRole, SourceStatus, SourceType
from app.ingestion.models import IngestionDispatch, MessageSource
from tests.wecom_helpers import TEST_KEY, TEST_KF, TEST_TOKEN, TEST_USER

pytestmark = pytest.mark.integration
SECOND_KF = "second-synthetic-account"
SECOND_USER = "second-synthetic-customer"


class Admission:
    """Exercise the new bridge independently while preserving existing ordinary ingestion."""

    def __init__(self, settings: Settings) -> None:
        self.supplement = WeComChannelSupplementBridge(settings)
        self.ordinary = WeComIngestionBridge(settings)
        self.handled: dict[str, bool] = {}

    async def admit(
        self, session: AsyncSession, user_id: UUID, message_id: UUID, message: NormalizedMessage
    ) -> bool:
        result = await self.supplement.admit(session, user_id, message_id, message)
        self.handled[message.msgid] = result
        return result or await self.ordinary.admit(session, user_id, message_id, message)


@dataclass
class Harness:
    settings: Settings
    store: WeComStore
    admission: Admission

    def user_id(self, external: str = TEST_USER) -> UUID:
        return user_identity(self.settings.wecom_corp_id, external)

    async def send(
        self,
        content: str | None = None,
        *,
        video: bool = False,
        user: str = TEST_USER,
        account: str = TEST_KF,
        sent_at: datetime | None = None,
    ) -> NormalizedMessage:
        message = NormalizedMessage(
            uuid4().hex,
            account,
            user,
            sent_at or datetime.now(UTC),
            "video" if video else "text",
            content,
            {"media_id": "synthetic-official-media"} if video else {},
        )
        assert await self.store.persist_message(message, None)
        return message

    async def card(self, user: str = TEST_USER) -> Source:
        await self.send("初始化", user=user)
        source = Source(
            id=uuid4(),
            user_id=self.user_id(user),
            title="合成视频号卡片",
            source_type=SourceType.WECHAT_CHANNEL,
            status=SourceStatus.METADATA_ONLY,
            sha256=uuid4().hex * 2,
            text="",
            metadata_={"parse_status": "metadata_only", "channels": {"nickname": "合成账号"}},
        )
        async with tenant_session(self.store.tenant_factory, source.user_id) as session:
            session.add(source)
            await session.flush()
        return source

    async def persisted(self, message: NormalizedMessage) -> Message:
        async with tenant_session(
            self.store.tenant_factory, self.user_id(message.external_userid)
        ) as session:
            return (
                await session.scalars(select(Message).where(Message.wechat_msg_id == message.msgid))
            ).one()

    async def jobs(self, user: str = TEST_USER) -> list[IngestionJob]:
        async with tenant_session(self.store.tenant_factory, self.user_id(user)) as session:
            return list(await session.scalars(select(IngestionJob)))

    async def reply(self, message: NormalizedMessage) -> str:
        inbound = await self.persisted(message)
        async with tenant_session(self.store.tenant_factory, inbound.user_id) as session:
            return (
                await session.scalars(
                    select(WeComOutbox.content).where(WeComOutbox.inbound_message_id == inbound.id)
                )
            ).one()


@pytest.fixture
def channel_harness(
    urls: dict[str, str], runtime_engine: AsyncEngine, connector_engine: AsyncEngine
) -> Harness:
    settings = Settings.model_validate(
        {
            "database_url": urls["TEST_DATABASE_URL"],
            "connector_database_url": urls["TEST_CONNECTOR_DATABASE_URL"],
            "redis_url": urls["TEST_REDIS_URL"],
            "wecom_enabled": True,
            "wecom_corp_id": "m8-admission-" + uuid4().hex,
            "wecom_secret": "synthetic-secret",
            "wecom_callback_token": TEST_TOKEN,
            "wecom_encoding_aes_key": TEST_KEY,
            "wecom_open_kfids": [TEST_KF, SECOND_KF],
            "wecom_allowed_user_ids": [TEST_USER, SECOND_USER],
            "ingestion_enabled": True,
            "s3_endpoint_url": "http://synthetic-storage.invalid",
            "s3_access_key_id": "synthetic-access",
            "s3_secret_access_key": "synthetic-secret",
            "s3_bucket": "synthetic-private-bucket",
        }
    )
    admission = Admission(settings)
    store = WeComStore(
        async_sessionmaker(runtime_engine, expire_on_commit=False),
        async_sessionmaker(connector_engine, expire_on_commit=False),
        settings.wecom_corp_id,
        admission,
    )
    return Harness(settings, store, admission)


async def test_success_is_atomic_reuses_source_and_consumes_intent_once(
    channel_harness: Harness,
) -> None:
    harness = channel_harness
    source = await harness.card()
    command = await harness.send(f"补充视频 {source.id}")
    video = await harness.send(video=True)
    jobs = await harness.jobs()
    assert len(jobs) == 1
    job = jobs[0]
    command_row, video_row = await harness.persisted(command), await harness.persisted(video)
    assert command_row.metadata_["channel_supplement"]["state"] == "consumed"
    assert job.source_id == source.id and job.message_id == video_row.id
    assert job.operation_key == f"channel_supplement:{source.id}"
    assert job.status == JobStatus.QUEUED
    assert job.input_data == {
        "kind": "channel_supplement",
        "media_id": "synthetic-official-media",
        "filename": None,
        "request_message_id": str(command_row.id),
    }
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        current = (await session.scalars(select(Source))).one()
        assert (current.status, current.sha256, current.created_at, current.metadata_) == (
            source.status,
            source.sha256,
            source.created_at,
            source.metadata_,
        )
        mapping = (await session.scalars(select(MessageSource))).one()
        assert mapping.message_id == video_row.id and mapping.source_id == source.id
        dispatch = (await session.scalars(select(IngestionDispatch))).one()
        assert dispatch.user_id == source.user_id and dispatch.job_id == job.id
        role = (
            await session.execute(
                text("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user")
            )
        ).one()
        assert role == (False, False)
    assert "10分钟" in await harness.reply(command)
    assert await harness.reply(video) == "收到，正在整理：合成视频号卡片"
    assert not await harness.store.persist_message(video, None)
    assert not await harness.store.persist_message(command, None)
    second = await harness.send(video=True)
    assert not harness.admission.handled[second.msgid]
    assert len(await harness.jobs()) == 2  # Next video is a separate ordinary source.


async def test_direct_admission_replay_does_not_rearm_or_duplicate(
    channel_harness: Harness,
) -> None:
    harness = channel_harness
    source = await harness.card()
    command = await harness.send(f"补充视频 {source.id}")
    video = await harness.send(video=True)
    for original in (command, video):
        inbound = await harness.persisted(original)
        async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
            assert await harness.admission.supplement.admit(
                session, source.user_id, inbound.id, original
            )
    assert len(await harness.jobs()) == 1
    assert (await harness.persisted(command)).metadata_["channel_supplement"]["state"] == "consumed"


async def test_cross_tenant_uuid_is_indistinguishable_from_missing(
    channel_harness: Harness,
) -> None:
    harness = channel_harness
    source = await harness.card(SECOND_USER)
    foreign = await harness.send(f"补充视频 {source.id}")
    missing = await harness.send(f"补充视频 {uuid4()}")
    assert await harness.reply(foreign) == await harness.reply(missing)
    assert "channel_supplement" not in (await harness.persisted(foreign)).metadata_
    assert not await harness.jobs() and not await harness.jobs(SECOND_USER)


async def test_pending_is_confined_to_customer_service_conversation(
    channel_harness: Harness,
) -> None:
    harness = channel_harness
    source = await harness.card()
    command = await harness.send(f"补充视频 {source.id}")
    elsewhere = await harness.send(video=True, account=SECOND_KF)
    assert not harness.admission.handled[elsewhere.msgid]
    assert (await harness.persisted(command)).metadata_["channel_supplement"]["state"] == "pending"
    await harness.send(video=True)
    jobs = await harness.jobs()
    assert len(jobs) == 2
    assert sum(job.source_id == source.id for job in jobs) == 1


async def test_cancel_and_replacement_have_single_pending_intent(channel_harness: Harness) -> None:
    harness = channel_harness
    first, second = await harness.card(), await harness.card()
    old = await harness.send(f"补充视频 {first.id}")
    new = await harness.send(f"补充视频 {second.id}")
    assert (await harness.persisted(old)).metadata_["channel_supplement"]["state"] == "replaced"
    await harness.send("取消补充视频")
    assert (await harness.persisted(new)).metadata_["channel_supplement"]["state"] == "cancelled"
    normal = await harness.send(video=True)
    assert not harness.admission.handled[normal.msgid]
    assert all(job.source_id not in {first.id, second.id} for job in await harness.jobs())


@pytest.mark.parametrize("offset", [-601, 61])
async def test_stale_or_far_future_command_never_creates_intent(
    channel_harness: Harness, offset: int
) -> None:
    harness = channel_harness
    source = await harness.card()
    command = await harness.send(
        f"补充视频 {source.id}", sent_at=datetime.now(UTC) + timedelta(seconds=offset)
    )
    assert "channel_supplement" not in (await harness.persisted(command)).metadata_
    assert "时间异常" in await harness.reply(command)
    assert not await harness.jobs()


async def test_expired_pending_rejects_video_without_silent_separate_ingestion(
    channel_harness: Harness,
) -> None:
    harness = channel_harness
    source = await harness.card()
    command = await harness.send(f"补充视频 {source.id}")
    persisted = await harness.persisted(command)
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        row = await session.get(Message, persisted.id)
        assert row is not None
        now = datetime.now(UTC)
        row.metadata_ = {
            **row.metadata_,
            "channel_supplement": {
                "source_id": str(source.id),
                "state": "pending",
                "requested_at": (now - timedelta(minutes=11)).isoformat(),
                "expires_at": (now - timedelta(minutes=1)).isoformat(),
            },
        }
    video = await harness.send(video=True)
    assert harness.admission.handled[video.msgid]
    assert "尚未关联或收录" in await harness.reply(video)
    assert not await harness.jobs()
    assert (await harness.persisted(command)).metadata_["channel_supplement"]["state"] == "expired"


async def test_out_of_order_video_and_cancel_cannot_consume_newer_request(
    channel_harness: Harness,
) -> None:
    harness = channel_harness
    source = await harness.card()
    now = datetime.now(UTC)
    command = await harness.send(f"补充视频 {source.id}", sent_at=now)
    stale_video = await harness.send(video=True, sent_at=now - timedelta(seconds=2))
    stale_cancel = await harness.send("取消补充视频", sent_at=now - timedelta(seconds=1))
    assert "尚未关联或收录" in await harness.reply(stale_video)
    assert "未改变" in await harness.reply(stale_cancel)
    assert not await harness.jobs()
    assert (await harness.persisted(command)).metadata_["channel_supplement"]["state"] == "pending"
    await harness.send(video=True)
    assert len(await harness.jobs()) == 1


@pytest.mark.parametrize(
    "state", [SourceStatus.DELETING, SourceStatus.DELETED, SourceStatus.STORED, SourceStatus.READY]
)
async def test_changed_or_deleted_target_is_not_revived(
    channel_harness: Harness, state: SourceStatus
) -> None:
    harness = channel_harness
    source = await harness.card()
    command = await harness.send(f"补充视频 {source.id}")
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        current = await session.get(Source, source.id)
        assert current is not None
        current.status = state
    video = await harness.send(video=True)
    assert "尚未关联或收录" in await harness.reply(video)
    assert not await harness.jobs()
    assert (await harness.persisted(command)).metadata_["channel_supplement"]["state"] == "rejected"
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        assert (await session.get(Source, source.id)).status == state


@pytest.mark.parametrize(
    "condition", ["file_id", "knowledge_journal", "supplemented", "wrong_type"]
)
async def test_only_unindexed_unsupplemented_channels_card_eligible(
    channel_harness: Harness, condition: str
) -> None:
    harness = channel_harness
    source = await harness.card()
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        current = await session.get(Source, source.id)
        assert current is not None
        if condition == "file_id":
            current.vector_file_id = "synthetic-" + uuid4().hex
        elif condition == "knowledge_journal":
            session.add(
                KnowledgeFile(user_id=source.user_id, source_id=source.id, document_sha256="a" * 64)
            )
        elif condition == "supplemented":
            current.metadata_ = {"channel_supplement": {"original_asset_id": str(uuid4())}}
        else:
            current.source_type = SourceType.VIDEO
    command = await harness.send(f"补充视频 {source.id}")
    assert "暂不可补充" in await harness.reply(command)
    assert "channel_supplement" not in (await harness.persisted(command)).metadata_
    assert not await harness.jobs()


@pytest.mark.parametrize("state", [JobStatus.QUEUED, JobStatus.COMPLETED])
async def test_existing_active_or_completed_operation_cannot_create_second_job(
    channel_harness: Harness, state: JobStatus
) -> None:
    harness = channel_harness
    source = await harness.card()
    await harness.send(f"补充视频 {source.id}")
    await harness.send(video=True)
    job = (await harness.jobs())[0]
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        current = await session.get(IngestionJob, job.id)
        assert current is not None
        current.status = state
        if state == JobStatus.FAILED:
            current.error_message = "synthetic_download_failure"
    command = await harness.send(f"补充视频 {source.id}")
    assert "管理员重试" in await harness.reply(command)
    assert "channel_supplement" not in (await harness.persisted(command)).metadata_
    assert [item.id for item in await harness.jobs()] == [job.id]


async def test_terminal_failure_can_bind_new_video_without_new_job_or_lost_diagnostic(
    channel_harness: Harness,
) -> None:
    harness = channel_harness
    source = await harness.card()
    await harness.send(f"补充视频 {source.id}")
    old_video = await harness.send(video=True)
    job = (await harness.jobs())[0]
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        current = await session.get(IngestionJob, job.id)
        current.status = JobStatus.FAILED
        current.attempts = 3
        current.error_message = "media_expired"
        dispatch = (await session.scalars(select(IngestionDispatch))).one()
        dispatch.finished_at = datetime.now(UTC)
        dispatch_id = dispatch.id
    command = await harness.send(f"补充视频 {source.id}")
    assert "10分钟" in await harness.reply(command)
    video = await harness.send(video=True)
    jobs = await harness.jobs()
    assert len(jobs) == 1 and jobs[0].id == job.id
    assert jobs[0].status == JobStatus.QUEUED and jobs[0].attempts == 0
    assert jobs[0].message_id == (await harness.persisted(video)).id
    assert jobs[0].error_message is None
    old = await harness.persisted(old_video)
    assert old.metadata_["supplement_failure"]["error_message"] == "media_expired"
    assert old.metadata_["supplement_failure"]["attempts"] == 3
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        dispatch = await session.get(IngestionDispatch, dispatch_id)
        assert dispatch and dispatch.finished_at is None and dispatch.published_at is None
        assert (await session.get(Source, source.id)).status == SourceStatus.METADATA_ONLY
        assert len(list(await session.scalars(select(MessageSource)))) == 2
    assert not await harness.store.persist_message(old_video, None)


@pytest.mark.parametrize("state", ["lease", "automatic_retry"])
async def test_failed_but_owned_or_scheduled_task_cannot_be_rebound(
    channel_harness: Harness,
    state: str,
) -> None:
    harness = channel_harness
    source = await harness.card()
    await harness.send(f"补充视频 {source.id}")
    await harness.send(video=True)
    job = (await harness.jobs())[0]
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        current = await session.get(IngestionJob, job.id)
        current.status, current.error_message = JobStatus.FAILED, "synthetic_failure"
        if state == "lease":
            current.lease_token = uuid4()
            current.lease_expires_at = datetime.now(UTC) + timedelta(minutes=1)
        else:
            current.next_retry_at = datetime.now(UTC) + timedelta(minutes=1)
    command = await harness.send(f"补充视频 {source.id}")
    assert "管理员重试" in await harness.reply(command)
    assert "channel_supplement" not in (await harness.persisted(command)).metadata_


async def test_old_command_cannot_reopen_after_newer_cancel(channel_harness: Harness) -> None:
    harness = channel_harness
    source = await harness.card()
    now = datetime.now(UTC)
    await harness.send("取消补充视频", sent_at=now)
    old = await harness.send(f"补充视频 {source.id}", sent_at=now - timedelta(seconds=1))
    assert "较早" in await harness.reply(old)
    assert "channel_supplement" not in (await harness.persisted(old)).metadata_


async def test_delayed_command_does_not_gain_another_ten_minutes(channel_harness: Harness) -> None:
    harness = channel_harness
    source = await harness.card()
    sent = datetime.now(UTC) - timedelta(minutes=9)
    command = await harness.send(f"补充视频 {source.id}", sent_at=sent)
    intent = (await harness.persisted(command)).metadata_["channel_supplement"]
    assert datetime.fromisoformat(intent["expires_at"]) == sent + timedelta(minutes=10)


async def test_old_command_cannot_reopen_after_newer_video_consumption(
    channel_harness: Harness,
) -> None:
    harness = channel_harness
    first, second = await harness.card(), await harness.card()
    now = datetime.now(UTC)
    await harness.send(f"补充视频 {first.id}", sent_at=now - timedelta(seconds=3))
    await harness.send(video=True, sent_at=now - timedelta(seconds=1))
    stale = await harness.send(f"补充视频 {second.id}", sent_at=now - timedelta(seconds=2))
    assert "较早" in await harness.reply(stale)
    assert "channel_supplement" not in (await harness.persisted(stale)).metadata_


async def test_event_watermark_is_independent_of_transaction_creation_order(
    channel_harness: Harness,
) -> None:
    harness = channel_harness
    first, second = await harness.card(), await harness.card()
    now = datetime.now(UTC)
    early = await harness.send(f"补充视频 {first.id}", sent_at=now - timedelta(seconds=3))
    late = await harness.send(f"补充视频 {second.id}", sent_at=now - timedelta(seconds=1))
    early_row = await harness.persisted(early)
    async with tenant_session(harness.store.tenant_factory, first.user_id) as session:
        row = await session.get(Message, early_row.id)
        row.created_at = now + timedelta(seconds=1)
    stale = await harness.send(f"补充视频 {first.id}", sent_at=now - timedelta(seconds=2))
    assert "较早" in await harness.reply(stale)
    assert (await harness.persisted(late)).metadata_["channel_supplement"]["state"] == "pending"


async def test_transaction_rollback_preserves_pending_and_creates_no_partial_job(
    channel_harness: Harness,
) -> None:
    harness = channel_harness
    source = await harness.card()
    command = await harness.send(f"补充视频 {source.id}")

    class RollbackAdmission:
        async def admit(
            self, session: AsyncSession, user_id: UUID, message_id: UUID, message: NormalizedMessage
        ) -> bool:
            assert await harness.admission.supplement.admit(session, user_id, message_id, message)
            await session.flush()
            raise ArtifactError("synthetic_admission_rollback")

    harness.store.admission = RollbackAdmission()
    with pytest.raises(ArtifactError, match="synthetic_admission_rollback"):
        await harness.send(video=True)
    harness.store.admission = harness.admission
    assert not await harness.jobs()
    assert (await harness.persisted(command)).metadata_["channel_supplement"]["state"] == "pending"
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        assert await session.scalar(select(func.count()).select_from(IngestionDispatch)) == 0
        assert await session.scalar(select(func.count()).select_from(MessageSource)) == 0
    await harness.send(video=True)
    assert len(await harness.jobs()) == 1


async def test_same_source_two_conversations_can_consume_only_one_job(
    channel_harness: Harness, urls: dict[str, str]
) -> None:
    harness = channel_harness
    source = await harness.card()
    first = await harness.send(f"补充视频 {source.id}")
    second = await harness.send(f"补充视频 {source.id}", account=SECOND_KF)
    engine = create_async_engine(urls["TEST_DATABASE_URL"], pool_size=4, hide_parameters=True)
    previous = harness.store.tenant_factory
    harness.store.tenant_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        videos = await asyncio.wait_for(
            asyncio.gather(harness.send(video=True), harness.send(video=True, account=SECOND_KF)),
            timeout=10,
        )
    finally:
        harness.store.tenant_factory = previous
        await engine.dispose()
    assert len(await harness.jobs()) == 1
    assert {
        (await harness.persisted(command)).metadata_["channel_supplement"]["state"]
        for command in (first, second)
    } == {"consumed", "rejected"}
    replies = [await harness.reply(video) for video in videos]
    assert sum("正在整理" in reply for reply in replies) == 1
    assert sum("管理员重试" in reply for reply in replies) == 1


async def test_concurrent_messages_in_same_conversation_do_not_deadlock_on_foreign_keys(
    channel_harness: Harness, urls: dict[str, str]
) -> None:
    harness = channel_harness
    first, second = await harness.card(), await harness.card()
    reached = 0
    inserted = asyncio.Event()

    class SimultaneousAdmission:
        async def admit(
            self, session: AsyncSession, user_id: UUID, message_id: UUID, message: NormalizedMessage
        ) -> bool:
            nonlocal reached
            # Both inserts hold a foreign-key KEY SHARE on the same Conversation.
            reached += 1
            if reached == 2:
                inserted.set()
            await inserted.wait()
            return await harness.admission.admit(session, user_id, message_id, message)

    engine = create_async_engine(urls["TEST_DATABASE_URL"], pool_size=4, hide_parameters=True)
    previous = harness.store.tenant_factory
    harness.store.tenant_factory = async_sessionmaker(engine, expire_on_commit=False)
    harness.store.admission = SimultaneousAdmission()
    sent_at = datetime.now(UTC)
    try:
        commands = await asyncio.wait_for(
            asyncio.gather(
                harness.send(f"补充视频 {first.id}", sent_at=sent_at),
                harness.send(f"补充视频 {second.id}", sent_at=sent_at),
            ),
            timeout=10,
        )
    finally:
        harness.store.admission = harness.admission
        harness.store.tenant_factory = previous
        await engine.dispose()
    assert {
        (await harness.persisted(command)).metadata_["channel_supplement"]["state"]
        for command in commands
    } == {"pending", "replaced"}
    await harness.send(video=True)
    assert len(await harness.jobs()) == 1


@pytest.mark.parametrize("media_id", [None, "", " " * 3, 42, "a" * 1025])
async def test_pending_requires_bounded_official_media_id(
    channel_harness: Harness, media_id: str | int | None
) -> None:
    harness = channel_harness
    source = await harness.card()
    command = await harness.send(f"补充视频 {source.id}")
    video = NormalizedMessage(
        uuid4().hex,
        TEST_KF,
        TEST_USER,
        datetime.now(UTC),
        "video",
        metadata={"media_id": media_id},
    )
    assert await harness.store.persist_message(video, None)
    assert "未取得官方视频素材编号" in await harness.reply(video)
    assert not await harness.jobs()
    assert (await harness.persisted(command)).metadata_["channel_supplement"]["state"] == "pending"


async def test_authority_and_normalized_payload_cannot_be_forged(channel_harness: Harness) -> None:
    harness = channel_harness
    source = await harness.card()
    original = await harness.send("初始化")
    inbound = await harness.persisted(original)
    forged = replace(original, text=f"补充视频 {source.id}")
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        with pytest.raises(WeComError, match="identity_rejected"):
            await harness.admission.supplement.admit(session, source.user_id, inbound.id, forged)
    assert not await harness.jobs()


async def test_revoked_allowlist_or_inactive_user_cannot_establish_intent(
    channel_harness: Harness,
) -> None:
    harness = channel_harness
    source = await harness.card()
    harness.settings.wecom_allowed_user_ids = frozenset()
    with pytest.raises(WeComError, match="identity_rejected"):
        await harness.send(f"补充视频 {source.id}")
    harness.settings.wecom_allowed_user_ids = frozenset({TEST_USER})
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        user = await session.get(User, source.user_id)
        assert user is not None
        user.is_active = False
    message = NormalizedMessage(
        uuid4().hex, TEST_KF, TEST_USER, datetime.now(UTC), "text", f"补充视频 {source.id}"
    )
    assert not await harness.store.persist_message(message, None)
    assert not await harness.jobs()


@pytest.mark.parametrize(
    "template",
    [
        "补充视频 {id}\n再执行其他操作",
        "补充视频 https://example.invalid/{id}",
        "补充视频 {id} extra",
        "补充视频",
        "补充视频 <{id}>",
    ],
)
async def test_nonexact_command_never_authorizes_upload(
    channel_harness: Harness, template: str
) -> None:
    harness = channel_harness
    source = await harness.card()
    command = await harness.send(template.format(id=source.id))
    assert "请发送" in await harness.reply(command)
    assert "channel_supplement" not in (await harness.persisted(command)).metadata_
    assert not await harness.jobs()


async def test_auto_reply_off_still_persists_authorized_job(channel_harness: Harness) -> None:
    harness = channel_harness
    harness.settings.wecom_auto_reply = False
    source = await harness.card()
    await harness.send(f"补充视频 {source.id}")
    await harness.send(video=True)
    assert len(await harness.jobs()) == 1
    async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
        assert await session.scalar(select(func.count()).select_from(WeComOutbox)) == 0


async def test_admission_does_not_deadlock_agent_source_lock_and_assistant_message(
    channel_harness: Harness,
    urls: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = channel_harness
    source = await harness.card()
    engine = create_async_engine(urls["TEST_DATABASE_URL"], pool_size=4, hide_parameters=True)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    previous_factory = harness.store.tenant_factory
    harness.store.tenant_factory = factory
    reached = asyncio.Event()
    original = harness.admission.supplement._target

    async def target(
        session: AsyncSession,
        user_id: UUID,
        source_id: UUID,
    ) -> tuple[Source | None, bool, IngestionJob | None]:
        reached.set()  # Admission now owns NO KEY UPDATE on Conversation.
        return await original(session, user_id, source_id)

    monkeypatch.setattr(harness.admission.supplement, "_target", target)
    task: asyncio.Task[NormalizedMessage] | None = None
    try:
        async with asyncio.timeout(10):
            async with tenant_session(factory, source.user_id) as session:
                assert await session.get(Source, source.id, with_for_update=True)
                conversation = (await session.scalars(select(Conversation))).one()
                task = asyncio.create_task(harness.send(f"补充视频 {source.id}"))
                await reached.wait()
                # An Agent completing under a Source lock inserts an assistant message.
                # Its FK KEY SHARE must remain compatible with admission's Conversation lock.
                session.add(
                    Message(
                        id=uuid4(),
                        user_id=source.user_id,
                        conversation_id=conversation.id,
                        role=MessageRole.ASSISTANT,
                        message_type="text",
                        content="合成回答",
                    )
                )
                await session.flush()
            assert await task
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        harness.store.tenant_factory = previous_factory
        await engine.dispose()


@pytest.mark.parametrize("operation", ["command", "video"])
async def test_expiry_is_rechecked_after_waiting_for_source_lock(
    channel_harness: Harness,
    urls: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    harness = channel_harness
    source = await harness.card()
    started = datetime.now(UTC)
    clock_now = started

    class Clock(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            return clock_now

    monkeypatch.setattr(channels_module, "datetime", Clock)
    if operation == "video":
        await harness.send(f"补充视频 {source.id}", sent_at=started)
    clock_now += timedelta(seconds=1)
    engine = create_async_engine(urls["TEST_DATABASE_URL"], pool_size=4, hide_parameters=True)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    previous_factory = harness.store.tenant_factory
    harness.store.tenant_factory = factory
    reached = asyncio.Event()
    original = harness.admission.supplement._target

    async def target(
        session: AsyncSession,
        user_id: UUID,
        source_id: UUID,
    ) -> tuple[Source | None, bool, IngestionJob | None]:
        reached.set()
        return await original(session, user_id, source_id)

    monkeypatch.setattr(harness.admission.supplement, "_target", target)
    task: asyncio.Task[NormalizedMessage] | None = None
    try:
        async with asyncio.timeout(10):
            async with tenant_session(factory, source.user_id) as session:
                assert await session.get(Source, source.id, with_for_update=True)
                task = asyncio.create_task(
                    harness.send(
                        f"补充视频 {source.id}" if operation == "command" else None,
                        video=operation == "video",
                        sent_at=clock_now,
                    )
                )
                await reached.wait()
                clock_now = started + timedelta(minutes=11)
            reply_to = await task
        assert "过期" in await harness.reply(reply_to)
        assert not await harness.jobs()
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        harness.store.tenant_factory = previous_factory
        await engine.dispose()
