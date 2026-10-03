from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.db.models import Asset, Conversation, IngestionJob, Message, Source, User
from app.db.session import tenant_session
from app.domain.enums import AssetKind, JobStatus, MessageRole, SourceStatus, SourceType

pytestmark = pytest.mark.integration


async def seed_users(engine: AsyncEngine) -> tuple[UUID, UUID]:
    first, second = uuid4(), uuid4()
    async with async_sessionmaker(engine).begin() as session:
        session.add_all(
            [
                User(id=user_id, wecom_corp_id="test-corp", wecom_external_user_id=str(user_id))
                for user_id in (first, second)
            ]
        )
    return first, second


def source(user_id: UUID, **changes: object) -> Source:
    return Source(user_id=user_id, source_type=SourceType.NOTE, title="tenant note", **changes)


async def test_real_runtime_role_cannot_bypass_rls(runtime_engine: AsyncEngine) -> None:
    async with runtime_engine.connect() as connection:
        role = (
            await connection.execute(
                text("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user")
            )
        ).one()
        assert role == (False, False)
        rows = (
            await connection.execute(
                text(
                    "SELECT relname, relrowsecurity, relforcerowsecurity FROM pg_class "
                    "WHERE relname IN ('users','sources','assets','conversations',"
                    "'messages','ingestion_jobs')"
                )
            )
        ).all()
        assert len(rows) == 6
        assert all(row[1] and row[2] for row in rows)


async def test_tenant_isolation_for_all_six_tables_and_pool_reset(
    admin_engine: AsyncEngine, runtime_engine: AsyncEngine
) -> None:
    first, second = await seed_users(admin_engine)
    factory = async_sessionmaker(runtime_engine, expire_on_commit=False)
    for user_id in (first, second):
        async with tenant_session(factory, user_id) as session:
            item = source(user_id)
            conversation = Conversation(user_id=user_id, open_kfid="test-kf")
            session.add_all([item, conversation])
            await session.flush()
            session.add_all(
                [
                    Asset(
                        user_id=user_id,
                        source_id=item.id,
                        kind=AssetKind.ORIGINAL,
                        storage_key=f"users/{user_id}/{item.id}",
                        size_bytes=0,
                    ),
                    IngestionJob(user_id=user_id, source_id=item.id),
                    Message(
                        user_id=user_id,
                        conversation_id=conversation.id,
                        role=MessageRole.USER,
                        message_type="text",
                        content="private",
                    ),
                ]
            )
    models = (User, Source, Asset, Conversation, Message, IngestionJob)
    for user_id in (first, second):
        async with tenant_session(factory, user_id) as session:
            for model in models:
                records = (await session.scalars(select(model))).all()
                assert len(records) == 1
                assert (records[0].id if model is User else records[0].user_id) == user_id
    async with factory() as session:
        for model in models:
            assert await session.scalar(select(func.count()).select_from(model)) == 0
        assert (
            await session.scalar(text("SELECT NULLIF(current_setting('app.user_id', true), '')"))
            is None
        )


async def test_no_tenant_write_and_cross_tenant_write_denied(
    admin_engine: AsyncEngine, runtime_engine: AsyncEngine
) -> None:
    first, second = await seed_users(admin_engine)
    factory = async_sessionmaker(runtime_engine)
    with pytest.raises(DBAPIError):
        async with factory.begin() as session:
            session.add(source(first))
            await session.flush()
    with pytest.raises(DBAPIError):
        async with tenant_session(factory, first) as session:
            session.add(source(second))
            await session.flush()


async def test_foreign_source_update_delete_and_tenant_move_denied(
    admin_engine: AsyncEngine, runtime_engine: AsyncEngine
) -> None:
    first, second = await seed_users(admin_engine)
    factory = async_sessionmaker(runtime_engine)
    async with tenant_session(factory, first) as session:
        item = source(first)
        session.add(item)
        await session.flush()
        source_id = item.id
    async with tenant_session(factory, second) as session:
        updated = await session.execute(
            text("UPDATE sources SET title='stolen' WHERE id=:id RETURNING id"), {"id": source_id}
        )
        assert updated.all() == []
        deleted = await session.execute(
            text("DELETE FROM sources WHERE id=:id RETURNING id"), {"id": source_id}
        )
        assert deleted.all() == []
    with pytest.raises(DBAPIError):
        async with tenant_session(factory, first) as session:
            await session.execute(
                text("UPDATE sources SET user_id=:other WHERE id=:id"),
                {"other": second, "id": source_id},
            )


@pytest.mark.parametrize("child", ["asset", "job", "message"])
async def test_composite_fk_rejects_cross_user_relationships(
    admin_engine: AsyncEngine, child: str
) -> None:
    first, second = await seed_users(admin_engine)
    factory = async_sessionmaker(admin_engine)
    async with factory.begin() as session:
        item = source(first)
        conversation = Conversation(user_id=first, open_kfid="test")
        session.add_all([item, conversation])
        await session.flush()
        source_id, conversation_id = item.id, conversation.id
    # Admin bypasses RLS: this test specifically proves the composite FK, not the policy.
    with pytest.raises(IntegrityError):
        async with factory.begin() as session:
            if child == "asset":
                session.add(
                    Asset(
                        user_id=second,
                        source_id=source_id,
                        kind=AssetKind.ORIGINAL,
                        storage_key="cross-user",
                        size_bytes=1,
                    )
                )
            elif child == "job":
                session.add(IngestionJob(user_id=second, source_id=source_id))
            else:
                session.add(
                    Message(
                        user_id=second,
                        conversation_id=conversation_id,
                        role=MessageRole.USER,
                        message_type="text",
                    )
                )
            await session.flush()


@pytest.mark.parametrize("field,value", [("sha256", "a" * 64), ("wechat_msg_id", "message-1")])
async def test_source_deduplication_is_per_user(
    admin_engine: AsyncEngine, runtime_engine: AsyncEngine, field: str, value: str
) -> None:
    first, second = await seed_users(admin_engine)
    factory = async_sessionmaker(runtime_engine)
    for user_id in (first, second):
        async with tenant_session(factory, user_id) as session:
            session.add(source(user_id, **{field: value}))
    with pytest.raises(IntegrityError):
        async with tenant_session(factory, first) as session:
            session.add(source(first, **{field: value}))
            await session.flush()


async def test_deleted_hash_can_be_ingested_again(
    admin_engine: AsyncEngine, runtime_engine: AsyncEngine
) -> None:
    user_id, _ = await seed_users(admin_engine)
    async with tenant_session(async_sessionmaker(runtime_engine), user_id) as session:
        session.add(source(user_id, sha256="c" * 64, status=SourceStatus.DELETED))
        session.add(source(user_id, sha256="c" * 64))


async def test_failed_job_requires_error_and_preserves_retry_metadata(
    admin_engine: AsyncEngine, runtime_engine: AsyncEngine
) -> None:
    user_id, _ = await seed_users(admin_engine)
    factory = async_sessionmaker(runtime_engine)
    async with tenant_session(factory, user_id) as session:
        item = source(user_id)
        session.add(item)
        await session.flush()
        source_id = item.id
    with pytest.raises(IntegrityError):
        async with tenant_session(factory, user_id) as session:
            session.add(IngestionJob(user_id=user_id, source_id=source_id, status=JobStatus.FAILED))
            await session.flush()
    async with tenant_session(factory, user_id) as session:
        session.add(
            IngestionJob(
                user_id=user_id,
                source_id=source_id,
                status=JobStatus.FAILED,
                error_message="download_timeout",
                attempts=1,
                max_attempts=3,
            )
        )
    async with tenant_session(factory, user_id) as session:
        job = (await session.scalars(select(IngestionJob))).one()
        assert job.error_message == "download_timeout"
        assert job.attempts == 1 and job.max_attempts == 3


async def test_rollback_clears_data_and_tenant_context(
    admin_engine: AsyncEngine, runtime_engine: AsyncEngine
) -> None:
    user_id, _ = await seed_users(admin_engine)
    factory = async_sessionmaker(runtime_engine)
    with pytest.raises(RuntimeError, match="simulated"):
        async with tenant_session(factory, user_id) as session:
            session.add(source(user_id))
            await session.flush()
            raise RuntimeError("simulated failure")
    async with tenant_session(factory, user_id) as session:
        assert await session.scalar(select(func.count()).select_from(Source)) == 0
    async with factory() as session:
        assert (
            await session.scalar(text("SELECT NULLIF(current_setting('app.user_id', true), '')"))
            is None
        )


@pytest.mark.parametrize("sha", ["invalid", "A" * 64])
async def test_invalid_hash_rejected(admin_engine: AsyncEngine, sha: str) -> None:
    user_id, _ = await seed_users(admin_engine)
    with pytest.raises(IntegrityError):
        async with async_sessionmaker(admin_engine).begin() as session:
            session.add(source(user_id, sha256=sha))
            await session.flush()


async def test_message_id_deduplication_and_utc_defaults(
    admin_engine: AsyncEngine, runtime_engine: AsyncEngine
) -> None:
    first, second = await seed_users(admin_engine)
    factory = async_sessionmaker(runtime_engine)
    conversations: dict[UUID, UUID] = {}
    for user_id in (first, second):
        async with tenant_session(factory, user_id) as session:
            conversation = Conversation(user_id=user_id, open_kfid="test-kf")
            session.add(conversation)
            await session.flush()
            conversations[user_id] = conversation.id
            message = Message(
                user_id=user_id,
                conversation_id=conversation.id,
                wechat_msg_id="duplicate-id",
                role=MessageRole.USER,
                message_type="text",
            )
            session.add(message)
            await session.flush()
            assert message.created_at.utcoffset().total_seconds() == 0
    with pytest.raises(IntegrityError):
        async with tenant_session(factory, first) as session:
            session.add(
                Message(
                    user_id=first,
                    conversation_id=conversations[first],
                    wechat_msg_id="duplicate-id",
                    role=MessageRole.USER,
                    message_type="text",
                )
            )
            await session.flush()


@pytest.mark.parametrize("attempts,max_attempts", [(-1, 3), (0, 0)])
async def test_invalid_retry_limits_rejected(
    admin_engine: AsyncEngine, attempts: int, max_attempts: int
) -> None:
    user_id, _ = await seed_users(admin_engine)
    with pytest.raises(IntegrityError):
        async with async_sessionmaker(admin_engine).begin() as session:
            item = source(user_id)
            session.add(item)
            await session.flush()
            session.add(
                IngestionJob(
                    user_id=user_id, source_id=item.id, attempts=attempts, max_attempts=max_attempts
                )
            )
            await session.flush()
