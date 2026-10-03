"""M2 real PostgreSQL/Redis gate; the official HTTP boundary is entirely synthetic."""

import argparse
import asyncio
import hashlib
import json
import socket
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from redis.asyncio import Redis
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from app.connectors.wecom.api import HttpWeComAPI
from app.connectors.wecom.callback import CallbackService
from app.connectors.wecom.contracts import APIError, CallbackNotification, SyncPage, WeComError
from app.connectors.wecom.crypto import WeComCrypto
from app.connectors.wecom.normalization import normalize_message
from app.connectors.wecom.persistence import WeComOutbox, WeComSyncState
from app.connectors.wecom.queue import RedisNotificationQueue
from app.connectors.wecom.replies import ReplyService
from app.connectors.wecom.store import WeComStore, connector_session, user_identity
from app.connectors.wecom.sync import MessageSyncService
from app.connectors.wecom.tokens import RedisTokenProvider
from app.connectors.wecom.worker import WeComWorker
from app.core.config import Settings
from app.core.health import InfrastructureHealthProbe
from app.db.models import Message, Source, User
from app.db.session import tenant_session
from app.main import create_app
from app.workers.wecom import run as run_worker
from tests.wecom_helpers import (
    TEST_KEY,
    TEST_KF,
    TEST_TOKEN,
    TEST_USER,
    FakeAPI,
    callback_fixture,
    customer_message,
)

pytestmark = pytest.mark.integration


@pytest.fixture
async def redis_client(urls: dict[str, str]) -> AsyncIterator[Redis]:
    client = Redis.from_url(urls["TEST_REDIS_URL"], decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def config(urls: dict[str, str]) -> Settings:
    return Settings.model_validate(
        {
            "database_url": urls["TEST_DATABASE_URL"],
            "connector_database_url": urls["TEST_CONNECTOR_DATABASE_URL"],
            "redis_url": urls["TEST_REDIS_URL"],
            "wecom_enabled": True,
            "wecom_corp_id": "synthetic-" + uuid4().hex,
            "wecom_secret": "synthetic-application-secret",
            "wecom_callback_token": TEST_TOKEN,
            "wecom_encoding_aes_key": TEST_KEY,
            "wecom_open_kfids": [TEST_KF],
            "wecom_allowed_user_ids": [TEST_USER, "second-customer"],
        }
    )


@pytest.fixture
def store(
    runtime_engine: AsyncEngine, connector_engine: AsyncEngine, config: Settings
) -> WeComStore:
    return WeComStore(
        async_sessionmaker(runtime_engine, expire_on_commit=False),
        async_sessionmaker(connector_engine, expire_on_commit=False),
        config.wecom_corp_id,
    )


async def outbox(store: WeComStore) -> list[WeComOutbox]:
    async with connector_session(store.connector_factory, store.corp_id) as session:
        return list(
            (await session.scalars(select(WeComOutbox).order_by(WeComOutbox.created_at))).all()
        )


async def state(store: WeComStore) -> WeComSyncState:
    async with connector_session(store.connector_factory, store.corp_id) as session:
        return (await session.scalars(select(WeComSyncState))).one()


async def seed(store: WeComStore, count: int = 1, *, hours_ago: int = 0) -> None:
    for index in range(count):
        message = normalize_message(
            customer_message(
                f"message-{index}",
                sent_at=int((datetime.now(UTC) - timedelta(hours=hours_ago)).timestamp()),
            ),
            TEST_KF,
        )
        assert message is not None
        assert await store.persist_message(message, "渠道已收到")


async def test_callback_to_worker_to_official_adapter_and_reply(
    store: WeComStore,
    config: Settings,
    redis_client: Redis,
    runtime_engine: AsyncEngine,
) -> None:
    """Actual crypto, durable stream, DB, token cache and adapter; no external network."""
    calls: list[tuple[str, dict]] = []

    def official(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else {}
        calls.append((request.url.path, body))
        if request.url.path.endswith("gettoken"):
            return httpx.Response(
                200, json={"errcode": 0, "access_token": "synthetic-token", "expires_in": 7200}
            )
        if request.url.path.endswith("sync_msg"):
            return httpx.Response(
                200,
                json={
                    "errcode": 0,
                    "next_cursor": "cursor-final",
                    "has_more": 0,
                    "msg_list": [customer_message()],
                },
            )
        assert request.url.path.endswith("send_msg")
        return httpx.Response(200, json={"errcode": 0, "msgid": body["msgid"]})

    queue = RedisNotificationQueue(redis_client, store.corp_id)
    callback = CallbackService(
        WeComCrypto(TEST_TOKEN, TEST_KEY, store.corp_id), queue, config.wecom_open_kfids
    )
    app = create_app(
        config,
        InfrastructureHealthProbe(runtime_engine, redis_client, require_durability=True),
        callback,
    )
    params, body = callback_fixture(corp_id=store.corp_id)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ingress,
    ):
        assert (
            await ingress.post("/wecom/callback", params=params, content=body)
        ).status_code == 200
        assert (
            await ingress.post("/wecom/callback", params=params, content=body)
        ).status_code == 200
        assert calls == []  # Callback cannot invoke gettoken/sync/send.
        assert await redis_client.xlen(queue.stream) == 1
        async with httpx.AsyncClient(transport=httpx.MockTransport(official)) as http:
            tokens = RedisTokenProvider(redis_client, http, store.corp_id, "synthetic-secret")
            api = HttpWeComAPI(http, tokens)
            worker = WeComWorker(
                queue,
                MessageSyncService(api, store, config),
                ReplyService(api, store, config),
                config,
            )
            await worker.tick()
    assert [path.rsplit("/", 1)[-1] for path, _ in calls] == ["gettoken", "sync_msg", "send_msg"]
    assert await redis_client.xlen(queue.stream) == 0
    assert (await state(store)).cursor == "cursor-final"
    replies = await outbox(store)
    assert len(replies) == 1 and replies[0].status == "sent"
    identity = user_identity(store.corp_id, TEST_USER)
    async with tenant_session(store.tenant_factory, identity) as session:
        saved = (await session.scalars(select(Message))).one()
        assert saved.content == "测试正文" and saved.role.value == "user"
        assert await session.scalar(select(func.count()).select_from(Source)) == 0


async def test_empty_pages_allowlist_media_and_replay(store: WeComStore, config: Settings) -> None:
    unauthorized = customer_message("blocked", external_userid="not-allowed")
    staff = customer_message("staff")
    staff["origin"] = 5
    messages = [
        customer_message(),
        customer_message("media", message_type="video"),
        unauthorized,
        staff,
    ]
    api = FakeAPI([SyncPage("empty", True, []), SyncPage("done", False, messages)])
    sync = MessageSyncService(api, store, config)
    await sync.sync_account(TEST_KF)
    assert [call[1] for call in api.sync_calls] == [None, "empty"]
    await store.retry_account(TEST_KF)
    api.pages.append(SyncPage("done", False, messages))
    await sync.sync_account(TEST_KF)
    assert len(await outbox(store)) == 2
    async with tenant_session(
        store.tenant_factory, user_identity(store.corp_id, TEST_USER)
    ) as session:
        values = list((await session.scalars(select(Message))).all())
        video = next(message for message in values if message.message_type == "video")
        assert video.metadata_["media_id"] == "media-1" and video.content is None
    async with tenant_session(
        store.tenant_factory, user_identity(store.corp_id, "not-allowed")
    ) as session:
        assert (await session.scalars(select(User))).all() == []


async def test_setup_verification_text_is_never_persisted_or_replied(
    store: WeComStore, config: Settings
) -> None:
    verification = customer_message("verification")
    verification["text"] = {"content": "微信验证 " + "a" * 32}
    regular = customer_message("regular")
    regular["text"] = {"content": "微信验证流程怎么使用？"}
    api = FakeAPI([SyncPage("after-setup", False, [verification, regular])])
    await MessageSyncService(api, store, config).sync_account(TEST_KF)
    assert (await state(store)).cursor == "after-setup"
    assert len(await outbox(store)) == 1
    async with tenant_session(
        store.tenant_factory, user_identity(store.corp_id, TEST_USER)
    ) as session:
        messages = list(await session.scalars(select(Message)))
        assert len(messages) == 1 and messages[0].wechat_msg_id == "regular"


async def test_partial_page_failure_replays_committed_message_without_duplicate_reply(
    store: WeComStore, config: Settings
) -> None:
    malformed = customer_message("bad")
    malformed["text"] = {"content": None}
    api = FakeAPI([SyncPage("must-not-commit", False, [customer_message(), malformed])])
    sync = MessageSyncService(api, store, config)
    with pytest.raises(WeComError, match="invalid_message"):
        await sync.sync_account(TEST_KF)
    current = await state(store)
    assert current.cursor is None and current.status == "failed" and current.error_message
    assert len(await outbox(store)) == 1
    await store.retry_account(TEST_KF)
    api.pages.append(SyncPage("recovered", False, [customer_message(), customer_message("bad")]))
    await sync.sync_account(TEST_KF)
    assert (await state(store)).cursor == "recovered"
    assert len(await outbox(store)) == 2


async def test_retry_schedule_limits_and_cursor_preservation(
    store: WeComStore, config: Settings
) -> None:
    api = FakeAPI([SyncPage("keep-me", False, [])])
    sync = MessageSyncService(api, store, config)
    await sync.sync_account(TEST_KF)
    await store.retry_account(TEST_KF)
    api.sync_error = APIError("wecom_45009", retryable=True)
    with pytest.raises(WeComError):
        await sync.sync_account(TEST_KF)
    current = await state(store)
    assert current.cursor == "keep-me" and current.status == "retry" and current.attempts == 1
    count = len(api.sync_calls)
    await sync.sync_account(TEST_KF, "new-notification")
    assert len(api.sync_calls) == count  # Notifications must not bypass error backoff.
    async with connector_session(store.connector_factory, store.corp_id) as session:
        await session.execute(
            update(WeComSyncState).values(
                attempts=config.wecom_max_attempts - 1, next_attempt_at=None
            )
        )
    with pytest.raises(WeComError):
        await sync.sync_account(TEST_KF)
    assert (await state(store)).status == "failed"


async def test_concurrent_synchronizers_fetch_one_page(store: WeComStore, config: Settings) -> None:
    api = FakeAPI([SyncPage("once", False, [customer_message()])])
    sync = MessageSyncService(api, store, config)
    await asyncio.gather(sync.sync_account(TEST_KF), sync.sync_account(TEST_KF))
    assert len(api.sync_calls) == 1 and len(await outbox(store)) == 1


async def test_tenant_and_corporate_isolation(
    store: WeComStore, config: Settings, connector_engine: AsyncEngine
) -> None:
    await seed(store)
    await MessageSyncService(FakeAPI(), store, config).sync_account(TEST_KF)
    async with tenant_session(
        store.tenant_factory, user_identity(store.corp_id, "second-customer")
    ) as session:
        assert (await session.scalars(select(Message))).all() == []
        assert (await session.scalars(select(WeComOutbox))).all() == []
    async with connector_session(store.connector_factory, "different-corp") as session:
        assert (await session.scalars(select(WeComOutbox))).all() == []
        assert (await session.scalars(select(WeComSyncState))).all() == []
    async with connector_engine.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM wecom_outbox")) == 0
        assert await connection.scalar(text("SELECT count(*) FROM wecom_sync_states")) == 0
    with pytest.raises(DBAPIError):
        async with connector_session(store.connector_factory, store.corp_id) as session:
            await session.execute(select(Message))
    with pytest.raises(DBAPIError):
        async with tenant_session(
            store.tenant_factory, user_identity(store.corp_id, TEST_USER)
        ) as session:
            await session.execute(select(WeComSyncState))


@pytest.mark.parametrize(
    "error,status",
    [
        (APIError("wecom_send_transport_failed", uncertain=True), "uncertain"),
        (APIError("wecom_95033"), "uncertain"),
        (APIError("wecom_95002"), "deferred"),
        (APIError("wecom_95018"), "deferred"),
        (APIError("wecom_48002"), "failed"),
    ],
)
async def test_reply_errors_are_persisted_and_not_blindly_retried(
    store: WeComStore, config: Settings, error: APIError, status: str
) -> None:
    await seed(store)
    api = FakeAPI()
    api.send_error = error
    replies = ReplyService(api, store, config)
    assert await replies.dispatch_one()
    item = (await outbox(store))[0]
    assert item.status == status and item.error_message == error.code and item.attempts == 1
    assert not await replies.dispatch_one()
    assert await store.retry_reply(item.id)
    api.send_error = None
    assert await replies.dispatch_one()
    assert api.replies[0][3] == api.replies[1][3]  # Operator retry retains the original ID.


async def test_rate_limit_retry_is_delayed_and_idempotent(
    store: WeComStore, config: Settings
) -> None:
    await seed(store)
    api = FakeAPI()
    api.send_error = APIError("wecom_45009", retryable=True)
    replies = ReplyService(api, store, config)
    await replies.dispatch_one()
    item = (await outbox(store))[0]
    assert item.status == "queued" and item.next_attempt_at > datetime.now(UTC)
    assert not await replies.dispatch_one()
    async with connector_session(store.connector_factory, store.corp_id) as session:
        await session.execute(update(WeComOutbox).values(next_attempt_at=None))
    api.send_error = None
    await replies.dispatch_one()
    assert (await outbox(store))[0].attempts == 2
    assert api.replies[0][3] == api.replies[1][3]


async def test_local_quota_and_window_prevent_send(store: WeComStore, config: Settings) -> None:
    await seed(store, count=6)
    api = FakeAPI()
    replies = ReplyService(api, store, config)
    for _ in range(6):
        assert await replies.dispatch_one()
    assert len(api.replies) == 5
    assert sum(item.status == "deferred" for item in await outbox(store)) == 1


async def test_expired_window_and_revoked_allowlist(store: WeComStore, config: Settings) -> None:
    await seed(store, hours_ago=49)
    api = FakeAPI()
    replies = ReplyService(api, store, config)
    await replies.dispatch_one()
    item = (await outbox(store))[0]
    assert item.error_message == "reply_window_closed" and not api.replies
    await store.retry_reply(item.id)
    config.wecom_allowed_user_ids = frozenset()
    await replies.dispatch_one()
    assert (await outbox(store))[0].error_message == "allowlist_revoked" and not api.replies


async def test_delivery_failure_event_is_not_customer_content(
    store: WeComStore, config: Settings
) -> None:
    await seed(store)
    api = FakeAPI()
    await ReplyService(api, store, config).dispatch_one()
    item = (await outbox(store))[0]
    api.pages.append(
        SyncPage(
            "failure-event",
            False,
            [
                {
                    "origin": 4,
                    "msgtype": "event",
                    "event": {
                        "event_type": "msg_send_fail",
                        "open_kfid": TEST_KF,
                        "external_userid": TEST_USER,
                        "fail_msgid": item.reply_msgid,
                        "fail_type": 4,
                    },
                }
            ],
        )
    )
    await MessageSyncService(api, store, config).sync_account(TEST_KF)
    item = (await outbox(store))[0]
    assert item.status == "failed" and item.error_message == "delivery_failure_4"
    async with tenant_session(store.tenant_factory, item.user_id) as session:
        assert await session.scalar(select(func.count()).select_from(Message)) == 1


async def test_stream_duplicate_concurrency_and_abandoned_claim(redis_client: Redis) -> None:
    corp = "synthetic-" + uuid4().hex
    queue = RedisNotificationQueue(redis_client, corp)
    event = CallbackNotification(corp, TEST_KF, "notification", int(datetime.now(UTC).timestamp()))
    ids = await asyncio.gather(*(queue.enqueue(event) for _ in range(12)))
    assert len(set(ids)) == 1 and await redis_client.xlen(queue.stream) == 1
    entry = await queue.receive()
    assert entry is not None and entry[1] == event
    await redis_client.xclaim(
        queue.stream, queue.group, "crashed-worker", 0, [entry[0]], idle=61000
    )
    replacement = RedisNotificationQueue(redis_client, corp)
    assert await replacement.receive() == entry
    await replacement.acknowledge(entry[0])
    assert await redis_client.xlen(queue.stream) == 0
    assert (await redis_client.xpending(queue.stream, queue.group))["pending"] == 0


async def test_real_token_lock_expiry_invalidation_and_credential_isolation(
    redis_client: Redis,
) -> None:
    corp = "synthetic-" + uuid4().hex
    calls = 0

    async def official(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.03)
        return httpx.Response(
            200, json={"errcode": 0, "access_token": f"token-{calls}", "expires_in": 7200}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(official)) as http:
        provider = RedisTokenProvider(redis_client, http, corp, "secret-one")
        results = await asyncio.gather(*(provider.get_token() for _ in range(30)))
        assert set(results) == {"token-1"} and calls == 1
        digest = hashlib.sha256((corp + "\0secret-one").encode()).hexdigest()
        key = f"pkb:wecom:token:{{{digest}}}:value"
        assert 6800 < await redis_client.ttl(key) <= 6900
        await provider.invalidate("stale-token")
        assert await provider.get_token() == "token-1"
        await provider.invalidate("token-1")
        assert await provider.get_token() == "token-2"
        await provider.invalidate("token-1")
        assert await provider.get_token() == "token-2"
        await redis_client.pexpire(key, 1)
        await asyncio.sleep(0.02)
        assert await provider.get_token() == "token-3"
        separate = RedisTokenProvider(redis_client, http, corp, "secret-two")
        assert await separate.get_token() == "token-4" and calls == 4


async def test_duplicate_requires_fresh_connection_durability_barrier(
    redis_client: Redis,
    urls: dict[str, str],
) -> None:
    """Dedicated disposable Redis only: temporarily disable fsync, always restore it."""
    corp = "synthetic-" + uuid4().hex
    queue = RedisNotificationQueue(redis_client, corp)
    event = CallbackNotification(corp, TEST_KF, "notification", int(datetime.now(UTC).timestamp()))
    await queue.enqueue(event)
    original = (await redis_client.config_get("appendfsync"))["appendfsync"]
    fresh = Redis.from_url(urls["TEST_REDIS_URL"], decode_responses=True)
    try:
        await redis_client.config_set("appendfsync", "no")
        duplicate = RedisNotificationQueue(fresh, corp, durability_ms=25)
        with pytest.raises(WeComError, match="queue_not_durable"):
            await duplicate.enqueue(event)
        assert await redis_client.xlen(queue.stream) == 1
    finally:
        await redis_client.config_set("appendfsync", original)
        await fresh.aclose()


async def test_worker_assembly_operator_retry_and_health(
    store: WeComStore,
    config: Settings,
    redis_client: Redis,
) -> None:
    args = argparse.Namespace(health=False, retry_account=TEST_KF, retry_outbox=None)
    assert await run_worker(config, args) == 0
    assert (await state(store)).status == "ready"
    args.health, args.retry_account = True, None
    assert await run_worker(config, args) == 1
    queue = RedisNotificationQueue(redis_client, store.corp_id)
    key = queue.prefix + ":heartbeat:" + socket.gethostname()
    await redis_client.set(key, "alive", ex=30)
    try:
        assert await run_worker(config, args) == 0
    finally:
        await redis_client.delete(key)


async def test_worker_rejects_privileged_connector_credentials(
    config: Settings,
    urls: dict[str, str],
) -> None:
    unsafe = Settings.model_validate(
        {
            **config.model_dump(),
            "connector_database_url": urls["TEST_DATABASE_ADMIN_URL"],
        }
    )
    args = argparse.Namespace(health=False, retry_account=TEST_KF, retry_outbox=None)
    with pytest.raises(RuntimeError, match="must not bypass"):
        await run_worker(unsafe, args)
