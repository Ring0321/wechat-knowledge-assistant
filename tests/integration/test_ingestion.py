"""M3 real PostgreSQL, durable Redis and private S3-compatible storage gate."""

import argparse
import asyncio
import hashlib
import json
import os
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, tzinfo
from pathlib import Path
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from redis.asyncio import Redis
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.adapters.s3 import S3ObjectStore
from app.connectors.wecom.api import HttpWeComAPI
from app.connectors.wecom.callback import CallbackService
from app.connectors.wecom.contracts import APIError, NormalizedMessage
from app.connectors.wecom.crypto import WeComCrypto
from app.connectors.wecom.ingestion import WeComIngestionBridge
from app.connectors.wecom.media import HttpWeComMedia
from app.connectors.wecom.normalization import normalize_message
from app.connectors.wecom.persistence import WeComOutbox
from app.connectors.wecom.queue import RedisNotificationQueue
from app.connectors.wecom.replies import ReplyService
from app.connectors.wecom.store import WeComStore, user_identity
from app.connectors.wecom.sync import MessageSyncService
from app.connectors.wecom.tokens import RedisTokenProvider
from app.connectors.wecom.worker import WeComWorker
from app.core.config import Settings
from app.core.health import InfrastructureHealthProbe
from app.db.models import Asset, IngestionJob, Message, Source
from app.db.session import corporate_session, tenant_session
from app.domain.artifacts import ArtifactError, DownloadedFile, StoredObject
from app.domain.enums import JobStatus, SourceStatus
from app.ingestion.contracts import WorkItem
from app.ingestion.models import IngestionDispatch, MessageSource
from app.ingestion.pipeline import IngestionPipeline
from app.ingestion.queue import IngestionQueue
from app.ingestion.repository import JobRepository
from app.ingestion.worker import IngestionWorker
from app.main import create_app
from app.workers.ingestion import run as run_ingestion
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


@pytest.fixture(scope="module")
def s3_config() -> dict[str, str]:
    names = ("TEST_S3_ENDPOINT", "TEST_S3_ACCESS_KEY", "TEST_S3_SECRET_KEY")
    if not all(os.environ.get(name) for name in names):
        pytest.skip("Real S3 endpoint absent; use the complete verification gate")
    values = {name: os.environ[name] for name in names}
    values["bucket"] = "pkb-test-" + uuid4().hex
    client = s3_client(values)
    for attempt in range(40):
        try:
            client.create_bucket(Bucket=values["bucket"], ACL="private")
            break
        except Exception:
            if attempt == 39:
                raise
            time.sleep(1)
    client.close()
    return values


def s3_client(values: dict[str, str]) -> S3Client:
    return boto3.client(
        "s3",
        endpoint_url=values["TEST_S3_ENDPOINT"],
        aws_access_key_id=values["TEST_S3_ACCESS_KEY"],
        aws_secret_access_key=values["TEST_S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            connect_timeout=2,
            read_timeout=5,
            retries={"max_attempts": 0},
            s3={"addressing_style": "path"},
        ),
    )


class Media:
    def __init__(self, body: bytes = b"synthetic content", filename: str = "readme.txt") -> None:
        self.body, self.filename, self.calls = body, filename, 0

    async def download(self, media_id: str, destination: Path, *, max_bytes: int) -> DownloadedFile:
        self.calls += 1
        assert len(self.body) <= max_bytes
        await asyncio.to_thread(destination.write_bytes, self.body)
        return DownloadedFile(
            destination,
            hashlib.sha256(self.body).hexdigest(),
            len(self.body),
            "application/octet-stream",
            self.filename,
        )


@dataclass
class Harness:
    settings: Settings
    store: WeComStore
    repository: JobRepository
    queue: IngestionQueue
    objects: S3ObjectStore
    media: Media

    def worker(self) -> IngestionWorker:
        return IngestionWorker(
            self.queue,
            self.repository,
            IngestionPipeline(self.repository, self.media, self.objects),
        )

    async def admit(
        self,
        content: str = "保存：测试知识",
        *,
        user: str = TEST_USER,
        msgid: str | None = None,
        file: bool = False,
    ) -> UUID:
        raw = customer_message(
            msgid or uuid4().hex, external_userid=user, message_type="file" if file else "text"
        )
        if not file:
            raw["text"] = {"content": content}
        message = normalize_message(raw, TEST_KF)
        assert message is not None
        assert await self.store.persist_message(message, self.settings.wecom_reply_text)
        return user_identity(self.settings.wecom_corp_id, user)

    async def dispatches(self) -> list[IngestionDispatch]:
        async with corporate_session(self.store.connector_factory, self.store.corp_id) as session:
            return list(
                (
                    await session.scalars(
                        select(IngestionDispatch).order_by(IngestionDispatch.created_at)
                    )
                ).all()
            )

    async def jobs(self, user: str = TEST_USER) -> list[IngestionJob]:
        async with tenant_session(
            self.store.tenant_factory, user_identity(self.store.corp_id, user)
        ) as session:
            return list(
                (
                    await session.scalars(select(IngestionJob).order_by(IngestionJob.created_at))
                ).all()
            )

    async def drain(self) -> None:
        await self.repository.publish_due(self.queue)
        for _ in range(len(await self.dispatches())):
            assert await self.worker().once()


@pytest.fixture
async def harness(
    urls: dict[str, str],
    runtime_engine: AsyncEngine,
    connector_engine: AsyncEngine,
    s3_config: dict[str, str],
) -> AsyncIterator[Harness]:
    settings = Settings.model_validate(
        {
            "database_url": urls["TEST_DATABASE_URL"],
            "connector_database_url": urls["TEST_CONNECTOR_DATABASE_URL"],
            "redis_url": urls["TEST_REDIS_URL"],
            "wecom_enabled": True,
            "wecom_corp_id": "m3-" + uuid4().hex,
            "wecom_secret": "synthetic-secret",
            "wecom_callback_token": TEST_TOKEN,
            "wecom_encoding_aes_key": TEST_KEY,
            "wecom_open_kfids": [TEST_KF],
            "wecom_allowed_user_ids": [TEST_USER, "second-customer"],
            "ingestion_enabled": True,
            "s3_endpoint_url": s3_config["TEST_S3_ENDPOINT"],
            "s3_access_key_id": s3_config["TEST_S3_ACCESS_KEY"],
            "s3_secret_access_key": s3_config["TEST_S3_SECRET_KEY"],
            "s3_bucket": s3_config["bucket"],
        }
    )
    bridge = WeComIngestionBridge(settings)
    store = WeComStore(
        async_sessionmaker(runtime_engine, expire_on_commit=False),
        async_sessionmaker(connector_engine, expire_on_commit=False),
        settings.wecom_corp_id,
        bridge,
    )
    client = Redis.from_url(urls["TEST_REDIS_URL"], decode_responses=True)
    objects = S3ObjectStore(
        settings.s3_endpoint_url,
        s3_config["TEST_S3_ACCESS_KEY"],
        s3_config["TEST_S3_SECRET_KEY"],
        settings.s3_bucket,
    )
    try:
        yield Harness(
            settings,
            store,
            JobRepository(store.tenant_factory, store.connector_factory, settings, bridge),
            IngestionQueue(client, store.corp_id),
            objects,
            Media(),
        )
    finally:
        await objects.aclose()
        await client.aclose()


async def test_note_to_private_original_canonical_markdown(
    harness: Harness, s3_config: dict[str, str]
) -> None:
    user_id = await harness.admit()
    await harness.drain()
    assert (await harness.jobs())[0].status == JobStatus.COMPLETED
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = (await session.scalars(select(Source))).one()
        assets = (await session.scalars(select(Asset))).all()
        assert source.status == SourceStatus.STORED and source.vector_file_id is None
        assert len(assets) == 3 and source.text == "保存：测试知识"
        assert source.metadata_["index_status"] == "deferred_m6"
    client = s3_client(s3_config)
    for asset in assets:
        result = client.get_object(
            Bucket=s3_config["bucket"], Key=asset.storage_key, ChecksumMode="ENABLED"
        )
        content = result["Body"].read()
        result["Body"].close()
        assert hashlib.sha256(content).hexdigest() == asset.sha256
        assert len(content) == asset.size_bytes
        assert f"users/{user_id}/" in asset.storage_key
        if asset.metadata_["format"] == "canonical":
            canonical = json.loads(content)
            assert canonical["text"] == source.text
            assert canonical["source_id"] == str(source.id)
        elif asset.metadata_["format"] == "markdown":
            assert str(source.id).encode() in content and b"deferred_m6" in content
    client.close()
    anonymous = boto3.client(
        "s3", endpoint_url=s3_config["TEST_S3_ENDPOINT"], config=Config(signature_version=UNSIGNED)
    )
    with pytest.raises(ClientError) as caught:
        anonymous.get_object(Bucket=s3_config["bucket"], Key=assets[0].storage_key)
    assert caught.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    anonymous.close()


async def test_multi_url_and_same_user_hash_dedup_cross_user_separation(harness: Harness) -> None:
    first = await harness.admit("https://example.org/a https://example.org/b")
    await harness.admit("https://example.org/a")
    second = await harness.admit("https://example.org/a", user="second-customer")
    await harness.drain()
    async with tenant_session(harness.store.tenant_factory, first) as session:
        sources = (await session.scalars(select(Source))).all()
        active = [source for source in sources if source.status != SourceStatus.DELETED]
        assert len(active) == 2 and len(sources) == 3
        assert all(source.status == SourceStatus.METADATA_ONLY for source in active)
        mappings = (await session.scalars(select(MessageSource))).all()
        assert len(mappings) == 3 and len({item.source_id for item in mappings}) == 2
        assert await session.scalar(select(func.count()).select_from(Asset)) == 6
    async with tenant_session(harness.store.tenant_factory, second) as session:
        source = (await session.scalars(select(Source))).one()
        assert source.id not in {item.id for item in active}
        assert source.sha256 in {item.sha256 for item in active}
        assert str(second) in source.storage_key
    assert harness.media.calls == 0  # No network fetch for URLs in M3.


@pytest.mark.parametrize(
    "filename,body,status",
    [
        ("file.pdf", b"%PDF-synthetic", SourceStatus.METADATA_ONLY),
        ("notes.txt", "中文笔记".encode(), SourceStatus.STORED),
        ("unknown.bin", b"\x00\xff", SourceStatus.METADATA_ONLY),
    ],
)
async def test_official_media_download_to_real_storage(
    harness: Harness, filename: str, body: bytes, status: SourceStatus
) -> None:
    user_id = await harness.admit(file=True)
    calls = []

    def official(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("gettoken"):
            return httpx.Response(
                200, json={"errcode": 0, "access_token": "synthetic", "expires_in": 7200}
            )
        assert request.url.path == "/cgi-bin/media/get"
        return httpx.Response(
            200,
            content=body,
            headers={
                "content-type": "application/octet-stream",
                "content-disposition": f'attachment; filename="{filename}"',
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(official)) as http:
        tokens = RedisTokenProvider(harness.queue.redis, http, harness.store.corp_id, "synthetic")
        pipeline = IngestionPipeline(
            harness.repository, HttpWeComMedia(http, tokens), harness.objects
        )
        await harness.repository.publish_due(harness.queue)
        assert await IngestionWorker(harness.queue, harness.repository, pipeline).once()
    assert len(calls) == 2
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = (await session.scalars(select(Source))).one()
        assert source.status == status and source.sha256 == hashlib.sha256(body).hexdigest()


async def test_transaction_admission_rolls_back_message_source_job_and_reply(
    harness: Harness,
) -> None:
    bridge = WeComIngestionBridge(harness.settings)

    class BrokenAdmission:
        async def admit(
            self, session: AsyncSession, user_id: UUID, message_id: UUID, message: NormalizedMessage
        ) -> bool:
            await bridge.admit(session, user_id, message_id, message)
            raise RuntimeError("synthetic transaction failure")

    harness.store.admission = BrokenAdmission()
    with pytest.raises(RuntimeError):
        await harness.admit()
    async with tenant_session(
        harness.store.tenant_factory, user_identity(harness.store.corp_id, TEST_USER)
    ) as session:
        for model in (Message, Source, IngestionJob, WeComOutbox, IngestionDispatch):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


async def test_replay_and_duplicate_workers_create_one_effect(harness: Harness) -> None:
    user_id = await harness.admit(msgid="stable")
    message = normalize_message(customer_message("stable"), TEST_KF)
    assert message is not None and not await harness.store.persist_message(message, "ignored")
    dispatch = (await harness.dispatches())[0]
    work, duplicate = await asyncio.gather(
        harness.repository.claim(dispatch.id), harness.repository.claim(dispatch.id)
    )
    assert (work is None) != (duplicate is None)
    winner = work or duplicate
    assert winner is not None
    await harness.worker().pipeline.process(winner)
    assert await harness.repository.claim(dispatch.id) is None
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        assert await session.scalar(select(func.count()).select_from(Asset)) == 3
        assert await session.scalar(select(func.count()).select_from(WeComOutbox)) == 2


async def test_expired_lease_is_reclaimed_and_stale_worker_cannot_finalize(
    harness: Harness,
) -> None:
    user_id = await harness.admit()
    dispatch = (await harness.dispatches())[0]
    old = await harness.repository.claim(dispatch.id)
    assert old is not None
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        await session.execute(
            update(IngestionJob).values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    new = await harness.repository.claim(dispatch.id)
    assert new is not None and old.lease_token != new.lease_token
    with pytest.raises(ArtifactError, match="job_lease_lost"):
        await harness.worker().pipeline.process(old)
    await harness.repository.fail(old, ArtifactError("stale_error"))
    await harness.worker().pipeline.process(new)
    job = (await harness.jobs())[0]
    assert job.status == JobStatus.COMPLETED and job.attempts == 2


async def test_partial_s3_write_failure_retry_is_idempotent(
    harness: Harness, s3_config: dict[str, str]
) -> None:
    user_id = await harness.admit()

    class FailingObjects:
        calls = 0

        async def put_file(
            self,
            *,
            user_id: UUID,
            source_id: UUID,
            kind: str,
            path: Path,
            sha256: str,
            content_type: str,
        ) -> StoredObject:
            self.calls += 1
            if self.calls == 2:
                raise ArtifactError("s3_unavailable", retryable=True)
            return await harness.objects.put_file(
                user_id=user_id,
                source_id=source_id,
                kind=kind,
                path=path,
                sha256=sha256,
                content_type=content_type,
            )

    await harness.repository.publish_due(harness.queue)
    broken = IngestionPipeline(harness.repository, harness.media, FailingObjects())
    await IngestionWorker(harness.queue, harness.repository, broken).once()
    job = (await harness.jobs())[0]
    assert (
        job.status == JobStatus.FAILED
        and job.error_message == "s3_unavailable"
        and job.next_retry_at
    )
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        assert await session.scalar(select(func.count()).select_from(Asset)) == 0
        await session.execute(
            update(IngestionJob).values(next_retry_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    dispatch = (await harness.dispatches())[0]
    work = await harness.repository.claim(dispatch.id)
    assert work is not None
    await harness.worker().pipeline.process(work)
    client = s3_client(s3_config)
    objects = client.list_objects_v2(Bucket=s3_config["bucket"], Prefix=f"users/{user_id}/")
    assert objects["KeyCount"] == 3
    client.close()


async def test_terminal_failure_manual_retry_and_allowlist_revocation(harness: Harness) -> None:
    await harness.admit()
    dispatch = (await harness.dispatches())[0]
    work = await harness.repository.claim(dispatch.id)
    assert work
    await harness.repository.fail(work, ArtifactError("unsupported_media_response"))
    assert await harness.repository.claim(dispatch.id) is None
    assert await harness.repository.retry(dispatch.id)
    assert (await harness.jobs())[0].error_message == "unsupported_media_response"
    harness.settings.wecom_allowed_user_ids = frozenset()
    assert await harness.repository.claim(dispatch.id) is None
    job = (await harness.jobs())[0]
    assert job.status == JobStatus.FAILED and job.error_message == "allowlist_revoked"
    assert harness.media.calls == 0


async def test_dispatch_tenant_corporate_rls_and_forged_queue(harness: Harness) -> None:
    first = await harness.admit()
    second = await harness.admit(user="second-customer")
    async with tenant_session(harness.store.tenant_factory, second) as session:
        source = (await session.scalars(select(Source))).one()
    with pytest.raises(DBAPIError):
        async with tenant_session(harness.store.tenant_factory, first) as session:
            await session.execute(update(MessageSource).values(source_id=source.id))
    async with corporate_session(harness.store.connector_factory, "other-corp") as session:
        assert list((await session.scalars(select(IngestionDispatch))).all()) == []
    async with corporate_session(harness.store.connector_factory, harness.store.corp_id) as session:
        with pytest.raises(DBAPIError):
            await session.execute(select(Source))
    await harness.queue.publish(
        {"dispatch_id": str(uuid4()), "user_id": str(first)}, dedup="forged", ttl=60
    )
    assert await harness.worker().once()
    assert all(job.status == JobStatus.QUEUED for job in await harness.jobs())
    await harness.queue.publish({"dispatch_id": "malformed"}, dedup="malformed", ttl=60)
    assert await harness.worker().once()


async def test_receipt_is_sent_before_completion_even_when_delayed(harness: Harness) -> None:
    await harness.admit()
    await harness.drain()
    api = FakeAPI()
    api.send_error = APIError("wecom_45009", retryable=True)
    replies = ReplyService(api, harness.store, harness.settings)
    await replies.dispatch_one()
    api.send_error = None
    await replies.dispatch_one()
    assert len(api.replies) == 1
    async with corporate_session(harness.store.connector_factory, harness.store.corp_id) as session:
        await session.execute(update(WeComOutbox).values(next_attempt_at=None))
    await replies.dispatch_one()
    await replies.dispatch_one()
    assert api.replies[-2][2].startswith("收到，正在整理")
    assert "尚未建立检索索引" in api.replies[-1][2]


async def test_ingestion_worker_cli_retry_and_privileged_role_guard(
    harness: Harness, urls: dict[str, str]
) -> None:
    await harness.admit()
    dispatch = (await harness.dispatches())[0]
    work = await harness.repository.claim(dispatch.id)
    assert work
    await harness.repository.fail(work, ArtifactError("synthetic_failure"))
    args = argparse.Namespace(health=False, retry=dispatch.id)
    assert await run_ingestion(harness.settings, args) == 0
    unsafe = Settings.model_validate(
        {**harness.settings.model_dump(), "connector_database_url": urls["TEST_DATABASE_ADMIN_URL"]}
    )
    with pytest.raises(RuntimeError, match="must not bypass"):
        await run_ingestion(unsafe, args)


async def test_callback_sync_ingestion_and_reply_end_to_end(
    harness: Harness, runtime_engine: AsyncEngine
) -> None:
    calls = []
    raw = customer_message()
    raw["text"] = {"content": "保存：端到端知识"}

    def official(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("gettoken"):
            return httpx.Response(
                200, json={"errcode": 0, "access_token": "synthetic-token", "expires_in": 7200}
            )
        if request.url.path.endswith("sync_msg"):
            return httpx.Response(
                200, json={"errcode": 0, "next_cursor": "done", "has_more": 0, "msg_list": [raw]}
            )
        assert request.url.path.endswith("send_msg")
        return httpx.Response(
            200, json={"errcode": 0, "msgid": json.loads(request.content)["msgid"]}
        )

    queue = RedisNotificationQueue(harness.queue.redis, harness.store.corp_id)
    callback = CallbackService(
        WeComCrypto(TEST_TOKEN, TEST_KEY, harness.store.corp_id),
        queue,
        harness.settings.wecom_open_kfids,
    )
    app = create_app(
        harness.settings, InfrastructureHealthProbe(runtime_engine, harness.queue.redis), callback
    )
    params, body = callback_fixture(corp_id=harness.store.corp_id)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ingress,
        httpx.AsyncClient(transport=httpx.MockTransport(official)) as http,
    ):
        assert (
            await ingress.post("/wecom/callback", params=params, content=body)
        ).status_code == 200
        assert not calls
        tokens = RedisTokenProvider(harness.queue.redis, http, harness.store.corp_id, "synthetic")
        api = HttpWeComAPI(http, tokens)
        replies = ReplyService(api, harness.store, harness.settings)
        await WeComWorker(
            queue,
            MessageSyncService(api, harness.store, harness.settings),
            replies,
            harness.settings,
        ).tick()
        await harness.drain()
        assert await replies.dispatch_one()
    assert calls.count("/cgi-bin/kf/send_msg") == 2
    assert (await harness.jobs())[0].status == JobStatus.COMPLETED


async def test_publish_ack_loss_replays_durable_dispatch(harness: Harness) -> None:
    await harness.admit()

    class AmbiguousQueue:
        async def enqueue(self, dispatch_id: UUID) -> str:
            await harness.queue.enqueue(dispatch_id)
            raise ArtifactError("synthetic_publish_ack_lost", retryable=True)

    with pytest.raises(ArtifactError, match="synthetic_publish_ack_lost"):
        await harness.repository.publish_due(AmbiguousQueue())
    assert (await harness.dispatches())[0].published_at is None
    assert await harness.repository.publish_due(harness.queue) == 1
    assert await harness.queue.redis.xlen(harness.queue.stream) == 1
    assert await harness.worker().once()
    assert (await harness.jobs())[0].status == JobStatus.COMPLETED


async def test_attempt_limit_and_timeout_keep_safe_errors(harness: Harness) -> None:
    user_id = await harness.admit()
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        await session.execute(update(IngestionJob).values(max_attempts=1))

    class SlowPipeline:
        async def process(self, work: WorkItem) -> None:
            await asyncio.sleep(60)

    harness.settings.ingestion_job_timeout_seconds = 0.01
    await harness.repository.publish_due(harness.queue)
    await IngestionWorker(harness.queue, harness.repository, SlowPipeline()).once()
    job = (await harness.jobs())[0]
    assert job.error_message == "job_timeout" and job.status == JobStatus.FAILED
    assert job.next_retry_at is None and job.attempts == 1
    assert (await harness.dispatches())[0].finished_at is not None


async def test_concurrent_same_hash_uses_one_source_and_three_objects(
    harness: Harness, urls: dict[str, str]
) -> None:
    user_id = await harness.admit()
    await harness.admit()
    dispatches = await harness.dispatches()
    engine = create_async_engine(urls["TEST_DATABASE_URL"], pool_size=3, hide_parameters=True)
    try:
        harness.repository.tenant_factory = async_sessionmaker(engine, expire_on_commit=False)
        jobs = await asyncio.gather(*(harness.repository.claim(row.id) for row in dispatches))
        assert all(jobs)
        await asyncio.gather(*(harness.worker().pipeline.process(job) for job in jobs if job))
        async with tenant_session(harness.store.tenant_factory, user_id) as session:
            assert await session.scalar(select(func.count()).select_from(Asset)) == 3
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(Source)
                    .where(Source.status == SourceStatus.STORED)
                )
                == 1
            )
            assert await session.scalar(select(func.count()).select_from(MessageSource)) == 2
    finally:
        await engine.dispose()


async def test_own_expired_lease_is_retryable_not_a_terminal_failure(harness: Harness) -> None:
    user_id = await harness.admit()
    dispatch = (await harness.dispatches())[0]
    work = await harness.repository.claim(dispatch.id)
    assert work
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        await session.execute(
            update(IngestionJob).values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    with pytest.raises(ArtifactError, match="job_lease_lost") as caught:
        await harness.repository.stage(work, JobStatus.PARSING)
    assert caught.value.retryable
    await harness.repository.fail(work, caught.value)
    job = (await harness.jobs())[0]
    assert job.status == JobStatus.FAILED and job.next_retry_at is not None
    assert (await harness.dispatches())[0].finished_at is None


async def test_lock_wait_does_not_consume_new_lease(
    harness: Harness,
    urls: dict[str, str],
    admin_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user_id = await harness.admit()
    dispatch = (await harness.dispatches())[0]

    class Clock:
        value = datetime.now(UTC)

        @classmethod
        def now(cls, timezone: tzinfo) -> datetime:
            return cls.value

    monkeypatch.setattr("app.ingestion.repository.datetime", Clock)
    engine = create_async_engine(urls["TEST_DATABASE_URL"], pool_size=2, hide_parameters=True)
    task = None
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        harness.repository.tenant_factory = factory
        async with tenant_session(factory, user_id) as holder:
            await holder.get(IngestionJob, dispatch.job_id, with_for_update=True)
            task = asyncio.create_task(harness.repository.claim(dispatch.id))
            for _ in range(100):
                async with admin_engine.connect() as connection:
                    waiting = await connection.scalar(
                        text(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                            "WHERE datname=current_database() AND wait_event_type='Lock' "
                            "AND query LIKE '%ingestion_jobs%')"
                        )
                    )
                if waiting:
                    break
                await asyncio.sleep(0.02)
            else:
                pytest.fail("claim did not reach the expected real PostgreSQL row lock")
            Clock.value += timedelta(seconds=600)
        work = await asyncio.wait_for(task, timeout=5)
        assert work
        job = (await harness.jobs())[0]
        assert job.started_at == Clock.value
        assert job.lease_expires_at == Clock.value + timedelta(
            seconds=harness.settings.ingestion_lease_seconds
        )
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await engine.dispose()
