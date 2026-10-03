"""M8 card preservation and explicit video supplementation with real tenant storage."""

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError

from app.connectors.wecom.normalization import normalize_message
from app.connectors.wecom.store import user_identity
from app.db.models import Asset, IngestionJob, Message, Source
from app.db.session import tenant_session
from app.domain.artifacts import ArtifactError, DownloadedFile, StoredObject
from app.domain.enums import AssetKind, JobStatus, MessageRole, SourceStatus, SourceType
from app.domain.knowledge import TenantContext
from app.domain.media import ParsedMedia
from app.domain.parsing import ParsedContent
from app.ingestion.channels import SUPPLEMENT
from app.ingestion.models import IngestionDispatch, MessageSource
from app.ingestion.pipeline import IngestionPipeline
from app.ingestion.worker import IngestionWorker
from app.knowledge.jobs import INDEX
from tests.integration.test_ingestion import Harness, Media, s3_client
from tests.integration.test_ingestion import harness as harness
from tests.integration.test_ingestion import s3_config as s3_config
from tests.integration.test_knowledge import dispatch_for, enable, execute, job_for, source_for
from tests.integration.test_media import OfficialMediaFixture, audio_file, pipeline, video_file
from tests.knowledge_helpers import FakeVector
from tests.wecom_helpers import TEST_KF, TEST_USER, customer_message

pytestmark = pytest.mark.integration

CARD = {"nickname": "合成视频号", "title": "同名合成视频", "sub_type": 1}


async def admit_card(
    h: Harness, *, user: str = TEST_USER, msgid: str | None = None
) -> tuple[TenantContext, UUID]:
    before = {row.id for row in await h.dispatches()}
    raw = customer_message(msgid or uuid4().hex, external_userid=user, message_type="channels")
    raw["channels"] = dict(CARD)
    message = normalize_message(raw, TEST_KF)
    assert message is not None and await h.store.persist_message(message, "synthetic")
    admission = next(row for row in await h.dispatches() if row.id not in before)
    work = await h.repository.claim(admission.id)
    assert work is not None
    await h.worker().pipeline.process(work)
    return TenantContext(work.user_id), work.source_id


async def supplement_job(h: Harness, context: TenantContext, source_id: UUID) -> UUID:
    """Start at the durable boundary; command authorization is covered separately."""
    async with tenant_session(h.store.tenant_factory, context.user_id) as session:
        card_message = (
            await session.scalars(
                select(Message)
                .join(MessageSource, MessageSource.message_id == Message.id)
                .where(MessageSource.source_id == source_id)
                .order_by(Message.created_at)
            )
        ).first()
        assert card_message is not None
        message = Message(
            id=uuid4(),
            user_id=context.user_id,
            conversation_id=card_message.conversation_id,
            wechat_msg_id=uuid4().hex,
            role=MessageRole.USER,
            message_type="video",
            metadata_={"sent_at": int(datetime.now(UTC).timestamp())},
        )
        session.add(message)
        await session.flush()
        session.add(
            MessageSource(
                user_id=context.user_id, message_id=message.id, source_id=source_id, item_index=0
            )
        )
        job = IngestionJob(
            id=uuid4(),
            user_id=context.user_id,
            source_id=source_id,
            message_id=message.id,
            operation_key=f"{SUPPLEMENT}:{source_id}",
            input_data={"kind": SUPPLEMENT, "media_id": "synthetic-official-video"},
        )
        session.add(job)
        await session.flush()
        dispatch = IngestionDispatch(
            id=uuid4(), corp_id=h.store.corp_id, user_id=context.user_id, job_id=job.id
        )
        session.add(dispatch)
        return dispatch.id


async def state(h: Harness, context: TenantContext, source_id: UUID) -> dict[str, object]:
    """Capture user-visible and original-object state, excluding job bookkeeping."""
    async with tenant_session(h.store.tenant_factory, context.user_id) as session:
        source = await session.get(Source, source_id)
        assert source is not None
        assets = list(await session.scalars(select(Asset).where(Asset.source_id == source_id)))
        return {
            "id": source.id,
            "title": source.title,
            "source_type": source.source_type,
            "status": source.status,
            "storage_key": source.storage_key,
            "sha256": source.sha256,
            "created_at": source.created_at,
            "text": source.text,
            "metadata": deepcopy(source.metadata_),
            "assets": frozenset((a.id, a.storage_key, a.sha256, a.kind) for a in assets),
        }


def stored_bytes(s3_config: dict[str, str], key: str) -> bytes:
    client = s3_client(s3_config)
    try:
        body = client.get_object(Bucket=s3_config["bucket"], Key=key)["Body"]
        try:
            return body.read()
        finally:
            body.close()
    finally:
        client.close()


async def run_queued(h: Harness, parser: IngestionPipeline) -> None:
    assert await h.repository.publish_due(h.queue) == 1
    assert await IngestionWorker(h.queue, h.repository, parser).once()


class EmptyVideo:
    """A decoder that recognizes video but extracts no usable text."""

    async def parse(
        self,
        path: Path,
        directory: Path,
        *,
        source_type: SourceType,
        on_transcribing: Callable[[], Awaitable[None]],
    ) -> ParsedMedia:
        assert await asyncio.to_thread(path.is_file)
        assert source_type == SourceType.VIDEO
        return ParsedMedia(
            ParsedContent(source_type=SourceType.VIDEO, metadata={"parser_version": 5})
        )


async def test_card_saves_json_and_markdown_without_download_or_index(
    harness: Harness, s3_config: dict[str, str]
) -> None:
    vector = FakeVector()
    enable(harness, vector)
    context, source_id = await admit_card(harness)
    source = await source_for(harness, context, source_id)
    assert source.status == SourceStatus.METADATA_ONLY and not source.text
    assert source.source_type == SourceType.WECHAT_CHANNEL and source.original_url is None
    assert source.vector_file_id is None and source.metadata_["index_status"] == "metadata_only"
    assert source.metadata_["channels"] == CARD
    assert harness.media.calls == 0 and not vector.calls
    assert len(await harness.jobs()) == 1
    assert (await harness.jobs())[0].status == JobStatus.COMPLETED
    assert source.storage_key is not None
    original = stored_bytes(s3_config, source.storage_key)
    assert hashlib.sha256(original).hexdigest() == source.sha256
    assert json.loads(original) == {
        "kind": "metadata",
        "channels": CARD,
        "card_source_id": str(source_id),
    }
    canonical = json.loads(stored_bytes(s3_config, str(source.metadata_["canonical_key"])))
    assert canonical["source_id"] == str(source_id) and canonical["text"] == ""
    rendered = stored_bytes(s3_config, str(source.metadata_["markdown_key"])).decode()
    assert "metadata_only" in rendered and "无视频转录或时间轴" in rendered
    assert CARD["title"] in rendered
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        assert await session.scalar(select(func.count()).select_from(Asset)) == 3


async def test_same_title_is_not_identity_but_message_replay_is_idempotent(
    harness: Harness,
) -> None:
    msgid = uuid4().hex
    context, first = await admit_card(harness, msgid=msgid)
    raw = customer_message(msgid, message_type="channels")
    raw["channels"] = dict(CARD)
    replay = normalize_message(raw, TEST_KF)
    assert replay is not None and not await harness.store.persist_message(replay, "synthetic")
    _, second = await admit_card(harness)
    assert first != second
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        sources = list(await session.scalars(select(Source)))
        assert len(sources) == 2 and len({s.sha256 for s in sources}) == 2
        assert all(s.status == SourceStatus.METADATA_ONLY for s in sources)
        assert set(await session.scalars(select(MessageSource.source_id))) == {first, second}
        assert await session.scalar(select(func.count()).select_from(IngestionJob)) == 2
        assert await session.scalar(select(func.count()).select_from(Asset)) == 6


async def test_supplement_real_video_indexes_same_source_and_preserves_card(
    harness: Harness, tmp_path: Path, s3_config: dict[str, str]
) -> None:
    vector = FakeVector()
    _, processor = enable(harness, vector)
    context, source_id = await admit_card(harness)
    before = await state(harness, context, source_id)
    original = stored_bytes(s3_config, str(before["storage_key"]))
    path = await video_file(tmp_path / "synthetic.mp4")
    harness.media = Media(path.read_bytes(), path.name)
    # Exercise the shipped WeCom bridge, durable queue, media parser and index together.
    await harness.admit(f"补充视频 {source_id}")
    raw = customer_message(uuid4().hex, message_type="video")
    raw["video"] = {"media_id": "synthetic-official-video"}
    incoming = normalize_message(raw, TEST_KF)
    assert incoming is not None and await harness.store.persist_message(incoming, None)
    dispatch_id = await dispatch_for(harness, context, source_id, SUPPLEMENT)
    fixture = OfficialMediaFixture()
    async with httpx.AsyncClient(transport=httpx.MockTransport(fixture), trust_env=False) as http:
        await run_queued(harness, pipeline(harness, http))
    assert (await job_for(harness, context, dispatch_id)).status == JobStatus.COMPLETED
    source = await source_for(harness, context, source_id)
    assert source.status == SourceStatus.STORED and source.metadata_["index_status"] == "queued"
    assert source.metadata_["channels"] == CARD and source.metadata_["original_video_available"]
    assert source.metadata_["supplement_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert 1 <= source.metadata_["keyframes"] <= 3
    assert any(call.endswith("transcriptions") for call in fixture.calls)
    assert any(call.endswith("responses") for call in fixture.calls)
    after = await state(harness, context, source_id)
    for field in ("id", "title", "source_type", "storage_key", "sha256", "created_at"):
        assert after[field] == before[field]
    assert before["assets"] < after["assets"]
    assert stored_bytes(s3_config, str(source.storage_key)) == original
    assert (
        stored_bytes(s3_config, str(source.metadata_["supplement_original_key"]))
        == path.read_bytes()
    )
    rendered = stored_bytes(s3_config, str(source.metadata_["markdown_key"])).decode()
    assert "[00:01.100–00:01.500]" in rendered and "未验证它与卡片内容相同" in rendered
    index_id = await dispatch_for(harness, context, source_id, INDEX)
    assert await execute(harness, processor, index_id) is None
    ready = await source_for(harness, context, source_id)
    assert ready.status == SourceStatus.READY and ready.vector_file_id in vector.contents
    assert str(source_id).encode() in vector.contents[ready.vector_file_id]
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        assert await session.scalar(select(func.count()).select_from(Source)) == 1
        assert set(await session.scalars(select(MessageSource.source_id))) == {source_id}
    assert await harness.repository.claim(dispatch_id) is None


async def test_existing_normal_video_hash_cannot_redirect_card_supplement(
    harness: Harness, tmp_path: Path
) -> None:
    path = await video_file(tmp_path / "same.mp4")
    harness.media = Media(path.read_bytes(), path.name)
    user_id = await harness.admit(file=True)
    fixture = OfficialMediaFixture()
    async with httpx.AsyncClient(transport=httpx.MockTransport(fixture), trust_env=False) as http:
        parser = pipeline(harness, http)
        await run_queued(harness, parser)
        async with tenant_session(harness.store.tenant_factory, user_id) as session:
            normal = (await session.scalars(select(Source))).one()
            normal_id = normal.id
            assert normal.status == SourceStatus.STORED
        context, card_id = await admit_card(harness)
        dispatch_id = await supplement_job(harness, context, card_id)
        await run_queued(harness, parser)
    assert (await job_for(harness, context, dispatch_id)).status == JobStatus.COMPLETED
    card = await source_for(harness, context, card_id)
    assert card.status == SourceStatus.STORED and card.source_type == SourceType.WECHAT_CHANNEL
    assert card.metadata_["supplement_sha256"] == normal.sha256 and card.sha256 != normal.sha256
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        sources = list(await session.scalars(select(Source)))
        assert len(sources) == 2 and all(s.status == SourceStatus.STORED for s in sources)
        assert set(await session.scalars(select(MessageSource.source_id))) == {card_id, normal_id}


@pytest.mark.parametrize("failure", ["download", "decode", "transcription", "s3"])
async def test_failed_supplement_preserves_card_and_retry_has_no_duplicate_assets(
    harness: Harness,
    tmp_path: Path,
    s3_config: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    context, source_id = await admit_card(harness)
    before = await state(harness, context, source_id)
    card_bytes = stored_bytes(s3_config, str(before["storage_key"]))
    path = await video_file(tmp_path / "retry.mp4")
    good_video = path.read_bytes()
    harness.media = Media(
        b"invalid synthetic video" if failure == "decode" else good_video, path.name
    )
    download = harness.media.download
    put = harness.objects.put_file

    async def fail_download(media_id: str, destination: Path, *, max_bytes: int) -> DownloadedFile:
        raise ArtifactError("wecom_media_unavailable", retryable=True)

    async def fail_after_upload(
        *, user_id: UUID, source_id: UUID, kind: str, path: Path, sha256: str, content_type: str
    ) -> StoredObject:
        result = await put(
            user_id=user_id,
            source_id=source_id,
            kind=kind,
            path=path,
            sha256=sha256,
            content_type=content_type,
        )
        if kind == "canonical":
            raise ArtifactError("s3_unavailable", retryable=True)
        return result

    if failure == "download":
        monkeypatch.setattr(harness.media, "download", fail_download)
    if failure == "s3":
        monkeypatch.setattr(harness.objects, "put_file", fail_after_upload)
    fixture = OfficialMediaFixture()
    if failure == "transcription":
        fixture.status = 429
    dispatch_id = await supplement_job(harness, context, source_id)
    async with httpx.AsyncClient(transport=httpx.MockTransport(fixture), trust_env=False) as http:
        parser = pipeline(harness, http)
        await run_queued(harness, parser)
        job = await job_for(harness, context, dispatch_id)
        assert job.status == JobStatus.FAILED and job.error_message
        assert "private" not in job.error_message and len(job.error_message) <= 128
        if failure == "decode":
            assert job.error_message.startswith("media_")
        else:
            assert (
                job.error_message
                == {
                    "download": "wecom_media_unavailable",
                    "transcription": "openai_rate_limited",
                    "s3": "s3_unavailable",
                }[failure]
            )
        if failure != "decode":
            assert job.next_retry_at is not None
        assert await state(harness, context, source_id) == before
        assert stored_bytes(s3_config, str(before["storage_key"])) == card_bytes
        fixture.status = 200
        harness.media.body = good_video
        monkeypatch.setattr(harness.media, "download", download)
        monkeypatch.setattr(harness.objects, "put_file", put)
        assert await harness.repository.retry(dispatch_id)
        assert await state(harness, context, source_id) == before
        work = await harness.repository.claim(dispatch_id)
        assert work is not None
        assert await state(harness, context, source_id) == before
        await parser.process(work)
    assert (await job_for(harness, context, dispatch_id)).status == JobStatus.COMPLETED
    source = await source_for(harness, context, source_id)
    assert source.status == SourceStatus.STORED
    assert source.sha256 == before["sha256"] and source.storage_key == before["storage_key"]
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        assets = list(await session.scalars(select(Asset).where(Asset.source_id == source_id)))
        keys = {asset.storage_key for asset in assets}
        assert len(keys) == len(assets)
        assert len([a for a in assets if a.kind == AssetKind.ORIGINAL]) == 2
    actual_keys = await harness.objects.list_keys(user_id=context.user_id, source_id=source_id)
    assert set(actual_keys) == keys
    assert stored_bytes(s3_config, str(source.storage_key)) == card_bytes


async def test_audio_named_mp4_is_not_accepted_as_video(harness: Harness, tmp_path: Path) -> None:
    context, source_id = await admit_card(harness)
    before = await state(harness, context, source_id)
    harness.media = Media(audio_file(tmp_path / "audio.wav").read_bytes(), "pretend.mp4")
    dispatch_id = await supplement_job(harness, context, source_id)
    fixture = OfficialMediaFixture()
    async with httpx.AsyncClient(transport=httpx.MockTransport(fixture), trust_env=False) as http:
        await run_queued(harness, pipeline(harness, http))
    job = await job_for(harness, context, dispatch_id)
    assert job.status == JobStatus.FAILED and job.error_message == "channel_supplement_not_video"
    assert await state(harness, context, source_id) == before


async def test_video_with_no_readable_text_keeps_original_without_index(
    harness: Harness,
) -> None:
    vector = FakeVector()
    enable(harness, vector)
    context, source_id = await admit_card(harness)
    before = await state(harness, context, source_id)
    dispatch_id = await supplement_job(harness, context, source_id)
    parser = IngestionPipeline(
        harness.repository, harness.media, harness.objects, media_parser=EmptyVideo()
    )
    await run_queued(harness, parser)
    assert (await job_for(harness, context, dispatch_id)).status == JobStatus.COMPLETED
    source = await source_for(harness, context, source_id)
    assert source.status == SourceStatus.METADATA_ONLY and not source.text
    assert source.metadata_["original_video_available"]
    assert source.metadata_["supplement_original_key"] != source.storage_key
    assert source.sha256 == before["sha256"] and source.metadata_["channels"] == CARD
    assert len(await harness.jobs()) == 2 and not vector.calls


@pytest.mark.parametrize("tombstone", [SourceStatus.DELETING, SourceStatus.DELETED])
@pytest.mark.parametrize("phase", ["claim", "finalize", "retry"])
async def test_deleted_card_cannot_be_resurrected_by_supplement(
    harness: Harness, tmp_path: Path, tombstone: SourceStatus, phase: str
) -> None:
    context, source_id = await admit_card(harness)
    before = await state(harness, context, source_id)
    dispatch_id = await supplement_job(harness, context, source_id)
    parser = IngestionPipeline(
        harness.repository, harness.media, harness.objects, media_parser=EmptyVideo()
    )
    work = prepared = None
    if phase != "claim":
        work = await harness.repository.claim(dispatch_id)
        assert work is not None
        if phase == "finalize":
            prepared = await parser.prepare(work, tmp_path)
        else:
            await harness.repository.fail(work, ArtifactError("synthetic_failure"))
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        source = await session.get(Source, source_id, with_for_update=True)
        assert source is not None
        source.status = tombstone
    if phase == "claim":
        assert await harness.repository.claim(dispatch_id) is None
    elif phase == "retry":
        with pytest.raises(ArtifactError, match="source_unavailable"):
            await harness.repository.retry(dispatch_id)
    else:
        assert work is not None and prepared is not None
        with pytest.raises(ArtifactError, match="source_unavailable") as caught:
            await parser.finalize(work, prepared, tmp_path)
        await harness.repository.fail(work, caught.value)
    after = await state(harness, context, source_id)
    assert after == {**before, "status": tombstone}


async def test_one_lease_and_expired_worker_cannot_finalize_supplement(
    harness: Harness, tmp_path: Path
) -> None:
    context, source_id = await admit_card(harness)
    before = await state(harness, context, source_id)
    dispatch_id = await supplement_job(harness, context, source_id)
    first, duplicate = await asyncio.gather(
        harness.repository.claim(dispatch_id), harness.repository.claim(dispatch_id)
    )
    assert (first is None) != (duplicate is None)
    old = first or duplicate
    assert old is not None and await state(harness, context, source_id) == before
    parser = IngestionPipeline(
        harness.repository, harness.media, harness.objects, media_parser=EmptyVideo()
    )
    prepared = await parser.prepare(old, tmp_path)
    async with tenant_session(harness.store.tenant_factory, context.user_id) as session:
        job = await session.get(IngestionJob, old.job_id)
        assert job is not None
        job.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    new = await harness.repository.claim(dispatch_id)
    assert new is not None and new.lease_token != old.lease_token
    with pytest.raises(ArtifactError, match="job_lease_lost"):
        await parser.finalize(old, prepared, tmp_path)
    await harness.repository.fail(old, ArtifactError("stale_worker_failure"))
    assert await state(harness, context, source_id) == before
    await parser.process(new)
    job = await job_for(harness, context, dispatch_id)
    assert job.status == JobStatus.COMPLETED and job.error_message is None and job.attempts == 2


async def test_supplement_foreign_tenant_reads_writes_and_forged_worker_are_denied(
    harness: Harness, tmp_path: Path
) -> None:
    context, source_id = await admit_card(harness)
    other, _ = await admit_card(harness, user="second-customer")
    assert other.user_id == user_identity(harness.store.corp_id, "second-customer")
    dispatch_id = await supplement_job(harness, context, source_id)
    work = await harness.repository.claim(dispatch_id)
    assert work is not None
    parser = IngestionPipeline(
        harness.repository, harness.media, harness.objects, media_parser=EmptyVideo()
    )
    prepared = await parser.prepare(work, tmp_path)
    before = await state(harness, context, source_id)
    async with tenant_session(harness.store.tenant_factory, other.user_id) as session:
        assert await session.get(Source, source_id) is None
        assert await session.get(IngestionJob, work.job_id) is None
        assert await session.get(IngestionDispatch, dispatch_id) is None
        assert not list(await session.scalars(select(Asset).where(Asset.source_id == source_id)))
    with pytest.raises(DBAPIError):
        async with tenant_session(harness.store.tenant_factory, other.user_id) as session:
            session.add(
                IngestionJob(
                    user_id=other.user_id,
                    source_id=source_id,
                    operation_key=f"{SUPPLEMENT}:{source_id}",
                    input_data={"kind": SUPPLEMENT, "media_id": "synthetic"},
                )
            )
            await session.flush()
    with pytest.raises(ArtifactError, match="job_lease_lost"):
        await parser.finalize(replace(work, user_id=other.user_id), prepared, tmp_path)
    assert await state(harness, context, source_id) == before
    await parser.finalize(work, prepared, tmp_path)
