"""Real ffmpeg, restricted PostgreSQL, Redis and S3; synthetic OpenAI transport."""

import asyncio
import hashlib
import io
import math
import struct
import subprocess
import wave
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from sqlalchemy import func, select

from app.adapters.ffmpeg import FFmpegExtractor
from app.adapters.openai_media import OpenAIMediaAdapter
from app.connectors.wecom.persistence import WeComOutbox
from app.db.models import Asset, Source
from app.db.session import tenant_session
from app.domain.artifacts import ArtifactError, StoredObject
from app.domain.enums import AssetKind, JobStatus, SourceStatus, SourceType
from app.domain.media import MediaLimits
from app.ingestion.models import MessageSource
from app.ingestion.pipeline import IngestionPipeline
from app.ingestion.worker import IngestionWorker
from app.parsers.media import AudioVideoParser
from tests.integration.test_ingestion import Harness, Media, s3_client
from tests.integration.test_ingestion import harness as harness
from tests.integration.test_ingestion import s3_config as s3_config

pytestmark = pytest.mark.integration


def audio_file(path: Path, duration: float = 2) -> Path:
    with wave.open(str(path), "wb") as stream:
        stream.setnchannels(1)
        stream.setsampwidth(2)
        stream.setframerate(16_000)
        stream.writeframes(
            b"".join(
                struct.pack("<h", int(3000 * math.sin(i / 16_000 * 2 * math.pi * 220)))
                for i in range(round(duration * 16_000))
            )
        )
    return path


async def video_file(path: Path, *, audio: bool = True) -> Path:
    args = [
        "ffmpeg",
        "-v",
        "error",
        "-nostdin",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "color=black:s=160x120:r=10:d=2",
        "-f",
        "lavfi",
        "-i",
        "color=white:s=160x120:r=10:d=2",
    ]
    if audio:
        args += ["-f", "lavfi", "-i", "sine=frequency=220:sample_rate=16000:duration=4"]
    args += ["-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0[v]", "-map", "[v]"]
    if audio:
        args += ["-map", "2:a", "-c:a", "aac"]
    args += ["-c:v", "mpeg4", "-threads", "1", str(path)]
    result = await asyncio.to_thread(
        subprocess.run,
        args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, "Synthetic ffmpeg fixture generation failed"
    return path


class OfficialMediaFixture:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.status = 200
        self.silent = False

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.openai.com"
        self.calls.append(request.url.path)
        if self.status != 200:
            return httpx.Response(
                self.status, json={"error": {"message": "private-provider-error"}}
            )
        if request.url.path.endswith("transcriptions"):
            body = request.content
            wav = body[body.index(b"RIFF") :]
            with wave.open(io.BytesIO(wav), "rb") as audio:
                duration = audio.getnframes() / audio.getframerate()
            end = min(0.5, duration)
            return httpx.Response(
                200,
                json={
                    "text": "" if self.silent else "合成测试资料",
                    "language": "chinese",
                    "segments": []
                    if self.silent
                    else [{"start": min(0.1, end / 2), "end": end, "text": "合成测试资料"}],
                },
            )
        assert request.url.path.endswith("responses")
        return httpx.Response(
            200,
            json={
                "id": "synthetic",
                "status": "completed",
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": [{"type": "output_text", "text": "画面为单色背景。"}],
                    }
                ],
            },
        )


def pipeline(harness: Harness, http: httpx.AsyncClient, **changes: object) -> IngestionPipeline:
    limits = MediaLimits.model_validate({"chunk_seconds": 1, "max_frames": 3, **changes})
    provider = OpenAIMediaAdapter(http, "synthetic", vision_model="synthetic-vision")
    parser = AudioVideoParser(
        FFmpegExtractor(limits), provider, provider, limits, vision_model="synthetic-vision"
    )
    return IngestionPipeline(
        harness.repository, harness.media, harness.objects, media_parser=parser
    )


async def run_one(harness: Harness, parser: IngestionPipeline) -> None:
    await harness.repository.publish_due(harness.queue)
    assert await IngestionWorker(harness.queue, harness.repository, parser).once()


async def test_real_audio_storage_offsets_dedup_and_tenant_isolation(
    harness: Harness, tmp_path: Path, s3_config: dict[str, str]
) -> None:
    audio = audio_file(tmp_path / "voice.wav")
    harness.media = Media(audio.read_bytes(), audio.name)
    first = await harness.admit(file=True)
    fixture = OfficialMediaFixture()
    async with httpx.AsyncClient(transport=httpx.MockTransport(fixture), trust_env=False) as http:
        parser = pipeline(harness, http)
        await run_one(harness, parser)
        assert (await harness.jobs())[-1].status == JobStatus.COMPLETED
        calls = len(fixture.calls)
        assert calls == 2
        async with tenant_session(harness.store.tenant_factory, first) as session:
            source = (await session.scalars(select(Source))).one()
            source_id = source.id
            assert source.source_type == SourceType.AUDIO and source.status == SourceStatus.STORED
            assert source.metadata_["parser_version"] == 5
            assert source.metadata_["segments"][1]["locator"]["start_seconds"] == 1.1
            assert source.vector_file_id is None
            assert {a.kind for a in await session.scalars(select(Asset))} == {
                AssetKind.ORIGINAL,
                AssetKind.NORMALIZED,
                AssetKind.AUDIO,
                AssetKind.TRANSCRIPT,
            }
            key = source.metadata_["markdown_key"]
        client = s3_client(s3_config)
        try:
            body = client.get_object(Bucket=s3_config["bucket"], Key=key)["Body"]
            assert "[00:01.100–00:01.500]" in body.read().decode()
            body.close()
        finally:
            client.close()
        await harness.admit(file=True)
        await run_one(harness, parser)
        assert len(fixture.calls) == calls  # Completed same-tenant source skips paid work.
        second = await harness.admit(file=True, user="second-customer")
        await run_one(harness, parser)
        assert len(fixture.calls) == calls * 2
        async with tenant_session(harness.store.tenant_factory, second) as session:
            assert await session.get(Source, source_id) is None
            other = (await session.scalars(select(Source))).one()
            assert other.status == SourceStatus.STORED and str(second) in other.storage_key


@pytest.mark.parametrize("audio", [False, True])
async def test_real_video_keyframes_and_metadata_upgrade(
    harness: Harness, tmp_path: Path, audio: bool
) -> None:
    path = await video_file(tmp_path / "video.mp4", audio=audio)
    harness.media = Media(path.read_bytes(), path.name)
    user_id = await harness.admit(file=True)
    await harness.drain()  # M3-style original-only source.
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = (await session.scalars(select(Source))).one()
        source_id, key = source.id, source.storage_key
        assert source.status == SourceStatus.METADATA_ONLY
    await harness.admit(file=True)
    fixture = OfficialMediaFixture()
    async with httpx.AsyncClient(transport=httpx.MockTransport(fixture), trust_env=False) as http:
        await run_one(harness, pipeline(harness, http))
    assert (await harness.jobs())[-1].status == JobStatus.COMPLETED
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = await session.get(Source, source_id)
        assert source and source.status == SourceStatus.STORED and source.storage_key == key
        assert source.source_type == SourceType.VIDEO
        assert 1 <= source.metadata_["keyframes"] <= 3
        assert source.metadata_["has_audio"] == audio
        assert set(await session.scalars(select(MessageSource.source_id))) == {source_id}
        assert AssetKind.KEYFRAME in {a.kind for a in await session.scalars(select(Asset))}
        assert any(s["method"] == "vision" for s in source.metadata_["segments"])
    assert any(p.endswith("transcriptions") for p in fixture.calls) == audio


async def test_silent_audio_upgrades_old_metadata_and_skips_repeat_provider_call(
    harness: Harness, tmp_path: Path
) -> None:
    path = audio_file(tmp_path / "quiet.wav")
    harness.media = Media(path.read_bytes(), path.name)
    user_id = await harness.admit(file=True)
    await harness.drain()
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = (await session.scalars(select(Source))).one()
        source_id, key = source.id, source.storage_key
    await harness.admit(file=True)
    fixture = OfficialMediaFixture()
    fixture.silent = True
    async with httpx.AsyncClient(transport=httpx.MockTransport(fixture), trust_env=False) as http:
        parser = pipeline(harness, http)
        await run_one(harness, parser)
        assert (await harness.jobs())[-1].status == JobStatus.COMPLETED
        async with tenant_session(harness.store.tenant_factory, user_id) as session:
            source = await session.get(Source, source_id)
            assert source and source.status == SourceStatus.METADATA_ONLY
            assert source.storage_key == key and not source.text
            assert source.metadata_["parser_version"] == 5
            assert source.metadata_["parse_status"] == "no_speech_detected"
            assert AssetKind.TRANSCRIPT in {a.kind for a in await session.scalars(select(Asset))}
            replies = list(await session.scalars(select(WeComOutbox.content)))
            assert any("音频已处理，未识别到语音" in reply for reply in replies)
        calls = len(fixture.calls)
        await harness.admit(file=True)
        await run_one(harness, parser)
        assert len(fixture.calls) == calls == 2
        assert (await harness.jobs())[-1].status == JobStatus.COMPLETED


async def test_provider_failure_is_retryable_and_original_not_falsely_completed(
    harness: Harness, tmp_path: Path
) -> None:
    path = audio_file(tmp_path / "voice.wav")
    harness.media = Media(path.read_bytes(), path.name)
    user_id = await harness.admit(file=True)
    fixture = OfficialMediaFixture()
    fixture.status = 429
    async with httpx.AsyncClient(transport=httpx.MockTransport(fixture), trust_env=False) as http:
        parser = pipeline(harness, http)
        await run_one(harness, parser)
        job = (await harness.jobs())[-1]
        assert job.status == JobStatus.FAILED and job.next_retry_at is not None
        assert "private-provider-error" not in job.error_message
        async with tenant_session(harness.store.tenant_factory, user_id) as session:
            assert await session.scalar(select(func.count()).select_from(Asset)) == 0
        dispatch = (await harness.dispatches())[-1]
        assert await harness.repository.retry(dispatch.id)
        digest = hashlib.sha256(str(dispatch.id).encode()).hexdigest()
        assert await harness.queue.redis.expire(harness.queue.stream + ":dedup:" + digest, 0)
        fixture.status = 200
        await run_one(harness, parser)
        assert (await harness.jobs())[-1].status == JobStatus.COMPLETED


@pytest.mark.parametrize("invalid", [True, False])
async def test_bad_or_overlong_media_preserves_original_without_api(
    harness: Harness, tmp_path: Path, invalid: bool
) -> None:
    data = b"invalid private bytes" if invalid else audio_file(tmp_path / "a.wav").read_bytes()
    harness.media = Media(data, "voice.wav")
    user_id = await harness.admit(file=True)
    fixture = OfficialMediaFixture()
    async with httpx.AsyncClient(transport=httpx.MockTransport(fixture), trust_env=False) as http:
        await run_one(harness, pipeline(harness, http, max_duration_seconds=1))
    assert not fixture.calls and (await harness.jobs())[-1].status == JobStatus.COMPLETED
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = (await session.scalars(select(Source))).one()
        assert source.status == SourceStatus.METADATA_ONLY and source.storage_key
        assert source.metadata_["parse_error"].startswith("media_")


async def test_real_ffmpeg_scene_detection_periodic_selection_and_network_playlist_rejection(
    tmp_path: Path,
) -> None:
    video = await video_file(tmp_path / "v.mp4", audio=False)
    limits = MediaLimits(max_frames=3, min_frame_gap_seconds=1, scene_threshold=0.1)
    result = await FFmpegExtractor(limits).extract(video, tmp_path)
    assert result.frames[0].timestamp < 0.2
    assert any(abs(frame.timestamp - 2) < 0.3 for frame in result.frames)
    assert len(result.frames) <= 3 and not result.audio
    playlist = tmp_path / "playlist.m3u8"
    playlist.write_text("#EXTM3U\n#EXTINF:1,\nhttp://127.0.0.1/internal\n", encoding="utf-8")
    with pytest.raises(ArtifactError, match="media_"):
        await FFmpegExtractor(limits).extract(playlist, tmp_path)


@pytest.mark.parametrize("failing_kind", ["audio", "keyframe"])
async def test_derivative_upload_failure_rolls_back_and_retry_has_no_duplicate_assets(
    harness: Harness,
    tmp_path: Path,
    s3_config: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    failing_kind: str,
) -> None:
    path = (
        audio_file(tmp_path / "voice.wav")
        if failing_kind == "audio"
        else await video_file(tmp_path / "video.mp4")
    )
    harness.media = Media(path.read_bytes(), path.name)
    user_id = await harness.admit(file=True)
    fixture = OfficialMediaFixture()
    put = harness.objects.put_file
    failed = False

    async def fail_after_upload(
        *, user_id: UUID, source_id: UUID, kind: str, path: Path, sha256: str, content_type: str
    ) -> StoredObject:
        nonlocal failed
        result = await put(
            user_id=user_id,
            source_id=source_id,
            kind=kind,
            path=path,
            sha256=sha256,
            content_type=content_type,
        )
        if kind == failing_kind and not failed:
            failed = True
            raise ArtifactError("s3_unavailable", retryable=True)
        return result

    monkeypatch.setattr(harness.objects, "put_file", fail_after_upload)
    async with httpx.AsyncClient(transport=httpx.MockTransport(fixture), trust_env=False) as http:
        parser = pipeline(harness, http)
        await run_one(harness, parser)
        job = (await harness.jobs())[-1]
        assert job.status == JobStatus.FAILED and job.error_message == "s3_unavailable"
        async with tenant_session(harness.store.tenant_factory, user_id) as session:
            assert await session.scalar(select(func.count()).select_from(Asset)) == 0
        dispatch = (await harness.dispatches())[-1]
        assert await harness.repository.retry(dispatch.id)
        digest = hashlib.sha256(str(dispatch.id).encode()).hexdigest()
        await harness.queue.redis.expire(harness.queue.stream + ":dedup:" + digest, 0)
        await run_one(harness, parser)
        assert (await harness.jobs())[-1].status == JobStatus.COMPLETED
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = (await session.scalars(select(Source))).one()
        assets = list(await session.scalars(select(Asset)))
        keys = {asset.storage_key for asset in assets}
        assert len(keys) == len(assets)
        assert all(ref["storage_key"] in keys for ref in source.metadata_["media_assets"])
    client = s3_client(s3_config)
    try:
        objects = client.list_objects_v2(Bucket=s3_config["bucket"], Prefix=f"users/{user_id}/")
        assert objects["KeyCount"] == len(keys)
    finally:
        client.close()


@pytest.mark.parametrize("kind", ["audio", "video", "video_without_audio"])
async def test_real_decoder_through_provider_adapter_and_timeline_without_services(
    tmp_path: Path, kind: str
) -> None:
    path = (
        audio_file(tmp_path / "voice.wav")
        if kind == "audio"
        else await video_file(tmp_path / "video.mp4", audio=kind == "video")
    )
    fixture = OfficialMediaFixture()
    limits = MediaLimits(chunk_seconds=1, max_frames=3)
    phases: list[str] = []

    async def mark() -> None:
        phases.append("transcribing")

    async with httpx.AsyncClient(transport=httpx.MockTransport(fixture), trust_env=False) as http:
        provider = OpenAIMediaAdapter(http, "synthetic", vision_model="synthetic-vision")
        parser = AudioVideoParser(
            FFmpegExtractor(limits), provider, provider, limits, vision_model="synthetic-vision"
        )
        result = await parser.parse(
            path, tmp_path, source_type=SourceType.OTHER, on_transcribing=mark
        )
    assert phases == ([] if kind == "video_without_audio" else ["transcribing"])
    assert result.content.metadata["parser_version"] == 5
    assert result.content.text and result.artifacts
    for segment in result.content.segments:
        locator = segment["locator"]
        assert 0 <= locator["start_seconds"] <= locator["end_seconds"] <= 4
