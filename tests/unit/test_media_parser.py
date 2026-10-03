import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.domain.artifacts import ArtifactError
from app.domain.documents import CanonicalDocument
from app.domain.enums import AssetKind, SourceType
from app.domain.media import (
    AudioChunk,
    ExtractedMedia,
    MediaInfo,
    MediaLimits,
    Transcript,
    TranscriptSegment,
    VideoFrame,
)
from app.ingestion.rendering import markdown, timestamp
from app.parsers.media import AudioVideoParser


class Extractor:
    def __init__(self, result: ExtractedMedia) -> None:
        self.result = result

    async def extract(self, path: Path, directory: Path) -> ExtractedMedia:
        return self.result


class Provider:
    def __init__(self, result: Transcript | None = None) -> None:
        self.result = result or Transcript(
            text="声音", segments=[TranscriptSegment(start=0, end=0.8, text="声音\x00")]
        )
        self.calls: list[str] = []

    async def transcribe(self, path: Path) -> Transcript:
        self.calls.append("audio")
        return self.result

    async def describe(self, path: Path) -> str:
        self.calls.append("vision")
        return "画面中似乎有一只猫\x00"


async def stage() -> None:
    pass


def make_parser(extracted: ExtractedMedia, provider: Provider | None = None) -> AudioVideoParser:
    provider = provider or Provider()
    return AudioVideoParser(
        Extractor(extracted),
        provider,
        provider,
        MediaLimits(chunk_seconds=1),
        vision_model="synthetic-vision",
    )


async def test_timeline_offsets_labels_and_sanitized_derivatives(tmp_path: Path) -> None:
    audio, image = tmp_path / "audio.wav", tmp_path / "frame.jpg"
    extracted = ExtractedMedia(
        MediaInfo(2, True, True),
        (AudioChunk(audio, 0, 1), AudioChunk(audio, 1, 2)),
        (VideoFrame(image, 0.5),),
    )
    phases: list[str] = []

    async def mark() -> None:
        phases.append("transcribing")

    parsed = await make_parser(extracted).parse(
        audio, tmp_path, source_type=SourceType.VIDEO, on_transcribing=mark
    )
    assert phases == ["transcribing"]
    assert [s["method"] for s in parsed.content.segments] == [
        "transcription",
        "vision",
        "transcription",
    ]
    assert parsed.content.segments[-1]["locator"] == {"start_seconds": 1.0, "end_seconds": 1.8}
    assert {a.kind for a in parsed.artifacts} == {
        AssetKind.AUDIO,
        AssetKind.KEYFRAME,
        AssetKind.TRANSCRIPT,
    }
    saved = json.loads((tmp_path / "transcript.json").read_text(encoding="utf-8"))
    assert saved["chunks"][1]["offset_seconds"] == 1
    assert "\x00" not in str(saved)
    document = CanonicalDocument(
        source_id=uuid4(),
        user_id=uuid4(),
        title="测试视频",
        source_type=SourceType.VIDEO,
        created_at=datetime.now(UTC),
        text=parsed.content.text,
        metadata={**parsed.content.metadata, "segments": parsed.content.segments},
    )
    rendered = markdown(document)
    assert "[00:01–00:01.800]（语音机器转录）" in rendered
    assert "[00:00.500–00:00.500]（画面描述·模型推断）" in rendered
    assert "\x00" not in rendered


async def test_video_without_audio_uses_only_frames(tmp_path: Path) -> None:
    provider = Provider()
    parsed = await make_parser(
        ExtractedMedia(MediaInfo(1, False, True), frames=(VideoFrame(tmp_path / "f.jpg", 0),)),
        provider,
    ).parse(tmp_path / "v", tmp_path, source_type=SourceType.VIDEO, on_transcribing=stage)
    assert provider.calls == ["vision"]
    assert parsed.content.metadata["transcription_model"] is None
    assert parsed.content.source_type == SourceType.VIDEO


async def test_silence_has_no_invented_text(tmp_path: Path) -> None:
    parsed = await make_parser(
        ExtractedMedia(MediaInfo(1, True, False), (AudioChunk(tmp_path / "a", 0, 1),)),
        Provider(Transcript()),
    ).parse(tmp_path / "a", tmp_path, source_type=SourceType.AUDIO, on_transcribing=stage)
    assert parsed.content.text == "" and parsed.content.segments == []
    assert parsed.content.metadata["parse_status"] == "no_speech_detected"


async def test_video_audio_may_end_before_video(tmp_path: Path) -> None:
    parsed = await make_parser(
        ExtractedMedia(
            MediaInfo(2, True, True),
            (AudioChunk(tmp_path / "a", 0, 1),),
            (VideoFrame(tmp_path / "f", 1.5),),
        )
    ).parse(tmp_path / "v", tmp_path, source_type=SourceType.VIDEO, on_transcribing=stage)
    assert [s["method"] for s in parsed.content.segments] == ["transcription", "vision"]


@pytest.mark.parametrize("duration", [float("nan"), float("inf"), -1, 0, 901])
async def test_invalid_duration_rejected(tmp_path: Path, duration: float) -> None:
    with pytest.raises(ArtifactError, match="media_duration_limit"):
        await make_parser(ExtractedMedia(MediaInfo(duration, False, False))).parse(
            tmp_path / "a", tmp_path, source_type=SourceType.AUDIO, on_transcribing=stage
        )


@pytest.mark.parametrize("start,end", [(0, 2), (0.5, 0.7)])
async def test_invalid_chunk_coverage(tmp_path: Path, start: float, end: float) -> None:
    with pytest.raises(ArtifactError, match="media_chunk_invalid"):
        await make_parser(
            ExtractedMedia(MediaInfo(1, True, False), (AudioChunk(tmp_path / "a", start, end),))
        ).parse(tmp_path / "a", tmp_path, source_type=SourceType.AUDIO, on_transcribing=stage)


@pytest.mark.parametrize(
    "transcript,code",
    [
        (Transcript(text="text"), "openai_timestamps_missing"),
        (
            Transcript(segments=[TranscriptSegment(start=1.05, end=1.08, text="x")]),
            "openai_timestamps_invalid",
        ),
        (
            Transcript(segments=[TranscriptSegment(start=0, end=2, text="x")]),
            "openai_timestamps_invalid",
        ),
        (
            Transcript(
                segments=[
                    TranscriptSegment(start=0, end=0.9, text="x"),
                    TranscriptSegment(start=0.2, end=0.8, text="y"),
                ]
            ),
            "openai_timestamps_invalid",
        ),
    ],
)
async def test_provider_timestamps_must_fit_chunk(
    tmp_path: Path, transcript: Transcript, code: str
) -> None:
    with pytest.raises(ArtifactError, match=code):
        await make_parser(
            ExtractedMedia(MediaInfo(1, True, False), (AudioChunk(tmp_path / "a", 0, 1),)),
            Provider(transcript),
        ).parse(tmp_path / "a", tmp_path, source_type=SourceType.AUDIO, on_transcribing=stage)


@pytest.mark.parametrize("start,end", [(float("nan"), 1), (0, float("inf")), (1, 0), (-1, 1)])
def test_transcript_contract_rejects_nonfinite_and_invalid_ranges(start: float, end: float) -> None:
    with pytest.raises(ValidationError):
        TranscriptSegment(start=start, end=end, text="x")


async def test_cancel_does_not_return_partial_timeline(tmp_path: Path) -> None:
    entered = asyncio.Event()

    class Waiting(Provider):
        async def describe(self, path: Path) -> str:
            entered.set()
            await asyncio.Event().wait()
            return "unreachable"

    parser = make_parser(
        ExtractedMedia(MediaInfo(1, False, True), frames=(VideoFrame(tmp_path / "f", 0),)),
        Waiting(),
    )
    task = asyncio.create_task(
        parser.parse(tmp_path / "v", tmp_path, source_type=SourceType.VIDEO, on_transcribing=stage)
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not (tmp_path / "transcript.json").exists()


@pytest.mark.parametrize("value,expected", [(0, "00:00"), (65.25, "01:05.250"), (3600, "60:00")])
def test_timestamp(value: float, expected: str) -> None:
    assert timestamp(value) == expected
