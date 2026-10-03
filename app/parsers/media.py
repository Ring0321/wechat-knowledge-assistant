"""Audio/video orchestration without provider or storage SDK dependencies."""

import asyncio
import json
import math
from collections.abc import Awaitable, Callable
from pathlib import Path

from pydantic import JsonValue

from app.domain.artifacts import ArtifactError
from app.domain.enums import AssetKind, SourceType
from app.domain.media import (
    AudioTranscriber,
    FrameDescriber,
    MediaArtifact,
    MediaExtractor,
    MediaLimits,
    ParsedMedia,
)
from app.domain.parsing import ParsedContent

MEDIA_SUFFIXES = {
    ".mp3": SourceType.AUDIO,
    ".wav": SourceType.AUDIO,
    ".m4a": SourceType.AUDIO,
    ".aac": SourceType.AUDIO,
    ".amr": SourceType.AUDIO,
    ".ogg": SourceType.AUDIO,
    ".flac": SourceType.AUDIO,
    ".mp4": SourceType.VIDEO,
    ".mov": SourceType.VIDEO,
    ".mkv": SourceType.VIDEO,
    ".webm": SourceType.VIDEO,
    ".avi": SourceType.VIDEO,
}


class AudioVideoParser:
    def __init__(
        self,
        extractor: MediaExtractor,
        transcriber: AudioTranscriber,
        vision: FrameDescriber,
        limits: MediaLimits,
        *,
        vision_model: str,
    ) -> None:
        self.extractor, self.transcriber, self.vision = extractor, transcriber, vision
        self.limits, self.vision_model = limits, vision_model

    async def parse(
        self,
        path: Path,
        directory: Path,
        *,
        source_type: SourceType,
        on_transcribing: Callable[[], Awaitable[None]],
    ) -> ParsedMedia:
        try:
            async with asyncio.timeout(self.limits.total_timeout_seconds):
                return await self._parse(path, directory, on_transcribing)
        except TimeoutError:
            raise ArtifactError("media_processing_timeout", retryable=True) from None

    async def _parse(
        self, path: Path, directory: Path, on_transcribing: Callable[[], Awaitable[None]]
    ) -> ParsedMedia:
        extracted = await self.extractor.extract(path, directory)
        duration = extracted.info.duration
        if not math.isfinite(duration) or not 0 < duration <= self.limits.max_duration_seconds:
            raise ArtifactError("media_duration_limit")
        if (
            len(extracted.frames) > self.limits.max_frames
            or len(extracted.audio) > math.ceil(duration / self.limits.chunk_seconds) + 1
        ):
            raise ArtifactError("media_output_limit")
        artifacts: list[MediaArtifact] = []
        segments: list[dict[str, JsonValue]] = []
        transcripts: list[JsonValue] = []
        text_size, last_chunk_end = 0, 0.0
        if extracted.audio:
            await on_transcribing()
        for chunk in extracted.audio:
            if (
                not all(math.isfinite(v) for v in (chunk.start, chunk.end))
                or chunk.start < last_chunk_end - 0.05
                or chunk.start > last_chunk_end + 0.05
                or not chunk.start < chunk.end <= duration + 0.05
            ):
                raise ArtifactError("media_chunk_invalid")
            last_chunk_end = chunk.end
            result = await self.transcriber.transcribe(chunk.path)
            if result.text.strip() and not result.segments:
                raise ArtifactError("openai_timestamps_missing")
            previous_end = 0.0
            for part in result.segments:
                if (
                    part.start < previous_end - 0.05
                    or part.start > chunk.end - chunk.start
                    or part.end > chunk.end - chunk.start + 0.1
                ):
                    raise ArtifactError("openai_timestamps_invalid")
                previous_end = part.end
                if not part.text.strip():
                    continue
                start = round(chunk.start + part.start, 3)
                end = round(min(chunk.end, chunk.start + part.end), 3)
                text_size += len(part.text)
                segments.append(
                    {
                        "locator": {"start_seconds": start, "end_seconds": end},
                        "method": "transcription",
                        "text": part.text,
                    }
                )
            # Store only the validated transcript fields, never the raw provider response.
            transcripts.append(
                {
                    "offset_seconds": chunk.start,
                    "end_seconds": chunk.end,
                    "language": result.language,
                    "segments": [part.model_dump(mode="json") for part in result.segments],
                }
            )
            artifacts.append(
                MediaArtifact(AssetKind.AUDIO, chunk.path, "audio/wav", chunk.start, chunk.end)
            )
            if text_size > self.limits.max_text_chars:
                raise ArtifactError("media_text_limit")
        if extracted.info.has_audio and (
            not extracted.audio
            or (not extracted.info.has_video and abs(last_chunk_end - duration) > 0.1)
        ):
            raise ArtifactError("media_chunk_invalid")
        last_frame = -1.0
        for frame in extracted.frames:
            if (
                not math.isfinite(frame.timestamp)
                or not 0 <= frame.timestamp < duration
                or frame.timestamp <= last_frame
            ):
                raise ArtifactError("media_frame_invalid")
            last_frame = frame.timestamp
            description = await self.vision.describe(frame.path)
            if not description.strip():
                raise ArtifactError("openai_vision_empty")
            text_size += len(description)
            if text_size > self.limits.max_text_chars:
                raise ArtifactError("media_text_limit")
            segments.append(
                {
                    "locator": {"start_seconds": frame.timestamp, "end_seconds": frame.timestamp},
                    "method": "vision",
                    "text": description,
                }
            )
            artifacts.append(
                MediaArtifact(
                    AssetKind.KEYFRAME, frame.path, "image/jpeg", frame.timestamp, frame.timestamp
                )
            )
        segments.sort(key=_segment_start)
        content = ParsedContent(
            source_type=SourceType.VIDEO if extracted.info.has_video else SourceType.AUDIO,
            text="\n\n".join(str(part["text"]) for part in segments),
            segments=segments,
            metadata={
                "parser_version": 5,
                "duration_seconds": duration,
                "has_audio": extracted.info.has_audio,
                "has_video": extracted.info.has_video,
                "transcription_model": "whisper-1" if extracted.audio else None,
                "vision_model": self.vision_model if extracted.frames else None,
                "audio_chunks": len(extracted.audio),
                "keyframes": len(extracted.frames),
                "timeline_note": "语音为机器转录；画面描述为模型推断，仅代表采样帧。",
                "parse_status": "normalized" if segments else "no_speech_detected",
            },
        )
        # Reuse ParsedContent's recursive control stripping for the derivative as well.
        safe = ParsedContent(source_type=content.source_type, metadata={"chunks": transcripts})
        transcript_path = directory / "transcript.json"
        transcript_path.write_text(
            json.dumps(
                {"model": "whisper-1", "chunks": safe.metadata["chunks"]},
                ensure_ascii=False,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        artifacts.append(MediaArtifact(AssetKind.TRANSCRIPT, transcript_path, "application/json"))
        return ParsedMedia(content, tuple(artifacts))


def _segment_start(segment: dict[str, JsonValue]) -> float:
    locator = segment["locator"]
    assert isinstance(locator, dict)
    start = locator["start_seconds"]
    assert isinstance(start, (int, float))
    return float(start)
