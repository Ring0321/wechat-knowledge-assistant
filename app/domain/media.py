"""Media boundaries; providers receive local derivatives, never tenant authority."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.domain.enums import AssetKind, SourceType
from app.domain.parsing import ParsedContent


class MediaLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    max_input_bytes: int = Field(default=20 * 1024 * 1024, ge=1024, le=100 * 1024 * 1024)
    max_duration_seconds: float = Field(default=900, gt=0, le=3600)
    chunk_seconds: int = Field(default=300, ge=1, le=600)
    max_frames: int = Field(default=12, ge=1, le=32)
    frame_interval_seconds: float = Field(default=30, ge=5, le=600)
    scene_threshold: float = Field(default=0.3, gt=0, lt=1)
    min_frame_gap_seconds: float = Field(default=2, ge=1, le=30)
    max_pixels: int = Field(default=8_294_400, ge=1000, le=33_177_600)
    frame_width: int = Field(default=960, ge=64, le=1280)
    process_timeout_seconds: float = Field(default=60, gt=0, le=120)
    total_timeout_seconds: float = Field(default=240, ge=10, le=500)
    max_output_bytes: int = Field(default=80 * 1024 * 1024, ge=1024)
    max_text_chars: int = Field(default=500_000, ge=100, le=2_000_000)


class TranscriptSegment(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, allow_inf_nan=False)
    start: float = Field(ge=0)
    end: float = Field(ge=0)
    text: str = Field(max_length=100_000)

    @model_validator(mode="after")
    def ordered(self) -> "TranscriptSegment":
        if self.end < self.start:
            raise ValueError("Invalid transcript time range")
        return self


class Transcript(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True, allow_inf_nan=False)
    text: str = Field(default="", max_length=500_000)
    segments: list[TranscriptSegment] = Field(default_factory=list, max_length=10_000)
    language: str | None = Field(default=None, max_length=100)


@dataclass(frozen=True)
class MediaInfo:
    duration: float
    has_audio: bool
    has_video: bool
    width: int = 0
    height: int = 0


@dataclass(frozen=True)
class AudioChunk:
    path: Path = field(repr=False)
    start: float
    end: float


@dataclass(frozen=True)
class VideoFrame:
    path: Path = field(repr=False)
    timestamp: float


@dataclass(frozen=True)
class ExtractedMedia:
    info: MediaInfo
    audio: tuple[AudioChunk, ...] = ()
    frames: tuple[VideoFrame, ...] = ()


@dataclass(frozen=True)
class MediaArtifact:
    kind: AssetKind
    path: Path = field(repr=False)
    content_type: str
    start: float | None = None
    end: float | None = None


@dataclass(frozen=True)
class ParsedMedia:
    content: ParsedContent
    artifacts: tuple[MediaArtifact, ...] = ()


class MediaExtractor(Protocol):
    async def extract(self, path: Path, directory: Path) -> ExtractedMedia: ...


class AudioTranscriber(Protocol):
    async def transcribe(self, path: Path) -> Transcript: ...


class FrameDescriber(Protocol):
    async def describe(self, path: Path) -> str: ...


class MediaParser(Protocol):
    async def parse(
        self,
        path: Path,
        directory: Path,
        *,
        source_type: SourceType,
        on_transcribing: Callable[[], Awaitable[None]],
    ) -> ParsedMedia: ...
