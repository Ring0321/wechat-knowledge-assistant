"""Bounded parser and web-fetch contracts, independent of frameworks and persistence."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from app.domain.enums import SourceType


class ParseLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    max_input_bytes: int = Field(default=20 * 1024 * 1024, ge=1024)
    max_text_chars: int = Field(default=500_000, ge=100)
    max_pages: int = Field(default=100, ge=1, le=1000)
    max_cells: int = Field(default=50_000, ge=1)
    max_archive_bytes: int = Field(default=50 * 1024 * 1024, ge=1024)
    max_archive_entries: int = Field(default=2000, ge=1)
    max_pixels: int = Field(default=20_000_000, ge=1000)
    timeout_seconds: float = Field(default=45, gt=0, le=300)


class ParsedContent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    text: str = ""
    title: str | None = None
    source_type: SourceType
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    segments: list[dict[str, JsonValue]] = Field(default_factory=list)

    @model_validator(mode="after")
    def strip_controls(self) -> "ParsedContent":
        # PostgreSQL text/JSONB cannot contain NUL; untrusted metadata is text too.
        def clean(value: JsonValue) -> JsonValue:
            if isinstance(value, str):
                return "".join(c for c in value if ord(c) >= 32 or c in "\n\t")
            if isinstance(value, list):
                return [clean(item) for item in value]
            if isinstance(value, dict):
                return {str(clean(key)): clean(item) for key, item in value.items()}
            return value

        self.text = str(clean(self.text))
        self.title = str(clean(self.title)) if self.title else None
        self.metadata = {str(clean(key)): clean(value) for key, value in self.metadata.items()}
        self.segments = [
            {str(clean(key)): clean(value) for key, value in item.items()} for item in self.segments
        ]
        return self


class FileParser(Protocol):
    async def parse(
        self, path: Path, *, filename: str | None, source_type: SourceType
    ) -> ParsedContent: ...


@dataclass(frozen=True)
class WebResponse:
    url: str
    status_code: int
    body: bytes = field(repr=False)
    content_type: str = "text/html"


class WebFetcher(Protocol):
    async def fetch(self, url: str, *, max_bytes: int) -> WebResponse: ...


class PageRenderer(Protocol):
    async def render(self, url: str, *, max_bytes: int) -> WebResponse: ...
