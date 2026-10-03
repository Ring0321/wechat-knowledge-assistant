"""Canonical data contract only; parsers and indexing are later milestones."""

from datetime import UTC, datetime
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator

from app.domain.enums import SourceType


class CanonicalDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: UUID
    user_id: UUID
    title: str = Field(min_length=1, max_length=512)
    source_type: SourceType
    original_url: str | None = None
    created_at: AwareDatetime
    text: str = ""
    summary: str | None = None
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("title")
    @classmethod
    def title_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Title cannot be blank")
        return value.strip()

    @field_validator("created_at")
    @classmethod
    def normalize_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @field_validator("original_url")
    @classmethod
    def validate_original_url(cls, value: str | None) -> str | None:
        # This is metadata validation, NOT an SSRF check or permission to fetch.
        if value is not None and not value.startswith(("https://", "http://")):
            raise ValueError("Original URL must be HTTP(S)")
        return value
