"""Provider-neutral retrieval contracts. Tenant identity is supplied by trusted callers only."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from pydantic import JsonValue

from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceStatus, SourceType


class VectorRequestRejected(ArtifactError):
    """Provider conclusively rejected a mutation; retry may safely submit it again."""


@dataclass(frozen=True)
class TenantContext:
    user_id: UUID

    def __post_init__(self) -> None:
        if not isinstance(self.user_id, UUID):
            raise TypeError("Verified tenant UUID required")


@dataclass(frozen=True)
class VectorHit:
    file_id: str
    score: float
    text: str = field(repr=False)
    attributes: dict[str, JsonValue] = field(default_factory=dict)


VectorFileStatus = Literal["in_progress", "completed", "cancelled", "failed"]


class VectorProvider(Protocol):
    async def find_store(self, user_id: UUID) -> str | None: ...

    async def create_store(self, user_id: UUID) -> str: ...

    async def find_file(self, filename: str) -> str | None: ...

    async def upload(self, filename: str, content: bytes) -> str: ...

    async def file_status(self, store_id: str, file_id: str) -> VectorFileStatus | None: ...

    async def attach(
        self, store_id: str, file_id: str, attributes: dict[str, JsonValue]
    ) -> VectorFileStatus: ...

    async def detach(self, store_id: str, file_id: str) -> None: ...

    async def delete_file(self, file_id: str) -> None: ...

    async def search(
        self, store_id: str, query: str, *, user_id: UUID, limit: int
    ) -> tuple[VectorHit, ...]: ...


class ObjectDeleter(Protocol):
    async def delete(self, *, user_id: UUID, source_id: UUID, key: str) -> None: ...

    async def list_keys(self, *, user_id: UUID, source_id: UUID) -> tuple[str, ...]: ...


@dataclass(frozen=True)
class SourceView:
    source_id: UUID
    title: str
    source_type: SourceType
    original_url: str | None
    created_at: datetime
    status: SourceStatus
    summary: str | None
    tags: tuple[str, ...]
    text: str = field(repr=False)


@dataclass(frozen=True)
class KnowledgeHit:
    source: SourceView
    score: float
    text: str = field(repr=False)
    locators: tuple[dict[str, JsonValue], ...] = ()
