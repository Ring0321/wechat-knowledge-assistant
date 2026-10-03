from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import IngestionJob, Source
from app.domain.enums import SourceType


@dataclass(frozen=True)
class WorkItem:
    dispatch_id: UUID
    user_id: UUID
    job_id: UUID
    source_id: UUID
    lease_token: UUID
    title: str
    source_type: SourceType
    original_url: str | None
    created_at: datetime
    input_data: dict[str, JsonValue] = field(repr=False)


class CompletionNotifier(Protocol):
    async def finished(
        self, session: AsyncSession, job: IngestionJob, source: Source, *, outcome: str
    ) -> None: ...
