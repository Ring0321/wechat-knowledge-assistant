from dataclasses import dataclass, field
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.models import QuestionJob
from app.db.models import Message
from app.domain.agent import Evidence
from app.domain.knowledge import SourceView


@dataclass(frozen=True)
class QuestionWork:
    dispatch_id: UUID
    user_id: UUID
    job_id: UUID
    message_id: UUID
    conversation_id: UUID
    lease_token: UUID
    question: str = field(repr=False)


@dataclass(frozen=True)
class ActionProposal:
    operation: str
    source: SourceView
    tag: str | None = None


@dataclass(frozen=True)
class AgentOutcome:
    selected: tuple[Evidence, ...] = ()
    proposal: ActionProposal | None = None


class AnswerNotifier(Protocol):
    async def finished(
        self, session: AsyncSession, job: QuestionJob, message: Message, parts: tuple[str, ...]
    ) -> None: ...
