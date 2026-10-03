"""Authenticated message admission and guarded delivery for background questions."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.models import AgentAction, QuestionDispatch, QuestionJob
from app.agent.security import lock_identity, lock_sources
from app.connectors.wecom.contracts import NormalizedMessage
from app.connectors.wecom.ingestion import WeComIngestionBridge
from app.connectors.wecom.outbox import enqueue_reply
from app.connectors.wecom.persistence import WeComOutbox
from app.connectors.wecom.store import WeComStore
from app.core.config import Settings
from app.db.models import Conversation, Message, User
from app.db.session import tenant_session
from app.domain.artifacts import ArtifactError
from app.domain.knowledge import TenantContext


class WeComQuestionBridge:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.ingestion = WeComIngestionBridge(settings)

    async def admit(
        self, session: AsyncSession, user_id: UUID, message_id: UUID, message: NormalizedMessage
    ) -> bool:
        if self.settings.ingestion_enabled and await self.ingestion.admit(
            session, user_id, message_id, message
        ):
            return True
        if (
            not self.settings.agent_enabled
            or message.message_type != "text"
            or not message.text
            or not message.text.strip()
        ):
            return False
        if len(message.text.encode()) > 16384:
            if not self.settings.wecom_auto_reply:
                return True
            await enqueue_reply(
                session,
                corp_id=self.settings.wecom_corp_id,
                user_id=user_id,
                message_id=message_id,
                open_kfid=message.open_kfid,
                external_userid=message.external_userid,
                received_at=message.sent_at,
                content="问题过长，请缩短后重试。",
            )
            return True
        job = QuestionJob(
            id=uuid4(),
            user_id=user_id,
            message_id=message_id,
            max_attempts=self.settings.agent_max_attempts,
        )
        session.add(job)
        await session.flush()
        session.add(
            QuestionDispatch(
                id=uuid4(), corp_id=self.settings.wecom_corp_id, user_id=user_id, job_id=job.id
            )
        )
        return True

    async def finished(
        self, session: AsyncSession, job: QuestionJob, message: Message, parts: tuple[str, ...]
    ) -> None:
        if not self.settings.wecom_auto_reply:
            return
        user = await session.get(User, job.user_id)
        conversation = await session.get(Conversation, message.conversation_id)
        if user is None or conversation is None:
            raise ArtifactError("agent_notification_identity_missing")
        sent_at = message.metadata_.get("sent_at")
        received_at = (
            datetime.fromisoformat(sent_at) if isinstance(sent_at, str) else message.created_at
        )
        for index, part in enumerate(parts):
            await enqueue_reply(
                session,
                corp_id=user.wecom_corp_id,
                user_id=user.id,
                message_id=message.id,
                open_kfid=conversation.open_kfid,
                external_userid=user.wecom_external_user_id,
                received_at=received_at,
                content=part,
                purpose=f"agent:{job.id}:{job.reply_generation}:{index}",
            )


def reply_identity(purpose: str) -> tuple[UUID, int, int]:
    try:
        prefix, job, generation, part = purpose.split(":")
        result = UUID(job), int(generation), int(part)
        if (
            prefix != "agent"
            or str(result[0]) != job
            or str(result[1]) != generation
            or str(result[2]) != part
            or result[1] < 0
            or not 0 <= result[2] < 4
        ):
            raise ValueError
        return result
    except (ValueError, TypeError):
        raise ArtifactError("agent_reply_invalid") from None


@asynccontextmanager
async def guard_agent_reply(
    store: WeComStore, settings: Settings, item: WeComOutbox
) -> AsyncIterator[None]:
    job_id, generation, index = reply_identity(item.purpose)
    async with tenant_session(store.tenant_factory, item.user_id) as session:
        job = await session.scalar(
            select(QuestionJob).where(QuestionJob.id == job_id).with_for_update(read=True)
        )
        await lock_identity(session, TenantContext(item.user_id), settings)
        if (
            job is None
            or job.status not in ("completed", "failed")
            or job.message_id != item.inbound_message_id
            or job.reply_generation != generation
            or index >= len(job.result_parts)
            or job.result_parts[index] != item.content
        ):
            raise ArtifactError("agent_reply_obsolete")
        action = await session.scalar(
            select(AgentAction)
            .where(AgentAction.question_job_id == job_id)
            .with_for_update(read=True)
        )
        if action is not None and (
            action.status != "pending" or action.expires_at <= datetime.now(UTC)
        ):
            raise ArtifactError("agent_confirmation_expired")
        snapshots: dict[str, str] = {}
        for key, value in job.source_snapshots.items():
            if not isinstance(value, str):
                raise ArtifactError("agent_evidence_invalid")
            snapshots[key] = value
        await lock_sources(session, snapshots)
        yield
