"""Persist verified customer messages using M1 tenant transactions, plus a reply outbox."""

import json
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.connectors.wecom.contracts import NormalizedMessage, WeComError
from app.connectors.wecom.outbox import enqueue_reply
from app.connectors.wecom.persistence import WeComOutbox, WeComSyncState
from app.connectors.wecom.retry import retry_delay
from app.db.models import Conversation, Message, User
from app.db.session import corporate_session as connector_session
from app.db.session import tenant_session
from app.domain.enums import MessageRole


def user_identity(corp_id: str, external_userid: str) -> UUID:
    """Stable ID from an authenticated identity, never an authorization mechanism."""
    return uuid5(NAMESPACE_URL, json.dumps(["wecom", corp_id, external_userid]))


class MessageAdmission(Protocol):
    async def admit(
        self, session: AsyncSession, user_id: UUID, message_id: UUID, message: NormalizedMessage
    ) -> bool: ...


class WeComStore:
    def __init__(
        self,
        tenant_factory: async_sessionmaker[AsyncSession],
        connector_factory: async_sessionmaker[AsyncSession],
        corp_id: str,
        admission: MessageAdmission | None = None,
    ) -> None:
        self.tenant_factory = tenant_factory
        self.connector_factory = connector_factory
        self.corp_id = corp_id
        self.admission = admission

    async def persist_message(self, message: NormalizedMessage, reply_text: str | None) -> bool:
        """Call only after allowlist admission; a committed duplicate creates no new reply."""
        user_id = user_identity(self.corp_id, message.external_userid)
        conversation_id = uuid5(user_id, "wecom-kf:" + message.open_kfid)
        message_id = uuid4()
        async with tenant_session(self.tenant_factory, user_id) as session:
            await session.execute(
                insert(User)
                .values(
                    id=user_id,
                    wecom_corp_id=self.corp_id,
                    wecom_external_user_id=message.external_userid,
                )
                .on_conflict_do_nothing(index_elements=[User.id])
            )
            user = await session.get(User, user_id)
            if user is None or user.wecom_external_user_id != message.external_userid:
                raise WeComError("identity_conflict")
            if not user.is_active:
                return False
            await session.execute(
                insert(Conversation)
                .values(
                    id=conversation_id,
                    user_id=user_id,
                    open_kfid=message.open_kfid,
                )
                .on_conflict_do_nothing(index_elements=[Conversation.id])
            )
            inserted = await session.scalar(
                insert(Message)
                .values(
                    id=message_id,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    wechat_msg_id=message.msgid,
                    role=MessageRole.USER,
                    message_type=message.message_type,
                    content=message.text,
                    metadata_={
                        **message.metadata,
                        "sent_at": message.sent_at.isoformat(),
                        "normalization_version": 1,
                    },
                )
                .on_conflict_do_nothing(index_elements=[Message.user_id, Message.wechat_msg_id])
                .returning(Message.id)
            )
            if inserted is None:
                return False
            handled = self.admission is not None and await self.admission.admit(
                session, user_id, message_id, message
            )
            if not handled and reply_text is not None:
                await enqueue_reply(
                    session,
                    corp_id=self.corp_id,
                    user_id=user_id,
                    message_id=message_id,
                    open_kfid=message.open_kfid,
                    external_userid=message.external_userid,
                    received_at=message.sent_at,
                    content=reply_text,
                )
        return True

    async def lock_sync_state(self, session: AsyncSession, open_kfid: str) -> WeComSyncState:
        await session.execute(
            insert(WeComSyncState)
            .values(corp_id=self.corp_id, open_kfid=open_kfid)
            .on_conflict_do_nothing()
        )
        return (
            await session.scalars(
                select(WeComSyncState)
                .where(
                    WeComSyncState.corp_id == self.corp_id, WeComSyncState.open_kfid == open_kfid
                )
                .with_for_update()
            )
        ).one()

    async def record_sync_failure(
        self, open_kfid: str, error: WeComError, *, max_attempts: int, retry_base: float
    ) -> None:
        async with connector_session(self.connector_factory, self.corp_id) as session:
            state = await self.lock_sync_state(session, open_kfid)
            state.attempts += 1
            state.error_message = error.code[:128]
            state.status = (
                "retry" if error.retryable and state.attempts < max_attempts else "failed"
            )
            state.next_attempt_at = datetime.now(UTC) + timedelta(
                seconds=retry_delay(retry_base, state.attempts)
            )

    async def retry_account(self, open_kfid: str) -> None:
        async with connector_session(self.connector_factory, self.corp_id) as session:
            state = await self.lock_sync_state(session, open_kfid)
            state.status, state.attempts, state.next_attempt_at = "ready", 0, None
            # Keep the last error until a successful retry for operator diagnosis.

    async def retry_reply(self, reply_id: UUID) -> bool:
        async with connector_session(self.connector_factory, self.corp_id) as session:
            result = await session.execute(
                update(WeComOutbox)
                .where(
                    WeComOutbox.id == reply_id,
                    WeComOutbox.corp_id == self.corp_id,
                    WeComOutbox.status.in_(["failed", "deferred", "uncertain"]),
                )
                .values(status="queued", attempts=0, next_attempt_at=None)
                .returning(WeComOutbox.id)
            )
            return result.scalar_one_or_none() is not None
