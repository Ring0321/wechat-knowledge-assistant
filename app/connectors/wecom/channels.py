"""Explicit, tenant-bound admission of a user-supplied Channels original video."""

import re
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.wecom.contracts import NormalizedMessage, WeComError
from app.connectors.wecom.outbox import enqueue_reply
from app.core.config import Settings
from app.db.models import Conversation, IngestionJob, Message, Source, User
from app.domain.enums import JobStatus, MessageRole
from app.ingestion.channels import SUPPLEMENT as SUPPLEMENT_KIND
from app.ingestion.channels import can_supplement
from app.ingestion.models import IngestionDispatch, MessageSource

INTENT_LIFETIME = timedelta(minutes=10)
FUTURE_SKEW = timedelta(seconds=60)
_COMMAND = re.compile(
    r"补充视频 ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
)
_RETRY_MESSAGE = "该来源已有视频补充任务，不能重复关联；失败任务可由管理员重试。"
_UNAVAILABLE_MESSAGE = "该来源不存在或暂不可补充；仅支持尚未补充、未建立索引的视频号卡片。"


def _timestamp(value: JsonValue | None) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone(UTC) if parsed.tzinfo is not None else None
    except (ValueError, OverflowError):
        return None


def _fresh(sent_at: datetime, now: datetime) -> bool:
    return sent_at.tzinfo is not None and now - INTENT_LIFETIME <= sent_at <= now + FUTURE_SKEW


def _intent(message: Message) -> dict[str, JsonValue]:
    data = message.metadata_.get(SUPPLEMENT_KIND)
    return data if isinstance(data, dict) else {}


def _finish_intent(message: Message, state: str, now: datetime) -> None:
    message.metadata_ = {
        **message.metadata_,
        SUPPLEMENT_KIND: {**_intent(message), "state": state, "resolved_at": now.isoformat()},
    }


def _event_time(sent_at: datetime) -> str:
    return sent_at.astimezone(UTC).isoformat(timespec="microseconds")


class WeComChannelSupplementBridge:
    """Call only within the transaction that persisted the authenticated inbound message."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def admit(
        self, session: AsyncSession, user_id: UUID, message_id: UUID, message: NormalizedMessage
    ) -> bool:
        text = message.text or ""
        command = message.message_type == "text" and text.startswith("补充视频")
        cancel = message.message_type == "text" and text == "取消补充视频"
        if not command and not cancel and message.message_type != "video":
            return False

        inbound, conversation = await self._identity(session, user_id, message_id, message)
        now = datetime.now(UTC)
        if command or cancel:
            # The transport already deduplicates official msgid; retain this local guard too.
            if SUPPLEMENT_KIND in inbound.metadata_:
                return True
            if not _fresh(message.sent_at, now):
                await self._reply(
                    session, inbound, message, "补充指令已过期或时间异常，请重新发送。"
                )
                return True
            match = _COMMAND.fullmatch(text)
            if command and match is None:
                await self._reply(
                    session, inbound, message, "请发送：补充视频 <来源ID>，或发送：取消补充视频。"
                )
                return True
            watermark = await session.scalar(
                select(
                    func.max(Message.metadata_[SUPPLEMENT_KIND]["event_sent_at"].as_string())
                ).where(
                    Message.user_id == user_id,
                    Message.conversation_id == conversation.id,
                    Message.metadata_.has_key(SUPPLEMENT_KIND),
                )
            )
            if watermark is not None:
                previous_sent = _timestamp(watermark)
                if previous_sent is None or message.sent_at < previous_sent:
                    await self._reply(
                        session,
                        inbound,
                        message,
                        "收到较早发送的指令，未改变当前补充请求，请重新发送。",
                    )
                    return True
            pending = await self._pending(session, user_id, conversation.id)
            if cancel:
                if pending is not None:
                    _finish_intent(pending, "cancelled", now)
                inbound.metadata_ = {
                    **inbound.metadata_,
                    SUPPLEMENT_KIND: {
                        "state": "cancelled",
                        "requested_at": now.isoformat(),
                        "event_sent_at": _event_time(message.sent_at),
                    },
                }
                await self._reply(
                    session, inbound, message, "已取消补充视频。后续普通视频将单独收录。"
                )
                return True
            assert match is not None
            source_id = UUID(match[1])
            source, existing, _ = await self._target(session, user_id, source_id)
            now = datetime.now(UTC)
            if not _fresh(message.sent_at, now):
                await self._reply(session, inbound, message, "补充指令已过期，请重新发送。")
                return True
            if existing or source is None:
                await self._reply(
                    session, inbound, message, _RETRY_MESSAGE if existing else _UNAVAILABLE_MESSAGE
                )
                return True
            if pending is not None:
                _finish_intent(pending, "replaced", now)
            inbound.metadata_ = {
                **inbound.metadata_,
                SUPPLEMENT_KIND: {
                    "source_id": str(source.id),
                    "state": "pending",
                    "event_sent_at": _event_time(message.sent_at),
                    "requested_at": min(now, message.sent_at).isoformat(),
                    "expires_at": (min(now, message.sent_at) + INTENT_LIFETIME).isoformat(),
                },
            }
            await self._reply(
                session,
                inbound,
                message,
                f"请在指令发送后10分钟内向本客服会话发送原视频，补充到：{source.title[:120]}。"
                "仅下一条普通视频会关联；请确保有权提供该视频。取消请发送：取消补充视频。",
            )
            return True

        replay = await session.scalar(
            select(IngestionJob.id).where(
                IngestionJob.user_id == user_id,
                IngestionJob.message_id == message_id,
                IngestionJob.input_data["kind"].as_string() == SUPPLEMENT_KIND,
            )
        )
        if replay is not None:
            return True
        pending = await self._pending(session, user_id, conversation.id)
        if pending is None:
            return False
        return await self._consume(session, inbound, message, pending, now)

    async def _identity(
        self, session: AsyncSession, user_id: UUID, message_id: UUID, message: NormalizedMessage
    ) -> tuple[Message, Conversation]:
        user = await session.scalar(
            select(User).where(User.id == user_id).with_for_update(read=True)
        )
        inbound = await session.get(Message, message_id)
        if (
            user is None
            or not user.is_active
            or user.wecom_corp_id != self.settings.wecom_corp_id
            or user.wecom_external_user_id != message.external_userid
            or message.external_userid not in self.settings.wecom_allowed_user_ids
            or message.open_kfid not in self.settings.wecom_open_kfids
            or inbound is None
            or inbound.user_id != user_id
            or inbound.role != MessageRole.USER
            or inbound.wechat_msg_id != message.msgid
            or inbound.message_type != message.message_type
            or inbound.content != message.text
            or _timestamp(inbound.metadata_.get("sent_at")) != message.sent_at
            or (
                message.message_type == "video"
                and inbound.metadata_.get("media_id") != message.metadata.get("media_id")
            )
        ):
            raise WeComError("channel_supplement_identity_rejected")
        conversation = await session.scalar(
            select(Conversation)
            .where(Conversation.id == inbound.conversation_id, Conversation.user_id == user_id)
            # FK checks on the newly inserted messages hold KEY SHARE. NO KEY UPDATE
            # serializes admission without deadlocking concurrent inserts in this conversation.
            .with_for_update(key_share=True)
        )
        if conversation is None or conversation.open_kfid != message.open_kfid:
            raise WeComError("channel_supplement_conversation_rejected")
        return inbound, conversation

    async def _pending(
        self, session: AsyncSession, user_id: UUID, conversation_id: UUID
    ) -> Message | None:
        return (
            await session.scalars(
                select(Message)
                .where(
                    Message.user_id == user_id,
                    Message.conversation_id == conversation_id,
                    Message.role == MessageRole.USER,
                    Message.metadata_[SUPPLEMENT_KIND]["state"].as_string() == "pending",
                )
                .order_by(Message.created_at.desc(), Message.id.desc())
                .limit(1)
                .with_for_update()
            )
        ).first()

    async def _target(
        self, session: AsyncSession, user_id: UUID, source_id: UUID
    ) -> tuple[Source | None, bool, IngestionJob | None]:
        # Use the same job -> source order as claim/fail/retry. Failed jobs can be
        # rebound to a fresh official video after terminal failure, never while leased.
        existing = await session.scalar(
            select(IngestionJob)
            .where(
                IngestionJob.user_id == user_id,
                IngestionJob.operation_key == f"{SUPPLEMENT_KIND}:{source_id}",
            )
            .with_for_update()
        )
        source = await session.scalar(
            select(Source)
            .where(Source.user_id == user_id, Source.id == source_id)
            .with_for_update()
        )
        if source is None:
            return None, False, None
        # A competing conversation may have inserted a job while we waited on Source.
        # Do not lock that newly visible job under the Source lock (inverse lock order).
        if existing is None:
            raced = await session.scalar(
                select(IngestionJob.id).where(
                    IngestionJob.user_id == user_id,
                    IngestionJob.operation_key == f"{SUPPLEMENT_KIND}:{source_id}",
                )
            )
            if raced is not None:
                return None, True, None
        if existing is not None:
            if (
                existing.status != JobStatus.FAILED
                or existing.lease_token is not None
                or existing.next_retry_at is not None
            ):
                return None, True, None
        if not await can_supplement(session, source):
            return None, False, None
        return source, False, existing

    async def _consume(
        self,
        session: AsyncSession,
        inbound: Message,
        message: NormalizedMessage,
        pending: Message,
        now: datetime,
    ) -> bool:
        intent = _intent(pending)
        expires_at = _timestamp(intent.get("expires_at"))
        requested_at = _timestamp(intent.get("requested_at"))
        requested_sent = _timestamp(pending.metadata_.get("sent_at"))
        if (
            expires_at is None
            or requested_at is None
            or requested_sent is None
            or expires_at != requested_at + INTENT_LIFETIME
            or now >= expires_at
        ):
            _finish_intent(pending, "expired", now)
            await self._reply(
                session,
                inbound,
                message,
                "补充请求已过期，视频尚未关联或收录。请重新发送补充指令及视频。",
            )
            return True
        if (
            not _fresh(message.sent_at, now)
            or message.sent_at < requested_sent
            or message.sent_at >= expires_at
        ):
            await self._reply(
                session,
                inbound,
                message,
                "视频发送时间不在本次补充范围，尚未关联或收录。请在有效期内重新发送视频。",
            )
            return True
        media_id = message.metadata.get("media_id")
        if (
            not isinstance(media_id, str)
            or not media_id.strip()
            or len(media_id) > 1024
            or "\x00" in media_id
        ):
            await self._reply(
                session,
                inbound,
                message,
                "未取得官方视频素材编号，尚未关联或收录，请重新发送视频。",
            )
            return True
        target = intent.get("source_id")
        try:
            source_id = UUID(target) if isinstance(target, str) else None
        except ValueError:
            source_id = None
        source, existing, failed_job = (
            await self._target(session, inbound.user_id, source_id)
            if source_id is not None
            else (None, False, None)
        )
        if source is None:
            _finish_intent(pending, "rejected", now)
            await self._reply(
                session,
                inbound,
                message,
                (_RETRY_MESSAGE if existing else _UNAVAILABLE_MESSAGE) + "本条视频尚未关联或收录。",
            )
            return True
        now = datetime.now(UTC)
        if now >= expires_at:
            _finish_intent(pending, "expired", now)
            await self._reply(
                session,
                inbound,
                message,
                "补充请求已过期，视频尚未关联或收录。请重新发送补充指令及视频。",
            )
            return True
        input_data: dict[str, JsonValue] = {
            "kind": SUPPLEMENT_KIND,
            "media_id": media_id,
            "filename": None,
            "request_message_id": str(pending.id),
        }
        if failed_job is not None:
            # The original inbound message retains its media_id. Preserve the failed
            # generation's safe diagnostic there before resetting this unique job.
            previous = (
                await session.get(Message, failed_job.message_id)
                if failed_job.message_id is not None
                else None
            )
            if previous is not None:
                previous.metadata_ = {
                    **previous.metadata_,
                    "supplement_failure": {
                        "job_id": str(failed_job.id),
                        "error_message": failed_job.error_message,
                        "attempts": failed_job.attempts,
                        "replaced_at": now.isoformat(),
                    },
                }
            failed_job.message_id = inbound.id
            failed_job.input_data = input_data
            failed_job.status, failed_job.attempts = JobStatus.QUEUED, 0
            failed_job.error_message = failed_job.next_retry_at = failed_job.completed_at = None
            failed_job.started_at = failed_job.lease_token = failed_job.lease_expires_at = None
            dispatch = await session.scalar(
                select(IngestionDispatch).where(
                    IngestionDispatch.job_id == failed_job.id,
                    IngestionDispatch.user_id == inbound.user_id,
                )
            )
            if dispatch is None:
                raise WeComError("channel_supplement_dispatch_missing")
            dispatch.finished_at = dispatch.published_at = None
            dispatch.available_at = now
        else:
            job_id = uuid4()
            session.add(
                IngestionJob(
                    id=job_id,
                    user_id=inbound.user_id,
                    source_id=source.id,
                    message_id=inbound.id,
                    operation_key=f"{SUPPLEMENT_KIND}:{source.id}",
                    input_data=input_data,
                    max_attempts=self.settings.ingestion_max_attempts,
                )
            )
            await session.flush()
            session.add(
                IngestionDispatch(
                    id=uuid4(),
                    corp_id=self.settings.wecom_corp_id,
                    user_id=inbound.user_id,
                    job_id=job_id,
                )
            )
        await session.flush()
        session.add(
            MessageSource(
                user_id=inbound.user_id,
                message_id=inbound.id,
                source_id=source.id,
                item_index=0,
            )
        )
        _finish_intent(pending, "consumed", now)
        pending.metadata_ = {
            **pending.metadata_,
            SUPPLEMENT_KIND: {
                **_intent(pending),
                "event_sent_at": _event_time(message.sent_at),
            },
        }
        await self._reply(session, inbound, message, f"收到，正在整理：{source.title[:120]}")
        return True

    async def _reply(
        self, session: AsyncSession, inbound: Message, message: NormalizedMessage, content: str
    ) -> None:
        if self.settings.wecom_auto_reply:
            await enqueue_reply(
                session,
                corp_id=self.settings.wecom_corp_id,
                user_id=inbound.user_id,
                message_id=inbound.id,
                open_kfid=message.open_kfid,
                external_userid=message.external_userid,
                received_at=message.sent_at,
                content=content,
                purpose=SUPPLEMENT_KIND,
            )
