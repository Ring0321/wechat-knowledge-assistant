"""Outbox sender: bounded retries, fixed message IDs, explicit ambiguous-delivery state."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, or_, select, text

from app.connectors.wecom.agent import guard_agent_reply, reply_identity
from app.connectors.wecom.contracts import APIError, WeComAPI, WeComError
from app.connectors.wecom.persistence import WeComOutbox
from app.connectors.wecom.retry import retry_delay
from app.connectors.wecom.store import WeComStore
from app.core.config import Settings
from app.db.session import corporate_session as connector_session
from app.domain.artifacts import ArtifactError

DEFERRED_CODES = frozenset(
    {"wecom_95001", "wecom_95002", "wecom_95013", "wecom_95018", "wecom_95031"}
)


class ReplyService:
    def __init__(self, api: WeComAPI, store: WeComStore, settings: Settings) -> None:
        self.api, self.store, self.settings = api, store, settings

    async def dispatch_one(self) -> bool:
        now = datetime.now(UTC)
        async with connector_session(self.store.connector_factory, self.store.corp_id) as session:
            item = (
                await session.scalars(
                    select(WeComOutbox)
                    .where(
                        WeComOutbox.corp_id == self.store.corp_id,
                        WeComOutbox.open_kfid.in_(self.settings.wecom_open_kfids),
                        WeComOutbox.status == "queued",
                        or_(
                            WeComOutbox.next_attempt_at.is_(None),
                            WeComOutbox.next_attempt_at <= now,
                        ),
                    )
                    .order_by(WeComOutbox.created_at, WeComOutbox.id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
            ).one_or_none()
            if item is None:
                return False
            if item.purpose.startswith("agent:"):
                try:
                    job_id, generation, index = reply_identity(item.purpose)
                except ArtifactError:
                    item.status, item.error_message = "failed", "agent_reply_invalid"
                    return True
                if index:
                    previous = await session.scalar(
                        select(WeComOutbox.status).where(
                            WeComOutbox.user_id == item.user_id,
                            WeComOutbox.inbound_message_id == item.inbound_message_id,
                            WeComOutbox.purpose == f"agent:{job_id}:{generation}:{index - 1}",
                        )
                    )
                    if previous in ("queued", "uncertain"):
                        item.next_attempt_at = now + timedelta(seconds=5)
                        return True
                    if previous != "sent":
                        item.status, item.error_message = (
                            "failed",
                            "agent_previous_reply_unavailable",
                        )
                        return True
            if item.purpose != "ack":
                acknowledgment = await session.scalar(
                    select(WeComOutbox.status).where(
                        WeComOutbox.user_id == item.user_id,
                        WeComOutbox.inbound_message_id == item.inbound_message_id,
                        WeComOutbox.purpose == "ack",
                    )
                )
                if acknowledgment in ("queued", "uncertain"):
                    # Do not overtake the receipt while it is retrying or unresolved.
                    item.next_attempt_at = now + timedelta(seconds=5)
                    return True
            # Serialize quota accounting for one customer across concurrent workers.
            lock_key = f"{item.corp_id}:{item.open_kfid}:{item.user_id}"
            acquired = await session.scalar(
                text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": lock_key},
            )
            if not acquired:
                return False
            if not self.settings.allows_wecom_user(item.external_userid):
                item.status, item.error_message = "failed", "allowlist_revoked"
                return True
            recipient = (
                WeComOutbox.corp_id == item.corp_id,
                WeComOutbox.open_kfid == item.open_kfid,
                WeComOutbox.user_id == item.user_id,
            )
            latest_received = await session.scalar(
                select(func.max(WeComOutbox.received_at)).where(*recipient)
            )
            assert latest_received is not None
            if latest_received < now - timedelta(hours=48) or latest_received > now + timedelta(
                minutes=5
            ):
                item.status, item.error_message = "deferred", "reply_window_closed"
                return True
            count = await session.scalar(
                select(func.count())
                .select_from(WeComOutbox)
                .where(
                    *recipient,
                    WeComOutbox.status.in_(["sent", "uncertain"]),
                    WeComOutbox.last_attempt_at >= latest_received,
                )
            )
            if count is not None and count >= 5:
                item.status, item.error_message = "deferred", "reply_quota_exhausted"
                return True
            item.attempts += 1
            item.last_attempt_at = now
            try:
                if item.purpose.startswith("agent:"):
                    try:
                        async with guard_agent_reply(self.store, self.settings, item):
                            remote_id = await self._send(item)
                    except ArtifactError:
                        item.status, item.error_message = (
                            "failed",
                            "agent_reply_no_longer_authorized",
                        )
                        return True
                else:
                    remote_id = await self._send(item)
                if remote_id != item.reply_msgid:
                    raise APIError("reply_id_mismatch", uncertain=True)
                item.status, item.accepted_at, item.error_message = "sent", now, None
                item.next_attempt_at = None
            except WeComError as error:
                item.error_message = error.code[:128]
                if isinstance(error, APIError) and (error.uncertain or error.code == "wecom_95033"):
                    # Duplicate only proves the ID was seen; it does not prove delivery.
                    item.status = "uncertain"
                elif error.code in DEFERRED_CODES:
                    item.status = "deferred"
                elif error.retryable and item.attempts < self.settings.wecom_max_attempts:
                    item.next_attempt_at = now + timedelta(
                        seconds=retry_delay(self.settings.wecom_retry_base_seconds, item.attempts)
                    )
                else:
                    item.status = "failed"
            return True

    async def _send(self, item: WeComOutbox) -> str:
        return await self.api.send_text(
            open_kfid=item.open_kfid,
            external_userid=item.external_userid,
            content=item.content,
            msgid=item.reply_msgid,
        )
