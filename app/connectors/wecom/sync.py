"""Cursor processing, durable message idempotency and event handling outside the callback."""

from datetime import UTC, datetime, timedelta

from pydantic import JsonValue
from sqlalchemy import update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.wecom.contracts import WeComAPI, WeComError
from app.connectors.wecom.normalization import is_setup_challenge_text, normalize_message
from app.connectors.wecom.persistence import WeComOutbox
from app.connectors.wecom.store import WeComStore
from app.core.config import Settings
from app.db.session import corporate_session as connector_session


class MessageSyncService:
    def __init__(self, api: WeComAPI, store: WeComStore, settings: Settings) -> None:
        self.api, self.store, self.settings = api, store, settings

    async def sync_account(self, open_kfid: str, token: str | None = None) -> None:
        if open_kfid not in self.settings.wecom_open_kfids:
            raise WeComError("unknown_customer_account")
        try:
            for _ in range(self.settings.wecom_max_pages_per_sync):
                more = await self._page(open_kfid, token)
                if not more:
                    break
        except WeComError as error:
            await self.store.record_sync_failure(
                open_kfid,
                error,
                max_attempts=self.settings.wecom_max_attempts,
                retry_base=self.settings.wecom_retry_base_seconds,
            )
            raise
        except SQLAlchemyError:
            # Do not commit a new cursor when a tenant transaction fails.
            database_error = WeComError("database_sync_failure", retryable=True)
            await self.store.record_sync_failure(
                open_kfid,
                database_error,
                max_attempts=self.settings.wecom_max_attempts,
                retry_base=self.settings.wecom_retry_base_seconds,
            )
            raise database_error from None

    async def _page(self, open_kfid: str, token: str | None) -> bool:
        async with connector_session(self.store.connector_factory, self.store.corp_id) as session:
            # The database row lock serializes page fetches, even across multiple workers.
            state = await self.store.lock_sync_state(session, open_kfid)
            now = datetime.now(UTC)
            if state.status == "failed" or (
                state.next_attempt_at
                and state.next_attempt_at > now
                and (token is None or state.status == "retry")
            ):
                return False
            page = await self.api.sync_msg(open_kfid=open_kfid, cursor=state.cursor, token=token)
            if page.has_more and page.next_cursor == state.cursor:
                raise WeComError("cursor_did_not_advance")
            for raw in page.messages:
                if raw.get("origin") == 4:
                    await self._event(session, raw, open_kfid)
                    continue
                external_userid = raw.get("external_userid")
                if raw.get("origin") != 3 or not isinstance(external_userid, str):
                    continue
                if not self.settings.allows_wecom_user(external_userid):
                    continue
                normalized = normalize_message(raw, open_kfid)
                if normalized is not None:
                    if normalized.message_type == "text" and is_setup_challenge_text(
                        normalized.text or ""
                    ):
                        continue
                    await self.store.persist_message(
                        normalized,
                        self.settings.wecom_reply_text if self.settings.wecom_auto_reply else None,
                    )
            # Every admitted message and its outbox are already committed. Replay is safe.
            state.cursor = page.next_cursor
            state.status, state.attempts, state.error_message = "ready", 0, None
            state.last_synced_at = now
            state.next_attempt_at = (
                None
                if page.has_more
                else now + timedelta(seconds=self.settings.wecom_poll_interval_seconds)
            )
            return page.has_more

    async def _event(
        self, session: AsyncSession, raw: dict[str, JsonValue], open_kfid: str
    ) -> None:
        event = raw.get("event")
        if not isinstance(event, dict) or event.get("event_type") != "msg_send_fail":
            return
        if event.get("open_kfid") != open_kfid:
            raise WeComError("event_account_mismatch")
        failed_id, customer, fail_type = (
            event.get("fail_msgid"),
            event.get("external_userid"),
            event.get("fail_type"),
        )
        if not isinstance(failed_id, str) or not isinstance(customer, str):
            raise WeComError("invalid_failure_event")
        if not isinstance(fail_type, int) or fail_type not in {0, 1, 2, 4, 5, 6, 8, 10, 11, 12, 13}:
            fail_type = 0
        await session.execute(
            update(WeComOutbox)
            .where(
                WeComOutbox.corp_id == self.store.corp_id,
                WeComOutbox.open_kfid == open_kfid,
                WeComOutbox.reply_msgid == failed_id,
                WeComOutbox.external_userid == customer,
            )
            .values(
                status="failed", error_message=f"delivery_failure_{fail_type}", next_attempt_at=None
            )
        )
