"""Message worker orchestration; the API process never calls this path."""

import asyncio
import logging
from time import monotonic, time

from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from app.connectors.wecom.contracts import WeComError
from app.connectors.wecom.queue import RedisNotificationQueue
from app.connectors.wecom.replies import ReplyService
from app.connectors.wecom.sync import MessageSyncService
from app.core.config import Settings

logger = logging.getLogger(__name__)


class WeComWorker:
    def __init__(
        self,
        queue: RedisNotificationQueue,
        sync: MessageSyncService,
        replies: ReplyService,
        settings: Settings,
    ) -> None:
        self.queue, self.sync, self.replies, self.settings = queue, sync, replies, settings
        self.next_poll = 0.0

    async def tick(self) -> None:
        entry = await self.queue.receive()
        if entry is not None:
            entry_id, notification = entry
            if (
                notification.corp_id == self.settings.wecom_corp_id
                and notification.open_kfid in self.settings.wecom_open_kfids
            ):
                token = notification.token if 0 <= time() - notification.created_at < 540 else None
                try:
                    await self.sync.sync_account(notification.open_kfid, token)
                except WeComError:
                    # Sync error and retry schedule are already durable in PostgreSQL.
                    logger.warning("wecom_sync_failed")
            await self.queue.acknowledge(entry_id)
        if monotonic() >= self.next_poll:
            for open_kfid in sorted(self.settings.wecom_open_kfids):
                try:
                    await self.sync.sync_account(open_kfid)
                except WeComError:
                    logger.warning("wecom_sync_failed")
            self.next_poll = monotonic() + self.settings.wecom_poll_interval_seconds
        for _ in range(10):
            if not await self.replies.dispatch_one():
                break

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.tick()
            except (RedisError, SQLAlchemyError, WeComError):
                logger.warning("wecom_worker_failed")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=1)
                except TimeoutError:
                    pass
