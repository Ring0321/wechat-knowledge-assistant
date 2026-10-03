"""WeCom notification DTO over the shared durable Redis transport."""

import hashlib
from typing import Protocol

from redis.asyncio import Redis

from app.connectors.wecom.contracts import CallbackNotification, WeComError
from app.core.streams import DurableStream, StreamError


class NotificationQueue(Protocol):
    async def enqueue(self, notification: CallbackNotification) -> str: ...


class RedisNotificationQueue(DurableStream):
    def __init__(self, redis: Redis, corp_id: str, *, durability_ms: int = 1500) -> None:
        self.prefix = "pkb:wecom:" + hashlib.sha256(corp_id.encode()).hexdigest()[:24]
        super().__init__(
            redis, self.prefix + ":notifications", "sync-workers", durability_ms=durability_ms
        )

    async def enqueue(self, notification: CallbackNotification) -> str:
        try:
            return await self.publish(
                {
                    "corp_id": notification.corp_id,
                    "open_kfid": notification.open_kfid,
                    "token": notification.token,
                    "created_at": str(notification.created_at),
                },
                dedup=notification.corp_id
                + "\0"
                + notification.open_kfid
                + "\0"
                + notification.token,
                ttl=600,
            )
        except StreamError as error:
            raise WeComError(error.code, retryable=True) from None

    async def receive(self) -> tuple[str, CallbackNotification] | None:
        try:
            entry = await self.read_entry()
        except StreamError as error:
            raise WeComError(error.code, retryable=True) from None
        if entry is None:
            return None
        entry_id, values = entry
        try:
            notification = CallbackNotification(
                corp_id=values["corp_id"],
                open_kfid=values["open_kfid"],
                token=values["token"],
                created_at=int(values["created_at"]),
            )
        except (KeyError, ValueError):
            raise WeComError("queue_invalid_entry") from None
        return entry_id, notification
