import hashlib
from uuid import UUID

from redis.asyncio import Redis

from app.core.streams import DurableStream


class QuestionQueue(DurableStream):
    def __init__(self, redis: Redis, corp_id: str, *, reclaim_ms: int = 150000) -> None:
        self.prefix = "agent:" + hashlib.sha256(corp_id.encode()).hexdigest()[:24]
        super().__init__(redis, self.prefix + ":jobs", "agent-workers", reclaim_ms=reclaim_ms)

    async def enqueue(self, dispatch_id: UUID) -> str:
        return await self.publish({"dispatch_id": str(dispatch_id)}, dedup=str(dispatch_id), ttl=60)
