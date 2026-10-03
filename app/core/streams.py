"""Shared at-least-once Redis Stream transport with same-connection AOF confirmation."""

import hashlib
from typing import cast
from uuid import uuid4

from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError


class StreamError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


ENQUEUE = """
local existing = redis.call('GET', KEYS[2])
if existing then
    redis.call('SET', KEYS[2], existing, 'KEEPTTL')
    return existing
end
if redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[2]) then
    return redis.error_reply('QUEUE_FULL')
end
local id = redis.call('XADD', KEYS[1], '*', unpack(ARGV, 3))
redis.call('SET', KEYS[2], id, 'EX', ARGV[1])
return id
"""


class DurableStream:
    def __init__(
        self,
        redis: Redis,
        stream: str,
        group: str,
        *,
        durability_ms: int = 1500,
        reclaim_ms: int = 60000,
    ) -> None:
        self.redis, self.stream, self.group = redis, stream, group
        self.consumer = uuid4().hex
        self.durability_ms, self.reclaim_ms = durability_ms, reclaim_ms

    async def publish(self, values: dict[str, str], *, dedup: str, ttl: int) -> str:
        digest = hashlib.sha256(dedup.encode()).hexdigest()
        fields = [value for pair in values.items() for value in pair]
        try:
            async with self.redis.pipeline(transaction=False) as pipe:
                pipe.eval(
                    ENQUEUE,
                    2,
                    self.stream,
                    self.stream + ":dedup:" + digest,
                    str(ttl),
                    "100000",
                    *fields,
                )
                pipe.execute_command("WAITAOF", 1, 0, self.durability_ms)
                results = await pipe.execute()
            if int(results[1][0]) != 1:
                raise StreamError("queue_not_durable")
            return str(results[0])
        except RedisError:
            raise StreamError("queue_unavailable") from None

    async def ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise StreamError("queue_unavailable") from None

    async def read_entry(self) -> tuple[str, dict[str, str]] | None:
        await self.ensure_group()
        claimed = await self.redis.xautoclaim(
            self.stream,
            self.group,
            self.consumer,
            min_idle_time=self.reclaim_ms,
            start_id="0-0",
            count=1,
        )
        entries = cast(list[tuple[str, dict[str, str]]], claimed[1])
        if not entries:
            result = await self.redis.xreadgroup(
                self.group, self.consumer, {self.stream: ">"}, count=1, block=1000
            )
            if not result:
                return None
            entries = result[0][1]
        return entries[0]

    async def acknowledge(self, entry_id: str) -> None:
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.xack(self.stream, self.group, entry_id)
            pipe.xdel(self.stream, entry_id)
            await pipe.execute()
