"""Credential-scoped distributed access token cache using official gettoken."""

import asyncio
import hashlib
import json
import secrets
import time
from collections.abc import Awaitable
from typing import cast

import httpx
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.connectors.wecom.contracts import APIError

_COMPARE_DELETE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""
_PUBLISH = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3])
redis.call('DEL', KEYS[1])
return 1
"""


class RedisTokenProvider:
    def __init__(
        self,
        redis: Redis,
        http: httpx.AsyncClient,
        corp_id: str,
        secret: str,
        namespace: str = "pkb:wecom",
    ) -> None:
        if not corp_id or not secret or not namespace:
            raise APIError("wecom_token_invalid_configuration")
        self._redis = redis
        self._http = http
        self._corp_id = corp_id
        self._secret = secret
        scope = hashlib.sha256((corp_id + "\0" + secret).encode()).hexdigest()
        # The hash tag keeps both keys on one Redis Cluster slot for the Lua operations.
        prefix = f"{namespace}:token:{{{scope}}}"
        self._cache_key = prefix + ":value"
        self._lock_key = prefix + ":lock"

    async def _eval(self, script: str, count: int, *args: str) -> int:
        # redis-py shares ScriptCommands annotations with its synchronous implementation.
        return await cast(Awaitable[int], self._redis.eval(script, count, *args))

    async def _cached(self) -> str | None:
        cached: object = await self._redis.get(self._cache_key)
        if cached is None:
            return None
        try:
            value = cached.decode("ascii") if isinstance(cached, bytes) else cached
        except UnicodeError:
            raise APIError("wecom_token_invalid_cache") from None
        if not isinstance(value, str) or not value or len(value) > 4096:
            raise APIError("wecom_token_invalid_cache")
        return value

    async def _fetch(self) -> tuple[str, int]:
        try:
            async with asyncio.timeout(6):
                async with self._http.stream(
                    "GET",
                    "https://qyapi.weixin.qq.com/cgi-bin/gettoken",
                    params={"corpid": self._corp_id, "corpsecret": self._secret},
                    timeout=httpx.Timeout(5, connect=3),
                    follow_redirects=False,
                ) as response:
                    if response.status_code != 200:
                        raise APIError("wecom_token_http_error", retryable=True)
                    content = bytearray()
                    async for chunk in response.aiter_bytes():
                        content.extend(chunk)
                        if len(content) > 65536:
                            raise APIError("wecom_token_invalid_response")
        except (httpx.HTTPError, TimeoutError):
            raise APIError("wecom_token_transport_failed", retryable=True) from None
        try:
            raw: object = json.loads(content)
        except (ValueError, UnicodeError):
            raise APIError("wecom_token_invalid_response") from None
        if not isinstance(raw, dict) or type(raw.get("errcode")) is not int:
            raise APIError("wecom_token_invalid_response")
        code = raw["errcode"]
        if code:
            raise APIError(f"wecom_{code}", retryable=code in (-1, 45009))
        value, ttl = raw.get("access_token"), raw.get("expires_in")
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 4096
            or not value.isascii()
            or type(ttl) is not int
            or ttl < 2
            or ttl > 86400
        ):
            raise APIError("wecom_token_invalid_response")
        return value, ttl - min(300, max(1, ttl // 10))

    async def get_token(self) -> str:
        lease = secrets.token_hex(16)
        acquired = False
        try:
            async with asyncio.timeout(12):
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    cached = await self._cached()
                    if cached is not None:
                        return cached
                    acquired = bool(await self._redis.set(self._lock_key, lease, nx=True, px=10000))
                    if acquired:
                        # A refresher may have published between GET and lock acquisition.
                        cached = await self._cached()
                        if cached is not None:
                            return cached
                        value, ttl = await self._fetch()
                        published = await self._eval(
                            _PUBLISH, 2, self._lock_key, self._cache_key, lease, value, str(ttl)
                        )
                        if not published:
                            raise APIError("wecom_token_refresh_lease_lost", retryable=True)
                        return value
                    await asyncio.sleep(0.05)
                raise APIError("wecom_token_refresh_busy", retryable=True)
        except (RedisError, TimeoutError):
            raise APIError("wecom_token_cache_unavailable", retryable=True) from None
        finally:
            if acquired:
                try:
                    async with asyncio.timeout(1):
                        await self._eval(_COMPARE_DELETE, 1, self._lock_key, lease)
                except (RedisError, TimeoutError):
                    # The lock has a short expiry; cleanup cannot expose raw Redis failures.
                    pass

    async def invalidate(self, token: str) -> None:
        try:
            async with asyncio.timeout(2):
                await self._eval(_COMPARE_DELETE, 1, self._cache_key, token)
        except (RedisError, TimeoutError):
            raise APIError("wecom_token_cache_unavailable", retryable=True) from None
