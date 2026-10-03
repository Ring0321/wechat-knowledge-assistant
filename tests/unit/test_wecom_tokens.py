import asyncio
from typing import cast

import httpx
import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from app.connectors.wecom.contracts import APIError
from app.connectors.wecom.tokens import RedisTokenProvider


class Cache:
    """Deterministic Redis command fake; Lua and expiry are covered against real Redis by CI."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.fail = False
        self.replace_lease_on_publish = False

    async def get(self, key: str) -> bytes | None:
        if self.fail:
            raise RedisConnectionError("credential-private-network-detail")
        value = self.values.get(key)
        return value.encode() if value is not None else None

    async def set(self, key: str, value: str, *, nx: bool, px: int) -> bool:
        assert nx is True and px == 10000
        if key in self.values:
            return False
        self.values[key] = value
        return True

    async def eval(self, script: str, count: int, *args: str | int) -> int:
        if count == 2:
            lock, key, lease, value, ttl = args
            assert all(isinstance(arg, str) for arg in (lock, key, lease, value))
            assert isinstance(ttl, str)
            if self.replace_lease_on_publish:
                self.values[str(lock)] = "new-owner-lease"
            if self.values.get(str(lock)) != lease:
                return 0
            self.values[str(key)] = str(value)
            self.ttls[str(key)] = int(ttl)
            self.values.pop(str(lock))
            return 1
        key, expected = args
        if self.values.get(str(key)) == expected:
            self.values.pop(str(key))
            return 1
        return 0


def provider(cache: Cache, client: httpx.AsyncClient, **kwargs: str) -> RedisTokenProvider:
    return RedisTokenProvider(cast(Redis, cache), client, "ww-corp", "corp-secret", **kwargs)


async def test_cache_hit_and_expiry_margin() -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.url.scheme == "https"
        assert request.url.host == "qyapi.weixin.qq.com"
        assert request.url.path == "/cgi-bin/gettoken"
        assert request.url.params["corpid"] == "ww-corp"
        assert request.url.params["corpsecret"] == "corp-secret"
        return httpx.Response(
            200, json={"errcode": 0, "access_token": "fresh-token", "expires_in": 7200}
        )

    cache = Cache()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        tokens = provider(cache, client)
        assert await tokens.get_token() == "fresh-token"
        assert await tokens.get_token() == "fresh-token"
    assert calls == 1
    assert list(cache.ttls.values()) == [6900]
    assert len(cache.values) == 1
    assert all("corp-secret" not in key and "ww-corp" not in key for key in cache.values)


async def test_concurrent_refresh_only_one_gettoken() -> None:
    calls = 0

    async def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return httpx.Response(200, json={"errcode": 0, "access_token": "new", "expires_in": 7200})

    cache = Cache()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        # Separate provider instances imitate different worker processes.
        result = await asyncio.gather(*(provider(cache, client).get_token() for _ in range(20)))
    assert result == ["new"] * 20
    assert calls == 1
    assert len(cache.values) == 1


async def test_stale_invalidation_preserves_newer_token() -> None:
    cache = Cache()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"errcode": 0, "access_token": "new", "expires_in": 7200}
            )
        )
    ) as client:
        tokens = provider(cache, client)
        assert await tokens.get_token() == "new"
        await tokens.invalidate("old")
        assert await tokens.get_token() == "new"
        await tokens.invalidate("new")
        assert not cache.values


async def test_credential_rotation_uses_separate_cache_scope() -> None:
    cache = Cache()

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "errcode": 0,
                "access_token": request.url.params["corpsecret"],
                "expires_in": 7200,
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        old = RedisTokenProvider(cast(Redis, cache), client, "ww-corp", "secret-old")
        new = RedisTokenProvider(cast(Redis, cache), client, "ww-corp", "secret-new")
        assert await old.get_token() == "secret-old"
        assert await new.get_token() == "secret-new"
    assert len(cache.values) == 2


async def test_lost_lease_cannot_overwrite_or_unlock_new_owner() -> None:
    cache = Cache()
    cache.replace_lease_on_publish = True
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"errcode": 0, "access_token": "obsolete", "expires_in": 7200}
            )
        )
    ) as client:
        with pytest.raises(APIError, match="wecom_token_refresh_lease_lost"):
            await provider(cache, client).get_token()
    assert list(cache.values.values()) == ["new-owner-lease"]


@pytest.mark.parametrize(
    "response",
    [
        {"errcode": 40001, "errmsg": "credential-private"},
        {"errcode": 0, "access_token": "abc", "expires_in": 1},
        {"errcode": 0, "access_token": "abc", "expires_in": True},
        {"errcode": 0, "access_token": "", "expires_in": 7200},
        {"errcode": 0, "access_token": "abc", "expires_in": "7200"},
        {"errcode": False},
    ],
)
async def test_refresh_failure_releases_lock_and_keeps_cache_empty(
    response: dict[str, object],
) -> None:
    cache = Cache()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=response))
    ) as client:
        with pytest.raises(APIError) as error:
            await provider(cache, client).get_token()
    assert "credential-private" not in str(error.value)
    assert not cache.values


async def test_redis_failure_has_safe_retryable_error() -> None:
    cache = Cache()
    cache.fail = True
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: pytest.fail("HTTP should not be called with unavailable cache")
        )
    ) as client:
        with pytest.raises(APIError, match="^wecom_token_cache_unavailable$") as error:
            await provider(cache, client).get_token()
    assert error.value.retryable


async def test_gettoken_does_not_follow_redirects() -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(302, headers={"Location": "http://localhost/credentials"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(APIError, match="wecom_token_http_error"):
            await provider(Cache(), client).get_token()
    assert calls == 1
