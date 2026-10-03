"""Bounded official WeCom HTTP adapter; no business or tenant policy lives here."""

import asyncio
import json
import re
from typing import cast

import httpx
from pydantic import JsonValue

from app.connectors.wecom.contracts import APIError, SyncPage, TokenProvider

_BASE = "https://qyapi.weixin.qq.com/cgi-bin/kf/"
_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
_INVALID_TOKEN = {40014, 42001}
_TRANSIENT = {-1, 45009}


def _fits(value: str, maximum: int) -> bool:
    try:
        return len(value.encode("utf-8")) <= maximum
    except UnicodeError:
        return False


def _identifier(value: str, *, maximum: int = 256) -> None:
    if not value or not _fits(value, maximum) or "\x00" in value:
        raise APIError("wecom_invalid_request")


class HttpWeComAPI:
    def __init__(
        self, http: httpx.AsyncClient, tokens: TokenProvider, *, timeout_seconds: float = 8
    ) -> None:
        self._http = http
        self._tokens = tokens
        if not 0 < timeout_seconds <= 30:
            raise APIError("wecom_invalid_configuration")
        self._timeout = timeout_seconds

    async def _request(
        self, endpoint: str, payload: dict[str, JsonValue], *, sending: bool
    ) -> dict[str, JsonValue]:
        refreshed = False
        omitted_notification_token = False
        while True:
            token = await self._tokens.get_token()
            try:
                async with asyncio.timeout(self._timeout + 2):
                    async with self._http.stream(
                        "POST",
                        _BASE + endpoint,
                        params={"access_token": token},
                        json=payload,
                        timeout=httpx.Timeout(self._timeout, connect=min(self._timeout, 5)),
                        follow_redirects=False,
                    ) as response:
                        if response.status_code != 200:
                            raise APIError(
                                "wecom_http_error",
                                retryable=not sending and response.status_code >= 500,
                                uncertain=sending,
                            )
                        content = bytearray()
                        async for chunk in response.aiter_bytes():
                            content.extend(chunk)
                            if len(content) > _MAX_RESPONSE_BYTES:
                                raise APIError("wecom_response_too_large", uncertain=sending)
            except (httpx.ConnectError, httpx.ConnectTimeout):
                raise APIError("wecom_connection_failed", retryable=True) from None
            except (httpx.HTTPError, TimeoutError):
                raise APIError(
                    "wecom_transport_failed", retryable=not sending, uncertain=sending
                ) from None
            try:
                raw: object = json.loads(content)
            except (ValueError, UnicodeError):
                raise APIError("wecom_invalid_response", uncertain=sending) from None
            if not isinstance(raw, dict) or type(raw.get("errcode")) is not int:
                raise APIError("wecom_invalid_response", uncertain=sending)
            code = raw["errcode"]
            if code in _INVALID_TOKEN and not refreshed:
                await self._tokens.invalidate(token)
                refreshed = True
                continue
            if (
                code == 95007
                and not sending
                and payload.get("token")
                and not omitted_notification_token
            ):
                payload = {key: value for key, value in payload.items() if key != "token"}
                omitted_notification_token = True
                continue
            if code != 0:
                raise APIError(f"wecom_{code}", retryable=code in _TRANSIENT)
            return cast(dict[str, JsonValue], raw)

    async def sync_msg(
        self, *, open_kfid: str, cursor: str | None, token: str | None = None
    ) -> SyncPage:
        _identifier(open_kfid, maximum=128)
        payload: dict[str, JsonValue] = {"open_kfid": open_kfid, "limit": 1000, "voice_format": 0}
        if cursor is not None:
            if not _fits(cursor, 64):
                raise APIError("wecom_invalid_request")
            payload["cursor"] = cursor
        if token is not None:
            _identifier(token, maximum=128)
            payload["token"] = token
        raw = await self._request("sync_msg", payload, sending=False)
        next_cursor, has_more, messages = (
            raw.get("next_cursor"),
            raw.get("has_more"),
            raw.get("msg_list"),
        )
        if (
            not isinstance(next_cursor, str)
            or not _fits(next_cursor, 64)
            or type(has_more) is not int
            or has_more not in (0, 1)
            or (has_more == 1 and not next_cursor)
            or not isinstance(messages, list)
            or len(messages) > 1000
            or any(not isinstance(message, dict) for message in messages)
        ):
            raise APIError("wecom_invalid_response")
        return SyncPage(
            next_cursor=next_cursor,
            has_more=bool(has_more),
            messages=cast(list[dict[str, JsonValue]], messages),
        )

    async def send_text(
        self, *, open_kfid: str, external_userid: str, content: str, msgid: str
    ) -> str:
        _identifier(open_kfid, maximum=128)
        _identifier(external_userid)
        try:
            size = len(content.encode("utf-8"))
        except UnicodeError:
            raise APIError("wecom_invalid_request") from None
        if not 0 < size <= 2048 or not re.fullmatch(r"[a-zA-Z0-9_-]{1,32}", msgid):
            raise APIError("wecom_invalid_request")
        raw = await self._request(
            "send_msg",
            {
                "open_kfid": open_kfid,
                "touser": external_userid,
                "msgid": msgid,
                "msgtype": "text",
                "text": {"content": content},
            },
            sending=True,
        )
        result = raw.get("msgid")
        if not isinstance(result, str) or result != msgid:
            raise APIError("wecom_invalid_response", uncertain=True)
        return result
