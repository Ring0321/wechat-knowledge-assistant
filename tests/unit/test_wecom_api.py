import json

import httpx
import pytest

from app.connectors.wecom.api import HttpWeComAPI
from app.connectors.wecom.contracts import APIError


class Tokens:
    def __init__(self) -> None:
        self.token = "cached-secret-token"
        self.invalidated: list[str] = []

    async def get_token(self) -> str:
        return self.token

    async def invalidate(self, token: str) -> None:
        self.invalidated.append(token)
        self.token = "fresh-secret-token"


async def test_sync_pagination_payload_and_empty_page_with_more() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert str(request.url).startswith("https://qyapi.weixin.qq.com/cgi-bin/kf/sync_msg?")
        assert request.method == "POST"
        assert json.loads(request.content) == {
            "open_kfid": "wk-account",
            "cursor": "existing-cursor",
            "token": "notification-token",
            "limit": 1000,
            "voice_format": 0,
        }
        return httpx.Response(
            200, json={"errcode": 0, "next_cursor": "next", "has_more": 1, "msg_list": []}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        page = await HttpWeComAPI(client, Tokens()).sync_msg(
            open_kfid="wk-account", cursor="existing-cursor", token="notification-token"
        )
    assert page.has_more is True
    assert page.next_cursor == "next"
    assert page.messages == []


@pytest.mark.parametrize("errcode", [40014, 42001])
async def test_token_refresh_once(errcode: int) -> None:
    tokens = Tokens()
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.url.params["access_token"] == (
            "cached-secret-token" if calls == 1 else "fresh-secret-token"
        )
        return httpx.Response(200, json={"errcode": errcode, "errmsg": "secret detail"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(APIError, match=f"^wecom_{errcode}$"):
            await HttpWeComAPI(client, tokens).sync_msg(open_kfid="wk-account", cursor=None)
    assert calls == 2
    assert tokens.invalidated == ["cached-secret-token"]


async def test_expired_notification_token_retry_keeps_cursor() -> None:
    payloads: list[dict[str, object]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        payloads.append(json.loads(request.content))
        if len(payloads) == 1:
            return httpx.Response(200, json={"errcode": 95007})
        return httpx.Response(
            200, json={"errcode": 0, "next_cursor": "next", "has_more": 0, "msg_list": []}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        await HttpWeComAPI(client, Tokens()).sync_msg(
            open_kfid="wk-account", cursor="saved", token="old-token"
        )
    assert len(payloads) == 2
    assert payloads[0]["token"] == "old-token"
    assert "token" not in payloads[1]
    assert payloads[1]["cursor"] == "saved"


async def test_send_payload_stable_msgid_and_utf8_limit() -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        payload = json.loads(request.content)
        assert payload == {
            "open_kfid": "wk-account",
            "touser": "wm-user",
            "msgid": "id_1-2",
            "msgtype": "text",
            "text": {"content": "中" * 682 + "ab"},
        }
        return httpx.Response(200, json={"errcode": 0, "msgid": payload["msgid"]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        api = HttpWeComAPI(client, Tokens())
        result = await api.send_text(
            open_kfid="wk-account",
            external_userid="wm-user",
            content="中" * 682 + "ab",
            msgid="id_1-2",
        )
        assert result == "id_1-2"
        for text, msgid in [
            ("中" * 683, "id"),
            ("", "id"),
            ("hello", "x" * 33),
            ("hello", "bad/id"),
        ]:
            with pytest.raises(APIError, match="wecom_invalid_request"):
                await api.send_text(
                    open_kfid="wk-account", external_userid="wm-user", content=text, msgid=msgid
                )
    assert calls == 1


@pytest.mark.parametrize("errcode", [95033, 95001, 95002, 95013, 95018, 95031, 40001])
async def test_business_and_credential_errors_are_not_retried(errcode: int) -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"errcode": errcode, "errmsg": "raw-secret-do-not-log"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(APIError) as error:
            await HttpWeComAPI(client, Tokens()).send_text(
                open_kfid="wk-account", external_userid="wm-user", content="hello", msgid="id"
            )
    assert calls == 1
    assert str(error.value) == f"wecom_{errcode}"
    assert not error.value.retryable
    assert not error.value.uncertain


@pytest.mark.parametrize("errcode", [-1, 45009])
async def test_explicit_transient_api_errors_can_be_retried(errcode: int) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"errcode": errcode})
        )
    ) as client:
        with pytest.raises(APIError) as error:
            await HttpWeComAPI(client, Tokens()).send_text(
                open_kfid="wk", external_userid="wm", content="test", msgid="id"
            )
    assert error.value.retryable
    assert not error.value.uncertain


@pytest.mark.parametrize("sending", [False, True])
async def test_timeout_is_uncertain_for_send_only(sending: bool) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("contains token and identity", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        api = HttpWeComAPI(client, Tokens())
        with pytest.raises(APIError) as error:
            if sending:
                await api.send_text(
                    open_kfid="wk", external_userid="wm", content="test", msgid="id"
                )
            else:
                await api.sync_msg(open_kfid="wk", cursor=None)
    assert error.value.uncertain is sending
    assert error.value.retryable is not sending
    assert str(error.value) == "wecom_transport_failed"


@pytest.mark.parametrize(
    "status,body",
    [
        (302, b"redirect"),
        (500, b"private"),
        (200, b"not-json"),
        (200, b'{"errcode":true}'),
        (200, b'{"errcode":0,"msgid":"different"}'),
    ],
)
async def test_send_ambiguous_response_is_never_success(status: int, body: bytes) -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status, content=body, headers={"Location": "http://localhost/"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(APIError) as error:
            await HttpWeComAPI(client, Tokens()).send_text(
                open_kfid="wk", external_userid="wm", content="test", msgid="id"
            )
    assert error.value.uncertain
    assert not error.value.retryable
    assert calls == 1


@pytest.mark.parametrize(
    "overrides",
    [{"has_more": True}, {"msg_list": [None]}, {"next_cursor": "", "has_more": 1}, {"has_more": 2}],
)
async def test_sync_rejects_malformed_page(overrides: dict[str, object]) -> None:
    response = {"errcode": 0, "next_cursor": "next", "has_more": 0, "msg_list": []} | overrides
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=response))
    ) as client:
        with pytest.raises(APIError, match="wecom_invalid_response"):
            await HttpWeComAPI(client, Tokens()).sync_msg(open_kfid="wk", cursor=None)


async def test_send_rejected_cached_token_refreshes_with_identical_payload() -> None:
    payloads: list[bytes] = []

    def handle(request: httpx.Request) -> httpx.Response:
        payloads.append(request.content)
        if len(payloads) == 1:
            return httpx.Response(200, json={"errcode": 42001})
        return httpx.Response(200, json={"errcode": 0, "msgid": "stable-id"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        assert (
            await HttpWeComAPI(client, Tokens()).send_text(
                open_kfid="wk", external_userid="wm", content="hello", msgid="stable-id"
            )
            == "stable-id"
        )
    assert len(payloads) == 2
    assert payloads[0] == payloads[1]


async def test_connection_failure_before_send_is_safe_to_retry() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("credential-private", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(APIError, match="wecom_connection_failed") as error:
            await HttpWeComAPI(client, Tokens()).send_text(
                open_kfid="wk", external_userid="wm", content="hello", msgid="id"
            )
    assert error.value.retryable
    assert not error.value.uncertain


async def test_sync_rejects_oversized_response() -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"x" * (8 * 1024 * 1024 + 1))
        )
    ) as client:
        with pytest.raises(APIError, match="wecom_response_too_large"):
            await HttpWeComAPI(client, Tokens()).sync_msg(open_kfid="wk", cursor=None)


@pytest.mark.parametrize(
    "cursor,token", [("a" * 65, None), ("中" * 22, None), (None, "a" * 129), (None, "中" * 43)]
)
async def test_official_cursor_and_notification_token_byte_limits(
    cursor: str | None, token: str | None
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: pytest.fail("invalid request must not call the network")
        )
    ) as client:
        with pytest.raises(APIError, match="wecom_invalid_request"):
            await HttpWeComAPI(client, Tokens()).sync_msg(
                open_kfid="wk", cursor=cursor, token=token
            )
