import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr

from app.connectors.wecom.callback import CallbackService
from app.connectors.wecom.contracts import CallbackNotification, WeComError
from app.connectors.wecom.crypto import WeComCrypto
from app.core.config import Settings
from app.main import create_app
from tests.unit.test_health import FakeProbe
from tests.wecom_helpers import (
    TEST_CORP,
    TEST_KEY,
    TEST_KF,
    TEST_TOKEN,
    callback_fixture,
    encrypt_message,
    signed_parameters,
)


class FakeQueue:
    def __init__(self, failure: bool = False, delay: float = 0) -> None:
        self.events: list[CallbackNotification] = []
        self.failure, self.delay = failure, delay

    async def enqueue(self, notification: CallbackNotification) -> str:
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.failure:
            raise WeComError("queue_unavailable", retryable=True)
        self.events.append(notification)
        return "1-0"


@asynccontextmanager
async def callback_client(queue: FakeQueue, **changes: object) -> AsyncIterator[AsyncClient]:
    settings = Settings.model_validate(
        {
            "database_url": SecretStr("postgresql+asyncpg://test:synthetic@localhost/test"),
            "redis_url": SecretStr("redis://localhost/0"),
            **changes,
        }
    )
    service = CallbackService(
        WeComCrypto(TEST_TOKEN, TEST_KEY, TEST_CORP), queue, frozenset({TEST_KF})
    )
    app = create_app(settings, FakeProbe(), service)
    async with (
        app.router.lifespan_context(app),
        AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client,
    ):
        yield client


async def test_get_verification_returns_exact_plaintext_and_never_enqueues() -> None:
    queue = FakeQueue()
    plaintext = b"challenge-without-newline"
    encrypted = encrypt_message(plaintext)
    async with callback_client(queue) as client:
        response = await client.get(
            "/wecom/callback", params={**signed_parameters(encrypted), "echostr": encrypted}
        )
    assert response.status_code == 200
    assert response.content == plaintext
    assert queue.events == []


async def test_valid_callback_only_enqueues_notification() -> None:
    queue = FakeQueue()
    params, body = callback_fixture()
    async with callback_client(queue) as client:
        response = await client.post("/wecom/callback", params=params, content=body)
    assert response.status_code == 200 and response.text == "success"
    assert len(queue.events) == 1
    assert queue.events[0].open_kfid == TEST_KF


@pytest.mark.parametrize("case", ["signature", "corp", "account", "xml", "query"])
async def test_untrusted_callback_cannot_enqueue(case: str) -> None:
    queue = FakeQueue()
    params, body = callback_fixture()
    if case == "signature":
        params["msg_signature"] = "0" * 40
    elif case == "corp":
        params, body = callback_fixture(corp_id="another-corp")
    elif case == "account":
        params, body = callback_fixture(open_kfid="not-configured")
    elif case == "xml":
        body = b"<!DOCTYPE xml [<!ENTITY entity SYSTEM 'file:///etc/passwd'>]><xml>&entity;</xml>"
    else:
        params.pop("nonce")
    async with callback_client(queue) as client:
        response = await client.post("/wecom/callback", params=params, content=body)
    assert response.status_code == 400
    assert response.text == "Invalid callback"
    assert queue.events == []


@pytest.mark.parametrize("queue", [FakeQueue(failure=True), FakeQueue(delay=1)])
async def test_queue_failure_never_acknowledges_success(queue: FakeQueue) -> None:
    params, body = callback_fixture()
    async with callback_client(queue, wecom_callback_budget_seconds=0.05) as client:
        response = await client.post("/wecom/callback", params=params, content=body)
    assert response.status_code == 503
    assert "notification-token" not in response.text


async def test_oversized_callback_and_duplicate_query_rejected() -> None:
    queue = FakeQueue()
    params, body = callback_fixture()
    async with callback_client(queue, wecom_max_callback_bytes=1024) as client:
        assert (
            await client.post("/wecom/callback", params=params, content=b"x" * 1025)
        ).status_code == 413
        values = list(params.items()) + [("nonce", "duplicate")]
        assert (
            await client.post("/wecom/callback", params=values, content=body)
        ).status_code == 400
    assert not queue.events
