"""Fast HTTP boundary: verify, decrypt, durably enqueue, acknowledge."""

import asyncio
from typing import Protocol

from fastapi import APIRouter, Request, Response
from starlette.responses import PlainTextResponse

from app.connectors.wecom.contracts import CallbackNotification, WeComError
from app.connectors.wecom.queue import NotificationQueue


class CallbackCrypto(Protocol):
    def decrypt(self, signature: str, timestamp: str, nonce: str, encrypted: str) -> bytes: ...

    def decode_callback(
        self, signature: str, timestamp: str, nonce: str, body: bytes
    ) -> CallbackNotification | None: ...


class CallbackService:
    def __init__(
        self, crypto: CallbackCrypto, queue: NotificationQueue, open_kfids: frozenset[str]
    ) -> None:
        self.crypto = crypto
        self.queue = queue
        self.open_kfids = open_kfids

    async def receive(self, signature: str, timestamp: str, nonce: str, body: bytes) -> None:
        event = self.crypto.decode_callback(signature, timestamp, nonce, body)
        if event is not None:
            if event.open_kfid not in self.open_kfids:
                raise WeComError("unknown_customer_account")
            await self.queue.enqueue(event)


router = APIRouter(prefix="/wecom", tags=["wecom"])


def callback_parameters(request: Request) -> tuple[str, str, str]:
    values = []
    for name, maximum in (("msg_signature", 40), ("timestamp", 20), ("nonce", 256)):
        value = request.query_params.get(name, "")
        if not value or len(value) > maximum or len(request.query_params.getlist(name)) != 1:
            raise WeComError("invalid_callback_parameters")
        values.append(value)
    return values[0], values[1], values[2]


@router.get("/callback", response_class=PlainTextResponse)
async def verify_url(request: Request) -> Response:
    service: CallbackService | None = getattr(request.app.state, "wecom_callback", None)
    if service is None:
        return PlainTextResponse("Not found", status_code=404)
    try:
        signature, timestamp, nonce = callback_parameters(request)
        echo = request.query_params.get("echostr", "")
        if not echo or len(echo) > 8192 or len(request.query_params.getlist("echostr")) != 1:
            raise WeComError("invalid_callback_parameters")
        plaintext = service.crypto.decrypt(signature, timestamp, nonce, echo)
        return Response(plaintext, media_type="text/plain")
    except WeComError:
        return PlainTextResponse("Invalid callback", status_code=400)


@router.post("/callback", response_class=PlainTextResponse)
async def receive_callback(request: Request) -> Response:
    service: CallbackService | None = getattr(request.app.state, "wecom_callback", None)
    if service is None:
        return PlainTextResponse("Not found", status_code=404)
    settings = request.app.state.settings
    try:
        async with asyncio.timeout(settings.wecom_callback_budget_seconds):
            signature, timestamp, nonce = callback_parameters(request)
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > settings.wecom_max_callback_bytes:
                    return PlainTextResponse("Request too large", status_code=413)
            await service.receive(signature, timestamp, nonce, bytes(body))
        return PlainTextResponse("success")
    except TimeoutError:
        return PlainTextResponse("Temporarily unavailable", status_code=503)
    except WeComError as error:
        return PlainTextResponse(
            "Temporarily unavailable" if error.retryable else "Invalid callback",
            status_code=503 if error.retryable else 400,
        )
