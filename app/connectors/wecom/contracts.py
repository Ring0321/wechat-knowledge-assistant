"""Typed transport boundary shared by adapter, callback and worker."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from pydantic import JsonValue


class WeComError(Exception):
    """Safe errors contain only a stable code, never a raw URL/body/credential."""

    def __init__(self, code: str, *, retryable: bool = False) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(code)


class CallbackError(WeComError):
    """Unauthentic or malformed encrypted callback."""


class APIError(WeComError):
    def __init__(self, code: str, *, retryable: bool = False, uncertain: bool = False) -> None:
        self.uncertain = uncertain
        super().__init__(code, retryable=retryable)


@dataclass(frozen=True)
class CallbackNotification:
    corp_id: str
    open_kfid: str
    token: str = field(repr=False)
    created_at: int


@dataclass(frozen=True)
class SyncPage:
    next_cursor: str
    has_more: bool
    messages: list[dict[str, JsonValue]] = field(repr=False)


@dataclass(frozen=True)
class NormalizedMessage:
    msgid: str
    open_kfid: str
    external_userid: str = field(repr=False)
    sent_at: datetime
    message_type: str
    text: str | None = field(default=None, repr=False)
    metadata: dict[str, JsonValue] = field(default_factory=dict, repr=False)


class TokenProvider(Protocol):
    async def get_token(self) -> str: ...

    async def invalidate(self, token: str) -> None: ...


class WeComAPI(Protocol):
    async def sync_msg(
        self, *, open_kfid: str, cursor: str | None, token: str | None = None
    ) -> SyncPage: ...

    async def send_text(
        self, *, open_kfid: str, external_userid: str, content: str, msgid: str
    ) -> str: ...
