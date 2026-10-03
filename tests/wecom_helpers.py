"""Synthetic official-protocol fixtures; no customer or production credentials."""

import base64
import hashlib
import struct
from datetime import UTC, datetime
from xml.sax.saxutils import escape

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from pydantic import JsonValue

from app.connectors.wecom.contracts import APIError, SyncPage

TEST_KEY = base64.b64encode(bytes(range(32))).decode()[:-1]
TEST_TOKEN = "synthetic-callback-token"
TEST_CORP = "synthetic-corp"
TEST_KF = "synthetic-kf"
TEST_USER = "synthetic-customer"


def encrypt_message(message: bytes, corp_id: str = TEST_CORP) -> str:
    key = bytes(range(32))
    plaintext = b"0123456789abcdef" + struct.pack(">I", len(message)) + message + corp_id.encode()
    padding = 32 - len(plaintext) % 32
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return base64.b64encode(
        encryptor.update(plaintext + bytes([padding]) * padding) + encryptor.finalize()
    ).decode()


def callback_fixture(
    *, corp_id: str = TEST_CORP, open_kfid: str = TEST_KF, event_token: str = "notification-token"
) -> tuple[dict[str, str], bytes]:
    timestamp = str(int(datetime.now(UTC).timestamp()))
    message = (
        f"<xml><ToUserName>{escape(corp_id)}</ToUserName><CreateTime>{timestamp}</CreateTime>"
        f"<MsgType>event</MsgType><Event>kf_msg_or_event</Event><Token>{escape(event_token)}</Token>"
        f"<OpenKfId>{escape(open_kfid)}</OpenKfId></xml>"
    ).encode()
    encrypted = encrypt_message(message, corp_id)
    params = signed_parameters(encrypted, timestamp)
    outer = f"<xml><ToUserName>{escape(corp_id)}</ToUserName><Encrypt>{encrypted}</Encrypt></xml>"
    return params, outer.encode()


def signed_parameters(encrypted: str, timestamp: str = "1789999999") -> dict[str, str]:
    nonce = "synthetic-nonce"
    digest = hashlib.sha1(
        "".join(sorted([TEST_TOKEN, timestamp, nonce, encrypted])).encode()
    ).hexdigest()
    return {"msg_signature": digest, "timestamp": timestamp, "nonce": nonce}


def customer_message(
    msgid: str = "message-1",
    *,
    external_userid: str = TEST_USER,
    open_kfid: str = TEST_KF,
    message_type: str = "text",
    sent_at: int | None = None,
) -> dict[str, JsonValue]:
    value: dict[str, JsonValue] = {
        "msgid": msgid,
        "origin": 3,
        "open_kfid": open_kfid,
        "external_userid": external_userid,
        "send_time": sent_at or int(datetime.now(UTC).timestamp()),
        "msgtype": message_type,
    }
    value[message_type] = (
        {"content": "测试正文"} if message_type == "text" else {"media_id": "media-1"}
    )
    return value


class FakeAPI:
    def __init__(self, pages: list[SyncPage] | None = None) -> None:
        self.pages = list(pages or [])
        self.sync_calls: list[tuple[str, str | None, str | None]] = []
        self.replies: list[tuple[str, str, str, str]] = []
        self.send_error: APIError | None = None
        self.sync_error: APIError | None = None

    async def sync_msg(
        self, *, open_kfid: str, cursor: str | None, token: str | None = None
    ) -> SyncPage:
        self.sync_calls.append((open_kfid, cursor, token))
        if self.sync_error is not None:
            raise self.sync_error
        return self.pages.pop(0) if self.pages else SyncPage(cursor or "empty-cursor", False, [])

    async def send_text(
        self, *, open_kfid: str, external_userid: str, content: str, msgid: str
    ) -> str:
        self.replies.append((open_kfid, external_userid, content, msgid))
        if self.send_error is not None:
            raise self.send_error
        return msgid
