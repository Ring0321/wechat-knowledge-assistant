import base64
import hashlib
import struct

import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from app.connectors.wecom.contracts import CallbackError
from app.connectors.wecom.crypto import WeComCrypto

KEY = bytes(range(32))
ENCODING_KEY = base64.b64encode(KEY).decode().rstrip("=")
CORP = "ww-test-corp"
TOKEN = "test-callback-token"
TIMESTAMP = "1720000000"
NONCE = "deterministic-nonce"


def encrypt(payload: bytes, *, receiver: bytes = CORP.encode(), frame: bytes | None = None) -> str:
    framed = frame if frame is not None else b"random-prefix-16" + struct.pack("!I", len(payload))
    if frame is None:
        framed += payload + receiver
    padder = padding.PKCS7(256).padder()
    padded = padder.update(framed) + padder.finalize()
    enc = Cipher(algorithms.AES(KEY), modes.CBC(KEY[:16])).encryptor()
    return base64.b64encode(enc.update(padded) + enc.finalize()).decode()


def signature(encrypted: str) -> str:
    return hashlib.sha1(
        "".join(sorted((TOKEN, TIMESTAMP, NONCE, encrypted))).encode(), usedforsecurity=False
    ).hexdigest()


def outer(encrypted: str, corp: str = CORP) -> bytes:
    return f"<xml><ToUserName>{corp}</ToUserName><Encrypt>{encrypted}</Encrypt></xml>".encode()


def inner(*, event: str = "kf_msg_or_event", corp: str = CORP) -> bytes:
    return (
        f"<xml><ToUserName>{corp}</ToUserName><MsgType>event</MsgType><Event>{event}</Event>"
        "<CreateTime>1720000000</CreateTime><Token>notification-token</Token>"
        "<OpenKfId>wk-account</OpenKfId></xml>"
    ).encode()


@pytest.fixture
def crypto() -> WeComCrypto:
    return WeComCrypto(TOKEN, ENCODING_KEY, CORP)


@pytest.mark.parametrize("payload", [b"echo-value", b"", "中文验证".encode(), b"x" * 32])
def test_decrypt_echo_preserves_bytes(crypto: WeComCrypto, payload: bytes) -> None:
    encrypted = encrypt(payload)
    assert crypto.decrypt(signature(encrypted), TIMESTAMP, NONCE, encrypted) == payload


def test_fixed_synthetic_wire_vector(crypto: WeComCrypto) -> None:
    # Frozen synthetic fixture: AES key bytes 0..31, 16-byte prefix random-prefix-16,
    # big-endian payload length, echo-value, ww-test-corp and PKCS7 block size 32.
    ciphertext = (
        "RvpzFhtJ1fYo8eb8iGQgqfyGH8ByFQMuw5uIH5oVBSKAhBFDm4wI7mABugPPxDNZBIuImavd+qGgEuSEz6H5eg=="
    )
    assert (
        crypto.decrypt("a4fcff152448dccab63b7a828d2b34b72c5a4e18", TIMESTAMP, NONCE, ciphertext)
        == b"echo-value"
    )


def test_notification_requires_both_identities(crypto: WeComCrypto) -> None:
    encrypted = encrypt(inner())
    event = crypto.decode_callback(signature(encrypted), TIMESTAMP, NONCE, outer(encrypted))
    assert event is not None
    assert (event.corp_id, event.open_kfid, event.created_at) == (CORP, "wk-account", 1720000000)
    assert event.token == "notification-token"
    assert event.token not in repr(event)
    with pytest.raises(CallbackError, match="callback_invalid_receiver"):
        crypto.decode_callback(signature(encrypted), TIMESTAMP, NONCE, outer(encrypted, "other"))
    encrypted = encrypt(inner(corp="other"))
    with pytest.raises(CallbackError, match="callback_invalid_receiver"):
        crypto.decode_callback(signature(encrypted), TIMESTAMP, NONCE, outer(encrypted))


def test_unknown_signed_event_is_ignored(crypto: WeComCrypto) -> None:
    encrypted = encrypt(inner(event="future_event"))
    assert crypto.decode_callback(signature(encrypted), TIMESTAMP, NONCE, outer(encrypted)) is None


@pytest.mark.parametrize("alteration", ["signature", "timestamp", "nonce", "ciphertext"])
def test_authenticated_fields_cannot_change(crypto: WeComCrypto, alteration: str) -> None:
    encrypted = encrypt(inner())
    args = [signature(encrypted), TIMESTAMP, NONCE, encrypted]
    args[["signature", "timestamp", "nonce", "ciphertext"].index(alteration)] += "x"
    with pytest.raises(CallbackError):
        crypto.decrypt(*args)


@pytest.mark.parametrize(
    "encrypted",
    [
        "not base64",
        base64.b64encode(b"too-short").decode(),
        encrypt(b"test", receiver=b"wrong-corp"),
        encrypt(b"", frame=b"r" * 16 + struct.pack("!I", 2**32 - 1) + b"x"),
        encrypt(b"", frame=b"r" * 16 + struct.pack("!I", 0) + CORP.encode() + b"extra"),
    ],
)
def test_bad_framing_rejected(crypto: WeComCrypto, encrypted: str) -> None:
    with pytest.raises(CallbackError, match="callback_invalid_ciphertext"):
        crypto.decrypt(signature(encrypted), TIMESTAMP, NONCE, encrypted)


def test_bad_padding_rejected(crypto: WeComCrypto) -> None:
    enc = Cipher(algorithms.AES(KEY), modes.CBC(KEY[:16])).encryptor()
    ciphertext = base64.b64encode(enc.update(b"\x00" * 64) + enc.finalize()).decode()
    with pytest.raises(CallbackError, match="callback_invalid_ciphertext"):
        crypto.decrypt(signature(ciphertext), TIMESTAMP, NONCE, ciphertext)


@pytest.mark.parametrize(
    "body",
    [
        b"<!DOCTYPE xml [<!ENTITY x 'xx'>]><xml>&x;</xml>",
        b"<xml><ToUserName>x</ToUserName><ToUserName>x</ToUserName></xml>",
        b"<wrong />",
        b"\x00",
        b"x" * (256 * 1024 + 1),
    ],
    ids=["dtd", "duplicate", "root", "malformed", "oversized"],
)
def test_unsafe_outer_xml_rejected(crypto: WeComCrypto, body: bytes) -> None:
    with pytest.raises(CallbackError):
        crypto.decode_callback("0" * 40, TIMESTAMP, NONCE, body)


def test_signed_inner_dtd_rejected(crypto: WeComCrypto) -> None:
    encrypted = encrypt(b"<!DOCTYPE xml [<!ENTITY x SYSTEM 'file:///secrets'>]><xml>&x;</xml>")
    with pytest.raises(CallbackError, match="callback_invalid_xml"):
        crypto.decode_callback(signature(encrypted), TIMESTAMP, NONCE, outer(encrypted))


@pytest.mark.parametrize("key", ["", "a" * 42, "a" * 44, "!" * 43])
def test_invalid_configuration_fails_closed(key: str) -> None:
    with pytest.raises(CallbackError, match="callback_invalid_configuration"):
        WeComCrypto(TOKEN, key, CORP)
