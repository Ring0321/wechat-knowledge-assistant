"""Authenticated decoding for the official WeCom callback wire format."""

import base64
import hashlib
import hmac
import re
import struct
from xml.etree.ElementTree import Element

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from defusedxml.ElementTree import fromstring  # type: ignore[import-untyped]

from app.connectors.wecom.contracts import CallbackError, CallbackNotification

_MAX_XML_BYTES = 256 * 1024


def _xml(body: bytes) -> Element:
    if not body or len(body) > _MAX_XML_BYTES:
        raise CallbackError("callback_invalid_xml")
    try:
        root: Element = fromstring(
            body, forbid_dtd=True, forbid_entities=True, forbid_external=True
        )
    except Exception:
        raise CallbackError("callback_invalid_xml") from None
    if root.tag != "xml":
        raise CallbackError("callback_invalid_xml")
    return root


def _field(root: Element, name: str, *, maximum: int = 512) -> str:
    matches = root.findall(name)
    if len(matches) != 1 or len(matches[0]) or not matches[0].text:
        raise CallbackError("callback_invalid_field")
    value = matches[0].text
    if len(value.encode("utf-8")) > maximum:
        raise CallbackError("callback_invalid_field")
    return value


class WeComCrypto:
    def __init__(self, token: str, encoding_aes_key: str, corp_id: str) -> None:
        if not token or len(token) > 256 or not corp_id or len(corp_id) > 128:
            raise CallbackError("callback_invalid_configuration")
        if not re.fullmatch(r"[A-Za-z0-9+/]{43}", encoding_aes_key):
            raise CallbackError("callback_invalid_configuration")
        try:
            key = base64.b64decode(encoding_aes_key + "=", validate=True)
            receiver = corp_id.encode("utf-8")
        except (ValueError, UnicodeError):
            raise CallbackError("callback_invalid_configuration") from None
        if len(key) != 32:
            raise CallbackError("callback_invalid_configuration")
        self._token = token
        self._key = key
        self._receiver = receiver
        self._corp_id = corp_id

    def decrypt(self, signature: str, timestamp: str, nonce: str, encrypted: str) -> bytes:
        if (
            not re.fullmatch(r"[0-9a-fA-F]{40}", signature)
            or not re.fullmatch(r"[0-9]{1,20}", timestamp)
            or not nonce
            or len(nonce) > 512
            or not encrypted
            or len(encrypted) > _MAX_XML_BYTES
        ):
            raise CallbackError("callback_invalid_signature")
        try:
            signed = "".join(sorted((self._token, timestamp, nonce, encrypted))).encode("utf-8")
        except UnicodeError:
            raise CallbackError("callback_invalid_signature") from None
        expected = hashlib.sha1(signed, usedforsecurity=False).hexdigest()
        if not hmac.compare_digest(expected, signature.lower()):
            raise CallbackError("callback_invalid_signature")
        try:
            ciphertext = base64.b64decode(encrypted, validate=True)
            if not ciphertext or len(ciphertext) % 16:
                raise ValueError
            decryptor = Cipher(algorithms.AES(self._key), modes.CBC(self._key[:16])).decryptor()
            padded = decryptor.update(ciphertext) + decryptor.finalize()
            unpadder = padding.PKCS7(256).unpadder()
            plaintext = unpadder.update(padded) + unpadder.finalize()
            if len(plaintext) < 20 + len(self._receiver):
                raise ValueError
            size = struct.unpack("!I", plaintext[16:20])[0]
            if size > len(plaintext) - 20:
                raise ValueError
            receiver = plaintext[20 + size :]
            if not hmac.compare_digest(receiver, self._receiver):
                raise ValueError
        except (ValueError, OverflowError, struct.error):
            raise CallbackError("callback_invalid_ciphertext") from None
        return plaintext[20 : 20 + size]

    def decode_callback(
        self, signature: str, timestamp: str, nonce: str, body: bytes
    ) -> CallbackNotification | None:
        outer = _xml(body)
        if _field(outer, "ToUserName", maximum=128) != self._corp_id:
            raise CallbackError("callback_invalid_receiver")
        encrypted = _field(outer, "Encrypt", maximum=_MAX_XML_BYTES)
        inner = _xml(self.decrypt(signature, timestamp, nonce, encrypted))
        if _field(inner, "ToUserName", maximum=128) != self._corp_id:
            raise CallbackError("callback_invalid_receiver")
        if _field(inner, "MsgType") != "event":
            raise CallbackError("callback_invalid_message_type")
        if _field(inner, "Event") != "kf_msg_or_event":
            return None
        created_at = _field(inner, "CreateTime", maximum=20)
        if not re.fullmatch(r"[0-9]{1,12}", created_at) or int(created_at) <= 0:
            raise CallbackError("callback_invalid_field")
        return CallbackNotification(
            corp_id=self._corp_id,
            open_kfid=_field(inner, "OpenKfId", maximum=128),
            token=_field(inner, "Token", maximum=128),
            created_at=int(created_at),
        )
