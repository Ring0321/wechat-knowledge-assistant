"""Validate official customer messages without fetching or parsing their content."""

import re
from datetime import UTC, datetime

from pydantic import JsonValue

from app.connectors.wecom.contracts import NormalizedMessage, WeComError


def _string(value: JsonValue, *, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or "\x00" in value:
        raise WeComError("wecom_invalid_message")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise WeComError("wecom_invalid_message") from None
    return value


def _body(raw: dict[str, JsonValue], key: str) -> dict[str, JsonValue]:
    body = raw.get(key)
    if not isinstance(body, dict):
        raise WeComError("wecom_invalid_message")
    return body


def _optional_fields(body: dict[str, JsonValue], fields: tuple[str, ...]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for field in fields:
        if field not in body:
            continue
        value = body[field]
        if not isinstance(value, str) or len(value) > 65536:
            raise WeComError("wecom_invalid_message")
        if value:
            _string(value, maximum=65536)
        result[field] = value
    return result


def normalize_message(
    raw: dict[str, JsonValue], expected_open_kfid: str
) -> NormalizedMessage | None:
    if not isinstance(raw, dict):
        raise WeComError("wecom_invalid_message")
    origin = raw.get("origin")
    if type(origin) is not int or origin not in (1, 2, 3, 4, 5):
        raise WeComError("wecom_invalid_origin")
    if origin != 3:
        return None
    msgid = _string(raw.get("msgid"), maximum=128)
    account = _string(raw.get("open_kfid"), maximum=128)
    if account != expected_open_kfid:
        raise WeComError("wecom_invalid_account")
    identity = _string(raw.get("external_userid"), maximum=256)
    timestamp = raw.get("send_time")
    if type(timestamp) is not int or timestamp <= 0:
        raise WeComError("wecom_invalid_message")
    try:
        sent_at = datetime.fromtimestamp(timestamp, UTC)
    except (OverflowError, OSError, ValueError):
        raise WeComError("wecom_invalid_message") from None
    kind = _string(raw.get("msgtype"), maximum=64)
    text: str | None = None
    metadata: dict[str, JsonValue] = {}
    if kind == "text":
        body = _body(raw, kind)
        text = _string(body.get("content"), maximum=65536)
        metadata.update(_optional_fields(body, ("menu_id",)))
    elif kind in ("image", "voice", "video", "file"):
        body = _body(raw, kind)
        metadata["media_id"] = _string(body.get("media_id"), maximum=1024)
        # Only documented transport fields are retained. Filenames are data, never paths.
        if kind == "file" and "file_name" in body:
            metadata["file_name"] = _string(body["file_name"], maximum=4096)
    elif kind == "link":
        body = _body(raw, kind)
        link = _optional_fields(body, ("title", "desc", "pic_url"))
        link["url"] = _string(body.get("url"), maximum=8192)
        metadata["link"] = link
        metadata["unparsed"] = True
    elif kind == "channels":
        # Some cards expose no details. Missing body is a compatibility fallback;
        # an explicit malformed body is still rejected.
        body = _body(raw, kind) if kind in raw else {}
        metadata["unparsed"] = True
        channels = _optional_fields(body, ("nickname", "title"))
        if "sub_type" in body:
            subtype = body["sub_type"]
            if type(subtype) is not int or not 0 <= subtype <= 2**32 - 1:
                raise WeComError("wecom_invalid_message")
            channels["sub_type"] = subtype
        metadata["channels"] = channels
    else:
        # Preserve the existence of unsupported official types, without trusting arbitrary
        # nested fields or allowing supplied user_id/URL values into an identity boundary.
        metadata["unparsed"] = True
    return NormalizedMessage(
        msgid=msgid,
        open_kfid=account,
        external_userid=identity,
        sent_at=sent_at,
        message_type=kind,
        text=text,
        metadata=metadata,
    )


def is_setup_challenge_text(value: str) -> bool:
    """Reserved operator verification text must never become knowledge or an AI question."""
    return re.fullmatch(r"微信验证 [a-f0-9]{32}", value) is not None
