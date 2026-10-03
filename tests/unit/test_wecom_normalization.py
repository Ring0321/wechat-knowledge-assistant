from datetime import UTC, datetime
from typing import cast

import pytest
from pydantic import JsonValue

from app.connectors.wecom.contracts import WeComError
from app.connectors.wecom.normalization import normalize_message


def message(kind: str = "text", body: dict[str, JsonValue] | None = None) -> dict[str, JsonValue]:
    return {
        "origin": 3,
        "msgid": "msg-1",
        "open_kfid": "wk-account",
        "external_userid": "wm-customer",
        "send_time": 1720000000,
        "msgtype": kind,
        kind: body if body is not None else {"content": "  保存这条笔记\n原样保留  "},
    }


def test_text_preserves_content_and_verified_identity() -> None:
    raw = message()
    raw["user_id"] = "attacker-supplied-internal-id"
    result = normalize_message(raw, "wk-account")
    assert result is not None
    assert result.text == "  保存这条笔记\n原样保留  "
    assert result.external_userid == "wm-customer"
    assert result.sent_at == datetime.fromtimestamp(1720000000, UTC)
    assert "user_id" not in result.metadata
    assert result.text not in repr(result)
    assert result.external_userid not in repr(result)


@pytest.mark.parametrize("kind", ["image", "voice", "video", "file"])
def test_media_metadata_allowlist(kind: str) -> None:
    raw = message(kind, {"media_id": "opaque-media", "url": "http://localhost/private"})
    result = normalize_message(raw, "wk-account")
    assert result is not None
    assert result.text is None
    assert result.metadata == {"media_id": "opaque-media"}


def test_channels_metadata_only_no_download_assumption() -> None:
    result = normalize_message(
        message(
            "channels",
            {
                "nickname": "测试视频号",
                "title": "测试标题",
                "sub_type": 1,
                "mp4": "https://invalid/private-video",
            },
        ),
        "wk-account",
    )
    assert result is not None
    assert result.metadata == {
        "unparsed": True,
        "channels": {"nickname": "测试视频号", "title": "测试标题", "sub_type": 1},
    }


@pytest.mark.parametrize("origin", [1, 2, 4, 5])
def test_noncustomer_messages_not_routed(origin: int) -> None:
    assert normalize_message({"origin": origin}, "wk-account") is None


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("origin", None),
        ("origin", True),
        ("origin", "3"),
        ("msgid", ""),
        ("msgid", None),
        ("external_userid", ""),
        ("external_userid", {"user_id": "x"}),
        ("open_kfid", "other-account"),
        ("send_time", True),
        ("send_time", -1),
        ("send_time", 10**30),
        ("msgtype", None),
        ("text", None),
        ("text", {"content": None}),
        ("text", {"content": ""}),
    ],
)
def test_malformed_customers_rejected(key: str, value: JsonValue) -> None:
    raw = message()
    raw[key] = value
    with pytest.raises(WeComError, match="wecom_invalid_"):
        normalize_message(raw, "wk-account")


@pytest.mark.parametrize("kind", ["image", "voice", "video", "file"])
def test_media_id_required(kind: str) -> None:
    with pytest.raises(WeComError, match="wecom_invalid_message"):
        normalize_message(message(kind, {}), "wk-account")


def test_null_record_is_a_safe_error() -> None:
    with pytest.raises(WeComError, match="wecom_invalid_message"):
        normalize_message(cast(dict[str, JsonValue], None), "wk-account")


def test_invalid_unicode_text_is_a_safe_error() -> None:
    with pytest.raises(WeComError, match="wecom_invalid_message"):
        normalize_message(message("text", {"content": "\ud800"}), "wk-account")


def test_optional_menu_id_preserved() -> None:
    result = normalize_message(
        message("text", {"content": "menu", "menu_id": "button"}), "wk-account"
    )
    assert result is not None
    assert result.metadata == {"menu_id": "button"}


def test_link_preserves_metadata_without_fetching_or_tenant_fields() -> None:
    result = normalize_message(
        message(
            "link",
            {
                "title": "title",
                "desc": "",
                "url": "http://localhost/private",
                "pic_url": "https://image.example/pic",
                "user_id": "attacker",
            },
        ),
        "wk-account",
    )
    assert result is not None
    assert result.metadata == {
        "unparsed": True,
        "link": {
            "title": "title",
            "desc": "",
            "url": "http://localhost/private",
            "pic_url": "https://image.example/pic",
        },
    }
