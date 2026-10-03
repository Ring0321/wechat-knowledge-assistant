from pathlib import Path
from typing import Any

import pytest

from app.connectors.wecom.contracts import WeComError
from app.connectors.wecom.normalization import normalize_message
from app.domain.artifacts import ArtifactError
from app.domain.documents import CanonicalDocument
from app.domain.enums import SourceStatus, SourceType
from app.ingestion.rendering import markdown
from app.parsers.channels import WeChatChannelsParser
from tests.unit.test_ingestion import make_pipeline, make_work
from tests.unit.test_wecom_normalization import message


@pytest.mark.parametrize(
    ("subtype", "label"),
    [
        (1, "动态"),
        (2, "直播"),
        (3, "名片"),
        (0, "未知类型"),
        (4, "未知类型"),
        (2**32 - 1, "未知类型"),
    ],
)
def test_known_and_future_types_are_metadata_only(subtype: int, label: str) -> None:
    parsed = WeChatChannelsParser().parse(
        {"nickname": "作者", "title": "标题", "sub_type": subtype}
    )
    assert parsed.text == "" and parsed.segments == []
    assert parsed.metadata["channel_kind"] == label
    assert parsed.metadata["parse_status"] == "metadata_only"
    assert parsed.metadata["original_video_available"] is False
    assert parsed.title == ("标题" if subtype == 1 else f"作者 · 视频号{label}")


@pytest.mark.parametrize("body", [{}, {"nickname": "", "title": ""}])
def test_empty_card_is_preserved(body: dict[str, Any]) -> None:
    parsed = WeChatChannelsParser().parse(body)
    assert parsed.title == "视频号未知类型" and not parsed.text


@pytest.mark.parametrize("value", [-1, 2**32, True, 1.0, "1", None])
def test_invalid_subtype_rejected_by_transport_and_parser(value: Any) -> None:
    with pytest.raises(WeComError, match="wecom_invalid_message"):
        normalize_message(message("channels", {"sub_type": value}), "wk-account")
    with pytest.raises(ArtifactError, match="channels_metadata_invalid"):
        WeChatChannelsParser().parse({"sub_type": value})


@pytest.mark.parametrize("value", [None, [], "card", 123])
def test_malformed_body_rejected(value: Any) -> None:
    raw = message("channels", {})
    raw["channels"] = value
    with pytest.raises(WeComError):
        normalize_message(raw, "wk-account")
    with pytest.raises(ArtifactError):
        WeChatChannelsParser().parse(value)


def test_missing_body_compatibility_fallback() -> None:
    raw = message("channels", {})
    raw.pop("channels")
    result = normalize_message(raw, "wk-account")
    assert result and result.metadata["channels"] == {}


@pytest.mark.parametrize(
    "value",
    [None, 1, "x\x00", "\ud800", "x" * 65537],
    ids=["null", "integer", "nul", "surrogate", "too_long"],
)
@pytest.mark.parametrize("key", ["nickname", "title"])
def test_invalid_text_fields_fail_safely(key: str, value: Any) -> None:
    with pytest.raises(ArtifactError, match="channels_metadata_invalid"):
        WeChatChannelsParser().parse({key: value})


async def test_pipeline_ignores_download_and_identity_injections(tmp_path: Path) -> None:
    pipeline, _, media = make_pipeline()
    data = {
        "kind": "metadata",
        "channels": {
            "nickname": "作者",
            "title": "<script>hi</script>",
            "sub_type": 1,
            "media_id": "forged",
            "url": "http://localhost/private",
            "user_id": "other",
            "parser_version": 99,
            "mp4": "http://169.254.169.254/secret",
        },
    }
    work = make_work(data, source_type=SourceType.WECHAT_CHANNEL)
    result = await pipeline.prepare(work, tmp_path)
    assert result.status == SourceStatus.METADATA_ONLY and not result.text and media.calls == []
    assert set(result.metadata["channels"]) == {"nickname", "title", "sub_type"}
    assert result.metadata["parser_version"] == 8
    doc = CanonicalDocument(
        source_id=work.source_id,
        user_id=work.user_id,
        title=result.title,
        source_type=result.source_type,
        created_at=work.created_at,
        text=result.text,
        metadata=result.metadata,
    )
    rendered = markdown(doc)
    assert "metadata_only" in rendered and "作者" in rendered and "无视频转录或时间轴" in rendered
    assert "localhost" not in rendered and "169.254" not in rendered


async def test_equal_titles_do_not_prove_same_video_but_retry_is_stable(tmp_path: Path) -> None:
    pipeline, _, _ = make_pipeline()
    data = {"kind": "metadata", "channels": {"title": "同名标题", "sub_type": 1}}
    first = make_work(data, source_type=SourceType.WECHAT_CHANNEL)
    second = make_work(data, source_type=SourceType.WECHAT_CHANNEL)
    one = await pipeline.prepare(first, tmp_path)
    again = await pipeline.prepare(first, tmp_path)
    other = await pipeline.prepare(second, tmp_path)
    assert one.original.sha256 == again.original.sha256 != other.original.sha256
