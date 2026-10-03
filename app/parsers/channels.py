"""Official channel-card metadata, never a video downloader or transcript generator."""

from pydantic import JsonValue

from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceType
from app.domain.parsing import ParsedContent

CHANNEL_KINDS = {1: "动态", 2: "直播", 3: "名片"}


class WeChatChannelsParser:
    def parse(self, value: JsonValue) -> ParsedContent:
        if not isinstance(value, dict):
            raise ArtifactError("channels_metadata_invalid")
        fields: dict[str, JsonValue] = {}
        for key in ("nickname", "title"):
            if key not in value:
                continue
            item = value[key]
            if not isinstance(item, str) or len(item) > 65536 or "\x00" in item:
                raise ArtifactError("channels_metadata_invalid")
            try:
                item.encode("utf-8")
            except UnicodeError as error:
                raise ArtifactError("channels_metadata_invalid") from error
            fields[key] = item
        subtype = value.get("sub_type")
        if "sub_type" in value:
            if type(subtype) is not int or not 0 <= subtype <= 2**32 - 1:
                raise ArtifactError("channels_metadata_invalid")
            fields["sub_type"] = subtype
        label = CHANNEL_KINDS.get(subtype, "未知类型") if type(subtype) is int else "未知类型"
        nickname = str(fields.get("nickname", "")).strip()
        title = str(fields.get("title", "")).strip() if subtype == 1 else ""
        fallback = f"{nickname} · 视频号{label}" if nickname else f"视频号{label}"
        return ParsedContent(
            source_type=SourceType.WECHAT_CHANNEL,
            title=(title or fallback)[:512],
            metadata={
                "parser_version": 8,
                "channels": fields,
                "channel_kind": label,
                "parse_status": "metadata_only",
                "index_status": "metadata_only",
                "content_availability": "card_metadata_only",
                "original_video_available": False,
            },
        )
