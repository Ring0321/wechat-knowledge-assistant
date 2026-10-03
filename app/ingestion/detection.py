"""Pure source detection. Recognizing a URL never grants permission to fetch it."""

import re
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from pydantic import JsonValue

from app.domain.enums import SourceType

URL_PATTERN = re.compile(r"https?://[^\s<>\"\x00-\x1f]+", re.IGNORECASE)
SAVE_WORDS = ("记住", "保存", "收录", "收藏")


@dataclass(frozen=True)
class SourceInput:
    source_type: SourceType
    title: str
    original_url: str | None = None
    data: dict[str, JsonValue] = field(default_factory=dict, repr=False)


def url_source(url: str, title: str | None = None, annotation: str | None = None) -> SourceInput:
    # Only syntax normalization here. M4 must enforce SSRF before any network access.
    url = url.strip().rstrip("。，；、！？.,;!")
    try:
        parsed = urlsplit(url)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname or len(url) > 8192:
            raise ValueError
    except ValueError:
        return SourceInput(
            SourceType.OTHER, "无法识别的链接", data={"kind": "metadata", "invalid_url": True}
        )
    canonical = parsed._replace(scheme=parsed.scheme.lower()).geturl()
    kind = (
        SourceType.WECHAT_ARTICLE
        if parsed.hostname.lower() == "mp.weixin.qq.com"
        else SourceType.WEB_PAGE
    )
    return SourceInput(
        kind,
        (title or parsed.hostname)[:512],
        canonical,
        {"kind": "url", "url": canonical, "annotation": annotation},
    )


def detect_sources(
    message_type: str, text: str | None, metadata: dict[str, JsonValue], *, max_urls: int = 10
) -> list[SourceInput]:
    if message_type == "text" and text:
        items: dict[str, SourceInput] = {}
        for match in URL_PATTERN.finditer(text):
            item = url_source(match.group(), annotation=text)
            items.setdefault(item.original_url or match.group(), item)
        if items:
            # Reject oversized batches explicitly rather than silently dropping extra links.
            if len(items) > max_urls:
                from app.domain.artifacts import ArtifactError

                raise ArtifactError("too_many_urls")
            return list(items.values())
        if any(word in text for word in SAVE_WORDS):
            return [
                SourceInput(
                    SourceType.NOTE,
                    text.strip().splitlines()[0][:80] or "笔记",
                    data={"kind": "note", "text": text},
                )
            ]
        return []
    if message_type == "link":
        link = metadata.get("link")
        url = link.get("url") if isinstance(link, dict) else None
        if isinstance(link, dict) and isinstance(url, str):
            title = link.get("title")
            return [url_source(url, title if isinstance(title, str) else None)]
    media_types = {
        "image": SourceType.IMAGE,
        "voice": SourceType.AUDIO,
        "video": SourceType.VIDEO,
        "file": SourceType.OTHER,
    }
    if message_type in media_types:
        media_id, filename = metadata.get("media_id"), metadata.get("file_name")
        if isinstance(media_id, str):
            title = (
                filename
                if isinstance(filename, str) and filename.strip()
                else {"image": "图片", "voice": "语音", "video": "视频", "file": "文件"}[
                    message_type
                ]
            )
            return [
                SourceInput(
                    media_types[message_type],
                    title[:512],
                    data={"kind": "media", "media_id": media_id, "filename": filename},
                )
            ]
    if message_type == "channels":
        return [
            SourceInput(
                SourceType.WECHAT_CHANNEL,
                "视频号分享",
                data={"kind": "metadata", "channels": metadata.get("channels", {})},
            )
        ]
    return [
        SourceInput(
            SourceType.OTHER, "附件信息", data={"kind": "metadata", "message_type": message_type}
        )
    ]
