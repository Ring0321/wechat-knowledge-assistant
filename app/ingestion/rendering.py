"""Portable Markdown derivative. Metadata and content remain distinguishable."""

import html
import json

from app.domain.documents import CanonicalDocument
from app.domain.enums import SourceType


def markdown(document: CanonicalDocument, *, index_status: str = "deferred_m6") -> str:
    title = html.escape(document.title.replace("\n", " ").replace("\r", " "))
    lines = [
        f"# {title}",
        "",
        f"- Source ID: `{document.source_id}`",
        f"- Source type: `{document.source_type.value}`",
        f"- Created at: `{document.created_at.isoformat()}`",
        f"- Index status: {index_status}",
    ]
    if document.original_url:
        lines.append("- Original URL: " + html.escape(document.original_url))
    if document.source_type == SourceType.WECHAT_CHANNEL:
        card = document.metadata.get("channels", {})
        lines.extend(
            [
                "",
                "## 视频号卡片信息",
                "",
                "以下是分享卡片字段，不代表已读取视频内容。",
                "```json",
                json.dumps(card, ensure_ascii=False, indent=2),
                "```",
                "",
                (
                    "原视频由用户主动补充；以下时间轴来自该文件，未验证它与卡片内容相同。"
                    if document.metadata.get("original_video_available")
                    else "状态：metadata_only。官方卡片未提供原视频；无视频转录或时间轴。"
                ),
            ]
        )
    content = document.text
    segments = document.metadata.get("segments")
    if isinstance(segments, list) and segments:
        parts = []
        for segment in segments:
            if not isinstance(segment, dict) or not isinstance(segment.get("text"), str):
                continue
            position = segment.get("locator", {})
            locator = json.dumps(position, ensure_ascii=False)
            method = segment.get("method")
            label = "（OCR 机器识别）" if method == "ocr" else ""
            if method in ("transcription", "vision") and isinstance(position, dict):
                start, end = position.get("start_seconds"), position.get("end_seconds")
                if isinstance(start, (int, float)) and isinstance(end, (int, float)):
                    locator = f"[{timestamp(start)}–{timestamp(end)}]"
                label = "（语音机器转录）" if method == "transcription" else "（画面描述·模型推断）"
            parts.append(f"### {html.escape(locator)}{label}\n\n{segment['text']}")
        content = "\n\n".join(parts) or content
    absent = (
        "原件或来源元数据已保存；未提取到可用正文，请查看解析状态。"
        if document.metadata.get("parser_version")
        else "原件或来源元数据已保存；此类型的内容解析尚未启用。"
    )
    if document.source_type == SourceType.WECHAT_CHANNEL:
        absent = "未提取到视频正文；卡片字段见上方。"
    lines.extend(
        [
            "",
            "## Content",
            "",
            content or absent,
            "",
        ]
    )
    return "\n".join(lines)


def timestamp(seconds: float) -> str:
    milliseconds = round(max(0, seconds) * 1000)
    minutes, remainder = divmod(milliseconds, 60_000)
    whole, fraction = divmod(remainder, 1000)
    return f"{minutes:02d}:{whole:02d}" + (f".{fraction:03d}" if fraction else "")
