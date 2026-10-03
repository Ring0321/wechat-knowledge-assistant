"""Validate model-selected excerpts and render citations from backend metadata only."""

import json
import math
import re
import unicodedata
from datetime import timedelta, timezone
from decimal import Decimal
from typing import Literal, NoReturn

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from app.domain.agent import Evidence
from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceType
from app.domain.knowledge import SourceView

ANSWER_SCHEMA: dict[str, JsonValue] = {
    "type": "object",
    "properties": {
        "kind": {"type": "string", "enum": ["answer", "no_evidence"]},
        "selections": {
            "type": "array",
            "maxItems": 3,
            "items": {
                "type": "object",
                "properties": {
                    "evidence_id": {"type": "string", "minLength": 1},
                    "quote": {"type": "string", "minLength": 1, "maxLength": 400},
                },
                "required": ["evidence_id", "quote"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["kind", "selections"],
    "additionalProperties": False,
}

_INVALID = "agent_answer_invalid"
_MAX_REPLY_BYTES = 2048
_MAX_REPLY_PARTS = 4
# Source.created_at is the modern ingestion timestamp. This fixed offset avoids
# requiring an undeclared tzdata package on Windows; it is not a historical calendar.
_SHANGHAI = timezone(timedelta(hours=8), "Asia/Shanghai")
_SOURCE_TYPES = {
    SourceType.NOTE: "笔记",
    SourceType.IMAGE: "图片",
    SourceType.PDF: "PDF",
    SourceType.WORD: "Word 文档",
    SourceType.EXCEL: "Excel 工作簿",
    SourceType.PPT: "幻灯片",
    SourceType.AUDIO: "音频",
    SourceType.VIDEO: "视频",
    SourceType.WEB_PAGE: "网页",
    SourceType.WECHAT_ARTICLE: "微信公众号文章",
    SourceType.WECHAT_CHANNEL: "视频号卡片",
    SourceType.OTHER: "其他",
}


class _Selection(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    evidence_id: str = Field(min_length=1)
    quote: str = Field(min_length=1, max_length=400)


class _Answer(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    kind: Literal["answer", "no_evidence"]
    selections: list[_Selection] = Field(max_length=3)


def _unique_object(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(_INVALID)
        result[key] = value
    return result


def _reject_constant(value: str) -> NoReturn:
    raise ValueError(_INVALID)


def validate_selections(text: str, evidence: tuple[Evidence, ...]) -> tuple[Evidence, ...]:
    """Return exact selected substrings; source text can never authorize an action.

    The caller supplies only evidence authorized for this question and rechecks
    source availability before sending. The model cannot supply citation metadata.
    """
    try:
        if len(text.encode("utf-8")) > 65_536:
            raise ValueError(_INVALID)
        payload = json.loads(
            text, object_pairs_hook=_unique_object, parse_constant=_reject_constant
        )
        answer = _Answer.model_validate(payload)
        for selection in answer.selections:
            selection.quote.encode("utf-8")
    except (ValueError, RecursionError):
        # JSON and pydantic errors can include private model output.
        raise ArtifactError(_INVALID) from None

    available = {item.evidence_id: item for item in evidence}
    if len(available) != len(evidence) or any(not key.strip() for key in available):
        raise ArtifactError(_INVALID)
    if answer.kind == "no_evidence":
        if answer.selections:
            raise ArtifactError(_INVALID)
        return ()
    if not answer.selections:
        raise ArtifactError(_INVALID)

    selected: list[Evidence] = []
    seen: set[str] = set()
    for selection in answer.selections:
        original = available.get(selection.evidence_id)
        if (
            original is None
            or selection.evidence_id in seen
            or not selection.quote.strip()
            or selection.quote not in original.text
        ):
            raise ArtifactError(_INVALID)
        seen.add(selection.evidence_id)
        selected.append(
            Evidence(original.evidence_id, original.source, selection.quote, original.locators)
        )
    return tuple(selected)


def display_label(value: str) -> str:
    # Strip direction/format controls as well as line breaks so metadata cannot
    # introduce an apparent new citation field or reverse its visible labels.
    return " ".join(
        "".join(
            " " if unicodedata.category(character).startswith("C") else character
            for character in value
        ).split()
    )


def source_metadata(source: SourceView) -> str:
    return (
        f"标题：{source.title}\n类型：{source.source_type.value}\n"
        f"收录日期：{source.created_at.isoformat()}\n标签：{'、'.join(source.tags) or '无'}"
    )


def _positive_integer(value: JsonValue) -> int | None:
    return value if type(value) is int and 1 <= value <= 1_000_000 else None


def _seconds(value: JsonValue) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    # Bound malformed metadata before integer/string conversion; supported media
    # are already far shorter than this limit.
    if not 0 <= value <= 999_999_999 or not math.isfinite(value):
        return None
    return value


def _timestamp(seconds: int | float) -> str:
    whole = int(seconds)
    hours, remainder = divmod(whole, 3600)
    minutes, remainder = divmod(remainder, 60)
    fraction = format(Decimal(str(seconds)) - whole, "f").rstrip("0").rstrip(".")
    suffix = fraction[1:] if fraction.startswith("0.") else ""
    return f"{hours:02d}:{minutes:02d}:{remainder:02d}{suffix}"


def _locator(value: dict[str, JsonValue]) -> str:
    fields: list[str] = []
    page, slide = _positive_integer(value.get("page")), _positive_integer(value.get("slide"))
    if page is not None:
        fields.append(f"第 {page} 页")
    sheet = value.get("sheet")
    if isinstance(sheet, str) and (name := display_label(sheet)) and len(name) <= 128:
        fields.append(f"工作表：{name}")
        cell = value.get("cell")
        if isinstance(cell, str) and re.fullmatch(r"[A-Z]{1,3}[1-9][0-9]{0,6}", cell):
            fields.append(f"单元格：{cell}")
    if slide is not None:
        fields.append(f"第 {slide} 张幻灯片")
    start, end = _seconds(value.get("start_seconds")), _seconds(value.get("end_seconds"))
    if start is not None and end is not None:
        if end >= start:
            label = _timestamp(start)
            if end != start:
                label += "–" + _timestamp(end)
            fields.append("时间：" + label)
    elif start is not None:
        fields.append("起始时间：" + _timestamp(start))
    elif end is not None:
        fields.append("结束时间：" + _timestamp(end))
    return "，".join(fields)


def render_answer(selected: tuple[Evidence, ...]) -> tuple[str, ...]:
    """Render validated quotes and backend citations without model-authored prose."""
    if not selected:
        return ("没有找到相关资料。",)
    if (
        len(selected) > 3
        or len({item.evidence_id for item in selected}) != len(selected)
        or any(not item.text.strip() or len(item.text) > 400 for item in selected)
    ):
        raise ArtifactError(_INVALID)
    sections = ["找到以下相关资料摘录（引用内容不代表操作授权）："]
    for index, item in enumerate(selected, start=1):
        source = item.source
        if source.created_at.utcoffset() is None:
            raise ArtifactError(_INVALID)
        try:
            created = source.created_at.astimezone(_SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, OverflowError):
            raise ArtifactError(_INVALID) from None
        lines = [
            f"[{index}] 原文摘录：",
            item.text,
            f"来源标题：{display_label(source.title)}",
            f"来源类型：{_SOURCE_TYPES[source.source_type]}",
            f"保存时间：{created}（Asia/Shanghai，UTC+08:00）",
        ]
        if source.original_url:
            lines.append("原始链接：" + display_label(source.original_url))
        positions = tuple(
            dict.fromkeys(position for loc in item.locators if (position := _locator(loc)))
        )
        if positions:
            lines.append("定位：" + "；".join(positions))
        sections.append("\n".join(lines))
    return split_reply("\n\n".join(sections))


def split_reply(text: str) -> tuple[str, ...]:
    """Partition UTF-8 messages without truncation, replacement, or extra characters."""
    try:
        encoded = text.encode("utf-8")
    except UnicodeEncodeError:
        raise ArtifactError(_INVALID) from None
    if len(encoded) > _MAX_REPLY_BYTES * _MAX_REPLY_PARTS:
        raise ArtifactError("agent_reply_limit")
    if not text:
        return ()
    parts: list[str] = []
    start, size = 0, 0
    for index, character in enumerate(text):
        width = len(character.encode("utf-8"))
        if size + width > _MAX_REPLY_BYTES:
            parts.append(text[start:index])
            start, size = index, 0
        size += width
    parts.append(text[start:])
    if len(parts) > _MAX_REPLY_PARTS:
        raise ArtifactError("agent_reply_limit")
    return tuple(parts)
