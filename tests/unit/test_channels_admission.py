"""Clock and routing boundaries that require no database or external provider."""

from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

import pytest
from pydantic import JsonValue
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.wecom.channels import WeComChannelSupplementBridge, _fresh, _timestamp
from app.connectors.wecom.contracts import NormalizedMessage
from app.core.config import Settings


@pytest.mark.parametrize(
    ("kind", "content"),
    [
        ("text", "什么是视频号？"),
        ("text", "请补充视频 00000000-0000-0000-0000-000000000000"),
        ("text", "记住：补充视频只是一条笔记"),
        ("text", "取消补充视频\n这不是取消命令"),
        ("channels", "补充视频 00000000-0000-0000-0000-000000000000"),
        ("file", None),
        ("image", None),
        ("voice", None),
    ],
)
async def test_unrelated_content_does_not_touch_database(kind: str, content: str | None) -> None:
    message = NormalizedMessage(
        "synthetic-message", "synthetic-account", "synthetic-user", datetime.now(UTC), kind, content
    )
    # A sentinel lacking all Session methods proves these routes cannot create an intent.
    settings = Settings.model_validate(
        {
            "database_url": "postgresql+asyncpg://synthetic:synthetic@localhost/synthetic_test",
            "redis_url": "redis://localhost/0",
        }
    )
    assert not await WeComChannelSupplementBridge(settings).admit(
        cast(AsyncSession, object()), uuid4(), uuid4(), message
    )


@pytest.mark.parametrize(
    ("offset", "accepted"),
    [(-601, False), (-600, True), (-599, True), (0, True), (60, True), (61, False)],
)
def test_command_recency_and_future_clock_tolerance(offset: int, accepted: bool) -> None:
    now = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
    assert _fresh(now + timedelta(seconds=offset), now) is accepted
    assert not _fresh(now.replace(tzinfo=None), now)


@pytest.mark.parametrize("value", [None, True, 1, "", "not-a-date", "2026-10-02T12:00:00", {}, []])
def test_persisted_expiry_requires_a_timezone(value: JsonValue) -> None:
    assert _timestamp(value) is None


def test_persisted_offset_timestamp_is_compared_as_utc() -> None:
    assert _timestamp("2026-10-02T20:00:00+08:00") == datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
