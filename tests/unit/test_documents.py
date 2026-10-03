from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.domain.documents import CanonicalDocument
from app.domain.enums import SourceType


def document(**changes: object) -> CanonicalDocument:
    fields: dict[str, object] = {
        "source_id": uuid4(),
        "user_id": uuid4(),
        "title": "视频号卡片",
        "source_type": SourceType.WECHAT_CHANNEL,
        "created_at": datetime.now(UTC),
    }
    fields.update(changes)
    return CanonicalDocument.model_validate(fields)


def test_metadata_only_document_serializes_without_video() -> None:
    value = document(metadata={"status": "metadata_only", "nickname": "作者"})
    restored = CanonicalDocument.model_validate_json(value.model_dump_json())
    assert restored == value
    assert restored.text == ""
    assert restored.original_url is None


def test_collections_are_not_shared() -> None:
    first, second = document(), document()
    first.tags.append("tag")
    first.metadata["test"] = True
    assert second.tags == []
    assert second.metadata == {}


def test_aware_dates_convert_to_utc() -> None:
    timestamp = datetime(2026, 9, 22, 8, tzinfo=timezone(timedelta(hours=8)))
    assert document(created_at=timestamp).created_at == datetime(2026, 9, 22, tzinfo=UTC)


@pytest.mark.parametrize(
    "changes",
    [
        {"created_at": datetime(2026, 9, 22)},
        {"title": "   "},
        {"original_url": "file:///etc/passwd"},
        {"unrecognized": "ignored?"},
    ],
)
def test_invalid_contract_rejected(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        document(**changes)
