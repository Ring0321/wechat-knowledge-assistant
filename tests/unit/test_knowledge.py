from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.db.models import Source
from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceStatus, SourceType
from app.domain.knowledge import TenantContext
from app.ingestion.worker import IngestionWorker
from app.knowledge.processor import document_bytes
from app.knowledge.service import KnowledgeService, matching_locators
from tests.unit.test_config import settings


def source() -> Source:
    return Source(
        id=uuid4(),
        user_id=uuid4(),
        title="测试视频",
        source_type=SourceType.VIDEO,
        status=SourceStatus.STORED,
        text="机器转录片段",
        tags=[],
        created_at=datetime.now(UTC),
        metadata_={
            "segments": [
                {
                    "text": "机器转录片段",
                    "method": "transcription",
                    "locator": {"start_seconds": 20, "end_seconds": 24},
                }
            ]
        },
    )


def test_index_bytes_preserve_actual_timeline_and_ignore_workflow_metadata() -> None:
    item = source()
    original = document_bytes(item)
    item.metadata_ = {**item.metadata_, "index_status": "ready", "document_sha256": "f" * 64}
    item.status = SourceStatus.READY
    assert document_bytes(item) == original
    assert "[00:20–00:24]" in original.decode()
    assert b"deferred_m6" not in original
    assert matching_locators(item, "机器转录片段") == ({"start_seconds": 20, "end_seconds": 24},)
    assert matching_locators(item, "没有依据的片段") == ()


def test_tenant_context_requires_uuid_and_has_no_store_override() -> None:
    with pytest.raises(TypeError):
        TenantContext("untrusted-input")  # type: ignore[arg-type]
    assert not hasattr(TenantContext(uuid4()), "vector_store_id")


def test_index_preserves_a_literal_status_line_inside_the_source_text() -> None:
    item = source()
    item.metadata_ = {}
    item.text = "用户原文\n- Index status: deferred_m6"
    assert item.text in document_bytes(item).decode()


def test_citations_cover_partial_cross_segment_chunks_and_reject_ambiguous_positions() -> None:
    item = source()
    item.metadata_ = {
        "segments": [
            {
                "text": "第一段开头。第一段尾句",
                "method": "transcription",
                "locator": {"start_seconds": 0, "end_seconds": 10},
            },
            {
                "text": "第二段首句。第二段结尾",
                "method": "transcription",
                "locator": {"start_seconds": 10, "end_seconds": 20},
            },
        ]
    }
    content = document_bytes(item).decode()
    excerpt = content[content.index("第一段尾句") : content.index("第二段首句") + len("第二段首句")]
    assert matching_locators(item, excerpt) == (
        {"start_seconds": 0, "end_seconds": 10},
        {"start_seconds": 10, "end_seconds": 20},
    )
    assert matching_locators(item, "第二段首句") == ({"start_seconds": 10, "end_seconds": 20},)
    # A title repeating a segment's text is not a second position in the body.
    item.title = "第二段首句"
    assert matching_locators(item, "第二段首句") == ({"start_seconds": 10, "end_seconds": 20},)
    item.metadata_ = {
        "segments": [
            {"text": "同一句话", "locator": {"start_seconds": 1}},
            {"text": "同一句话", "locator": {"start_seconds": 20}},
        ]
    }
    assert matching_locators(item, "同一句话") == ()


def test_knowledge_configuration_is_opt_in_without_media_model_requirement() -> None:
    assert not settings().knowledge_enabled
    with pytest.raises(ValidationError, match="Knowledge indexing requires"):
        settings(knowledge_enabled=True)
    data = {
        "wecom_enabled": True,
        "wecom_corp_id": "synthetic",
        "wecom_open_kfids": ["synthetic"],
        "wecom_secret": "synthetic",
        "wecom_callback_token": "synthetic",
        "wecom_encoding_aes_key": "synthetic",
        "ingestion_enabled": True,
        "s3_endpoint_url": "https://s3.example.com",
        "s3_bucket": "synthetic",
        "s3_access_key_id": "synthetic",
        "s3_secret_access_key": "synthetic",
        "knowledge_enabled": True,
        "openai_api_key": "synthetic-private",
    }
    config = settings(**data)
    assert config.knowledge_enabled and not config.media_parsing_enabled
    assert "synthetic-private" not in repr(config)
    with pytest.raises(ValidationError):
        settings(**{**data, "openai_api_key": " "})
    with pytest.raises(ValidationError):
        settings(**{**data, "knowledge_max_document_bytes": 8388609})


@pytest.mark.parametrize(
    "query,limit",
    [(" ", 10), ("字" * 8192, 10), ("hello", 0), ("hello", 51)],
    ids=["blank", "utf8_limit", "zero_limit", "high_limit"],
)
async def test_invalid_search_never_touches_database_or_provider(query: str, limit: int) -> None:
    factory, provider = Mock(), Mock()
    service = KnowledgeService(
        factory, settings().model_copy(update={"knowledge_enabled": True}), provider
    )
    with pytest.raises(ArtifactError, match="knowledge_query_invalid"):
        await service.search(TenantContext(uuid4()), query, limit=limit)
    factory.assert_not_called()
    assert not provider.mock_calls


@pytest.mark.parametrize("operation", ["knowledge_index", "knowledge_delete"])
async def test_worker_routes_knowledge_jobs_away_from_parsers(operation: str) -> None:
    dispatch_id = uuid4()
    work = Mock(input_data={"kind": operation})
    queue = Mock(
        read_entry=AsyncMock(return_value=("entry", {"dispatch_id": str(dispatch_id)})),
        acknowledge=AsyncMock(),
    )
    repository = Mock(claim=AsyncMock(return_value=work), fail=AsyncMock())
    repository.settings.ingestion_job_timeout_seconds = 1
    pipeline, knowledge = Mock(process=AsyncMock()), Mock(process=AsyncMock())
    await IngestionWorker(queue, repository, pipeline, knowledge=knowledge).once()
    knowledge.process.assert_awaited_once_with(work)
    pipeline.process.assert_not_awaited()
    queue.acknowledge.assert_awaited_once_with("entry")


async def test_missing_knowledge_runtime_preserves_retryable_failure() -> None:
    dispatch_id = uuid4()
    work = Mock(input_data={"kind": "knowledge_index"})
    queue = Mock(
        read_entry=AsyncMock(return_value=("entry", {"dispatch_id": str(dispatch_id)})),
        acknowledge=AsyncMock(),
    )
    repository = Mock(claim=AsyncMock(return_value=work), fail=AsyncMock())
    repository.settings.ingestion_job_timeout_seconds = 1
    pipeline = Mock(process=AsyncMock())
    await IngestionWorker(queue, repository, pipeline).once()
    error = repository.fail.await_args.args[1]
    assert error.code == "knowledge_disabled" and error.retryable
    pipeline.process.assert_not_awaited()
