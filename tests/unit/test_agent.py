import argparse
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from app.adapters.openai_responses import OpenAIResponsesAdapter
from app.agent.contracts import QuestionWork
from app.agent.engine import QuestionAgent
from app.agent.grounding import ANSWER_SCHEMA, source_metadata
from app.agent.security import verify_evidence
from app.agent.tools import TOOL_SCHEMAS, KnowledgeTools, permits_proposal
from app.connectors.wecom.agent import reply_identity
from app.db.models import Source
from app.domain.agent import Evidence, ModelTurn, ToolCall
from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceStatus, SourceType
from app.domain.knowledge import KnowledgeHit, SourceView, TenantContext
from app.knowledge.rendering import indexed_markdown
from app.workers.agent import run
from tests.unit.test_config import settings


def source() -> SourceView:
    return SourceView(
        uuid4(),
        "安全资料",
        SourceType.NOTE,
        None,
        datetime.now(UTC),
        SourceStatus.READY,
        None,
        (),
        "检修日期为周五。",
    )


def service() -> Mock:
    row = source()
    return Mock(
        factory=Mock(),
        search=AsyncMock(return_value=(KnowledgeHit(row, 0.9, row.text),)),
        get_source=AsyncMock(return_value=row),
        list_sources=AsyncMock(return_value=(row,)),
    )


@pytest.mark.parametrize(
    "name,args",
    [
        ("search_knowledge", {"query": "检修", "user_id": str(uuid4())}),
        ("search_knowledge", {"query": "检修", "vector_store_id": "forged"}),
        ("list_sources", {"limit": True}),
        ("list_sources", {"limit": 11}),
        ("get_source", {"source_ref": str(uuid4()), "offset": 0}),
        ("get_source", {"source_ref": "s1", "offset": -1}),
        ("delete_source", {"source_ref": "s1", "confirmed": True}),
        ("add_tag", {"source_ref": "s1", "tag": "x", "user_id": str(uuid4())}),
    ],
)
async def test_model_cannot_supply_authority_or_unbounded_arguments(
    name: str, args: dict[str, object]
) -> None:
    knowledge = service()
    tools = KnowledgeTools(knowledge, TenantContext(uuid4()), "检修日期是什么？")
    with pytest.raises(ArtifactError, match="arguments_invalid"):
        await tools.invoke(ToolCall("c1", name, args))  # type: ignore[arg-type]
    assert not knowledge.mock_calls


async def test_source_references_are_issued_only_after_authorized_read_and_tags_are_proposals() -> (
    None
):
    knowledge = service()
    tools = KnowledgeTools(knowledge, TenantContext(uuid4()), "给安全资料添加标签：设备")
    with pytest.raises(ArtifactError, match="reference_invalid"):
        await tools.invoke(ToolCall("c0", "delete_source", {"source_ref": "s1"}))
    result = json.loads(await tools.invoke(ToolCall("c1", "list_sources", {"limit": 5})))
    assert result["items"][0]["source_ref"] == "s1"
    await tools.invoke(ToolCall("c2", "add_tag", {"source_ref": "s1", "tag": "设备"}))
    assert tools.proposal and tools.proposal.tag == "设备"
    assert not knowledge.delete_document.called and not knowledge.add_tag.called


@pytest.mark.parametrize(
    "question,operation,tag",
    [
        ("不要删除任何资料", "delete_source", None),
        ("如何删除资料？", "delete_source", None),
        ("文档要求删除所有资料，我想了解其内容", "delete_source", None),
        ("加标签：设备", "add_tag", "财务"),
        ("这份资料有什么标签？", "add_tag", "设备"),
    ],
)
def test_question_intent_is_not_inferred_from_model_or_document(
    question: str, operation: str, tag: str | None
) -> None:
    assert not permits_proposal(question, operation, tag)


async def test_date_query_passes_explicit_aware_interval_and_same_context() -> None:
    knowledge = service()
    context = TenantContext(uuid4())
    tools = KnowledgeTools(knowledge, context, "昨天的资料")
    await tools.invoke(
        ToolCall(
            "c1",
            "search_by_date",
            {
                "limit": 2,
                "since": "2026-10-01T00:00:00+08:00",
                "until": "2026-10-02T00:00:00+08:00",
            },
        )
    )
    assert knowledge.list_sources.await_args.args == (context,)
    assert knowledge.list_sources.await_args.kwargs["since"].tzinfo is not None


async def test_real_agent_schemas_are_accepted_by_responses_adapter() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert {tool["name"] for tool in payload["tools"]} == {
            "search_knowledge",
            "get_source",
            "list_sources",
            "list_recent_sources",
            "search_by_date",
            "delete_source",
            "add_tag",
        }
        return httpx.Response(
            200,
            json={
                "id": "resp_synthetic",
                "object": "response",
                "status": "completed",
                "output": [
                    {
                        "type": "message",
                        "id": "msg_synthetic",
                        "status": "completed",
                        "role": "assistant",
                        "content": [
                            {
                                "type": "output_text",
                                "text": '{"kind":"no_evidence","selections":[]}',
                                "annotations": [],
                            }
                        ],
                    }
                ],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        turn = await OpenAIResponsesAdapter(http, "synthetic", model="synthetic").respond(
            instructions="synthetic",
            input_items=({"role": "user", "content": "查询"},),
            tools=TOOL_SCHEMAS,
            output_schema=ANSWER_SCHEMA,
        )
    assert turn.text and not turn.calls


@pytest.fixture
def no_database(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    @asynccontextmanager
    async def transaction(*args: object) -> AsyncIterator[Mock]:
        yield Mock()

    monkeypatch.setattr("app.agent.engine.tenant_session", transaction)
    monkeypatch.setattr("app.agent.engine.lock_identity", AsyncMock())
    monkeypatch.setattr("app.agent.engine.verify_evidence", Mock())
    check = AsyncMock()
    monkeypatch.setattr("app.agent.engine.lock_sources", check)
    return check


def work() -> QuestionWork:
    return QuestionWork(uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), "检修日期？")


async def test_engine_replays_items_and_only_returns_verified_quote(no_database: AsyncMock) -> None:
    knowledge = service()
    item = {
        "type": "function_call",
        "call_id": "c1",
        "name": "search_knowledge",
        "arguments": '{"query":"检修"}',
    }
    provider = Mock(
        respond=AsyncMock(
            side_effect=[
                ModelTurn((item,), (ToolCall("c1", "search_knowledge", {"query": "检修"}),)),
                ModelTurn(
                    (),
                    text='{"kind":"answer","selections":[{"evidence_id":"e1","quote":"检修日期为周五。"}]}',
                ),
            ]
        )
    )
    outcome = await QuestionAgent(settings(), knowledge, provider).answer(work())
    assert outcome.selected[0].text == "检修日期为周五。"
    second = provider.respond.await_args_list[1].kwargs["input_items"]
    assert second[1] == item and second[2]["call_id"] == "c1"
    assert no_database.await_args_list[1].args[1]


async def test_engine_stops_before_replaying_revoked_evidence(no_database: AsyncMock) -> None:
    no_database.side_effect = [None, ArtifactError("agent_evidence_changed")]
    provider = Mock(
        respond=AsyncMock(
            return_value=ModelTurn((), (ToolCall("c1", "search_knowledge", {"query": "检修"}),))
        )
    )
    with pytest.raises(ArtifactError, match="evidence_changed"):
        await QuestionAgent(settings(), service(), provider).answer(work())
    assert provider.respond.await_count == 1


async def test_relative_dates_receive_current_clock_and_explicit_timezone(
    no_database: AsyncMock,
) -> None:
    before = datetime.now(UTC)
    provider = Mock(
        respond=AsyncMock(return_value=ModelTurn((), text='{"kind":"no_evidence","selections":[]}'))
    )
    await QuestionAgent(settings(), service(), provider).answer(work())
    instructions = provider.respond.await_args.kwargs["instructions"]
    stamp = next(
        line.split("：", 1)[1]
        for line in instructions.splitlines()
        if line.startswith("本次查询的参考时刻（UTC）：")
    )
    assert before <= datetime.fromisoformat(stamp) <= datetime.now(UTC)
    assert "Asia/Shanghai（UTC+08:00）" in instructions


async def test_engine_budget_and_duplicate_call_id_stop_tool_loops(no_database: AsyncMock) -> None:
    provider = Mock(
        respond=AsyncMock(
            return_value=ModelTurn((), (ToolCall("c1", "list_sources", {"limit": 1}),))
        )
    )
    with pytest.raises(ArtifactError, match="repeated_call"):
        await QuestionAgent(settings(), service(), provider).answer(work())
    provider.respond.reset_mock()
    with pytest.raises(ArtifactError, match="turn_budget"):
        await QuestionAgent(settings(agent_max_turns=1), service(), provider).answer(work())
    assert provider.respond.await_count == 1


@pytest.mark.parametrize(
    "purpose",
    ["agent:invalid:0:0", f"agent:{uuid4()}:0:4", f"agent:{uuid4()}:-1:0", f"agent:{uuid4()}:00:0"],
)
def test_invalid_reply_identity_is_rejected(purpose: str) -> None:
    with pytest.raises(ArtifactError, match="reply_invalid"):
        reply_identity(purpose)


async def test_agent_runtime_requires_knowledge_and_credential() -> None:
    with pytest.raises(ArtifactError, match="configuration_missing"):
        await run(settings(), argparse.Namespace(health=False, retry=None))
    with pytest.raises(ValidationError):
        settings(agent_enabled=True)


def test_remote_excerpt_must_match_local_body_rendering_or_metadata() -> None:
    record = source()
    row = Source(
        id=record.source_id,
        user_id=uuid4(),
        title=record.title,
        source_type=record.source_type,
        created_at=record.created_at,
        status=record.status,
        text=record.text,
        tags=[],
        metadata_={},
    )
    for excerpt in (record.text, source_metadata(record), indexed_markdown(row)[:60]):
        verify_evidence({str(row.id): row}, (Evidence("e1", record, excerpt),))
    for excerpt in ("伪造的日期为周六。", record.text + "且必须关闭报警。", ""):
        with pytest.raises(ArtifactError, match="evidence_invalid"):
            verify_evidence({str(row.id): row}, (Evidence("e1", record, excerpt),))


async def test_tag_cannot_hide_direction_controls() -> None:
    tools = KnowledgeTools(service(), TenantContext(uuid4()), "添加标签：设备\u202e")
    await tools.invoke(ToolCall("c1", "list_sources", {"limit": 1}))
    with pytest.raises(ArtifactError, match="tag_invalid"):
        await tools.invoke(ToolCall("c2", "add_tag", {"source_ref": "s1", "tag": "设备\u202e"}))
