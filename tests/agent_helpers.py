"""Deterministic model turns around the real M3/M6 storage and question workers."""

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import uuid4

from pydantic import JsonValue, SecretStr
from sqlalchemy import select

from app.agent.engine import QuestionAgent
from app.agent.models import QuestionDispatch, QuestionJob
from app.agent.queue import QuestionQueue
from app.agent.repository import QuestionRepository
from app.agent.worker import QuestionWorker
from app.connectors.wecom.agent import WeComQuestionBridge
from app.connectors.wecom.contracts import NormalizedMessage
from app.connectors.wecom.normalization import normalize_message
from app.connectors.wecom.persistence import WeComOutbox
from app.connectors.wecom.store import user_identity
from app.db.models import Source
from app.db.session import tenant_session
from app.domain.agent import ModelTurn, ResponsesProvider, ToolCall
from app.domain.artifacts import ArtifactError
from app.domain.knowledge import TenantContext
from app.knowledge.processor import KnowledgeProcessor
from app.knowledge.service import KnowledgeService
from tests.integration.test_ingestion import Harness
from tests.integration.test_knowledge import execute, ingest
from tests.knowledge_helpers import FakeVector
from tests.wecom_helpers import TEST_KF, TEST_USER, customer_message


def tool_turn(name: str, arguments: dict[str, JsonValue], call_id: str = "call-1") -> ModelTurn:
    call = ToolCall(call_id, name, arguments)
    return ModelTurn(
        items=(
            {
                "type": "function_call",
                "call_id": call_id,
                "name": name,
                "arguments": json.dumps(arguments, ensure_ascii=False),
            },
        ),
        calls=(call,),
    )


def answer_turn(*selections: tuple[str, str]) -> ModelTurn:
    return ModelTurn(
        items=(),
        text=json.dumps(
            {
                "kind": "answer" if selections else "no_evidence",
                "selections": [
                    {"evidence_id": evidence_id, "quote": quote}
                    for evidence_id, quote in selections
                ],
            },
            ensure_ascii=False,
        ),
    )


class ScriptedResponses:
    def __init__(self, turns: Sequence[ModelTurn | ArtifactError]) -> None:
        self.turns = list(turns)
        self.inputs: list[tuple[dict[str, JsonValue], ...]] = []

    async def respond(
        self,
        *,
        instructions: str,
        input_items: tuple[dict[str, JsonValue], ...],
        tools: tuple[dict[str, JsonValue], ...],
        output_schema: dict[str, JsonValue],
        tool_choice: Literal["auto", "none"] = "auto",
    ) -> ModelTurn:
        assert instructions and tools and output_schema and tool_choice == "auto"
        self.inputs.append(input_items)
        assert self.turns, "unexpected additional model request"
        turn = self.turns.pop(0)
        if isinstance(turn, ArtifactError):
            raise turn
        return turn


def question_message(
    content: str = "测试知识是什么？",
    *,
    user: str = TEST_USER,
    open_kfid: str = TEST_KF,
    msgid: str | None = None,
) -> NormalizedMessage:
    raw = customer_message(msgid or uuid4().hex, external_userid=user, open_kfid=open_kfid)
    raw["text"] = {"content": content}
    result = normalize_message(raw, open_kfid)
    assert result is not None
    return result


@dataclass
class AgentHarness:
    base: Harness
    bridge: WeComQuestionBridge
    repository: QuestionRepository
    queue: QuestionQueue
    vector: FakeVector
    knowledge: KnowledgeService
    processor: KnowledgeProcessor

    @classmethod
    def enable(cls, base: Harness) -> "AgentHarness":
        settings = base.settings.model_copy(
            update={
                "agent_enabled": True,
                "knowledge_enabled": True,
                "openai_agent_model": "synthetic",
                "openai_api_key": SecretStr("synthetic"),
            }
        )
        bridge = WeComQuestionBridge(settings)
        base.settings = base.repository.settings = settings
        base.store.admission = bridge
        base.repository.notifier = bridge.ingestion
        vector = FakeVector()
        return cls(
            base,
            bridge,
            QuestionRepository(
                base.store.tenant_factory, base.store.connector_factory, settings, bridge
            ),
            QuestionQueue(base.queue.redis, base.store.corp_id),
            vector,
            KnowledgeService(base.store.tenant_factory, settings, vector),
            KnowledgeProcessor(base.repository, vector, base.objects),
        )

    def worker(self, provider: ResponsesProvider) -> QuestionWorker:
        return QuestionWorker(
            self.queue,
            self.repository,
            QuestionAgent(self.base.settings, self.knowledge, provider),
        )

    async def question(
        self, content: str = "测试知识是什么？", *, user: str = TEST_USER, open_kfid: str = TEST_KF
    ) -> QuestionDispatch:
        message = question_message(content, user=user, open_kfid=open_kfid)
        assert await self.base.store.persist_message(message, None)
        return await self.dispatch_for(message)

    async def dispatch_for(self, message: NormalizedMessage) -> QuestionDispatch:
        from app.db.models import Message

        context = user_identity(self.base.store.corp_id, message.external_userid)
        async with tenant_session(self.base.store.tenant_factory, context) as session:
            return (
                await session.scalars(
                    select(QuestionDispatch)
                    .join(QuestionJob, QuestionJob.id == QuestionDispatch.job_id)
                    .join(Message, Message.id == QuestionJob.message_id)
                    .where(Message.wechat_msg_id == message.msgid)
                )
            ).one()

    async def job(self, dispatch: QuestionDispatch) -> QuestionJob:
        async with tenant_session(self.base.store.tenant_factory, dispatch.user_id) as session:
            job = await session.get(QuestionJob, dispatch.job_id)
            assert job is not None
            return job

    async def outbox(self, dispatch: QuestionDispatch) -> list[WeComOutbox]:
        job = await self.job(dispatch)
        async with tenant_session(self.base.store.tenant_factory, dispatch.user_id) as session:
            return list(
                await session.scalars(
                    select(WeComOutbox)
                    .where(WeComOutbox.inbound_message_id == job.message_id)
                    .order_by(WeComOutbox.purpose)
                )
            )

    async def index(
        self, content: str = "保存：测试知识", *, user: str = TEST_USER
    ) -> tuple[TenantContext, Source]:
        # Setup notifications must not consume the question's real WeCom send quota.
        previous = self.base.settings.wecom_auto_reply
        self.base.settings.wecom_auto_reply = False
        try:
            context, source_id, dispatch_id = await ingest(self.base, content=content, user=user)
            assert await execute(self.base, self.processor, dispatch_id) is None
        finally:
            self.base.settings.wecom_auto_reply = previous
        async with tenant_session(self.base.store.tenant_factory, context.user_id) as session:
            source = await session.get(Source, source_id)
            assert source is not None
            return context, source

    async def due(self, dispatch: QuestionDispatch) -> None:
        due = datetime.now(UTC) - timedelta(seconds=1)
        async with tenant_session(self.base.store.tenant_factory, dispatch.user_id) as session:
            job = await session.get(QuestionJob, dispatch.job_id)
            record = await session.get(QuestionDispatch, dispatch.id)
            assert job is not None and record is not None
            job.next_retry_at = due
            record.available_at, record.published_at = due, None
        digest = hashlib.sha256(str(dispatch.id).encode()).hexdigest()
        await self.queue.redis.delete(self.queue.stream + ":dedup:" + digest)

    async def run(self, dispatch: QuestionDispatch, provider: ResponsesProvider) -> QuestionJob:
        await self.queue.enqueue(dispatch.id)
        assert await self.worker(provider).once()
        return await self.job(dispatch)
