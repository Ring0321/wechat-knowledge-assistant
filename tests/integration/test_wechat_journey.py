"""M9 user journeys: real application/services, synthetic official HTTP only.

These gates do not prove a public callback, a real customer's permissions, or model quality.
No service/repository/parser is replaced; only WeCom and OpenAI responses are scripted.
"""

import asyncio
import hashlib
import json
from collections import Counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from email import policy
from email.parser import BytesParser
from time import monotonic
from typing import Any
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.openai_responses import OpenAIResponsesAdapter
from app.adapters.openai_vector import OpenAIVectorAdapter
from app.agent.engine import QuestionAgent
from app.agent.models import QuestionJob
from app.agent.queue import QuestionQueue
from app.agent.repository import QuestionRepository
from app.agent.worker import QuestionWorker
from app.connectors.wecom.agent import WeComQuestionBridge
from app.connectors.wecom.api import HttpWeComAPI
from app.connectors.wecom.callback import CallbackService
from app.connectors.wecom.crypto import WeComCrypto
from app.connectors.wecom.media import HttpWeComMedia
from app.connectors.wecom.persistence import WeComOutbox
from app.connectors.wecom.queue import RedisNotificationQueue
from app.connectors.wecom.replies import ReplyService
from app.connectors.wecom.store import user_identity
from app.connectors.wecom.sync import MessageSyncService
from app.connectors.wecom.tokens import RedisTokenProvider
from app.connectors.wecom.worker import WeComWorker
from app.core.health import InfrastructureHealthProbe
from app.db.models import Asset, IngestionJob, KnowledgeFile, Message, Source
from app.db.session import corporate_session, tenant_session
from app.domain.enums import JobStatus, MessageRole, SourceStatus
from app.ingestion.pipeline import IngestionPipeline
from app.ingestion.worker import IngestionWorker
from app.knowledge.processor import KnowledgeProcessor
from app.knowledge.service import KnowledgeService
from app.main import create_app
from app.operations.status import safe_error
from tests.integration.test_ingestion import Harness, s3_client
from tests.integration.test_ingestion import harness as harness
from tests.integration.test_ingestion import s3_config as s3_config
from tests.wecom_helpers import (
    TEST_KEY,
    TEST_KF,
    TEST_TOKEN,
    TEST_USER,
    callback_fixture,
    customer_message,
)

pytestmark = pytest.mark.integration

FACT = "合成项目交付日期为十月十五日。"
PRIVATE_FACT = "第二位用户的私有代号是海棠九号。"


class OfficialHTTP:
    """Small stateful wire fixture, not a substitute implementation of business services."""

    def __init__(self) -> None:
        # The real integration database retains previous scenarios. Remote IDs are
        # globally unique there, even when each HTTP fixture starts its own counters.
        self.namespace = uuid4().hex
        self.calls: Counter[str] = Counter()
        self.pages: list[list[dict[str, Any]]] = []
        self.sent: list[dict[str, Any]] = []
        self.stores: dict[str, dict[str, Any]] = {}
        self.files: dict[str, dict[str, Any]] = {}
        self.attachments: dict[tuple[str, str], dict[str, Any]] = {}
        self.search_requests: list[tuple[str, dict[str, Any]]] = []
        self.search_data: list[dict[str, Any]] = []
        self.response_inputs: list[dict[str, Any]] = []
        self.answer_quote: str | None = FACT
        self.block_sync = False
        self.sync_entered, self.sync_release = asyncio.Event(), asyncio.Event()

    @staticmethod
    def listed(items: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "object": "list",
            "data": items,
            "has_more": False,
            "first_id": items[0]["id"] if items else None,
            "last_id": items[-1]["id"] if items else None,
        }

    def hit(self, source: Source, *, tenant: str | None = None) -> dict[str, Any]:
        store_id, file_id = next(key for key in self.attachments if key[1] == source.vector_file_id)
        return {
            "file_id": file_id,
            "filename": self.files[file_id]["filename"],
            "score": 0.99,
            "attributes": {
                **self.attachments[store_id, file_id]["attributes"],
                **({"user_id": tenant} if tenant is not None else {}),
            },
            "content": [{"type": "text", "text": source.text}],
        }

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls[f"{request.method} {path}"] += 1
        body = (
            json.loads(request.content)
            if "application/json" in request.headers.get("content-type", "")
            else {}
        )
        if request.url.host == "qyapi.weixin.qq.com":
            if path == "/cgi-bin/gettoken":
                return httpx.Response(
                    200, json={"errcode": 0, "access_token": "synthetic", "expires_in": 7200}
                )
            if path == "/cgi-bin/kf/sync_msg":
                assert body["open_kfid"] == TEST_KF
                if self.block_sync:
                    self.sync_entered.set()
                    await self.sync_release.wait()
                return httpx.Response(
                    200,
                    json={
                        "errcode": 0,
                        "next_cursor": f"cursor-{self.calls['POST ' + path]}",
                        "has_more": 0,
                        "msg_list": self.pages.pop(0) if self.pages else [],
                    },
                )
            if path == "/cgi-bin/media/get":
                assert request.url.params["media_id"] == "media-1"
                return httpx.Response(
                    200,
                    content=FACT.encode(),
                    headers={
                        "content-type": "text/plain",
                        "content-disposition": 'attachment; filename="synthetic.txt"',
                    },
                )
            assert path == "/cgi-bin/kf/send_msg"
            assert body["msgtype"] == "text" and body["open_kfid"] == TEST_KF
            assert body["touser"] in (TEST_USER, "second-customer")
            self.sent.append(body)
            return httpx.Response(200, json={"errcode": 0, "msgid": body["msgid"]})
        assert request.url.host == "api.openai.com"
        if path == "/v1/vector_stores":
            if request.method == "GET":
                return httpx.Response(200, json=self.listed(list(self.stores.values())))
            store_id = f"vs_journey_{self.namespace}_{len(self.stores) + 1}"
            item = {
                "id": store_id,
                "object": "vector_store",
                "status": "completed",
                "metadata": body["metadata"],
            }
            self.stores[store_id] = item
            return httpx.Response(200, json=item)
        if path == "/v1/files":
            if request.method == "GET":
                return httpx.Response(200, json=self.listed(list(self.files.values())))
            multipart = BytesParser(policy=policy.default).parsebytes(
                f"Content-Type: {request.headers['content-type']}\r\n\r\n".encode()
                + request.content
            )
            part = next(part for part in multipart.iter_parts() if part.get_filename())
            content = part.get_payload(decode=True)
            assert isinstance(content, bytes)
            assert FACT.encode() in content or PRIVATE_FACT.encode() in content
            file_id = f"file-journey-{self.namespace}-{len(self.files) + 1}"
            item = {
                "id": file_id,
                "object": "file",
                "purpose": "assistants",
                "filename": part.get_filename(),
                "bytes": len(content),
            }
            self.files[file_id] = item
            return httpx.Response(200, json=item)
        if path.endswith("/search"):
            store_id = path.split("/")[3]
            assert body["filters"] == {
                "type": "eq",
                "key": "user_id",
                "value": self.stores[store_id]["metadata"]["pkb_user_id"],
            }
            self.search_requests.append((store_id, body))
            return httpx.Response(
                200,
                json={
                    "object": "vector_store.search_results.page",
                    "data": self.search_data,
                    "has_more": False,
                    "next_page": None,
                    "search_query": [body["query"]],
                },
            )
        if path.startswith("/v1/vector_stores/") and "/files" in path:
            store_id = path.split("/")[3]
            file_id = body["file_id"] if request.method == "POST" else path.rsplit("/", 1)[1]
            if request.method == "POST":
                self.attachments[store_id, file_id] = {
                    "id": file_id,
                    "object": "vector_store.file",
                    "vector_store_id": store_id,
                    "status": "completed",
                    "attributes": body["attributes"],
                }
            item = self.attachments.get((store_id, file_id))
            return httpx.Response(200 if item else 404, json=item or {"error": "absent"})
        assert path == "/v1/responses" and request.method == "POST"
        assert body["store"] is False and body["parallel_tool_calls"] is False
        self.response_inputs.append(body)
        if not any(item.get("type") == "function_call_output" for item in body["input"]):
            output = {
                "id": "fc_journey",
                "type": "function_call",
                "status": "completed",
                "call_id": "call_journey",
                "name": "search_knowledge",
                "arguments": json.dumps({"query": "合成项目"}, ensure_ascii=False),
            }
        else:
            answer = (
                {
                    "kind": "answer",
                    "selections": [{"evidence_id": "e1", "quote": self.answer_quote}],
                }
                if self.answer_quote
                else {"kind": "no_evidence", "selections": []}
            )
            output = {
                "id": "msg_journey",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": json.dumps(answer, ensure_ascii=False),
                        "annotations": [],
                    }
                ],
            }
        return httpx.Response(
            200,
            json={
                "id": "resp_journey",
                "object": "response",
                "status": "completed",
                "output": [output],
            },
        )


class Journey:
    def __init__(self, base: Harness, http: httpx.AsyncClient, official: OfficialHTTP) -> None:
        self.base, self.official = base, official
        settings = base.settings.model_copy(
            update={
                "knowledge_enabled": True,
                "agent_enabled": True,
                "openai_api_key": SecretStr("synthetic"),
                "openai_agent_model": "synthetic",
            }
        )
        bridge = WeComQuestionBridge(settings)
        base.settings = base.repository.settings = settings
        base.store.admission, base.repository.notifier = bridge, bridge.ingestion
        tokens = RedisTokenProvider(base.queue.redis, http, base.store.corp_id, "synthetic")
        self.api = HttpWeComAPI(http, tokens)
        self.notifications = RedisNotificationQueue(base.queue.redis, base.store.corp_id)
        self.callback = CallbackService(
            WeComCrypto(TEST_TOKEN, TEST_KEY, base.store.corp_id),
            self.notifications,
            settings.wecom_open_kfids,
        )
        self.replies = ReplyService(self.api, base.store, settings)
        self.wecom = WeComWorker(
            self.notifications,
            MessageSyncService(self.api, base.store, settings),
            self.replies,
            settings,
        )
        vector = OpenAIVectorAdapter(http, "synthetic")
        self.ingestion = IngestionWorker(
            base.queue,
            base.repository,
            IngestionPipeline(base.repository, HttpWeComMedia(http, tokens), base.objects),
            knowledge=KnowledgeProcessor(base.repository, vector, base.objects),
        )
        self.question_queue = QuestionQueue(base.queue.redis, base.store.corp_id)
        self.question_repository = QuestionRepository(
            base.store.tenant_factory, base.store.connector_factory, settings, bridge
        )
        self.questions = QuestionWorker(
            self.question_queue,
            self.question_repository,
            QuestionAgent(
                settings,
                KnowledgeService(base.store.tenant_factory, settings, vector),
                OpenAIResponsesAdapter(http, "synthetic", model="synthetic"),
            ),
        )

    @asynccontextmanager
    async def ingress(self, runtime_engine: AsyncEngine) -> AsyncIterator[httpx.AsyncClient]:
        app = create_app(
            self.base.settings,
            InfrastructureHealthProbe(
                runtime_engine, self.base.queue.redis, require_durability=True
            ),
            self.callback,
        )
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as ingress,
        ):
            yield ingress

    async def deliver(
        self, ingress: httpx.AsyncClient, messages: list[dict[str, Any]], token: str
    ) -> None:
        self.official.pages.append(messages)
        params, body = callback_fixture(corp_id=self.base.store.corp_id, event_token=token)
        before = self.official.calls.copy()
        for _ in range(2):
            response = await ingress.post("/wecom/callback", params=params, content=body)
            assert response.status_code == 200 and response.text == "success"
        assert self.official.calls == before
        assert await self.base.queue.redis.xlen(self.notifications.stream) == 1
        await self.wecom.tick()
        assert await self.base.queue.redis.xlen(self.notifications.stream) == 0

    async def index(self, *, user: str = TEST_USER) -> Source:
        # First admitted ingestion, then its durable knowledge_index dispatch.
        for _ in range(2):
            assert await self.base.repository.publish_due(self.base.queue) == 1
            assert await self.ingestion.once()
        async with tenant_session(
            self.base.store.tenant_factory, user_identity(self.base.store.corp_id, user)
        ) as session:
            source = (await session.scalars(select(Source))).one()
            jobs = list(await session.scalars(select(IngestionJob)))
            assert len(jobs) == 2
            for job in jobs:
                # Diagnose the failed stage without exposing input_data or raw errors.
                assert job.status == JobStatus.COMPLETED, {
                    "index_job": job.input_data.get("kind") == "knowledge_index",
                    "status": job.status.value,
                    "attempts": job.attempts,
                    "error": safe_error(job.error_message),
                }
            assert source.status == SourceStatus.READY and source.vector_file_id
            assert len(list(await session.scalars(select(KnowledgeFile)))) == 1
        await self.flush_replies()
        return source

    async def answer(self, *, user: str = TEST_USER) -> QuestionJob:
        assert await self.question_repository.publish_due(self.question_queue) == 1
        assert await self.questions.once()
        await self.flush_replies()
        async with tenant_session(
            self.base.store.tenant_factory, user_identity(self.base.store.corp_id, user)
        ) as session:
            jobs = list(await session.scalars(select(QuestionJob).order_by(QuestionJob.created_at)))
            assert jobs[-1].status == "completed", safe_error(jobs[-1].error_message)
            return jobs[-1]

    async def flush_replies(self) -> None:
        for _ in range(10):
            if not await self.replies.dispatch_one():
                return
        pytest.fail("reply queue did not drain within ten dispatches")


def text_message(msgid: str, content: str, *, user: str = TEST_USER) -> dict[str, Any]:
    raw = customer_message(msgid, external_userid=user)
    raw["text"] = {"content": content}
    return raw


@pytest.mark.parametrize("attachment", [False, True], ids=["note", "file"])
async def test_encrypted_callback_ingest_index_question_reply_and_replay(
    harness: Harness,
    runtime_engine: AsyncEngine,
    s3_config: dict[str, str],
    attachment: bool,
) -> None:
    official = OfficialHTTP()
    async with httpx.AsyncClient(transport=httpx.MockTransport(official)) as http:
        journey = Journey(harness, http, official)
        async with journey.ingress(runtime_engine) as ingress:
            material = (
                customer_message("journey-material", message_type="file")
                if attachment
                else text_message("journey-material", "保存：" + FACT)
            )
            await journey.deliver(ingress, [material, material], "material")
            assert (
                len(official.sent) == 1 and "收到，正在整理" in official.sent[0]["text"]["content"]
            )
            source = await journey.index()
            assert len(official.sent) == 2 and "已收录" in official.sent[1]["text"]["content"]
            official.search_data = [official.hit(source)]
            question = text_message("journey-question", "合成项目什么时候交付？")
            await journey.deliver(ingress, [question, question], "question")
            job = await journey.answer()
            assert set(job.source_snapshots) == {str(source.id)}
            answer = official.sent[-1]["text"]["content"]
            assert FACT in answer and source.title in answer
            assert "来源标题" in answer and "来源类型" in answer and "保存时间" in answer
            assert len(official.sent) == 3 and len(official.response_inputs) == 2
            async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
                assets = list(await session.scalars(select(Asset)))
                messages = list(await session.scalars(select(Message)))
                assert len(assets) == 3 and len(messages) == 3
                assert sum(message.role == MessageRole.ASSISTANT for message in messages) == 1
            client = s3_client(s3_config)
            try:
                for asset in assets:
                    result = client.get_object(Bucket=s3_config["bucket"], Key=asset.storage_key)
                    content = result["Body"].read()
                    result["Body"].close()
                    assert hashlib.sha256(content).hexdigest() == asset.sha256
                    assert (
                        str(source.user_id) in asset.storage_key
                        and str(source.id) in asset.storage_key
                    )
            finally:
                client.close()
            before_calls, before_sent = official.calls.copy(), list(official.sent)
            await journey.deliver(ingress, [material, question], "replayed-page")
            assert await harness.repository.publish_due(harness.queue) == 0
            assert await journey.question_repository.publish_due(journey.question_queue) == 0
            assert official.sent == before_sent
            for key, count in official.calls.items():
                assert count == before_calls[key] + int(key == "POST /cgi-bin/kf/sync_msg")
            async with tenant_session(harness.store.tenant_factory, source.user_id) as session:
                assert len(list(await session.scalars(select(Source)))) == 1
                assert len(list(await session.scalars(select(IngestionJob)))) == 2
                assert len(list(await session.scalars(select(QuestionJob)))) == 1
                assert len(list(await session.scalars(select(WeComOutbox)))) == 3
            assert official.calls["GET /cgi-bin/gettoken"] == 1
            assert official.calls["GET /cgi-bin/media/get"] == int(attachment)


async def test_foreign_provider_hit_never_becomes_model_evidence_or_wechat_answer(
    harness: Harness,
    runtime_engine: AsyncEngine,
) -> None:
    official = OfficialHTTP()
    async with httpx.AsyncClient(transport=httpx.MockTransport(official)) as http:
        journey = Journey(harness, http, official)
        async with journey.ingress(runtime_engine) as ingress:
            await journey.deliver(
                ingress, [text_message("first-material", "保存：" + FACT)], "first-material"
            )
            first = await journey.index()
            await journey.deliver(
                ingress,
                [text_message("second-material", "保存：" + PRIVATE_FACT, user="second-customer")],
                "second-material",
            )
            second = await journey.index(user="second-customer")
            assert first.user_id != second.user_id and first.vector_file_id != second.vector_file_id
            assert len(official.stores) == 2
            # Malicious provider lies about tenant attributes, but returns the other's file ID.
            # The real service must reject it with the local RLS-protected file journal.
            official.search_data = [official.hit(second, tenant=str(first.user_id))]
            official.answer_quote = None
            await journey.deliver(
                ingress, [text_message("first-question", "有没有相关资料？")], "first-question"
            )
            first_job = await journey.answer()
            assert first_job.source_snapshots == {}
            assert "没有找到" in official.sent[-1]["text"]["content"]
            assert PRIVATE_FACT not in json.dumps(official.response_inputs, ensure_ascii=False)
            assert PRIVATE_FACT not in official.sent[-1]["text"]["content"]
            assert str(second.id) not in json.dumps(official.response_inputs)
            tool_output = next(
                item["output"]
                for item in official.response_inputs[-1]["input"]
                if item.get("type") == "function_call_output"
            )
            assert json.loads(tool_output) == {"items": []}
            first_store = official.search_requests[-1][0]
            official.search_data = [official.hit(second)]
            official.answer_quote = PRIVATE_FACT
            await journey.deliver(
                ingress,
                [text_message("second-question", "我的私有代号是什么？", user="second-customer")],
                "second-question",
            )
            second_job = await journey.answer(user="second-customer")
            assert set(second_job.source_snapshots) == {str(second.id)}
            assert official.sent[-1]["touser"] == "second-customer"
            assert PRIVATE_FACT in official.sent[-1]["text"]["content"]
            assert first_store != official.search_requests[-1][0]
            async with corporate_session(
                harness.store.connector_factory, harness.store.corp_id
            ) as session:
                replies = list(await session.scalars(select(WeComOutbox)))
                assert all(reply.status == "sent" for reply in replies)


async def test_parallel_callbacks_stay_under_five_seconds_while_sync_worker_is_blocked(
    harness: Harness,
    runtime_engine: AsyncEngine,
) -> None:
    official = OfficialHTTP()
    official.block_sync = True
    async with httpx.AsyncClient(transport=httpx.MockTransport(official)) as http:
        journey = Journey(harness, http, official)
        async with journey.ingress(runtime_engine) as ingress:
            params, body = callback_fixture(
                corp_id=harness.store.corp_id, event_token="slow-worker"
            )
            assert (
                await ingress.post("/wecom/callback", params=params, content=body)
            ).status_code == 200
            worker = asyncio.create_task(journey.wecom.tick())
            try:
                await asyncio.wait_for(official.sync_entered.wait(), timeout=5)
                events = [
                    callback_fixture(corp_id=harness.store.corp_id, event_token=f"parallel-{index}")
                    for index in range(12)
                ]

                async def post(event: tuple[dict[str, str], bytes]) -> float:
                    params, body = event
                    started = monotonic()
                    response = await ingress.post("/wecom/callback", params=params, content=body)
                    assert response.status_code == 200 and response.text == "success"
                    return monotonic() - started

                started = monotonic()
                durations = await asyncio.wait_for(
                    asyncio.gather(*(post(event) for event in events + events[:4])), timeout=5
                )
                assert max(durations) < 5 and monotonic() - started < 5
                assert not worker.done() and not official.sync_release.is_set()
                # Includes the first notification held by the blocked worker, plus 12 unique.
                assert await harness.queue.redis.xlen(journey.notifications.stream) == 13
                assert official.calls["POST /cgi-bin/kf/sync_msg"] == 1
            finally:
                official.sync_release.set()
                await asyncio.wait_for(worker, timeout=10)
            for _ in range(12):
                await journey.wecom.tick()
            assert await harness.queue.redis.xlen(journey.notifications.stream) == 0
            assert official.calls["POST /cgi-bin/kf/sync_msg"] == 13
            assert official.sent == []
