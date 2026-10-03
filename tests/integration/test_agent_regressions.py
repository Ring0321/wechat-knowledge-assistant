"""Independent review regressions with real row locks and persisted evidence."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.agent.contracts import AgentOutcome
from app.agent.models import AgentAction
from app.agent.repository import QuestionRepository
from app.connectors.wecom.agent import guard_agent_reply
from app.connectors.wecom.replies import ReplyService
from app.db.session import tenant_session
from app.domain.agent import Evidence
from app.domain.artifacts import ArtifactError
from app.domain.knowledge import VectorHit
from app.knowledge.service import view
from tests.agent_helpers import AgentHarness, ScriptedResponses, tool_turn
from tests.integration.test_agent import agent as agent
from tests.integration.test_agent import claim, proposal
from tests.integration.test_ingestion import harness as harness
from tests.integration.test_ingestion import s3_config as s3_config
from tests.wecom_helpers import FakeAPI

pytestmark = pytest.mark.integration


async def test_forged_remote_body_with_valid_mapping_never_reaches_model(
    agent: AgentHarness,
) -> None:
    context, source = await agent.index()
    store_id = agent.vector.stores[context.user_id]
    file_id = source.vector_file_id
    assert file_id is not None
    marker = "正确文件属性不能证明这段虚构内容。"
    agent.vector.extra_hits = (
        VectorHit(file_id, 1.0, marker, agent.vector.attributes[store_id, file_id]),
    )
    dispatch = await agent.question()
    provider = ScriptedResponses([tool_turn("search_knowledge", {"query": "测试知识"})])
    result = await agent.run(dispatch, provider)
    assert result.status == "failed" and result.error_message == "agent_evidence_invalid"
    assert len(provider.inputs) == 1 and marker not in str(provider.inputs)
    assert marker not in "".join(result.result_parts)


async def test_commit_rechecks_excerpt_bytes_against_local_source(agent: AgentHarness) -> None:
    _, source = await agent.index()
    dispatch = await agent.question()
    work = await claim(agent, dispatch)
    with pytest.raises(ArtifactError, match="agent_evidence_invalid"):
        await agent.repository.complete(
            work, AgentOutcome(selected=(Evidence("e1", view(source), "从未存在的正文"),))
        )
    assert (await agent.job(dispatch)).answer_message_id is None
    assert await agent.outbox(dispatch) == []


@pytest.mark.parametrize("state", ["expired", "executed"])
async def test_delayed_proposal_is_not_sent_after_expiry_or_execution(
    agent: AgentHarness, state: str
) -> None:
    source, dispatch, token = await proposal(agent, "add_tag")
    if state == "expired":
        async with tenant_session(agent.base.store.tenant_factory, source.user_id) as session:
            await session.execute(
                update(AgentAction)
                .where(AgentAction.question_job_id == dispatch.job_id)
                .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
            )
    else:
        confirmation = await agent.question("确认 " + token)
        await agent.run(confirmation, ScriptedResponses([]))
    api = FakeAPI()
    assert await ReplyService(api, agent.base.store, agent.base.settings).dispatch_one()
    assert api.replies == []
    assert (await agent.outbox(dispatch))[0].status == "failed"


async def test_manual_retry_waits_until_current_reply_send_guard_finishes(
    agent: AgentHarness, urls: dict[str, str]
) -> None:
    dispatch = await agent.question()
    await agent.run(dispatch, ScriptedResponses([ArtifactError("synthetic_terminal_failure")]))
    item = (await agent.outbox(dispatch))[0]
    engine = create_async_engine(urls["TEST_DATABASE_URL"], hide_parameters=True)
    repository = QuestionRepository(
        async_sessionmaker(engine, expire_on_commit=False),
        agent.base.store.connector_factory,
        agent.base.settings,
        agent.bridge,
    )
    pending: asyncio.Task[bool] | None = None
    try:
        async with guard_agent_reply(agent.base.store, agent.base.settings, item):
            pending = asyncio.create_task(repository.retry(dispatch.id))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(pending), timeout=0.2)
            assert not pending.done()
        assert await asyncio.wait_for(pending, timeout=5)
        with pytest.raises(ArtifactError, match="agent_reply_obsolete"):
            async with guard_agent_reply(agent.base.store, agent.base.settings, item):
                pytest.fail("obsolete generation entered send section")
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await engine.dispose()
