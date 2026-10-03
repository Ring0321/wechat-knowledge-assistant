"""Durable question leases, confirmed actions and atomic answer/outbox commits."""

import hashlib
import re
import secrets
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.agent.contracts import AgentOutcome, AnswerNotifier, QuestionWork
from app.agent.grounding import display_label, render_answer, split_reply
from app.agent.models import AgentAction, QuestionDispatch, QuestionJob
from app.agent.queue import QuestionQueue
from app.agent.security import lock_identity, lock_sources, signature, verify_evidence
from app.agent.tools import permits_proposal
from app.core.config import Settings
from app.core.retry import retry_delay
from app.db.models import Message, Source, User
from app.db.session import corporate_session, tenant_session
from app.domain.agent import Evidence
from app.domain.artifacts import ArtifactError
from app.domain.enums import MessageRole, SourceStatus
from app.domain.knowledge import TenantContext
from app.knowledge.jobs import DELETE, enqueue
from app.knowledge.service import matching_locators, view

CONFIRM = re.compile(r"确认\s+([a-f0-9]{16})")


class QuestionRepository:
    def __init__(
        self,
        tenant_factory: async_sessionmaker[AsyncSession],
        connector_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        notifier: AnswerNotifier,
    ) -> None:
        self.tenant_factory, self.connector_factory = tenant_factory, connector_factory
        self.settings, self.notifier = settings, notifier

    async def resolve(
        self, dispatch_id: UUID, *, include_finished: bool = False
    ) -> tuple[UUID, UUID] | None:
        async with corporate_session(
            self.connector_factory, self.settings.wecom_corp_id
        ) as session:
            dispatch = await session.get(QuestionDispatch, dispatch_id)
            if dispatch is None or (dispatch.finished_at is not None and not include_finished):
                return None
            return dispatch.user_id, dispatch.job_id

    async def claim(self, dispatch_id: UUID) -> QuestionWork | None:
        identity = await self.resolve(dispatch_id)
        if identity is None or not self.settings.agent_enabled:
            return None
        user_id, job_id = identity
        async with tenant_session(self.tenant_factory, user_id) as session:
            job = await session.get(QuestionJob, job_id, with_for_update=True)
            now = datetime.now(UTC)
            if (
                job is None
                or job.status == "completed"
                or (job.lease_expires_at is not None and job.lease_expires_at > now)
            ):
                return None
            if (job.status == "failed" and job.next_retry_at is None) or (
                job.next_retry_at is not None and job.next_retry_at > now
            ):
                return None
            user = await session.get(User, user_id)
            message = await session.get(Message, job.message_id)
            if user is None or message is None:
                raise ArtifactError("question_relationship_missing")
            denied = (
                user.wecom_corp_id != self.settings.wecom_corp_id
                or not user.is_active
                or not self.settings.allows_wecom_user(user.wecom_external_user_id)
            )
            if (
                denied
                or message.role != MessageRole.USER
                or message.message_type != "text"
                or not message.content
                or job.attempts >= job.max_attempts
            ):
                job.status, job.error_message = (
                    "failed",
                    "agent_access_denied" if denied else "agent_question_unavailable",
                )
                job.lease_token = job.lease_expires_at = job.next_retry_at = None
                dispatch = await session.get(QuestionDispatch, dispatch_id)
                assert dispatch is not None
                dispatch.finished_at = now
                return None
            job.status, job.attempts = "processing", job.attempts + 1
            job.lease_token, job.lease_expires_at = (
                uuid4(),
                now + timedelta(seconds=self.settings.agent_lease_seconds),
            )
            job.next_retry_at = None
            return QuestionWork(
                dispatch_id,
                user_id,
                job.id,
                message.id,
                message.conversation_id,
                job.lease_token,
                message.content,
            )

    async def locked_job(self, session: AsyncSession, work: QuestionWork) -> QuestionJob:
        job = await session.get(QuestionJob, work.job_id, with_for_update=True)
        if (
            job is None
            or job.status != "processing"
            or job.lease_token != work.lease_token
            or job.lease_expires_at is None
            or job.lease_expires_at <= datetime.now(UTC)
        ):
            raise ArtifactError("agent_lease_lost", retryable=True)
        return job

    async def complete(self, work: QuestionWork, outcome: AgentOutcome) -> None:
        async with tenant_session(self.tenant_factory, work.user_id) as session:
            job = await self.locked_job(session, work)
            await lock_identity(session, TenantContext(work.user_id), self.settings)
            snapshots = {
                str(item.source.source_id): signature(item.source) for item in outcome.selected
            }
            if outcome.proposal is not None:
                snapshots[str(outcome.proposal.source.source_id)] = signature(
                    outcome.proposal.source
                )
            rows = await lock_sources(session, snapshots)
            verify_evidence(rows, outcome.selected)
            if outcome.proposal is not None:
                proposal = outcome.proposal
                if not permits_proposal(work.question, proposal.operation, proposal.tag):
                    raise ArtifactError("agent_action_denied")
                token = secrets.token_hex(8)
                expires_at = datetime.now(UTC) + timedelta(minutes=10)
                session.add(
                    AgentAction(
                        user_id=work.user_id,
                        question_job_id=job.id,
                        conversation_id=work.conversation_id,
                        source_id=proposal.source.source_id,
                        operation=proposal.operation,
                        tag=proposal.tag,
                        token_sha256=hashlib.sha256(token.encode()).hexdigest(),
                        source_signature=signature(proposal.source),
                        expires_at=expires_at,
                    )
                )
                title = display_label(proposal.source.title)[:120]
                action = (
                    "删除资料"
                    if proposal.operation == "delete_source"
                    else f"添加标签「{display_label(proposal.tag or '')}」"
                )
                parts = split_reply(
                    f"待确认：{action}\n来源：{title}\n来源编号：{proposal.source.source_id}\n"
                    f"确认后执行；有效期至 {expires_at.isoformat(timespec='seconds')}（UTC）。\n"
                    f"回复：确认 {token}"
                )
            else:
                selected = tuple(
                    Evidence(
                        item.evidence_id,
                        view(rows[str(item.source.source_id)]),
                        item.text,
                        matching_locators(rows[str(item.source.source_id)], item.text),
                    )
                    for item in outcome.selected
                )
                parts = render_answer(selected)
            job.source_snapshots = dict(snapshots)
            await self._save_answer(session, job, work, parts)

    async def confirmation(self, work: QuestionWork) -> bool:
        match = CONFIRM.fullmatch(work.question.strip())
        if match is None:
            return False
        digest = hashlib.sha256(match[1].encode()).hexdigest()
        async with tenant_session(self.tenant_factory, work.user_id) as session:
            job = await self.locked_job(session, work)
            await lock_identity(session, TenantContext(work.user_id), self.settings)
            action = await session.scalar(
                select(AgentAction)
                .where(
                    AgentAction.token_sha256 == digest,
                    AgentAction.conversation_id == work.conversation_id,
                )
                .with_for_update()
            )
            result = "确认码无效或已过期，请重新提出操作。"
            if action is not None and action.status == "executed":
                result = "该操作已处理，无需重复确认。"
            elif action is not None and action.status == "pending":
                source = await session.get(Source, action.source_id, with_for_update=True)
                if (
                    action.expires_at <= datetime.now(UTC)
                    or source is None
                    or source.status in (SourceStatus.DELETED, SourceStatus.DELETING)
                    or signature(view(source)) != action.source_signature
                ):
                    action.status = "expired"
                else:
                    if action.operation == "delete_source":
                        await enqueue(session, source, self.settings, operation=DELETE)
                        result = "已提交删除，资料已从检索中隐藏，正在清理原件与索引。"
                    elif action.operation == "add_tag" and action.tag:
                        if action.tag not in source.tags:
                            if len(source.tags) >= 50:
                                raise ArtifactError("agent_tag_limit")
                            source.tags = [*source.tags, action.tag]
                        result = "已添加标签。"
                    else:
                        raise ArtifactError("agent_action_invalid")
                    action.status, action.executed_at, action.confirmation_message_id = (
                        "executed",
                        datetime.now(UTC),
                        work.message_id,
                    )
            await self._save_answer(session, job, work, (result,))
        return True

    async def _save_answer(
        self,
        session: AsyncSession,
        job: QuestionJob,
        work: QuestionWork,
        parts: tuple[str, ...],
        *,
        failed: bool = False,
    ) -> None:
        message = await session.get(Message, work.message_id)
        assert message is not None
        answer = Message(
            id=uuid4(),
            user_id=work.user_id,
            conversation_id=message.conversation_id,
            role=MessageRole.ASSISTANT,
            message_type="text",
            content="\n".join(parts),
            metadata_={
                "question_job_id": str(job.id),
                "grounding": "verified_excerpt_v1",
                "source_ids": list(job.source_snapshots),
            },
        )
        session.add(answer)
        await session.flush()
        job.answer_message_id, job.result_parts = answer.id, list(parts)
        job.status, job.completed_at = "failed" if failed else "completed", datetime.now(UTC)
        job.lease_token = job.lease_expires_at = job.next_retry_at = None
        if not failed:
            job.error_message = None
        dispatch = await session.get(QuestionDispatch, work.dispatch_id)
        assert dispatch is not None
        dispatch.finished_at = datetime.now(UTC)
        await self.notifier.finished(session, job, message, parts)

    async def fail(self, work: QuestionWork, error: ArtifactError) -> None:
        async with tenant_session(self.tenant_factory, work.user_id) as session:
            job = await session.get(QuestionJob, work.job_id, with_for_update=True)
            if job is None or job.status != "processing" or job.lease_token != work.lease_token:
                return
            job.status, job.error_message = "failed", error.code[:128]
            job.lease_token = job.lease_expires_at = None
            dispatch = await session.get(QuestionDispatch, work.dispatch_id)
            assert dispatch is not None
            if error.retryable and job.attempts < job.max_attempts:
                job.next_retry_at = datetime.now(UTC) + timedelta(
                    seconds=retry_delay(5, job.attempts)
                )
                dispatch.available_at, dispatch.published_at = job.next_retry_at, None
            else:
                job.next_retry_at = None
                dispatch.finished_at = datetime.now(UTC)
                await self._save_answer(
                    session,
                    job,
                    work,
                    ("本次查询未能完成，任务已保留，可由管理员重试。",),
                    failed=True,
                )

    async def retry(self, dispatch_id: UUID) -> bool:
        identity = await self.resolve(dispatch_id, include_finished=True)
        if identity is None:
            return False
        user_id, job_id = identity
        async with tenant_session(self.tenant_factory, user_id) as session:
            job = await session.get(QuestionJob, job_id, with_for_update=True)
            if job is None or job.status != "failed" or job.lease_token is not None:
                return False
            await lock_identity(session, TenantContext(user_id), self.settings)
            job.status, job.attempts, job.next_retry_at = "queued", 0, None
            job.reply_generation += 1
            job.result_parts, job.source_snapshots, job.answer_message_id = [], {}, None
            dispatch = await session.get(QuestionDispatch, dispatch_id)
            assert dispatch is not None
            dispatch.finished_at = dispatch.published_at = None
            dispatch.available_at = datetime.now(UTC)
            return True

    async def publish_due(self, queue: QuestionQueue) -> int:
        now = datetime.now(UTC)
        async with corporate_session(
            self.connector_factory, self.settings.wecom_corp_id
        ) as session:
            rows = (
                await session.scalars(
                    select(QuestionDispatch)
                    .where(
                        QuestionDispatch.finished_at.is_(None),
                        QuestionDispatch.available_at <= now,
                        or_(
                            QuestionDispatch.published_at.is_(None),
                            QuestionDispatch.published_at <= now - timedelta(seconds=60),
                        ),
                    )
                    .order_by(QuestionDispatch.available_at)
                    .limit(20)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            for dispatch in rows:
                await queue.enqueue(dispatch.id)
                dispatch.published_at = now
            return len(rows)
