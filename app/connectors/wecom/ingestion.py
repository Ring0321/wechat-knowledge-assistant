"""Bridge authenticated WeCom messages and transactional ingestion; no external HTTP."""

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.wecom.channels import WeComChannelSupplementBridge
from app.connectors.wecom.contracts import NormalizedMessage
from app.connectors.wecom.outbox import enqueue_reply
from app.core.config import Settings
from app.db.models import Conversation, IngestionJob, Message, Source, User
from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceStatus, SourceType
from app.ingestion.channels import SUPPLEMENT
from app.ingestion.detection import detect_sources
from app.ingestion.models import IngestionDispatch, MessageSource


class WeComIngestionBridge:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def admit(
        self, session: AsyncSession, user_id: UUID, message_id: UUID, message: NormalizedMessage
    ) -> bool:
        if await WeComChannelSupplementBridge(self.settings).admit(
            session, user_id, message_id, message
        ):
            return True
        try:
            inputs = detect_sources(
                message.message_type,
                message.text,
                message.metadata,
                max_urls=self.settings.ingestion_max_urls,
            )
        except ArtifactError:
            inputs = []
            if self.settings.wecom_auto_reply:
                await enqueue_reply(
                    session,
                    corp_id=self.settings.wecom_corp_id,
                    user_id=user_id,
                    message_id=message_id,
                    open_kfid=message.open_kfid,
                    external_userid=message.external_userid,
                    received_at=message.sent_at,
                    content=(
                        f"本条链接数量超过 {self.settings.ingestion_max_urls} 个，"
                        "请分批发送；本条尚未收录。"
                    ),
                )
            return True
        if not inputs:
            return False
        for index, item in enumerate(inputs):
            source_id, job_id = uuid4(), uuid4()
            session.add(
                Source(
                    id=source_id,
                    user_id=user_id,
                    wechat_msg_id=message.msgid,
                    source_item_index=index,
                    source_type=item.source_type,
                    title=item.title,
                    original_url=item.original_url,
                    metadata_={
                        "index_status": "deferred_m6",
                        "message_sent_at": message.sent_at.isoformat(),
                    },
                )
            )
            await session.flush()
            session.add(
                IngestionJob(
                    id=job_id,
                    user_id=user_id,
                    source_id=source_id,
                    message_id=message_id,
                    input_data=item.data,
                    max_attempts=self.settings.ingestion_max_attempts,
                )
            )
            await session.flush()
            session.add_all(
                [
                    MessageSource(
                        user_id=user_id,
                        message_id=message_id,
                        item_index=index,
                        source_id=source_id,
                    ),
                    IngestionDispatch(
                        id=uuid4(),
                        corp_id=self.settings.wecom_corp_id,
                        user_id=user_id,
                        job_id=job_id,
                    ),
                ]
            )
        if self.settings.wecom_auto_reply:
            title = inputs[0].title[:120] + (f" 等 {len(inputs)} 项" if len(inputs) > 1 else "")
            await enqueue_reply(
                session,
                corp_id=self.settings.wecom_corp_id,
                user_id=user_id,
                message_id=message_id,
                open_kfid=message.open_kfid,
                external_userid=message.external_userid,
                received_at=message.sent_at,
                content="收到，正在整理：" + title,
            )
        return True

    async def finished(
        self, session: AsyncSession, job: IngestionJob, source: Source, *, outcome: str
    ) -> None:
        if not self.settings.wecom_auto_reply or job.message_id is None:
            return
        message = await session.get(Message, job.message_id)
        user = await session.get(User, job.user_id)
        if message is None or user is None:
            raise ArtifactError("notification_identity_missing")
        conversation = await session.get(Conversation, message.conversation_id)
        if conversation is None:
            raise ArtifactError("notification_conversation_missing")
        title = source.title[:120]
        if outcome == "indexed" and source.status == SourceStatus.READY:
            content = f"已收录：{title}"
        elif outcome == "failed" and job.input_data.get("kind") == SUPPLEMENT:
            content = f"补充视频失败：{title}。原卡片资料已保留，任务可由管理员重试。"
        elif outcome == "failed" and job.input_data.get("kind") == "knowledge_index":
            content = f"已保存：{title}（检索索引建立失败，任务已保留，可重试）"
        elif outcome == "failed":
            content = f"整理失败：{title}。任务已保留，可由管理员重试。"
        elif outcome == "duplicate":
            content = f"已有相同资料：{title}，已关联已有记录。"
        elif (
            source.source_type == SourceType.WECHAT_CHANNEL
            and source.status == SourceStatus.METADATA_ONLY
        ):
            if source.metadata_.get("original_video_available"):
                content = f"已保存：{title}（已补充视频，未提取到可用正文，尚未建立检索索引）"
            else:
                content = (
                    f"已收录：{title}（仅视频号卡片信息，未读取原视频，尚未建立检索索引）\n"
                    f"来源编号：{source.id}\n"
                    f"如有合法原视频，发送：补充视频 {source.id}"
                )
        elif source.metadata_.get("parse_status") == "no_speech_detected":
            content = f"已保存：{title}（音频已处理，未识别到语音，尚未建立检索索引）"
        elif source.status == SourceStatus.METADATA_ONLY:
            content = f"已保存：{title}（仅原件或链接/卡片信息，尚未解析和建立检索索引）"
        else:
            content = f"已保存：{title}（尚未建立检索索引）"
        sent_at = message.metadata_.get("sent_at")
        received_at = (
            datetime.fromisoformat(sent_at) if isinstance(sent_at, str) else message.created_at
        )
        await enqueue_reply(
            session,
            corp_id=user.wecom_corp_id,
            user_id=user.id,
            message_id=message.id,
            open_kfid=conversation.open_kfid,
            external_userid=user.wecom_external_user_id,
            received_at=received_at,
            content=content,
            purpose=f"ingestion:{job.id}:{outcome}",
        )
