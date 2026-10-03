"""Questions have their own lifecycle and never create fictitious knowledge sources."""

from datetime import datetime
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base, IdentityMixin, TimestampMixin


class QuestionJob(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "question_jobs"
    __table_args__ = (
        UniqueConstraint("user_id", "id"),
        UniqueConstraint("user_id", "message_id"),
        ForeignKeyConstraint(["user_id", "message_id"], ["messages.user_id", "messages.id"]),
        ForeignKeyConstraint(
            ["user_id", "answer_message_id"],
            ["messages.user_id", "messages.id"],
            name="fk_question_jobs_answer_message",
        ),
        CheckConstraint(
            "status IN ('queued','processing','completed','failed')", name="valid_status"
        ),
        CheckConstraint("attempts >= 0 AND max_attempts > 0", name="valid_attempts"),
        CheckConstraint("(lease_token IS NULL) = (lease_expires_at IS NULL)", name="paired_lease"),
        CheckConstraint(
            "status <> 'failed' OR (error_message IS NOT NULL AND length(trim(error_message)) > 0)",
            name="failed_requires_error",
        ),
    )
    user_id: Mapped[UUID]
    message_id: Mapped[UUID]
    answer_message_id: Mapped[UUID | None]
    status: Mapped[str] = mapped_column(String(16), default="queued", server_default="queued")
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, default=3, server_default="3")
    lease_token: Mapped[UUID | None]
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(Text)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reply_generation: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    result_parts: Mapped[list[str]] = mapped_column(
        JSONB, default=list, server_default=text("'[]'::jsonb")
    )
    source_snapshots: Mapped[dict[str, JsonValue]] = mapped_column(
        JSONB, default=dict, server_default=text("'{}'::jsonb")
    )


class QuestionDispatch(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "question_dispatches"
    __table_args__ = (
        ForeignKeyConstraint(["user_id", "corp_id"], ["users.id", "users.wecom_corp_id"]),
        ForeignKeyConstraint(["user_id", "job_id"], ["question_jobs.user_id", "question_jobs.id"]),
        UniqueConstraint("job_id"),
        Index("ix_question_dispatches_corp_due", "corp_id", "finished_at", "available_at"),
    )
    corp_id: Mapped[str] = mapped_column(String(128))
    user_id: Mapped[UUID]
    job_id: Mapped[UUID]
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AgentAction(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "agent_actions"
    __table_args__ = (
        UniqueConstraint("user_id", "question_job_id"),
        UniqueConstraint("token_sha256"),
        ForeignKeyConstraint(
            ["user_id", "question_job_id"], ["question_jobs.user_id", "question_jobs.id"]
        ),
        ForeignKeyConstraint(
            ["user_id", "conversation_id"], ["conversations.user_id", "conversations.id"]
        ),
        ForeignKeyConstraint(["user_id", "source_id"], ["sources.user_id", "sources.id"]),
        ForeignKeyConstraint(
            ["user_id", "confirmation_message_id"], ["messages.user_id", "messages.id"]
        ),
        CheckConstraint("operation IN ('delete_source','add_tag')", name="valid_operation"),
        CheckConstraint("status IN ('pending','executed','expired')", name="valid_status"),
        CheckConstraint("token_sha256 ~ '^[0-9a-f]{64}$'", name="valid_token_sha256"),
    )
    user_id: Mapped[UUID]
    question_job_id: Mapped[UUID]
    conversation_id: Mapped[UUID]
    source_id: Mapped[UUID]
    confirmation_message_id: Mapped[UUID | None]
    operation: Mapped[str] = mapped_column(String(32))
    tag: Mapped[str | None] = mapped_column(String(64))
    token_sha256: Mapped[str] = mapped_column(String(64))
    source_signature: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), default="pending", server_default="pending")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
