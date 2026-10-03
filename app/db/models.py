"""Initial persistent schema. PostgreSQL constraints also protect tenant relationships."""

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy import (
    text as sql_text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base, IdentityMixin, TimestampMixin
from app.domain.enums import AssetKind, JobStatus, MessageRole, SourceStatus, SourceType


def enum_type(enum: type[StrEnum], name: str) -> Enum:
    return Enum(
        enum,
        name=name,
        native_enum=False,
        create_constraint=True,
        validate_strings=True,
        values_callable=lambda members: [member.value for member in members],
    )


class User(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("wecom_corp_id", "wecom_external_user_id"),
        UniqueConstraint("id", "wecom_corp_id"),
    )

    wecom_corp_id: Mapped[str] = mapped_column(String(128))
    wecom_external_user_id: Mapped[str] = mapped_column(String(256))
    display_name: Mapped[str | None] = mapped_column(String(256))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default=sql_text("true"))
    vector_store_id: Mapped[str | None] = mapped_column(String(256), unique=True)
    vector_store_pending: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=sql_text("false")
    )


class Source(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "sources"
    __table_args__ = (
        UniqueConstraint("user_id", "id"),
        UniqueConstraint("user_id", "wechat_msg_id", "source_item_index"),
        CheckConstraint("source_item_index >= 0", name="valid_item_index"),
        CheckConstraint("sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$'", name="valid_sha256"),
        Index("ix_sources_user_created", "user_id", "created_at"),
        Index(
            "uq_sources_user_sha256_active",
            "user_id",
            "sha256",
            unique=True,
            postgresql_where=sql_text("sha256 IS NOT NULL AND status <> 'deleted'"),
        ),
    )

    user_id: Mapped[UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    wechat_msg_id: Mapped[str | None] = mapped_column(String(256))
    source_item_index: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    source_type: Mapped[SourceType] = mapped_column(enum_type(SourceType, "source_type"))
    title: Mapped[str] = mapped_column(String(512))
    original_url: Mapped[str | None] = mapped_column(Text)
    storage_key: Mapped[str | None] = mapped_column(String(1024))
    status: Mapped[SourceStatus] = mapped_column(
        enum_type(SourceStatus, "source_status"),
        default=SourceStatus.RECEIVED,
        server_default="received",
    )
    text: Mapped[str | None] = mapped_column(Text)
    summary: Mapped[str | None] = mapped_column(Text)
    tags: Mapped[list[str]] = mapped_column(
        JSONB, default=list, server_default=sql_text("'[]'::jsonb")
    )
    metadata_: Mapped[dict[str, JsonValue]] = mapped_column(
        "metadata", JSONB, default=dict, server_default=sql_text("'{}'::jsonb")
    )
    vector_file_id: Mapped[str | None] = mapped_column(String(256), index=True)
    sha256: Mapped[str | None] = mapped_column(String(64))


class KnowledgeFile(TimestampMixin, Base):
    """Durable remote mutation journal; one immutable Markdown version per source."""

    __tablename__ = "knowledge_files"
    __table_args__ = (
        ForeignKeyConstraint(
            ["user_id", "source_id"], ["sources.user_id", "sources.id"], ondelete="CASCADE"
        ),
        CheckConstraint("document_sha256 ~ '^[0-9a-f]{64}$'", name="valid_document_sha256"),
    )
    user_id: Mapped[UUID] = mapped_column(primary_key=True)
    source_id: Mapped[UUID] = mapped_column(primary_key=True)
    document_sha256: Mapped[str] = mapped_column(String(64))
    index_started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("now()")
    )
    upload_started: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=sql_text("false")
    )
    vector_store_id: Mapped[str | None] = mapped_column(String(256))
    vector_file_id: Mapped[str | None] = mapped_column(String(256), unique=True)


class Asset(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "assets"
    __table_args__ = (
        ForeignKeyConstraint(
            ["user_id", "source_id"], ["sources.user_id", "sources.id"], ondelete="CASCADE"
        ),
        UniqueConstraint("user_id", "storage_key"),
        CheckConstraint("size_bytes >= 0", name="nonnegative_size"),
        CheckConstraint("sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$'", name="valid_sha256"),
        Index("ix_assets_user_source", "user_id", "source_id"),
    )

    user_id: Mapped[UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    source_id: Mapped[UUID]
    kind: Mapped[AssetKind] = mapped_column(enum_type(AssetKind, "asset_kind"))
    storage_key: Mapped[str] = mapped_column(String(1024))
    filename: Mapped[str | None] = mapped_column(String(512))
    content_type: Mapped[str | None] = mapped_column(String(256))
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    sha256: Mapped[str | None] = mapped_column(String(64))
    metadata_: Mapped[dict[str, JsonValue]] = mapped_column(
        "metadata", JSONB, default=dict, server_default=sql_text("'{}'::jsonb")
    )


class Conversation(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "conversations"
    __table_args__ = (
        UniqueConstraint("user_id", "id"),
        Index("ix_conversations_user_created", "user_id", "created_at"),
    )

    user_id: Mapped[UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    open_kfid: Mapped[str] = mapped_column(String(256))
    title: Mapped[str | None] = mapped_column(String(512))


class Message(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint("user_id", "id"),
        ForeignKeyConstraint(
            ["user_id", "conversation_id"],
            ["conversations.user_id", "conversations.id"],
            ondelete="CASCADE",
        ),
        UniqueConstraint("user_id", "wechat_msg_id"),
        Index("ix_messages_user_conversation_created", "user_id", "conversation_id", "created_at"),
    )

    user_id: Mapped[UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    conversation_id: Mapped[UUID]
    wechat_msg_id: Mapped[str | None] = mapped_column(String(256))
    role: Mapped[MessageRole] = mapped_column(enum_type(MessageRole, "message_role"))
    message_type: Mapped[str] = mapped_column(String(64))
    content: Mapped[str | None] = mapped_column(Text)
    metadata_: Mapped[dict[str, JsonValue]] = mapped_column(
        "metadata", JSONB, default=dict, server_default=sql_text("'{}'::jsonb")
    )


class IngestionJob(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "ingestion_jobs"
    __table_args__ = (
        UniqueConstraint("user_id", "id"),
        UniqueConstraint("user_id", "operation_key"),
        ForeignKeyConstraint(["user_id", "message_id"], ["messages.user_id", "messages.id"]),
        ForeignKeyConstraint(
            ["user_id", "source_id"], ["sources.user_id", "sources.id"], ondelete="CASCADE"
        ),
        CheckConstraint("attempts >= 0 AND max_attempts > 0", name="valid_attempts"),
        CheckConstraint(
            "status <> 'failed' OR (error_message IS NOT NULL AND length(trim(error_message)) > 0)",
            name="failed_requires_error",
        ),
        Index("ix_ingestion_jobs_user_status_retry", "user_id", "status", "next_retry_at"),
        CheckConstraint("(lease_token IS NULL) = (lease_expires_at IS NULL)", name="paired_lease"),
    )

    user_id: Mapped[UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    source_id: Mapped[UUID]
    message_id: Mapped[UUID | None]
    operation_key: Mapped[str | None] = mapped_column(String(128))
    input_data: Mapped[dict[str, JsonValue]] = mapped_column(
        JSONB, default=dict, server_default=sql_text("'{}'::jsonb")
    )
    lease_token: Mapped[UUID | None]
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[JobStatus] = mapped_column(
        enum_type(JobStatus, "job_status"), default=JobStatus.QUEUED, server_default="queued"
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, default=3, server_default="3")
    error_message: Mapped[str | None] = mapped_column(Text)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
