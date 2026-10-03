"""Customer-service control records, separated from tenant knowledge and message content."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base, IdentityMixin, TimestampMixin


class WeComSyncState(TimestampMixin, Base):
    __tablename__ = "wecom_sync_states"
    __table_args__ = (
        CheckConstraint("status IN ('ready','retry','failed')", name="valid_status"),
        CheckConstraint("attempts >= 0", name="valid_attempts"),
    )
    corp_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    open_kfid: Mapped[str] = mapped_column(String(256), primary_key=True)
    cursor: Mapped[str | None] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), default="ready", server_default="ready")
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    error_message: Mapped[str | None] = mapped_column(String(128))
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WeComOutbox(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "wecom_outbox"
    __table_args__ = (
        ForeignKeyConstraint(["user_id", "corp_id"], ["users.id", "users.wecom_corp_id"]),
        ForeignKeyConstraint(
            ["user_id", "inbound_message_id"], ["messages.user_id", "messages.id"]
        ),
        UniqueConstraint("corp_id", "open_kfid", "reply_msgid"),
        UniqueConstraint("user_id", "inbound_message_id", "purpose"),
        CheckConstraint(
            "status IN ('queued','sent','deferred','failed','uncertain')", name="valid_status"
        ),
        CheckConstraint("attempts >= 0", name="valid_attempts"),
        CheckConstraint("octet_length(content) BETWEEN 1 AND 2048", name="text_byte_limit"),
        CheckConstraint("reply_msgid ~ '^[A-Za-z0-9_-]{1,32}$'", name="valid_reply_msgid"),
        CheckConstraint(
            "status NOT IN ('failed','uncertain','deferred') OR "
            "(error_message IS NOT NULL AND length(error_message) > 0)",
            name="failure_has_error",
        ),
        Index("ix_wecom_outbox_corp_status_retry", "corp_id", "status", "next_attempt_at"),
    )
    corp_id: Mapped[str] = mapped_column(String(128))
    open_kfid: Mapped[str] = mapped_column(String(256))
    user_id: Mapped[UUID]
    inbound_message_id: Mapped[UUID]
    purpose: Mapped[str] = mapped_column(String(128), default="ack", server_default="ack")
    external_userid: Mapped[str] = mapped_column(String(256))
    reply_msgid: Mapped[str] = mapped_column(String(32))
    content: Mapped[str] = mapped_column(Text)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), default="queued", server_default="queued")
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    error_message: Mapped[str | None] = mapped_column(String(128))
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
