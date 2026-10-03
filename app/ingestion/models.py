from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base, IdentityMixin, TimestampMixin


class MessageSource(Base):
    __tablename__ = "message_sources"
    __table_args__ = (
        ForeignKeyConstraint(
            ["user_id", "message_id"], ["messages.user_id", "messages.id"], ondelete="CASCADE"
        ),
        ForeignKeyConstraint(["user_id", "source_id"], ["sources.user_id", "sources.id"]),
        CheckConstraint("item_index >= 0", name="valid_item_index"),
    )
    user_id: Mapped[UUID] = mapped_column(primary_key=True)
    message_id: Mapped[UUID] = mapped_column(primary_key=True)
    item_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[UUID]


class IngestionDispatch(IdentityMixin, TimestampMixin, Base):
    __tablename__ = "ingestion_dispatches"
    __table_args__ = (
        ForeignKeyConstraint(["user_id", "corp_id"], ["users.id", "users.wecom_corp_id"]),
        ForeignKeyConstraint(
            ["user_id", "job_id"], ["ingestion_jobs.user_id", "ingestion_jobs.id"]
        ),
        UniqueConstraint("job_id"),
        Index("ix_ingestion_dispatches_corp_due", "corp_id", "finished_at", "available_at"),
    )
    corp_id: Mapped[str] = mapped_column(String(128))
    user_id: Mapped[UUID]
    job_id: Mapped[UUID]
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
