"""Multi-source ingestion, durable dispatch, task leases and staged reply idempotency."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0003_ingestion"
down_revision = "0002_wecom"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "sources", sa.Column("source_item_index", sa.Integer(), nullable=False, server_default="0")
    )
    op.drop_constraint("uq_sources_user_id_wechat_msg_id", "sources", type_="unique")
    op.create_unique_constraint(
        "uq_sources_user_id_wechat_msg_id_source_item_index",
        "sources",
        ["user_id", "wechat_msg_id", "source_item_index"],
    )
    op.create_check_constraint("valid_item_index", "sources", "source_item_index >= 0")
    op.drop_constraint(op.f("ck_sources_source_status"), "sources", type_="check")
    op.create_check_constraint(
        "source_status",
        "sources",
        "status IN ('received','processing','stored','ready',"
        "'metadata_only','failed','deleting','deleted')",
    )
    op.add_column("ingestion_jobs", sa.Column("message_id", sa.Uuid()))
    op.add_column(
        "ingestion_jobs",
        sa.Column("input_data", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
    )
    op.add_column("ingestion_jobs", sa.Column("lease_token", sa.Uuid()))
    op.add_column("ingestion_jobs", sa.Column("lease_expires_at", sa.DateTime(timezone=True)))
    op.create_unique_constraint("uq_ingestion_jobs_user_id_id", "ingestion_jobs", ["user_id", "id"])
    op.create_foreign_key(
        "fk_ingestion_jobs_user_id_messages",
        "ingestion_jobs",
        "messages",
        ["user_id", "message_id"],
        ["user_id", "id"],
    )
    op.create_check_constraint(
        "paired_lease", "ingestion_jobs", "(lease_token IS NULL) = (lease_expires_at IS NULL)"
    )
    op.add_column(
        "wecom_outbox", sa.Column("purpose", sa.String(128), nullable=False, server_default="ack")
    )
    op.drop_constraint("uq_wecom_outbox_user_id_inbound_message_id", "wecom_outbox", type_="unique")
    op.create_unique_constraint(
        "uq_wecom_outbox_user_id_inbound_message_id_purpose",
        "wecom_outbox",
        ["user_id", "inbound_message_id", "purpose"],
    )
    op.create_table(
        "message_sources",
        sa.Column("user_id", sa.Uuid(), primary_key=True),
        sa.Column("message_id", sa.Uuid(), primary_key=True),
        sa.Column("item_index", sa.Integer(), primary_key=True),
        sa.Column("source_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_id", "message_id"], ["messages.user_id", "messages.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["user_id", "source_id"], ["sources.user_id", "sources.id"]),
        sa.CheckConstraint("item_index >= 0", name="valid_item_index"),
    )
    op.create_table(
        "ingestion_dispatches",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("corp_id", sa.String(128), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column(
            "available_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(["user_id", "corp_id"], ["users.id", "users.wecom_corp_id"]),
        sa.ForeignKeyConstraint(
            ["user_id", "job_id"], ["ingestion_jobs.user_id", "ingestion_jobs.id"]
        ),
        sa.UniqueConstraint("job_id"),
    )
    op.create_index(
        "ix_ingestion_dispatches_corp_due",
        "ingestion_dispatches",
        ["corp_id", "finished_at", "available_at"],
    )
    for table in ("message_sources", "ingestion_dispatches"):
        op.execute(f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY')
        op.execute(f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY')
        tenant = "user_id = NULLIF(current_setting('app.user_id', true), '')::uuid"
        op.execute(
            f'CREATE POLICY tenant_scope ON "{table}" TO pkb_app '
            f"USING ({tenant}) WITH CHECK ({tenant})"
        )
        op.execute(f'GRANT SELECT, INSERT, UPDATE ON "{table}" TO pkb_app')
    op.execute("GRANT SELECT, UPDATE ON ingestion_dispatches TO pkb_connector")
    op.execute(
        "CREATE POLICY connector_scope ON ingestion_dispatches TO pkb_connector "
        "USING (corp_id = current_setting('app.wecom_corp_id', true)) "
        "WITH CHECK (corp_id = current_setting('app.wecom_corp_id', true))"
    )


def downgrade() -> None:
    # Refuse a lossy rollback after real M3 use; export/migrate those records explicitly first.
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM ingestion_dispatches) "
            "OR EXISTS (SELECT 1 FROM message_sources) "
            "OR EXISTS (SELECT 1 FROM sources WHERE status='stored' OR source_item_index <> 0) "
            "OR EXISTS (SELECT 1 FROM wecom_outbox WHERE purpose <> 'ack')"
        )
    ):
        raise RuntimeError("M3 data exists; downgrade requires an explicit data migration")
    op.drop_table("ingestion_dispatches")
    op.drop_table("message_sources")
    op.drop_constraint(
        "uq_wecom_outbox_user_id_inbound_message_id_purpose", "wecom_outbox", type_="unique"
    )
    op.drop_column("wecom_outbox", "purpose")
    op.create_unique_constraint(
        "uq_wecom_outbox_user_id_inbound_message_id",
        "wecom_outbox",
        ["user_id", "inbound_message_id"],
    )
    op.drop_constraint("fk_ingestion_jobs_user_id_messages", "ingestion_jobs", type_="foreignkey")
    op.drop_constraint("uq_ingestion_jobs_user_id_id", "ingestion_jobs", type_="unique")
    op.drop_constraint(op.f("ck_ingestion_jobs_paired_lease"), "ingestion_jobs", type_="check")
    for column in ("message_id", "input_data", "lease_token", "lease_expires_at"):
        op.drop_column("ingestion_jobs", column)
    op.drop_constraint(op.f("ck_sources_source_status"), "sources", type_="check")
    op.create_check_constraint(
        "source_status",
        "sources",
        "status IN ('received','processing','ready','metadata_only','failed','deleting','deleted')",
    )
    op.drop_constraint(
        "uq_sources_user_id_wechat_msg_id_source_item_index", "sources", type_="unique"
    )
    op.drop_constraint(op.f("ck_sources_valid_item_index"), "sources", type_="check")
    op.drop_column("sources", "source_item_index")
    op.create_unique_constraint(
        "uq_sources_user_id_wechat_msg_id", "sources", ["user_id", "wechat_msg_id"]
    )
