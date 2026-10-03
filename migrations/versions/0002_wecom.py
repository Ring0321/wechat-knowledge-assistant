"""WeCom cursor and transactional reply outbox with a restricted connector role."""

import sqlalchemy as sa
from alembic import op

revision = "0002_wecom"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint("uq_users_id_wecom_corp_id", "users", ["id", "wecom_corp_id"])
    op.create_unique_constraint("uq_messages_user_id_id", "messages", ["user_id", "id"])
    op.create_table(
        "wecom_sync_states",
        sa.Column("corp_id", sa.String(128), primary_key=True),
        sa.Column("open_kfid", sa.String(256), primary_key=True),
        sa.Column("cursor", sa.String(64)),
        sa.Column("status", sa.String(16), server_default="ready", nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_message", sa.String(128)),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True)),
        sa.Column("last_synced_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("status IN ('ready','retry','failed')", name="valid_status"),
        sa.CheckConstraint("attempts >= 0", name="valid_attempts"),
    )
    op.create_table(
        "wecom_outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("corp_id", sa.String(128), nullable=False),
        sa.Column("open_kfid", sa.String(256), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("inbound_message_id", sa.Uuid(), nullable=False),
        sa.Column("external_userid", sa.String(256), nullable=False),
        sa.Column("reply_msgid", sa.String(32), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(16), server_default="queued", nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_message", sa.String(128)),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True)),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True)),
        sa.Column("accepted_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["user_id", "corp_id"], ["users.id", "users.wecom_corp_id"]),
        sa.ForeignKeyConstraint(
            ["user_id", "inbound_message_id"], ["messages.user_id", "messages.id"]
        ),
        sa.UniqueConstraint("corp_id", "open_kfid", "reply_msgid"),
        sa.UniqueConstraint("user_id", "inbound_message_id"),
        sa.CheckConstraint(
            "status IN ('queued','sent','deferred','failed','uncertain')", name="valid_status"
        ),
        sa.CheckConstraint("attempts >= 0", name="valid_attempts"),
        sa.CheckConstraint("octet_length(content) BETWEEN 1 AND 2048", name="text_byte_limit"),
        sa.CheckConstraint("reply_msgid ~ '^[A-Za-z0-9_-]{1,32}$'", name="valid_reply_msgid"),
        sa.CheckConstraint(
            "status NOT IN ('failed','uncertain','deferred') OR "
            "(error_message IS NOT NULL AND length(error_message) > 0)",
            name="failure_has_error",
        ),
    )
    op.create_index(
        "ix_wecom_outbox_corp_status_retry",
        "wecom_outbox",
        ["corp_id", "status", "next_attempt_at"],
    )
    op.execute("GRANT SELECT ON alembic_version TO pkb_connector")
    op.execute("GRANT SELECT, INSERT, UPDATE ON wecom_sync_states TO pkb_connector")
    op.execute("GRANT SELECT, UPDATE ON wecom_outbox TO pkb_connector")
    op.execute("GRANT SELECT, INSERT ON wecom_outbox TO pkb_app")
    for table in ("wecom_sync_states", "wecom_outbox"):
        op.execute(f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY')
        op.execute(f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY')
        op.execute(
            f'CREATE POLICY connector_scope ON "{table}" TO pkb_connector '
            "USING (corp_id = current_setting('app.wecom_corp_id', true)) "
            "WITH CHECK (corp_id = current_setting('app.wecom_corp_id', true))"
        )
    op.execute(
        "CREATE POLICY tenant_outbox ON wecom_outbox TO pkb_app "
        "USING (user_id = NULLIF(current_setting('app.user_id', true), '')::uuid) "
        "WITH CHECK (user_id = NULLIF(current_setting('app.user_id', true), '')::uuid)"
    )


def downgrade() -> None:
    op.drop_table("wecom_outbox")
    op.drop_table("wecom_sync_states")
    op.drop_constraint("uq_messages_user_id_id", "messages", type_="unique")
    op.drop_constraint("uq_users_id_wecom_corp_id", "users", type_="unique")
    op.execute("REVOKE SELECT ON alembic_version FROM pkb_connector")
