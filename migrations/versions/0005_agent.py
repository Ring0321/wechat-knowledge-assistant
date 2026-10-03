"""Durable question processing and tenant-bound confirmed actions."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0005_agent"
down_revision = "0004_knowledge"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "question_jobs",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("answer_message_id", sa.Uuid(), nullable=True),
        sa.Column("status", sa.String(length=16), server_default="queued", nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("max_attempts", sa.Integer(), server_default="3", nullable=False),
        sa.Column("lease_token", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reply_generation", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "result_parts",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "source_snapshots",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status <> 'failed' OR (error_message IS NOT NULL AND length(trim(error_message)) > 0)",
            name=op.f("ck_question_jobs_failed_requires_error"),
        ),
        sa.CheckConstraint(
            "status IN ('queued','processing','completed','failed')",
            name=op.f("ck_question_jobs_valid_status"),
        ),
        sa.CheckConstraint(
            "(lease_token IS NULL) = (lease_expires_at IS NULL)",
            name=op.f("ck_question_jobs_paired_lease"),
        ),
        sa.CheckConstraint(
            "attempts >= 0 AND max_attempts > 0", name=op.f("ck_question_jobs_valid_attempts")
        ),
        sa.ForeignKeyConstraint(
            ["user_id", "answer_message_id"],
            ["messages.user_id", "messages.id"],
            name="fk_question_jobs_answer_message",
        ),
        sa.ForeignKeyConstraint(
            ["user_id", "message_id"],
            ["messages.user_id", "messages.id"],
            name=op.f("fk_question_jobs_user_id_messages"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_question_jobs")),
        sa.UniqueConstraint("user_id", "id", name=op.f("uq_question_jobs_user_id_id")),
        sa.UniqueConstraint(
            "user_id", "message_id", name=op.f("uq_question_jobs_user_id_message_id")
        ),
    )
    op.create_table(
        "question_dispatches",
        sa.Column("corp_id", sa.String(length=128), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id", "corp_id"],
            ["users.id", "users.wecom_corp_id"],
            name=op.f("fk_question_dispatches_user_id_users"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id", "job_id"],
            ["question_jobs.user_id", "question_jobs.id"],
            name=op.f("fk_question_dispatches_user_id_question_jobs"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_question_dispatches")),
        sa.UniqueConstraint("job_id", name=op.f("uq_question_dispatches_job_id")),
    )
    op.create_index(
        "ix_question_dispatches_corp_due",
        "question_dispatches",
        ["corp_id", "finished_at", "available_at"],
        unique=False,
    )
    op.create_table(
        "agent_actions",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("question_job_id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("source_id", sa.Uuid(), nullable=False),
        sa.Column("confirmation_message_id", sa.Uuid(), nullable=True),
        sa.Column("operation", sa.String(length=32), nullable=False),
        sa.Column("tag", sa.String(length=64), nullable=True),
        sa.Column("token_sha256", sa.String(length=64), nullable=False),
        sa.Column("source_signature", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), server_default="pending", nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "operation IN ('delete_source','add_tag')",
            name=op.f("ck_agent_actions_valid_operation"),
        ),
        sa.CheckConstraint(
            "status IN ('pending','executed','expired')", name=op.f("ck_agent_actions_valid_status")
        ),
        sa.CheckConstraint(
            "token_sha256 ~ '^[0-9a-f]{64}$'", name=op.f("ck_agent_actions_valid_token_sha256")
        ),
        sa.ForeignKeyConstraint(
            ["user_id", "confirmation_message_id"],
            ["messages.user_id", "messages.id"],
            name=op.f("fk_agent_actions_user_id_messages"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id", "conversation_id"],
            ["conversations.user_id", "conversations.id"],
            name=op.f("fk_agent_actions_user_id_conversations"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id", "question_job_id"],
            ["question_jobs.user_id", "question_jobs.id"],
            name=op.f("fk_agent_actions_user_id_question_jobs"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id", "source_id"],
            ["sources.user_id", "sources.id"],
            name=op.f("fk_agent_actions_user_id_sources"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_agent_actions")),
        sa.UniqueConstraint("token_sha256", name=op.f("uq_agent_actions_token_sha256")),
        sa.UniqueConstraint(
            "user_id", "question_job_id", name=op.f("uq_agent_actions_user_id_question_job_id")
        ),
    )
    for table in ("question_jobs", "question_dispatches", "agent_actions"):
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        tenant = "user_id = NULLIF(current_setting('app.user_id', true), '')::uuid"
        op.execute(
            f"CREATE POLICY tenant_scope ON {table} TO pkb_app "
            f"USING ({tenant}) WITH CHECK ({tenant})"
        )
        op.execute(f"GRANT SELECT, INSERT, UPDATE ON {table} TO pkb_app")
    op.execute("GRANT SELECT, UPDATE ON question_dispatches TO pkb_connector")
    op.execute(
        "CREATE POLICY connector_scope ON question_dispatches TO pkb_connector "
        "USING (corp_id = current_setting('app.wecom_corp_id', true)) "
        "WITH CHECK (corp_id = current_setting('app.wecom_corp_id', true))"
    )


def downgrade() -> None:
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM question_jobs) OR EXISTS (SELECT 1 FROM agent_actions)"
        )
    ):
        raise RuntimeError("M7 data exists; downgrade requires an explicit data migration")
    op.drop_table("agent_actions")
    op.drop_table("question_dispatches")
    op.drop_table("question_jobs")
