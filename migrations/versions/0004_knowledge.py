"""Durable tenant-scoped OpenAI mutation checkpoints."""

import sqlalchemy as sa
from alembic import op

revision = "0004_knowledge"
down_revision = "0003_ingestion"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("ingestion_jobs", sa.Column("operation_key", sa.String(128)))
    op.create_unique_constraint(
        "uq_ingestion_jobs_user_id_operation_key", "ingestion_jobs", ["user_id", "operation_key"]
    )
    op.add_column(
        "users",
        sa.Column("vector_store_pending", sa.Boolean(), nullable=False, server_default="false"),
    )
    op.create_table(
        "knowledge_files",
        sa.Column("user_id", sa.Uuid(), primary_key=True),
        sa.Column("source_id", sa.Uuid(), primary_key=True),
        sa.Column("document_sha256", sa.String(64), nullable=False),
        sa.Column(
            "index_started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("upload_started", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("vector_store_id", sa.String(256)),
        sa.Column("vector_file_id", sa.String(256)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.ForeignKeyConstraint(
            ["user_id", "source_id"], ["sources.user_id", "sources.id"], ondelete="CASCADE"
        ),
        sa.UniqueConstraint("vector_file_id"),
        sa.CheckConstraint("document_sha256 ~ '^[0-9a-f]{64}$'", name="valid_document_sha256"),
    )
    op.execute("ALTER TABLE knowledge_files ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE knowledge_files FORCE ROW LEVEL SECURITY")
    tenant = "user_id = NULLIF(current_setting('app.user_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY tenant_scope ON knowledge_files TO pkb_app "
        f"USING ({tenant}) WITH CHECK ({tenant})"
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON knowledge_files TO pkb_app")


def downgrade() -> None:
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM knowledge_files) "
            "OR EXISTS (SELECT 1 FROM users "
            "WHERE vector_store_pending OR vector_store_id IS NOT NULL)"
        )
    ):
        raise RuntimeError("M6 data exists; downgrade requires an explicit data migration")
    op.drop_table("knowledge_files")
    op.drop_column("users", "vector_store_pending")
    op.drop_constraint("uq_ingestion_jobs_user_id_operation_key", "ingestion_jobs", type_="unique")
    op.drop_column("ingestion_jobs", "operation_key")
