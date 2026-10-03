"""Evidence checks use the same tenant transaction for validation and protected work."""

import hashlib
import json
from dataclasses import asdict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.grounding import source_metadata
from app.core.config import Settings
from app.db.models import Source, User
from app.domain.agent import Evidence
from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceStatus
from app.domain.knowledge import SourceView, TenantContext
from app.knowledge.rendering import indexed_markdown
from app.knowledge.service import authorize, view


def signature(source: SourceView) -> str:
    return hashlib.sha256(
        json.dumps(asdict(source), sort_keys=True, default=str, ensure_ascii=False).encode()
    ).hexdigest()


async def lock_identity(session: AsyncSession, context: TenantContext, settings: Settings) -> None:
    # Re-read under a shared lock: an active flag update cannot race a model/send call.
    await session.scalar(
        select(User)
        .where(User.id == context.user_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    await authorize(session, context, settings)


async def lock_sources(session: AsyncSession, snapshots: dict[str, str]) -> dict[str, Source]:
    rows: dict[str, Source] = {}
    from uuid import UUID

    for key in sorted(snapshots):
        try:
            source_id = UUID(key)
        except ValueError:
            raise ArtifactError("agent_evidence_invalid") from None
        source = await session.scalar(
            select(Source)
            .where(Source.id == source_id)
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
        if (
            source is None
            or source.status in (SourceStatus.DELETING, SourceStatus.DELETED)
            or signature(view(source)) != snapshots[key]
        ):
            raise ArtifactError("agent_evidence_changed", retryable=True)
        rows[key] = source
    return rows


def verify_evidence(rows: dict[str, Source], evidence: tuple[Evidence, ...]) -> None:
    """A matching remote file identity does not prove the excerpt's bytes."""
    for item in evidence:
        source = rows.get(str(item.source.source_id))
        if (
            source is None
            or not item.text.strip()
            or not any(
                item.text in body
                for body in (
                    source.text or "",
                    indexed_markdown(source),
                    source_metadata(view(source)),
                )
            )
        ):
            raise ArtifactError("agent_evidence_invalid")
