"""Only locally authorized, live source mappings become retrieval evidence."""

from datetime import datetime
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings
from app.db.models import KnowledgeFile, Source, User
from app.db.session import tenant_session
from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceStatus
from app.domain.knowledge import KnowledgeHit, SourceView, TenantContext, VectorProvider
from app.knowledge.jobs import DELETE, enqueue
from app.knowledge.rendering import indexed_markdown


async def authorize(session: AsyncSession, context: TenantContext, settings: Settings) -> User:
    user = await session.get(User, context.user_id)
    if (
        user is None
        or not user.is_active
        or user.wecom_corp_id != settings.wecom_corp_id
        or not settings.allows_wecom_user(user.wecom_external_user_id)
    ):
        raise ArtifactError("knowledge_access_denied")
    return user


def view(source: Source) -> SourceView:
    return SourceView(
        source.id,
        source.title,
        source.source_type,
        source.original_url,
        source.created_at,
        source.status,
        source.summary,
        tuple(source.tags),
        source.text or "",
    )


def matching_locators(source: Source, excerpt: str) -> tuple[dict[str, JsonValue], ...]:
    """Return only stored segment locators whose text overlaps a retrieved excerpt.

    No timestamp is inferred from a model or from the whole video's duration.
    """
    segments = source.metadata_.get("segments")
    if not isinstance(segments, list):
        return ()
    result: list[dict[str, JsonValue]] = []
    raw_markdown = indexed_markdown(source)
    header, _, _ = raw_markdown.partition("\n## Content\n\n")
    content_start = len(" ".join((header + "\n## Content\n\n").split()))
    rendered = " ".join(raw_markdown.split())
    needle = " ".join(excerpt.split())
    if not needle:
        return ()
    excerpt_start = rendered.find(needle, content_start)
    if excerpt_start < 0:
        excerpt_start = rendered.find(needle)
    # Ambiguous repeated text cannot identify a particular video position.
    unique = excerpt_start >= 0 and rendered.find(needle, excerpt_start + 1) < 0
    cursor = content_start
    for segment in segments:
        if not isinstance(segment, dict):
            continue
        body, locator = segment.get("text"), segment.get("locator")
        if isinstance(body, str) and body.strip() and isinstance(locator, dict):
            normalized = " ".join(body.split())
            start = rendered.find(normalized, cursor)
            if start < 0:
                continue
            end = start + len(normalized)
            cursor = end
            if unique and start < excerpt_start + len(needle) and end > excerpt_start:
                result.append(dict(locator))
            elif excerpt_start < 0 and normalized in needle:
                # Providers may reformat Markdown; a complete stored segment remains evidence.
                result.append(dict(locator))
    return tuple(result[:20])


class KnowledgeService:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        provider: VectorProvider,
    ) -> None:
        self.factory, self.settings, self.provider = factory, settings, provider

    async def index_document(self, context: TenantContext, source_id: UUID) -> UUID | None:
        async with tenant_session(self.factory, context.user_id) as session:
            await authorize(session, context, self.settings)
            source = await session.get(Source, source_id, with_for_update=True)
            if source is None:
                raise ArtifactError("knowledge_source_not_found")
            return await enqueue(session, source, self.settings)

    async def delete_document(self, context: TenantContext, source_id: UUID) -> UUID | None:
        async with tenant_session(self.factory, context.user_id) as session:
            await authorize(session, context, self.settings)
            source = await session.get(Source, source_id, with_for_update=True)
            if source is None:
                raise ArtifactError("knowledge_source_not_found")
            return await enqueue(session, source, self.settings, operation=DELETE)

    async def get_source(self, context: TenantContext, source_id: UUID) -> SourceView | None:
        async with tenant_session(self.factory, context.user_id) as session:
            await authorize(session, context, self.settings)
            source = await session.get(Source, source_id)
            if source is None or source.status in (SourceStatus.DELETED, SourceStatus.DELETING):
                return None
            return view(source)

    async def list_sources(
        self,
        context: TenantContext,
        *,
        limit: int = 20,
        offset: int = 0,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> tuple[SourceView, ...]:
        if not 1 <= limit <= 100 or not 0 <= offset <= 10000:
            raise ArtifactError("knowledge_pagination_invalid")
        if any(value is not None and value.tzinfo is None for value in (since, until)):
            raise ArtifactError("knowledge_date_invalid")
        if since is not None and until is not None and since > until:
            raise ArtifactError("knowledge_date_invalid")
        async with tenant_session(self.factory, context.user_id) as session:
            await authorize(session, context, self.settings)
            statement = select(Source).where(
                Source.status.not_in((SourceStatus.DELETED, SourceStatus.DELETING))
            )
            if since is not None:
                statement = statement.where(Source.created_at >= since)
            if until is not None:
                statement = statement.where(Source.created_at < until)
            rows = await session.scalars(
                statement.order_by(Source.created_at.desc(), Source.id).limit(limit).offset(offset)
            )
            return tuple(view(row) for row in rows)

    async def search(
        self,
        context: TenantContext,
        query: str,
        *,
        limit: int = 10,
    ) -> tuple[KnowledgeHit, ...]:
        if not self.settings.knowledge_enabled:
            raise ArtifactError("knowledge_disabled")
        if not query.strip() or len(query.encode("utf-8")) > 8192 or not 1 <= limit <= 50:
            raise ArtifactError("knowledge_query_invalid")
        async with tenant_session(self.factory, context.user_id) as session:
            user = await authorize(session, context, self.settings)
            store_id = user.vector_store_id
        if store_id is None:
            return ()
        hits = await self.provider.search(store_id, query, user_id=context.user_id, limit=limit)
        results: list[KnowledgeHit] = []
        # Authorization and tombstones are checked AFTER remote search, never trust its attributes.
        async with tenant_session(self.factory, context.user_id) as session:
            user = await authorize(session, context, self.settings)
            if user.vector_store_id != store_id:
                raise ArtifactError("knowledge_store_changed", retryable=True)
            for hit in hits[:50]:
                if hit.attributes.get("user_id") != str(context.user_id) or not hit.text.strip():
                    continue
                row = (
                    await session.execute(
                        select(Source, KnowledgeFile)
                        .join(KnowledgeFile, KnowledgeFile.source_id == Source.id)
                        .where(
                            Source.status == SourceStatus.READY,
                            Source.vector_file_id == hit.file_id,
                            KnowledgeFile.vector_file_id == hit.file_id,
                            KnowledgeFile.vector_store_id == store_id,
                        )
                    )
                ).one_or_none()
                if row is None:
                    continue
                source, record = row
                if (
                    hit.attributes.get("source_id") != str(source.id)
                    or hit.attributes.get("document_sha256") != record.document_sha256
                ):
                    continue
                results.append(
                    KnowledgeHit(
                        view(source), hit.score, hit.text, matching_locators(source, hit.text)
                    )
                )
                if len(results) >= limit:
                    break
        return tuple(results)
