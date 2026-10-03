"""Stable index representation shared by upload hashing and citation positioning."""

from app.db.models import Source
from app.domain.documents import CanonicalDocument
from app.ingestion.rendering import markdown


def indexed_markdown(source: Source) -> str:
    return markdown(
        CanonicalDocument(
            source_id=source.id,
            user_id=source.user_id,
            title=source.title,
            source_type=source.source_type,
            original_url=source.original_url,
            created_at=source.created_at,
            text=source.text or "",
            summary=source.summary,
            tags=source.tags,
            metadata=source.metadata_,
        ),
        index_status="normalized_markdown",
    )


def document_bytes(source: Source) -> bytes:
    return indexed_markdown(source).encode("utf-8")
