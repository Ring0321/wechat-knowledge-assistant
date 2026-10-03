"""Bounded originals, optional M4 parsing, canonical JSON and Markdown.

Indexing is a separate durable task. Object writes use stable keys;
if the database commit fails, a retry overwrites the same keys instead of duplicating them.
"""

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import JsonValue
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Asset, Source, User
from app.db.session import tenant_session
from app.domain.artifacts import ArtifactError, DownloadedFile, MediaDownloader, ObjectStore
from app.domain.documents import CanonicalDocument
from app.domain.enums import AssetKind, JobStatus, SourceStatus, SourceType
from app.domain.media import MediaArtifact, MediaParser
from app.domain.parsing import FileParser, ParsedContent
from app.ingestion.channels import SUPPLEMENT, can_supplement
from app.ingestion.contracts import WorkItem
from app.ingestion.models import IngestionDispatch, MessageSource
from app.ingestion.rendering import markdown
from app.ingestion.repository import JobRepository
from app.knowledge.jobs import enqueue as enqueue_index
from app.parsers.channels import WeChatChannelsParser
from app.parsers.media import MEDIA_SUFFIXES
from app.parsers.web import WebPageParser

EXTENSIONS = {
    ".pdf": SourceType.PDF,
    ".doc": SourceType.WORD,
    ".docx": SourceType.WORD,
    ".xls": SourceType.EXCEL,
    ".xlsx": SourceType.EXCEL,
    ".ppt": SourceType.PPT,
    ".pptx": SourceType.PPT,
}


@dataclass(frozen=True)
class Prepared:
    original: DownloadedFile
    text: str
    source_type: SourceType
    status: SourceStatus
    metadata: dict[str, JsonValue]
    title: str | None = None
    media_artifacts: tuple[MediaArtifact, ...] = ()


def local_file(path: Path, content: bytes, content_type: str) -> DownloadedFile:
    path.write_bytes(content)
    return DownloadedFile(path, hashlib.sha256(content).hexdigest(), len(content), content_type)


class IngestionPipeline:
    def __init__(
        self,
        repository: JobRepository,
        media: MediaDownloader,
        objects: ObjectStore,
        *,
        parser: FileParser | None = None,
        webpages: WebPageParser | None = None,
        media_parser: MediaParser | None = None,
    ) -> None:
        self.repository, self.media, self.objects = repository, media, objects
        self.parser, self.webpages = parser, webpages
        self.media_parser = media_parser

    async def prepare(self, work: WorkItem, directory: Path) -> Prepared:
        data, settings = work.input_data, self.repository.settings
        kind = data.get("kind")
        if kind == SUPPLEMENT:
            return await self._prepare_supplement(work, directory)
        if work.source_type == SourceType.WECHAT_CHANNEL:
            return await self._prepare_card(work, directory)
        body = ""
        source_type: SourceType = work.source_type
        parsed: ParsedContent | None = None
        media_artifacts: tuple[MediaArtifact, ...] = ()
        metadata: dict[str, JsonValue] = {
            "schema_version": 1,
            "index_status": "deferred_m6",
            "parse_status": "metadata_only",
            "hash_basis": {
                "note": "original_utf8_text",
                "url": "normalized_url_utf8",
                "media": "original_bytes",
            }.get(str(kind), "metadata_json"),
        }
        status = SourceStatus.METADATA_ONLY
        if kind == "media":
            media_id = data.get("media_id")
            if not isinstance(media_id, str) or not media_id:
                raise ArtifactError("media_id_missing")
            original = await self.media.download(
                media_id, directory / "original", max_bytes=settings.ingestion_max_file_bytes
            )
        elif kind == "url" and self.webpages is not None:
            if work.original_url is None:
                raise ArtifactError("source_input_invalid")
            original, parsed = await self.webpages.load(work.original_url, directory / "original")
            metadata["source_metadata"] = data
            metadata["hash_basis"] = "downloaded_html_bytes"
        elif kind == "note":
            value = data.get("text")
            if not isinstance(value, str) or not value.strip():
                raise ArtifactError("note_empty")
            encoded = value.encode("utf-8")
            if len(encoded) > settings.ingestion_max_text_bytes:
                raise ArtifactError("text_too_large")
            original = local_file(directory / "original", encoded, "text/plain; charset=utf-8")
            body, status = value.replace("\r\n", "\n").replace("\r", "\n"), SourceStatus.STORED
        else:
            payload = (
                work.original_url
                if kind == "url"
                else json.dumps(data, ensure_ascii=False, sort_keys=True)
            )
            if not isinstance(payload, str):
                raise ArtifactError("source_input_invalid")
            original = local_file(
                directory / "original",
                payload.encode("utf-8"),
                "text/plain; charset=utf-8" if kind == "url" else "application/json",
            )
            metadata["source_metadata"] = data
        await self.repository.stage(work, JobStatus.PARSING)
        if kind == "media":
            filename = original.filename or data.get("filename")
            extension = Path(filename).suffix.lower() if isinstance(filename, str) else ""
            # Extension classification is a display hint, never a decoder or executable choice.
            source_type = EXTENSIONS.get(extension, source_type)
            metadata.update(
                {
                    "filename": filename,
                    "content_type": original.content_type,
                    "parse_status": "unsupported_format",
                }
            )
            if (
                extension in (".txt", ".md", ".markdown")
                and original.size_bytes <= settings.ingestion_max_text_bytes
            ):
                try:
                    body = original.path.read_bytes().decode("utf-8-sig")
                    if any(ord(char) < 32 and char not in "\n\r\t" for char in body):
                        raise ValueError("binary")
                except (UnicodeError, ValueError):
                    body = ""
                    metadata["parse_status"] = "unsupported_encoding"
                else:
                    body = body.replace("\r\n", "\n").replace("\r", "\n")
                    status = SourceStatus.STORED
            elif extension in (".txt", ".md", ".markdown"):
                metadata["parse_status"] = "text_limit_exceeded"
            elif self.media_parser is not None and MEDIA_SUFFIXES.get(extension, source_type) in (
                SourceType.AUDIO,
                SourceType.VIDEO,
            ):
                # Avoid another paid provider call for an already completed tenant source.
                async with tenant_session(self.repository.tenant_factory, work.user_id) as session:
                    completed = await session.scalar(
                        select(Source).where(
                            Source.sha256 == original.sha256,
                            Source.id != work.source_id,
                            Source.status.in_(
                                (
                                    SourceStatus.STORED,
                                    SourceStatus.READY,
                                    SourceStatus.METADATA_ONLY,
                                )
                            ),
                        )
                    )
                if completed is not None and _reusable_media(completed):
                    return Prepared(original, "", completed.source_type, completed.status, metadata)

                async def transcribing() -> None:
                    await self.repository.stage(work, JobStatus.TRANSCRIBING)

                try:
                    result = await self.media_parser.parse(
                        original.path,
                        directory,
                        source_type=MEDIA_SUFFIXES.get(extension, source_type),
                        on_transcribing=transcribing,
                    )
                    parsed, media_artifacts = result.content, result.artifacts
                except ArtifactError as error:
                    if error.retryable or not error.code.startswith("media_"):
                        raise
                    metadata.update(
                        {
                            "parse_status": "unreadable",
                            "parse_error": error.code,
                            "parser_version": 5,
                        }
                    )
            elif self.parser is not None:
                try:
                    parsed = await self.parser.parse(
                        original.path,
                        filename=filename if isinstance(filename, str) else None,
                        source_type=source_type,
                    )
                except ArtifactError as error:
                    if error.retryable:
                        raise
                    # Original remains valuable even if encrypted, unsupported or resource-limited.
                    metadata.update(
                        {
                            "parse_status": "unreadable",
                            "parse_error": error.code,
                            "parser_version": 4,
                        }
                    )
        if parsed is not None:
            body, source_type = parsed.text, parsed.source_type
            segments: list[JsonValue] = list(parsed.segments)
            metadata.update({"parser_version": 4, **parsed.metadata, "segments": segments})
            status = SourceStatus.STORED if body.strip() else SourceStatus.METADATA_ONLY
        if status == SourceStatus.STORED:
            metadata["parse_status"] = "normalized"
        metadata["original_sha256"] = original.sha256
        return Prepared(
            original,
            body,
            source_type,
            status,
            metadata,
            parsed.title if parsed else None,
            media_artifacts,
        )

    async def process(self, work: WorkItem) -> None:
        with TemporaryDirectory(prefix="pkb-ingestion-") as temp:
            directory = Path(temp)  # TemporaryDirectory provides an absolute local path.
            prepared = await self.prepare(work, directory)
            await self.finalize(work, prepared, directory)

    async def _prepare_card(self, work: WorkItem, directory: Path) -> Prepared:
        parsed = WeChatChannelsParser().parse(work.input_data.get("channels", {}))
        # Titles/names are not a stable video identity. Preserve each distinct share;
        # transport message deduplication and job leases still make retries idempotent.
        data: dict[str, JsonValue] = {
            "kind": "metadata",
            "channels": parsed.metadata["channels"],
            "card_source_id": str(work.source_id),
        }
        original = local_file(
            directory / "original",
            json.dumps(data, ensure_ascii=False, sort_keys=True).encode("utf-8"),
            "application/json",
        )
        await self.repository.stage(work, JobStatus.PARSING)
        return Prepared(
            original,
            "",
            SourceType.WECHAT_CHANNEL,
            SourceStatus.METADATA_ONLY,
            {
                **parsed.metadata,
                "schema_version": 1,
                "source_metadata": data,
                "hash_basis": "card_event_metadata_json",
                "original_sha256": original.sha256,
            },
            parsed.title,
        )

    async def _prepare_supplement(self, work: WorkItem, directory: Path) -> Prepared:
        if work.source_type != SourceType.WECHAT_CHANNEL:
            raise ArtifactError("channel_supplement_target_invalid")
        if self.media_parser is None:
            raise ArtifactError("media_parser_unavailable", retryable=True)
        media_id = work.input_data.get("media_id")
        if not isinstance(media_id, str) or not media_id:
            raise ArtifactError("media_id_missing")
        original = await self.media.download(
            media_id,
            directory / "original",
            max_bytes=self.repository.settings.ingestion_max_file_bytes,
        )
        await self.repository.stage(work, JobStatus.PARSING)

        async def transcribing() -> None:
            await self.repository.stage(work, JobStatus.TRANSCRIBING)

        result = await self.media_parser.parse(
            original.path,
            directory,
            source_type=SourceType.VIDEO,
            on_transcribing=transcribing,
        )
        parsed = result.content
        if parsed.source_type != SourceType.VIDEO:
            raise ArtifactError("channel_supplement_not_video")
        return Prepared(
            original,
            parsed.text,
            SourceType.WECHAT_CHANNEL,
            SourceStatus.STORED if parsed.text.strip() else SourceStatus.METADATA_ONLY,
            {
                **parsed.metadata,
                "segments": list(parsed.segments),
                "parse_status": "normalized" if parsed.text.strip() else "no_speech_detected",
                "content_availability": "user_supplied_video",
                "original_video_available": True,
                "supplement_sha256": original.sha256,
                "supplement_job_id": str(work.job_id),
                "index_status": "pending" if parsed.text.strip() else "metadata_only",
            },
            media_artifacts=result.artifacts,
        )

    async def finalize(self, work: WorkItem, prepared: Prepared, directory: Path) -> None:
        repo = self.repository
        supplement = work.input_data.get("kind") == SUPPLEMENT
        async with tenant_session(repo.tenant_factory, work.user_id) as session:
            job = await repo.locked_job(session, work)
            source = await session.get(Source, work.source_id, with_for_update=True)
            user = await session.get(User, work.user_id)
            if source is None or source.status in (SourceStatus.DELETING, SourceStatus.DELETED):
                raise ArtifactError("source_unavailable")
            if (
                user is None
                or not user.is_active
                or not repo.settings.allows_wecom_user(user.wecom_external_user_id)
            ):
                raise ArtifactError("allowlist_revoked")
            if supplement and not await can_supplement(session, source):
                raise ArtifactError("channel_supplement_target_invalid")
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": f"ingestion:{work.user_id}:{prepared.original.sha256}"},
            )
            existing = (
                None
                if supplement
                else (
                    await session.scalars(
                        select(Source)
                        .where(
                            Source.sha256 == prepared.original.sha256,
                            Source.id != source.id,
                            Source.status != SourceStatus.DELETED,
                        )
                        .with_for_update()
                    )
                ).one_or_none()
            )
            outcome = "completed"
            notification_source = source
            upgrade = (
                existing is not None
                and existing.status == SourceStatus.METADATA_ONLY
                and (
                    prepared.status == SourceStatus.STORED
                    or (
                        bool(prepared.media_artifacts)
                        and prepared.metadata.get("parse_status") == "no_speech_detected"
                        and not _reusable_media(existing)
                    )
                )
            )
            candidate = source
            if existing is not None and not upgrade:
                if existing.status not in (
                    SourceStatus.STORED,
                    SourceStatus.METADATA_ONLY,
                    SourceStatus.READY,
                ):
                    raise ArtifactError("matching_source_not_ready", retryable=True)
                await session.execute(
                    update(MessageSource)
                    .where(MessageSource.source_id == source.id)
                    .values(source_id=existing.id)
                )
                source.status = SourceStatus.DELETED
                source.metadata_ = {"duplicate_of": str(existing.id)}
                outcome, notification_source = "duplicate", existing
            else:
                if upgrade:
                    assert existing is not None
                    source = existing
                    # A successful reparse supersedes the prior parsing diagnostic.
                    source.metadata_ = {
                        key: value
                        for key, value in source.metadata_.items()
                        if key != "parse_error"
                    }
                    await session.execute(
                        update(MessageSource)
                        .where(MessageSource.source_id == candidate.id)
                        .values(source_id=existing.id)
                    )
                    candidate.status = SourceStatus.DELETED
                    candidate.metadata_ = {"duplicate_of": str(existing.id), "upgraded": True}
                    notification_source = existing
                if prepared.title:
                    source.title = prepared.title[:512]
                media_metadata: dict[str, JsonValue] = {}
                if prepared.media_artifacts:
                    media_metadata["media_assets"] = await self._store_media(
                        session, work, source, prepared.media_artifacts, directory
                    )
                document = CanonicalDocument(
                    source_id=source.id,
                    user_id=work.user_id,
                    title=source.title,
                    source_type=prepared.source_type,
                    original_url=source.original_url,
                    created_at=source.created_at,
                    text=prepared.text,
                    summary=source.summary,
                    tags=source.tags,
                    metadata={**source.metadata_, **prepared.metadata, **media_metadata},
                )
                canonical = local_file(
                    directory / "canonical.json",
                    document.model_dump_json(indent=2).encode("utf-8"),
                    "application/json",
                )
                rendered = local_file(
                    directory / "document.md",
                    markdown(document).encode("utf-8"),
                    "text/markdown; charset=utf-8",
                )
                stored_keys: dict[str, JsonValue] = {}
                for kind, artifact in (
                    ("original", prepared.original),
                    ("canonical", canonical),
                    ("markdown", rendered),
                ):
                    if upgrade and kind == "original":
                        stored_keys["original_key"] = source.storage_key
                        continue
                    stored = await self.objects.put_file(
                        user_id=work.user_id,
                        source_id=source.id,
                        kind=kind,
                        path=artifact.path,
                        sha256=artifact.sha256,
                        content_type=artifact.content_type,
                    )
                    session.add(
                        Asset(
                            user_id=work.user_id,
                            source_id=source.id,
                            kind=AssetKind.ORIGINAL if kind == "original" else AssetKind.NORMALIZED,
                            storage_key=stored.key,
                            size_bytes=stored.size_bytes,
                            sha256=stored.sha256,
                            content_type=stored.content_type,
                            filename=artifact.filename,
                            metadata_={"format": kind},
                        )
                    )
                    key_name = "supplement_original" if supplement and kind == "original" else kind
                    stored_keys[key_name + "_key"] = stored.key
                    if kind == "original" and not supplement:
                        source.storage_key = stored.key
                source.status, source.source_type = prepared.status, prepared.source_type
                if not supplement:
                    source.sha256 = prepared.original.sha256
                source.text = prepared.text
                source.metadata_ = {
                    **source.metadata_,
                    **prepared.metadata,
                    **media_metadata,
                    **stored_keys,
                }
            # Expired workers cannot finalize after slow external uploads.
            if job.lease_expires_at is None or job.lease_expires_at <= datetime.now(UTC):
                raise ArtifactError("job_lease_lost", retryable=True)
            job.status, job.completed_at = JobStatus.COMPLETED, datetime.now(UTC)
            job.lease_token = job.lease_expires_at = job.next_retry_at = job.error_message = None
            dispatch = await session.get(IngestionDispatch, work.dispatch_id)
            assert dispatch is not None
            dispatch.finished_at = datetime.now(UTC)
            if (
                repo.settings.knowledge_enabled
                and notification_source.status == SourceStatus.STORED
            ):
                await enqueue_index(
                    session, notification_source, repo.settings, message_id=job.message_id
                )
                if outcome == "duplicate":
                    await repo.notifier.finished(session, job, notification_source, outcome=outcome)
            else:
                await repo.notifier.finished(session, job, notification_source, outcome=outcome)

    async def _store_media(
        self,
        session: AsyncSession,
        work: WorkItem,
        source: Source,
        artifacts: tuple[MediaArtifact, ...],
        directory: Path,
    ) -> list[JsonValue]:
        references: list[JsonValue] = []
        known: dict[tuple[AssetKind, str], str] = {}
        for item in artifacts:
            if item.kind not in (AssetKind.AUDIO, AssetKind.KEYFRAME, AssetKind.TRANSCRIPT):
                raise ArtifactError("media_artifact_invalid")
            digest = await asyncio.to_thread(_artifact_hash, item.path, directory)
            identity = (item.kind, digest)
            key = known.get(identity)
            if key is None:
                stored = await self.objects.put_file(
                    user_id=work.user_id,
                    source_id=source.id,
                    kind=item.kind.value,
                    path=item.path,
                    sha256=digest,
                    content_type=item.content_type,
                )
                key = known[identity] = stored.key
                # Repeated identical frames/chunks share a blob; timeline retains all positions.
                asset = await session.scalar(select(Asset).where(Asset.storage_key == key))
                if asset is None:
                    session.add(
                        Asset(
                            user_id=work.user_id,
                            source_id=source.id,
                            kind=item.kind,
                            storage_key=key,
                            size_bytes=stored.size_bytes,
                            sha256=digest,
                            content_type=item.content_type,
                            metadata_={"parser_version": 5},
                        )
                    )
            references.append(
                {
                    "kind": item.kind.value,
                    "storage_key": key,
                    "sha256": digest,
                    "start_seconds": item.start,
                    "end_seconds": item.end,
                }
            )
        return references


def _reusable_media(source: Source) -> bool:
    return source.status in (SourceStatus.STORED, SourceStatus.READY) or (
        source.status == SourceStatus.METADATA_ONLY
        and source.metadata_.get("parser_version") == 5
        and source.metadata_.get("parse_status") == "no_speech_detected"
    )


def _artifact_hash(path: Path, directory: Path) -> str:
    if not path.is_absolute() or ".." in path.parts or not path.is_relative_to(directory):
        raise ArtifactError("media_artifact_invalid")
    for part in (path, *path.parents):
        if part.is_symlink() or part.is_junction():
            raise ArtifactError("media_artifact_invalid")
    if not path.is_file() or path.stat().st_size > 80 * 1024 * 1024:
        raise ArtifactError("media_artifact_invalid")
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()
