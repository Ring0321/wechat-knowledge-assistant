"""M4 real OCR/Chromium and tenant-scoped ingestion storage acceptance."""

import asyncio
import hashlib
import os
from pathlib import Path
from uuid import UUID

import pytest
from PIL import Image, ImageDraw, ImageFont
from sqlalchemy import func, select

from app.adapters.browser import GuardedBrowserRenderer
from app.adapters.web_http import SafeWebFetcher
from app.db.models import Asset, Source
from app.db.session import tenant_session
from app.domain.artifacts import ArtifactError, StoredObject
from app.domain.enums import JobStatus, SourceStatus, SourceType
from app.domain.parsing import ParseLimits, WebResponse
from app.ingestion.models import MessageSource
from app.ingestion.pipeline import IngestionPipeline
from app.ingestion.worker import IngestionWorker
from app.parsers.local import LocalFileParser
from app.parsers.web import WebPageParser
from tests.integration.test_ingestion import Harness, Media, s3_client
from tests.integration.test_ingestion import harness as harness
from tests.integration.test_ingestion import s3_config as s3_config
from tests.parser_helpers import text_pdf
from tests.unit.test_office import docx, pptx, write, xlsx

pytestmark = pytest.mark.integration


def browser_endpoint() -> str:
    value = os.environ.get("TEST_BROWSER_WS_URL")
    assert value, "Full gate must provide the isolated browser endpoint"
    return value


async def run_one(harness: Harness, pipeline: IngestionPipeline) -> None:
    await harness.repository.publish_due(harness.queue)
    assert await IngestionWorker(harness.queue, harness.repository, pipeline).once()
    assert (await harness.jobs())[-1].status == JobStatus.COMPLETED


async def test_real_pdf_pipeline_upgrades_metadata_preserving_source_and_assets(
    harness: Harness, tmp_path: Path, s3_config: dict[str, str]
) -> None:
    path = tmp_path / "sample.pdf"
    text_pdf(path, pages=2)
    harness.media = Media(path.read_bytes(), "sample.pdf")
    user_id = await harness.admit(file=True)
    await harness.drain()  # M3 original + metadata only.
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        old = (await session.scalars(select(Source))).one()
        assert old.status == SourceStatus.METADATA_ONLY
        old.metadata_ = {**old.metadata_, "parse_error": "ocr_unavailable"}
        source_id, original_key = old.id, old.storage_key
    await harness.admit(file=True)
    pipeline = IngestionPipeline(
        harness.repository, harness.media, harness.objects, parser=LocalFileParser(ParseLimits())
    )
    await run_one(harness, pipeline)
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = await session.get(Source, source_id)
        assert source and source.status == SourceStatus.STORED
        assert source.storage_key == original_key and "page 2" in source.text
        assert source.title == "Synthetic knowledge" and source.vector_file_id is None
        assert "parse_error" not in source.metadata_
        assert await session.scalar(select(func.count()).select_from(Asset)) == 5
        assert set(await session.scalars(select(MessageSource.source_id))) == {source_id}
        markdown_key = source.metadata_["markdown_key"]
    client = s3_client(s3_config)
    try:
        body = client.get_object(Bucket=s3_config["bucket"], Key=markdown_key)["Body"]
        content = body.read().decode()
        body.close()
        assert "page" in content and "page 2" in content and "deferred_m6" in content
    finally:
        client.close()


async def test_broken_pdf_keeps_original_and_safe_diagnostic(harness: Harness) -> None:
    harness.media = Media(b"sensitive invalid content", "broken.pdf")
    user_id = await harness.admit(file=True)
    await run_one(
        harness,
        IngestionPipeline(
            harness.repository,
            harness.media,
            harness.objects,
            parser=LocalFileParser(ParseLimits()),
        ),
    )
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = (await session.scalars(select(Source))).one()
        assert source.status == SourceStatus.METADATA_ONLY and source.storage_key
        assert source.metadata_["parse_error"] == "document_invalid" and source.text == ""
        assert "sensitive" not in str(source.metadata_)


async def test_upgrade_upload_failure_rolls_back_and_retry_reuses_original(
    harness: Harness, tmp_path: Path, s3_config: dict[str, str]
) -> None:
    path = tmp_path / "sample.pdf"
    text_pdf(path)
    harness.media = Media(path.read_bytes(), path.name)
    user_id = await harness.admit(file=True)
    await harness.drain()
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        original = (await session.scalars(select(Source))).one()
        source_id, key = original.id, original.storage_key
    await harness.admit(file=True)

    class FailingUpload:
        calls = 0

        async def put_file(
            self,
            *,
            user_id: UUID,
            source_id: UUID,
            kind: str,
            path: Path,
            sha256: str,
            content_type: str,
        ) -> StoredObject:
            self.calls += 1
            if self.calls == 2:
                raise ArtifactError("s3_unavailable", retryable=True)
            return await harness.objects.put_file(
                user_id=user_id,
                source_id=source_id,
                kind=kind,
                path=path,
                sha256=sha256,
                content_type=content_type,
            )

    parser = LocalFileParser(ParseLimits())
    broken = IngestionPipeline(harness.repository, harness.media, FailingUpload(), parser=parser)
    await harness.repository.publish_due(harness.queue)
    assert await IngestionWorker(harness.queue, harness.repository, broken).once()
    assert (await harness.jobs())[-1].error_message == "s3_unavailable"
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = await session.get(Source, source_id)
        assert source and source.status == SourceStatus.METADATA_ONLY and source.storage_key == key
        assert await session.scalar(select(func.count()).select_from(Asset)) == 3
        assert len(set(await session.scalars(select(MessageSource.source_id)))) == 2
    dispatch = (await harness.dispatches())[-1]
    assert await harness.repository.retry(dispatch.id)
    # Advance the existing transport dedup window without a 60-second wall-clock wait.
    digest = hashlib.sha256(str(dispatch.id).encode()).hexdigest()
    assert await harness.queue.redis.expire(harness.queue.stream + ":dedup:" + digest, 0)
    await run_one(
        harness,
        IngestionPipeline(harness.repository, harness.media, harness.objects, parser=parser),
    )
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = await session.get(Source, source_id)
        assert source and source.status == SourceStatus.STORED and source.storage_key == key
        assert await session.scalar(select(func.count()).select_from(Asset)) == 5
        assert set(await session.scalars(select(MessageSource.source_id))) == {source_id}
    client = s3_client(s3_config)
    try:
        result = client.list_objects_v2(Bucket=s3_config["bucket"], Prefix=f"users/{user_id}/")
        assert result["KeyCount"] == 5
    finally:
        client.close()


async def test_same_hash_other_user_cannot_upgrade_previous_users_source(
    harness: Harness, tmp_path: Path
) -> None:
    path = tmp_path / "sample.pdf"
    text_pdf(path)
    harness.media = Media(path.read_bytes(), path.name)
    first = await harness.admit(file=True)
    await harness.drain()
    async with tenant_session(harness.store.tenant_factory, first) as session:
        old = (await session.scalars(select(Source))).one()
        old_id, old_hash = old.id, old.sha256
    second = await harness.admit(file=True, user="second-customer")
    pipeline = IngestionPipeline(
        harness.repository, harness.media, harness.objects, parser=LocalFileParser(ParseLimits())
    )
    await harness.repository.publish_due(harness.queue)
    assert await IngestionWorker(harness.queue, harness.repository, pipeline).once()
    async with tenant_session(harness.store.tenant_factory, second) as session:
        source = (await session.scalars(select(Source))).one()
        assert source.id != old_id and source.sha256 == old_hash
        assert source.status == SourceStatus.STORED and str(second) in source.storage_key
        assert await session.get(Source, old_id) is None
    async with tenant_session(harness.store.tenant_factory, first) as session:
        unchanged = await session.get(Source, old_id)
        assert unchanged and unchanged.status == SourceStatus.METADATA_ONLY
        assert await session.scalar(select(func.count()).select_from(Asset)) == 3


@pytest.mark.parametrize(
    "extension,kind,make",
    [
        ("docx", SourceType.WORD, docx),
        ("xlsx", SourceType.EXCEL, xlsx),
        ("pptx", SourceType.PPT, pptx),
    ],
)
async def test_office_child_process_to_real_storage(
    harness: Harness, tmp_path: Path, extension: str, kind: SourceType, make: object
) -> None:
    path = write(tmp_path, make())
    harness.media = Media(path.read_bytes(), "document." + extension)
    user_id = await harness.admit(file=True)
    await run_one(
        harness,
        IngestionPipeline(
            harness.repository,
            harness.media,
            harness.objects,
            parser=LocalFileParser(ParseLimits()),
        ),
    )
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = (await session.scalars(select(Source))).one()
        assert source.status == SourceStatus.STORED and source.source_type == kind
        assert source.text and source.metadata_["segments"]


@pytest.mark.parametrize("extension,kind", [("png", SourceType.IMAGE), ("pdf", SourceType.PDF)])
async def test_real_tesseract_image_and_scanned_pdf(
    tmp_path: Path, extension: str, kind: SourceType
) -> None:
    image = Image.new("RGB", (1100, 250), "white")
    font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 60)
    ImageDraw.Draw(image).text((30, 70), "KNOWLEDGE ARCHIVE", fill="black", font=font)
    path = tmp_path / ("scan." + extension)
    image.save(path)
    result = await LocalFileParser(ParseLimits()).parse(path, filename=path.name, source_type=kind)
    assert "KNOWLEDGE" in result.text and "ARCHIVE" in result.text
    assert result.metadata["ocr"] and result.segments[0]["method"] == "ocr"


class DynamicWeb:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def fetch(self, url: str, *, max_bytes: int) -> WebResponse:
        self.calls.append(url)
        if url == "https://example.com/app.js":
            body = b"document.querySelector('article').textContent='Rendered knowledge content';"
            return WebResponse(url, 200, body, "application/javascript")
        if url == "https://example.com/page":
            return WebResponse(
                url,
                200,
                b'<title>Dynamic</title><article></article><script src="/app.js"></script>',
            )
        raise ArtifactError("web_address_blocked")


async def test_real_isolated_browser_dynamic_page_to_canonical(harness: Harness) -> None:
    fetcher = DynamicWeb()
    renderer = GuardedBrowserRenderer(fetcher, browser_endpoint())
    webpages = WebPageParser(fetcher, renderer, ParseLimits())
    user_id = await harness.admit("https://example.com/page")
    await run_one(
        harness,
        IngestionPipeline(harness.repository, harness.media, harness.objects, webpages=webpages),
    )
    async with tenant_session(harness.store.tenant_factory, user_id) as session:
        source = (await session.scalars(select(Source))).one()
        assert source.title == "Dynamic" and source.text == "Rendered knowledge content"
        assert source.source_type == SourceType.WEB_PAGE and source.metadata_["rendered"]
        assert source.original_url == "https://example.com/page"
        assert source.metadata_["final_url"] == source.original_url
    assert "https://example.com/app.js" in fetcher.calls


async def test_real_browser_blocks_private_requests_websockets_post_and_file() -> None:
    canary_calls: list[bytes] = []

    async def canary(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        canary_calls.append(await reader.read(4096))
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(canary, "0.0.0.0", 0)
    port = server.sockets[0].getsockname()[1]
    private = f"http://tests:{port}/sensitive"
    safe = SafeWebFetcher()

    class Attacker:
        calls: list[str] = []

        async def fetch(self, url: str, *, max_bytes: int) -> WebResponse:
            self.calls.append(url)
            if url == "https://example.com/attack":
                body = (
                    "<article>Safe visible content</article><script>"
                    f'fetch("{private}").catch(()=>{{}});'
                    f'fetch("{private}",{{method:"POST",body:"secret"}}).catch(()=>{{}});'
                    f'new WebSocket("ws://tests:{port}/socket");'
                    '</script><iframe src="http://127.0.0.1/private"></iframe>'
                    '<iframe src="file:///etc/passwd"></iframe>'
                ).encode()
                return WebResponse(url, 200, body)
            return await safe.fetch(url, max_bytes=max_bytes)

    attacker = Attacker()
    try:
        renderer = GuardedBrowserRenderer(attacker, browser_endpoint())
        result = await renderer.render("https://example.com/attack", max_bytes=100_000)
        assert b"Safe visible content" in result.body and not canary_calls
        assert attacker.calls == ["https://example.com/attack"]
        with pytest.raises(ArtifactError, match="web_url_invalid"):
            await renderer.render("file:///etc/passwd", max_bytes=1000)
    finally:
        server.close()
        await server.wait_closed()
        await safe.aclose()


async def test_real_browser_navigation_to_private_address_fails_closed() -> None:
    class Redirect:
        async def fetch(self, url: str, *, max_bytes: int) -> WebResponse:
            return WebResponse(
                url, 200, b'<script>location.href="http://127.0.0.1/private";</script>'
            )

    with pytest.raises(ArtifactError, match="web_address_blocked"):
        await GuardedBrowserRenderer(Redirect(), browser_endpoint()).render(
            "https://example.com/page", max_bytes=10000
        )


@pytest.mark.parametrize("budget", ["requests", "bytes"])
async def test_real_browser_resource_limits_do_not_return_partial_success(budget: str) -> None:
    fetcher = DynamicWeb()
    renderer = GuardedBrowserRenderer(
        fetcher,
        browser_endpoint(),
        max_requests=1 if budget == "requests" else 40,
        total_bytes=1 if budget == "bytes" else 100000,
    )
    with pytest.raises(ArtifactError, match="browser_.*limit"):
        await renderer.render("https://example.com/page", max_bytes=100000)
