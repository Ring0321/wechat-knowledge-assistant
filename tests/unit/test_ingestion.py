"""M3 source routing and preparation, without database or external services."""

import asyncio
import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import JsonValue

from app.domain.artifacts import ArtifactError, DownloadedFile, StoredObject
from app.domain.documents import CanonicalDocument
from app.domain.enums import JobStatus, SourceStatus, SourceType
from app.ingestion.contracts import WorkItem
from app.ingestion.detection import detect_sources, url_source
from app.ingestion.pipeline import IngestionPipeline
from app.ingestion.rendering import markdown
from app.ingestion.repository import JobRepository


@dataclass
class PreparationSettings:
    ingestion_max_file_bytes: int = 4096
    ingestion_max_text_bytes: int = 1024


@dataclass
class PreparationRepository:
    settings: PreparationSettings = field(default_factory=PreparationSettings)
    stages: list[tuple[UUID, JobStatus]] = field(default_factory=list)

    async def stage(self, work: WorkItem, status: JobStatus) -> None:
        self.stages.append((work.job_id, status))


@dataclass
class FakeMedia:
    content: bytes = b""
    filename: str | None = None
    content_type: str = "application/octet-stream"
    error: ArtifactError | None = None
    calls: list[tuple[str, Path, int]] = field(default_factory=list)

    async def download(self, media_id: str, destination: Path, *, max_bytes: int) -> DownloadedFile:
        self.calls.append((media_id, destination, max_bytes))
        if self.error is not None:
            raise self.error
        await asyncio.to_thread(destination.write_bytes, self.content)
        return DownloadedFile(
            destination,
            hashlib.sha256(self.content).hexdigest(),
            len(self.content),
            self.content_type,
            self.filename,
        )


class NoObjectWrites:
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
        raise AssertionError("prepare must not upload or index documents")


def make_work(
    data: dict[str, JsonValue],
    *,
    source_type: SourceType = SourceType.OTHER,
    original_url: str | None = None,
) -> WorkItem:
    return WorkItem(
        dispatch_id=uuid4(),
        user_id=uuid4(),
        job_id=uuid4(),
        source_id=uuid4(),
        lease_token=uuid4(),
        title="测试来源",
        source_type=source_type,
        original_url=original_url,
        created_at=datetime(2026, 9, 22, tzinfo=UTC),
        input_data=data,
    )


def make_pipeline(
    media: FakeMedia | None = None,
) -> tuple[IngestionPipeline, PreparationRepository, FakeMedia]:
    repository = PreparationRepository()
    media = media or FakeMedia(error=ArtifactError("unexpected_media_download"))
    pipeline = IngestionPipeline(cast(JobRepository, repository), media, NoObjectWrites())
    return pipeline, repository, media


@pytest.mark.parametrize("word", ["记住", "保存", "收录", "收藏"])
def test_save_words_route_to_notes_and_preserve_text(word: str) -> None:
    text = f"  {word}：下午三点开会\n会议室 A  "
    sources = detect_sources("text", text, {})
    assert len(sources) == 1
    assert sources[0].source_type == SourceType.NOTE
    assert sources[0].title == f"{word}：下午三点开会"
    assert sources[0].data == {"kind": "note", "text": text}


@pytest.mark.parametrize("text", ["之前那份报告说了什么？", "你好", "请总结历史资料"])
def test_ordinary_text_is_left_for_question_route(text: str) -> None:
    assert detect_sources("text", text, {}) == []


def test_multiple_urls_keep_order_deduplicate_and_preserve_annotation() -> None:
    text = "保存这两篇 https://example.com/one https://example.com/two https://example.com/one"
    sources = detect_sources("text", text, {})
    assert [source.original_url for source in sources] == [
        "https://example.com/one",
        "https://example.com/two",
    ]
    assert all(source.source_type == SourceType.WEB_PAGE for source in sources)
    assert all(source.data["annotation"] == text for source in sources)


def test_url_deduplication_uses_normalized_urls() -> None:
    sources = detect_sources("text", "https://example.com/a https://example.com/a。", {})
    assert len(sources) == 1
    assert sources[0].original_url == "https://example.com/a"


def test_url_limit_rejects_entire_batch_instead_of_losing_sources() -> None:
    with pytest.raises(ArtifactError) as caught:
        detect_sources("text", "https://example.com/a https://example.com/b", {}, max_urls=1)
    assert caught.value.code == "too_many_urls"
    assert not caught.value.retryable


def test_repeated_identical_url_does_not_consume_batch_limit() -> None:
    assert len(detect_sources("text", "https://example.com/a " * 5, {}, max_urls=1)) == 1


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://mp.weixin.qq.com/s/article", SourceType.WECHAT_ARTICLE),
        ("HTTPS://MP.WEIXIN.QQ.COM/s/article", SourceType.WECHAT_ARTICLE),
        ("https://mp.weixin.qq.com.evil.example/s/article", SourceType.WEB_PAGE),
        ("https://example.com/?host=mp.weixin.qq.com", SourceType.WEB_PAGE),
    ],
)
def test_wechat_article_classification_uses_exact_hostname(url: str, expected: SourceType) -> None:
    source = url_source(url)
    assert source.source_type == expected
    assert source.original_url is not None
    assert source.original_url.startswith("https://")


def test_link_card_preserves_official_title() -> None:
    sources = detect_sources(
        "link", None, {"link": {"title": "课程资料", "url": "https://example.com/course"}}
    )
    assert len(sources) == 1
    assert sources[0].title == "课程资料"
    assert sources[0].original_url == "https://example.com/course"


@pytest.mark.parametrize("url", ["file:///etc/passwd", "https://", "http://[broken", "x" * 8193])
def test_invalid_link_is_kept_as_metadata(url: str) -> None:
    source = url_source(url)
    assert source.source_type == SourceType.OTHER
    assert source.original_url is None
    assert source.data == {"kind": "metadata", "invalid_url": True}


@pytest.mark.parametrize(
    ("message_type", "source_type", "title"),
    [
        ("image", SourceType.IMAGE, "图片"),
        ("voice", SourceType.AUDIO, "语音"),
        ("video", SourceType.VIDEO, "视频"),
        ("file", SourceType.OTHER, "文件"),
    ],
)
def test_media_detection_preserves_identifier_and_type_hint(
    message_type: str, source_type: SourceType, title: str
) -> None:
    source = detect_sources(message_type, None, {"media_id": "synthetic-media"})[0]
    assert source.source_type == source_type
    assert source.title == title
    assert source.data["media_id"] == "synthetic-media"
    assert source.data["kind"] == "media"


def test_missing_media_identifier_preserves_metadata_without_download_instruction() -> None:
    source = detect_sources("file", None, {})[0]
    assert source.source_type == SourceType.OTHER
    assert source.data == {"kind": "metadata", "message_type": "file"}


def test_channels_and_unknown_cards_do_not_invent_video_urls() -> None:
    card: dict[str, JsonValue] = {"nickname": "作者", "title": "介绍", "sub_type": 1}
    source = detect_sources("channels", None, {"channels": card})[0]
    assert source.source_type == SourceType.WECHAT_CHANNEL
    assert source.original_url is None
    assert source.data == {"kind": "metadata", "channels": card}
    unknown = detect_sources("future_type", None, {"unexpected": "data"})[0]
    assert unknown.source_type == SourceType.OTHER
    assert unknown.data == {"kind": "metadata", "message_type": "future_type"}


async def test_note_preserves_original_bytes_normalizes_lines_and_defers_index(
    tmp_path: Path,
) -> None:
    pipeline, repository, media = make_pipeline()
    original = "记住：第一行\r\n第二行\r第三行"
    work = make_work({"kind": "note", "text": original}, source_type=SourceType.NOTE)
    prepared = await pipeline.prepare(work, tmp_path)
    assert prepared.original.path.read_bytes() == original.encode("utf-8")
    assert prepared.original.sha256 == hashlib.sha256(original.encode("utf-8")).hexdigest()
    assert prepared.original.size_bytes == len(original.encode("utf-8"))
    assert prepared.text == "记住：第一行\n第二行\n第三行"
    assert prepared.source_type == SourceType.NOTE
    assert prepared.status == SourceStatus.STORED
    assert prepared.metadata["parse_status"] == "normalized"
    assert prepared.metadata["index_status"] == "deferred_m6"
    assert prepared.metadata["original_sha256"] == prepared.original.sha256
    assert repository.stages == [(work.job_id, JobStatus.PARSING)]
    assert media.calls == []


@pytest.mark.parametrize("value", [None, "", " \r\n\t", 123])
async def test_invalid_note_is_rejected_before_parsing(value: JsonValue, tmp_path: Path) -> None:
    pipeline, repository, _ = make_pipeline()
    with pytest.raises(ArtifactError) as caught:
        await pipeline.prepare(make_work({"kind": "note", "text": value}), tmp_path)
    assert caught.value.code == "note_empty"
    assert repository.stages == []
    assert await asyncio.to_thread(lambda: list(tmp_path.iterdir())) == []


async def test_note_limit_counts_utf8_bytes(tmp_path: Path) -> None:
    pipeline, repository, _ = make_pipeline()
    repository.settings.ingestion_max_text_bytes = 5
    with pytest.raises(ArtifactError) as caught:
        await pipeline.prepare(make_work({"kind": "note", "text": "记住"}), tmp_path)
    assert caught.value.code == "text_too_large"
    assert repository.stages == []


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/admin",
        "http://127.0.0.1/secret",
        "http://10.0.0.1/",
        "http://172.16.0.1/",
        "http://192.168.0.1/",
        "http://169.254.169.254/latest/meta-data/",
        "http://[::1]/",
        "https://example.com/article",
    ],
)
async def test_urls_are_metadata_only_without_any_http_or_media_download(
    url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def reject_http(*args: object, **kwargs: object) -> httpx.Response:
        raise AssertionError("M3 must not fetch URL content")

    monkeypatch.setattr(httpx.AsyncClient, "request", reject_http)
    pipeline, repository, media = make_pipeline()
    source = url_source(url, annotation="用户附注")
    work = make_work(source.data, source_type=source.source_type, original_url=source.original_url)
    prepared = await pipeline.prepare(work, tmp_path)
    assert prepared.original.path.read_text(encoding="utf-8") == url
    assert prepared.text == ""
    assert prepared.status == SourceStatus.METADATA_ONLY
    assert prepared.metadata["parse_status"] == "metadata_only"
    assert prepared.metadata["index_status"] == "deferred_m6"
    assert prepared.metadata["source_metadata"] == source.data
    assert repository.stages == [(work.job_id, JobStatus.PARSING)]
    assert media.calls == []


async def test_channels_metadata_original_is_json_without_generated_transcript(
    tmp_path: Path,
) -> None:
    pipeline, _, media = make_pipeline()
    data: dict[str, JsonValue] = {
        "kind": "metadata",
        "channels": {"nickname": "作者", "title": "视频介绍", "sub_type": 1},
    }
    work = make_work(data, source_type=SourceType.WECHAT_CHANNEL)
    prepared = await pipeline.prepare(work, tmp_path)
    saved = {**data, "card_source_id": str(work.source_id)}
    assert json.loads(prepared.original.path.read_text(encoding="utf-8")) == saved
    assert prepared.original.content_type == "application/json"
    assert prepared.status == SourceStatus.METADATA_ONLY
    assert prepared.source_type == SourceType.WECHAT_CHANNEL
    assert prepared.text == ""
    assert prepared.metadata["source_metadata"] == saved
    assert media.calls == []


@pytest.mark.parametrize("extension", [".txt", ".md", ".markdown", ".TXT"])
async def test_utf8_text_media_decodes_bom_and_normalizes_line_endings(
    extension: str, tmp_path: Path
) -> None:
    content = b"\xef\xbb\xbf" + "# 标题\r\n内容\r结尾\t值".encode()
    media = FakeMedia(content, f"notes{extension}")
    pipeline, repository, _ = make_pipeline(media)
    work = make_work({"kind": "media", "media_id": "synthetic-media"})
    prepared = await pipeline.prepare(work, tmp_path)
    assert media.calls == [
        ("synthetic-media", tmp_path / "original", repository.settings.ingestion_max_file_bytes)
    ]
    assert prepared.original.path.read_bytes() == content
    assert prepared.text == "# 标题\n内容\n结尾\t值"
    assert prepared.status == SourceStatus.STORED
    assert prepared.metadata["parse_status"] == "normalized"
    assert prepared.metadata["index_status"] == "deferred_m6"
    assert repository.stages == [(work.job_id, JobStatus.PARSING)]


@pytest.mark.parametrize("content", [b"text\x00secret", b"text\x1b[31m", b"\xff\xfe\x80"])
async def test_binary_or_non_utf8_text_file_keeps_original_only(
    content: bytes, tmp_path: Path
) -> None:
    pipeline, _, _ = make_pipeline(FakeMedia(content, "notes.txt"))
    prepared = await pipeline.prepare(
        make_work({"kind": "media", "media_id": "synthetic-media"}), tmp_path
    )
    assert prepared.original.path.read_bytes() == content
    assert prepared.text == ""
    assert prepared.status == SourceStatus.METADATA_ONLY
    assert prepared.metadata["parse_status"] == "unsupported_encoding"


async def test_oversized_text_media_is_preserved_but_not_read_as_text(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    content = b"a" * 1025
    pipeline, _, _ = make_pipeline(FakeMedia(content, "notes.txt"))

    def reject_read(path: Path) -> bytes:
        raise AssertionError("oversized text must not be loaded for parsing")

    with monkeypatch.context() as patch:
        patch.setattr(Path, "read_bytes", reject_read)
        prepared = await pipeline.prepare(
            make_work({"kind": "media", "media_id": "synthetic-media"}), tmp_path
        )
    assert prepared.original.path.read_bytes() == content
    assert prepared.status == SourceStatus.METADATA_ONLY
    assert prepared.metadata["parse_status"] == "text_limit_exceeded"
    assert prepared.text == ""


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("report.PDF", SourceType.PDF),
        ("report.doc", SourceType.WORD),
        ("report.docx", SourceType.WORD),
        ("report.xls", SourceType.EXCEL),
        ("report.xlsx", SourceType.EXCEL),
        ("slides.ppt", SourceType.PPT),
        ("slides.pptx", SourceType.PPT),
        ("unknown.bin", SourceType.OTHER),
    ],
)
async def test_rich_file_extension_is_only_a_type_hint(
    filename: str, expected: SourceType, tmp_path: Path
) -> None:
    content = b"This is not parsed or executed"
    pipeline, _, _ = make_pipeline(FakeMedia(content, filename))
    prepared = await pipeline.prepare(
        make_work({"kind": "media", "media_id": "synthetic-media"}), tmp_path
    )
    assert prepared.source_type == expected
    assert prepared.status == SourceStatus.METADATA_ONLY
    assert prepared.text == ""
    assert prepared.original.path.read_bytes() == content
    assert prepared.metadata["filename"] == filename
    assert prepared.metadata["parse_status"] == "unsupported_format"


@pytest.mark.parametrize("source_type", [SourceType.IMAGE, SourceType.AUDIO, SourceType.VIDEO])
async def test_nontext_media_keeps_source_type_without_fabricated_content(
    source_type: SourceType, tmp_path: Path
) -> None:
    pipeline, _, _ = make_pipeline(FakeMedia(b"media", None))
    prepared = await pipeline.prepare(
        make_work({"kind": "media", "media_id": "synthetic-media"}, source_type=source_type),
        tmp_path,
    )
    assert prepared.source_type == source_type
    assert prepared.status == SourceStatus.METADATA_ONLY
    assert prepared.text == ""
    assert prepared.metadata["index_status"] == "deferred_m6"


async def test_message_filename_is_fallback_but_download_filename_takes_precedence(
    tmp_path: Path,
) -> None:
    work = make_work(
        {"kind": "media", "media_id": "synthetic-media", "filename": "from-message.txt"}
    )
    pipeline, _, _ = make_pipeline(FakeMedia(b"readable", None))
    assert (await pipeline.prepare(work, tmp_path)).text == "readable"
    pipeline, _, _ = make_pipeline(FakeMedia(b"readable", "from-download.pdf"))
    prepared = await pipeline.prepare(work, tmp_path)
    assert prepared.source_type == SourceType.PDF
    assert prepared.status == SourceStatus.METADATA_ONLY
    assert prepared.text == ""


@pytest.mark.parametrize("media_id", [None, "", 42])
async def test_missing_media_id_fails_before_adapter_or_stage(
    media_id: JsonValue, tmp_path: Path
) -> None:
    pipeline, repository, media = make_pipeline()
    with pytest.raises(ArtifactError) as caught:
        await pipeline.prepare(make_work({"kind": "media", "media_id": media_id}), tmp_path)
    assert caught.value.code == "media_id_missing"
    assert repository.stages == []
    assert media.calls == []


async def test_media_adapter_failure_is_preserved_for_worker_retry(tmp_path: Path) -> None:
    error = ArtifactError("media_unavailable", retryable=True)
    pipeline, repository, media = make_pipeline(FakeMedia(error=error))
    with pytest.raises(ArtifactError) as caught:
        await pipeline.prepare(
            make_work({"kind": "media", "media_id": "synthetic-media"}), tmp_path
        )
    assert caught.value is error
    assert caught.value.retryable
    assert len(media.calls) == 1
    assert repository.stages == []


async def test_url_job_without_original_url_fails_explicitly(tmp_path: Path) -> None:
    pipeline, repository, media = make_pipeline()
    with pytest.raises(ArtifactError) as caught:
        await pipeline.prepare(make_work({"kind": "url"}), tmp_path)
    assert caught.value.code == "source_input_invalid"
    assert repository.stages == []
    assert media.calls == []


def test_markdown_has_source_identity_type_utc_date_original_url_and_body() -> None:
    source_id = uuid4()
    document = CanonicalDocument(
        source_id=source_id,
        user_id=uuid4(),
        title="资料 <script>\n第二行",
        source_type=SourceType.NOTE,
        original_url="https://example.com/?a=1&b=2",
        created_at=datetime(2026, 9, 22, 8, 30, tzinfo=timezone(timedelta(hours=8))),
        text="原始正文\n保留第二行",
        metadata={"index_status": "deferred_m6"},
    )
    rendered = markdown(document)
    assert rendered.startswith("# 资料 &lt;script&gt; 第二行\n")
    assert f"Source ID: `{source_id}`" in rendered
    assert "Source type: `note`" in rendered
    assert "Created at: `2026-09-22T00:30:00+00:00`" in rendered
    assert "Original URL: https://example.com/?a=1&amp;b=2" in rendered
    assert "Index status: deferred_m6" in rendered
    assert "## Content\n\n原始正文\n保留第二行\n" in rendered


def test_markdown_for_metadata_only_document_explains_missing_content() -> None:
    document = CanonicalDocument(
        source_id=uuid4(),
        user_id=uuid4(),
        title="视频号分享",
        source_type=SourceType.WECHAT_CHANNEL,
        created_at=datetime(2026, 9, 22, tzinfo=UTC),
        metadata={"parse_status": "metadata_only", "index_status": "deferred_m6"},
    )
    rendered = markdown(document)
    assert "官方卡片未提供原视频；无视频转录或时间轴" in rendered
    assert "Original URL" not in rendered
    assert "Index status: deferred_m6" in rendered
