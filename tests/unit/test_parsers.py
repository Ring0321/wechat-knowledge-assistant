import asyncio
import sys
from pathlib import Path

import pytest
from PIL import EpsImagePlugin, Image
from pypdf import PdfWriter

from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceType
from app.domain.parsing import ParsedContent, ParseLimits, WebResponse
from app.parsers.local import LocalFileParser
from app.parsers.pdf_image import parse_image, parse_pdf
from app.parsers.web import WebPageParser, WeChatArticleParser, html_content
from tests.parser_helpers import text_pdf


def test_pdf_text_and_page_locators(tmp_path: Path) -> None:
    path = tmp_path / "fixture.pdf"
    text_pdf(path, pages=2)
    result = parse_pdf(path, ParseLimits())
    assert "page 1" in result.text and "page 2" in result.text
    assert result.title == "Synthetic knowledge" and result.segments[1]["locator"] == {"page": 2}
    assert not result.metadata["ocr"]


def test_pdf_page_and_text_limits(tmp_path: Path) -> None:
    path = tmp_path / "fixture.pdf"
    text_pdf(path, pages=5)
    with pytest.raises(ArtifactError, match="parse_page_limit"):
        parse_pdf(path, ParseLimits(max_pages=1))
    with pytest.raises(ArtifactError, match="parse_text_limit"):
        parse_pdf(path, ParseLimits(max_text_chars=100))


def test_encrypted_pdf_is_explicit(tmp_path: Path) -> None:
    path = tmp_path / "encrypted.pdf"
    writer = PdfWriter()
    writer.add_blank_page(width=50, height=50)
    writer.encrypt("synthetic-only")
    writer.write(path)
    with pytest.raises(ArtifactError, match="pdf_encrypted"):
        parse_pdf(path, ParseLimits())


def test_image_pixel_limits_and_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "image.png"
    Image.new("RGB", (100, 100), "white").save(path)
    with pytest.raises(ArtifactError, match="image_pixel_limit"):
        parse_image(path, ParseLimits(max_pixels=1000))
    monkeypatch.setattr("app.parsers.pdf_image.ocr", lambda *args, **kwargs: "识别文字")
    result = parse_image(path, ParseLimits())
    assert result.text == "识别文字" and result.metadata["ocr"]


def test_broken_image_is_not_success(tmp_path: Path) -> None:
    path = tmp_path / "broken.png"
    path.write_bytes(b"not an image")
    with pytest.raises(ArtifactError, match="image_invalid"):
        parse_image(path, ParseLimits())


def test_disguised_eps_never_invokes_ghostscript(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "disguised.png"
    path.write_bytes(b"%!PS-Adobe-3.0 EPSF-3.0\n%%BoundingBox: 0 0 100 100\n%%EndComments\n")

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("Ghostscript must not be invoked by the raster image parser")

    monkeypatch.setattr(EpsImagePlugin, "Ghostscript", forbidden)
    with pytest.raises(ArtifactError, match="image_invalid"):
        parse_image(path, ParseLimits())


async def test_real_parser_subprocess_pdf_and_safe_error(tmp_path: Path) -> None:
    path = tmp_path / "sample.pdf"
    text_pdf(path)
    parser = LocalFileParser(ParseLimits())
    result = await parser.parse(path, filename="sample.pdf", source_type=SourceType.OTHER)
    assert "Personal knowledge" in result.text
    path.write_bytes(b"bad pdf")
    with pytest.raises(ArtifactError, match="document_invalid"):
        await parser.parse(path, filename="sample.pdf", source_type=SourceType.PDF)
    with pytest.raises(ArtifactError, match="legacy_or_macro_format_unsupported"):
        await parser.parse(path, filename="file.docm", source_type=SourceType.WORD)


async def test_parser_timeout_kills_child_and_filters_secrets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_spawn = asyncio.create_subprocess_exec
    children: list[asyncio.subprocess.Process] = []
    monkeypatch.setenv("OPENAI_API_KEY", "synthetic-sensitive")
    monkeypatch.setenv("DATABASE_URL", "synthetic-sensitive")

    async def spawn(*args: object, **kwargs: object) -> asyncio.subprocess.Process:
        if args[0] == "taskkill":
            return await real_spawn(*args, **kwargs)
        assert "OPENAI_API_KEY" not in kwargs["env"] and "DATABASE_URL" not in kwargs["env"]
        child = await real_spawn(sys.executable, "-c", "import time; time.sleep(60)", **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    with pytest.raises(ArtifactError, match="parser_timeout"):
        await LocalFileParser(ParseLimits(timeout_seconds=0.1)).parse(
            tmp_path / "file", filename="file.pdf", source_type=SourceType.PDF
        )
    assert children and all(child.returncode is not None for child in children)


def test_web_and_wechat_extraction() -> None:
    html = b"""<html><head><title>Article</title><meta name="author" content="Writer">
    <meta property="article:published_time" content="2026-09-23"></head><body><nav>noise</nav>
    <article><h1>Heading</h1><p>Actual body</p><img src="/photo.jpg"><script>secret()</script>
    </article></body></html>"""
    result = html_content(html, "https://example.com/page", ParseLimits())
    assert (
        "Actual body" in result.text and "secret" not in result.text and "noise" not in result.text
    )
    assert result.metadata["author"] == "Writer" and result.metadata["publish_time"] == "2026-09-23"
    assert result.metadata["images"] == ["https://example.com/photo.jpg"]
    article = WeChatArticleParser.extract(
        (
            '<h1 id="activity-name">公众号标题</h1><span id="js_name">作者</span>'
            '<em id="publish_time">2026-09-23</em><div id="js_content"><p>正文</p>'
            '<img data-src="https://example.com/image.jpg"></div>'
        ).encode(),
        "https://mp.weixin.qq.com/s/example",
        ParseLimits(),
    )
    assert article.title == "公众号标题" and article.text == "正文"
    assert article.metadata["author"] == "作者" and article.source_type == SourceType.WECHAT_ARTICLE


def test_web_text_limits_and_private_image_url_metadata() -> None:
    with pytest.raises(ArtifactError, match="parse_text_limit"):
        html_content(
            ("<p>" + "x" * 101 + "</p>").encode(),
            "https://example.com",
            ParseLimits(max_text_chars=100),
        )
    result = html_content(
        b'<article><p>text</p><img src="http://127.0.0.1/a"></article>',
        "https://example.com",
        ParseLimits(),
    )
    assert result.text == "text"


async def test_web_http_then_renderer_security_error_no_fallback(tmp_path: Path) -> None:
    class Fetcher:
        error: ArtifactError | None = None

        async def fetch(self, url: str, *, max_bytes: int) -> WebResponse:
            if self.error:
                raise self.error
            return WebResponse(url, 200, b'<div id="app"></div><script src="/app.js"></script>')

    class Renderer:
        calls = 0

        async def render(self, url: str, *, max_bytes: int) -> WebResponse:
            self.calls += 1
            return WebResponse(
                url, 200, b"<article><h1>Rendered</h1><p>Actual content</p></article>"
            )

    fetcher, renderer = Fetcher(), Renderer()
    parser = WebPageParser(fetcher, renderer, ParseLimits())
    original, parsed = await parser.load("https://example.com/page", tmp_path / "page.html")
    assert "Actual content" in parsed.text and parsed.metadata["rendered"]
    assert original.size_bytes > 0 and renderer.calls == 1
    fetcher.error = ArtifactError("ssrf_blocked")
    with pytest.raises(ArtifactError, match="ssrf_blocked"):
        await parser.load("http://127.0.0.1/", tmp_path / "blocked")
    assert renderer.calls == 1


def test_hidden_nested_html_and_control_characters() -> None:
    result = html_content(
        b"<article>Visible<div hidden><p>Secret</p></div><p>Text\x00</p></article>",
        "https://example.com",
        ParseLimits(),
    )
    assert "Secret" not in result.text and "\x00" not in result.text
    parsed = ParsedContent(
        source_type=SourceType.OTHER,
        title="a\x00b",
        metadata={"author": "c\x00d", "x\x00": {"nested": "\x00"}},
        segments=[{"text": "x\x00y", "locator": {"sheet": "s\x00"}}],
    )
    assert "\u0000" not in parsed.model_dump_json() and parsed.title == "ab"
    assert parsed.metadata["author"] == "cd" and parsed.segments[0]["text"] == "xy"


async def test_web_unreadable_retains_original_without_browser_retry(tmp_path: Path) -> None:
    class BinaryWeb:
        async def fetch(self, url: str, *, max_bytes: int) -> WebResponse:
            return WebResponse(url, 200, b"original bytes", "application/octet-stream")

    artifact, parsed = await WebPageParser(BinaryWeb(), None, ParseLimits()).load(
        "https://example.com", tmp_path / "original"
    )
    assert artifact.path.read_bytes() == b"original bytes" and parsed.text == ""
    assert parsed.metadata["parse_error"] == "web_content_type_unsupported"
