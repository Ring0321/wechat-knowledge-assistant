"""HTML extraction and web orchestration. HTML never grants permission to fetch a URL."""

import asyncio
import hashlib
from pathlib import Path
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup, Tag

from app.domain.artifacts import ArtifactError, DownloadedFile
from app.domain.enums import SourceType
from app.domain.parsing import PageRenderer, ParsedContent, ParseLimits, WebFetcher, WebResponse


def field(soup: BeautifulSoup, selector: str, attribute: str | None = None) -> str | None:
    item = soup.select_one(selector)
    if item is None:
        return None
    value = item.get(attribute) if attribute else item.get_text(" ", strip=True)
    return value.strip()[:512] if isinstance(value, str) and value.strip() else None


def html_content(
    body: bytes, url: str, limits: ParseLimits, *, wechat: bool = False
) -> ParsedContent:
    if len(body) > min(limits.max_input_bytes, 2 * 1024 * 1024):
        raise ArtifactError("web_body_limit")
    soup = BeautifulSoup(body, "html.parser")
    if len(soup.find_all(True, limit=50_001)) > 50_000:
        raise ArtifactError("html_node_limit")
    scripts = soup.find("script") is not None
    title = field(soup, "#activity-name") if wechat else None
    title = (
        title
        or field(soup, "meta[property='og:title']", "content")
        or field(soup, "h1")
        or field(soup, "title")
    )
    author = field(soup, "#js_name") if wechat else None
    author = (
        author or field(soup, "meta[name='author']", "content") or field(soup, "[rel='author']")
    )
    published = field(soup, "#publish_time") if wechat else None
    published = (
        published
        or field(soup, "meta[property='article:published_time']", "content")
        or field(soup, "time[datetime]", "datetime")
    )
    article = soup.select_one("#js_content") if wechat else None
    article = article or soup.select_one("article,main,[role='main']") or soup.body or soup
    images: list[str] = []
    for element in article.find_all("img", limit=100):
        candidate = element.get("data-src") or element.get("src")
        if isinstance(candidate, str) and len(candidate) <= 8192:
            full = urljoin(url, candidate)
            try:
                parsed = urlsplit(full)
                valid = (
                    parsed.scheme in ("http", "https") and parsed.hostname and not parsed.username
                )
            except ValueError:
                valid = False
            if valid and full not in images:
                images.append(full)
    for element in article.select(
        "script,style,noscript,nav,footer,aside,form,iframe,svg,template"
    ):
        element.decompose()
    for element in article.find_all(True):
        if (
            isinstance(element, Tag)
            and not element.decomposed
            and (element.has_attr("hidden") or element.get("aria-hidden") == "true")
        ):
            element.decompose()
    lines = [
        line.strip() for line in article.get_text("\n", strip=True).splitlines() if line.strip()
    ]
    text = "\n".join(lines)
    if len(text) > limits.max_text_chars or len(lines) > 10_000:
        raise ArtifactError("parse_text_limit")
    return ParsedContent(
        title=title,
        text=text,
        source_type=SourceType.WECHAT_ARTICLE if wechat else SourceType.WEB_PAGE,
        metadata={
            "parser": "wechat_article" if wechat else "web_page",
            "parse_status": "normalized" if text else "empty",
            "author": author,
            "publish_time": published,
            "images": images,
            "url": url,
            "needs_render": scripts and len(text) < 80,
        },
        segments=[
            {"text": line, "locator": {"paragraph": index + 1, "url": url}}
            for index, line in enumerate(lines)
        ],
    )


class WeChatArticleParser:
    """Separate article field mapping, without login/captcha or download bypasses."""

    @staticmethod
    def extract(body: bytes, url: str, limits: ParseLimits) -> ParsedContent:
        return html_content(body, url, limits, wechat=True)


class WebPageParser:
    def __init__(
        self, fetcher: WebFetcher, renderer: PageRenderer | None, limits: ParseLimits
    ) -> None:
        self.fetcher, self.renderer, self.limits = fetcher, renderer, limits

    async def load(self, url: str, destination: Path) -> tuple[DownloadedFile, ParsedContent]:
        from app.parsers.local import LocalFileParser

        limit = min(self.limits.max_input_bytes, 2 * 1024 * 1024)
        rendered = False
        try:
            response = await self.fetcher.fetch(url, max_bytes=limit)
        except ArtifactError as error:
            if not error.retryable or self.renderer is None:
                raise
            response = await self.renderer.render(url, max_bytes=limit)
            rendered = True

        async def extract(value: WebResponse) -> ParsedContent:
            await asyncio.to_thread(destination.write_bytes, value.body)
            kind = (
                SourceType.WECHAT_ARTICLE
                if urlsplit(value.url).hostname == "mp.weixin.qq.com"
                else SourceType.WEB_PAGE
            )
            if value.content_type.split(";", 1)[0].strip().lower() not in (
                "text/html",
                "application/xhtml+xml",
            ):
                return ParsedContent(
                    source_type=kind,
                    metadata={
                        "parse_status": "unreadable",
                        "parse_error": "web_content_type_unsupported",
                    },
                )
            try:
                return await LocalFileParser(self.limits).parse(
                    destination, filename=None, source_type=kind, url=value.url
                )
            except ArtifactError as error:
                if error.retryable:
                    raise
                return ParsedContent(
                    source_type=kind,
                    metadata={"parse_status": "unreadable", "parse_error": error.code},
                )

        parsed = await extract(response)
        if (
            not rendered
            and not parsed.metadata.get("parse_error")
            and (not parsed.text or parsed.metadata.get("needs_render"))
            and self.renderer
        ):
            response = await self.renderer.render(response.url, max_bytes=limit)
            parsed = await extract(response)
            rendered = True
        parsed.metadata.update(
            {"rendered": rendered, "requested_url": url, "final_url": response.url}
        )
        artifact = DownloadedFile(
            destination,
            hashlib.sha256(response.body).hexdigest(),
            len(response.body),
            "text/html; charset=utf-8" if rendered else response.content_type,
            "page.html" if response.content_type.startswith("text/html") else "page.bin",
        )
        return artifact, parsed
