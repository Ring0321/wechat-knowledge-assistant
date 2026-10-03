"""Remote Chromium rendering with every HTTP request fulfilled by the safe fetch port.

The browser service has an internal-only Docker network and no application credentials.
Offline mode and a dead proxy add browser-level barriers; no route continues or fetches
using the browser's network stack. Never enable Playwright expose_network.
"""

import asyncio
from urllib.parse import urlsplit

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Route, WebSocketRoute, async_playwright

from app.adapters.web_http import validate_web_url
from app.domain.artifacts import ArtifactError
from app.domain.parsing import WebFetcher, WebResponse


class GuardedBrowserRenderer:
    def __init__(
        self,
        fetcher: WebFetcher,
        endpoint: str,
        *,
        timeout_seconds: float = 20,
        max_requests: int = 40,
        total_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        if urlsplit(endpoint).scheme not in ("ws", "wss"):
            raise ArtifactError("browser_configuration_invalid")
        self.fetcher, self.endpoint = fetcher, endpoint
        self.timeout_seconds, self.max_requests, self.total_bytes = (
            timeout_seconds,
            max_requests,
            total_bytes,
        )

    async def render(self, url: str, *, max_bytes: int) -> WebResponse:
        url = str(validate_web_url(url))
        requests, used = 0, 0
        main_errors: list[ArtifactError] = []
        lock = asyncio.Lock()

        async def handle(route: Route) -> None:
            nonlocal requests, used
            request = route.request
            if request.method != "GET" or request.resource_type in (
                "image",
                "media",
                "font",
                "eventsource",
            ):
                await route.abort()
                return
            requests += 1
            if requests > self.max_requests:
                main_errors.append(ArtifactError("browser_request_limit"))
                await route.abort()
                return
            try:
                async with lock:
                    allowance = min(max_bytes, self.total_bytes - used)
                    if allowance <= 0:
                        raise ArtifactError("browser_byte_limit")
                    validate_web_url(request.url)
                    response = await self.fetcher.fetch(request.url, max_bytes=allowance)
                    used += len(response.body)
                    if used > self.total_bytes:
                        raise ArtifactError("browser_byte_limit")
                if response.url != request.url:
                    await route.fulfill(status=302, headers={"location": response.url}, body="")
                else:
                    await route.fulfill(
                        status=response.status_code,
                        content_type=response.content_type,
                        body=response.body,
                    )
            except ArtifactError as error:
                if (
                    request.is_navigation_request() and request.frame.parent_frame is None
                ) or error.code in {"browser_byte_limit", "web_body_too_large", "web_body_limit"}:
                    main_errors.append(error)
                await route.abort()
            except Exception:
                main_errors.append(ArtifactError("browser_route_failed", retryable=True))
                await route.abort()

        async def close_socket(route: WebSocketRoute) -> None:
            await route.close()

        try:
            async with asyncio.timeout(self.timeout_seconds), async_playwright() as playwright:
                browser = await playwright.chromium.connect(self.endpoint, timeout=5000)
                try:
                    context = await browser.new_context(
                        service_workers="block",
                        offline=True,
                        accept_downloads=False,
                        proxy={"server": "http://127.0.0.1:9", "bypass": "<-loopback>"},
                    )
                    try:
                        await context.route("**/*", handle)
                        await context.route_web_socket("**/*", close_socket)
                        page = await context.new_page()
                        page.set_default_timeout(self.timeout_seconds * 1000)
                        await page.goto(url, wait_until="networkidle")
                        if main_errors:
                            raise main_errors[0]
                        body = (await page.content()).encode("utf-8")
                        if len(body) > max_bytes:
                            raise ArtifactError("web_body_limit")
                        final_url = str(validate_web_url(page.url))
                        return WebResponse(final_url, 200, body, "text/html; charset=utf-8")
                    finally:
                        await context.close()
                finally:
                    await browser.close()
        except (TimeoutError, PlaywrightError):
            if main_errors:
                raise main_errors[0] from None
            raise ArtifactError("browser_render_failed", retryable=True) from None
