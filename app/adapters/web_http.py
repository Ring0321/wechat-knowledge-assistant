"""Bounded HTTP fetching with DNS validation at the actual TCP connection boundary.

The public HTTPCore backend interface pins validated numeric addresses while the
pool retains the original hostname for HTTP Host and TLS certificate/SNI checks.
There are no proxy, insecure TLS, arbitrary-header, or private-network options.
"""

# ruff: noqa: ASYNC109 -- HTTPCore's public interface requires timeout parameters.

import asyncio
import ipaddress
import re
import socket
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Sequence
from urllib.parse import urlsplit

import httpcore
import httpx

from app.domain.artifacts import ArtifactError
from app.domain.parsing import WebResponse

type Resolver = Callable[[str, int], Awaitable[Sequence[str]]]
type SocketOption = (
    tuple[int, int, int] | tuple[int, int, bytes | bytearray] | tuple[int, int, None, int]
)

_INTERNAL_SUFFIXES = (
    ".localhost",
    ".local",
    ".localdomain",
    ".internal",
    ".home",
    ".lan",
    ".invalid",
    ".test",
    ".onion",
    ".arpa",
    ".svc",
    ".cluster",
    ".corp",
    ".intranet",
)
_HOST_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")
_IP_TRANSLATION = ipaddress.ip_network("64:ff9b::/96")
_REDIRECTS = frozenset({301, 302, 303, 307, 308})
_MAX_BODY_BYTES = 20 * 1024 * 1024


def _public_ip(value: str) -> str:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        raise ArtifactError("web_address_blocked") from None
    if (
        not address.is_global
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
        or address.is_loopback
        or address.is_link_local
        or "%" in value
    ):
        raise ArtifactError("web_address_blocked")
    if isinstance(address, ipaddress.IPv6Address) and (
        address.ipv4_mapped is not None
        or address.sixtofour is not None
        or address.teredo is not None
        or address in _IP_TRANSLATION
        or address.is_site_local
    ):
        raise ArtifactError("web_address_blocked")
    return str(address)


def _host(value: str) -> str:
    value = value.lower().rstrip(".")
    try:
        ipaddress.ip_address(value)
    except ValueError:
        # Reject legacy decimal/octal/hex IP spellings before any OS resolver can
        # reinterpret them. Names need a real DNS suffix, never a search domain.
        labels = value.split(".")
        if (
            len(value) > 253
            or len(labels) < 2
            or value.endswith(_INTERNAL_SUFFIXES)
            or all(re.fullmatch(r"(?:0x[0-9a-f]+|[0-9]+)", part) for part in labels)
            or not all(_HOST_LABEL.fullmatch(part) for part in labels)
        ):
            raise ArtifactError("web_address_blocked") from None
        return value
    return _public_ip(value)


def validate_web_url(value: str) -> httpx.URL:
    """Validate syntax and literal addresses; DNS is checked when connecting."""
    if (
        not isinstance(value, str)
        or len(value) > 8192
        or "\\" in value
        or any(ord(char) <= 32 or ord(char) == 127 for char in value)
    ):
        raise ArtifactError("web_url_invalid")
    try:
        parts = urlsplit(value)
        url = httpx.URL(value)
        if (
            parts.scheme not in {"http", "https"}
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.port not in {None, 80, 443}
            or "%" in parts.hostname
            or not url.is_absolute_url
        ):
            raise ArtifactError("web_url_invalid")
        host = _host(url.raw_host.decode("ascii"))
        return url.copy_with(host=host, fragment=None)
    except (ValueError, UnicodeError, httpx.InvalidURL):
        raise ArtifactError("web_url_invalid") from None


async def system_resolver(host: str, port: int) -> Sequence[str]:
    # The trailing dot disables DNS search suffixes. The TLS/Host name is not
    # changed. No resolver output is ever passed back as a hostname to connect.
    answers = await asyncio.get_running_loop().getaddrinfo(
        host + ".",
        port,
        family=socket.AF_UNSPEC,
        type=socket.SOCK_STREAM,
        proto=socket.IPPROTO_TCP,
    )
    return [str(answer[4][0]) for answer in answers]


class PublicNetworkBackend(httpcore.AsyncNetworkBackend):
    """Resolve once per connection and dial only that checked set of IPs.

    Resolver/backend injection is for deterministic tests. The injected backend
    still only receives checked IP literals, never the original DNS hostname.
    """

    def __init__(
        self,
        *,
        resolver: Resolver = system_resolver,
        backend: httpcore.AsyncNetworkBackend | None = None,
    ) -> None:
        self._resolver = resolver
        self._backend = backend if backend is not None else httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[SocketOption] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        if port not in {80, 443} or local_address is not None:
            raise ArtifactError("web_address_blocked")
        checked_host = _host(host)
        try:
            async with asyncio.timeout(timeout if timeout is not None else 10):
                try:
                    ipaddress.ip_address(checked_host)
                except ValueError:
                    answers = await self._resolver(checked_host, port)
                    if not answers or len(answers) > 64:
                        raise ArtifactError("web_dns_invalid") from None
                    # Reject mixed public/private results rather than choosing a
                    # convenient public answer from an attacker-controlled set.
                    addresses = tuple(dict.fromkeys(_public_ip(item) for item in answers))
                else:
                    addresses = (checked_host,)
                for address in addresses:
                    try:
                        return await self._backend.connect_tcp(
                            address,
                            port,
                            timeout=timeout,
                            socket_options=socket_options,
                        )
                    except (httpcore.ConnectError, httpcore.ConnectTimeout, OSError):
                        continue
                raise ArtifactError("web_connect_failed", retryable=True)
        except TimeoutError:
            raise ArtifactError("web_timeout", retryable=True) from None
        except OSError:
            raise ArtifactError("web_dns_failed", retryable=True) from None

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[SocketOption] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise ArtifactError("web_address_blocked")

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class _ResponseStream(httpx.AsyncByteStream):
    def __init__(self, response: httpcore.Response) -> None:
        self._response = response

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self._response.aiter_stream():
            yield chunk

    async def aclose(self) -> None:
        await self._response.aclose()


class _PublicTransport(httpx.AsyncBaseTransport):
    def __init__(self, backend: PublicNetworkBackend) -> None:
        self._pool = httpcore.AsyncConnectionPool(
            ssl_context=httpcore.default_ssl_context(),
            network_backend=backend,
            max_connections=4,
            max_keepalive_connections=0,
            retries=0,
            http2=False,
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.method != "GET":
            raise ArtifactError("web_method_blocked")
        validate_web_url(str(request.url))
        response = await self._pool.handle_async_request(
            httpcore.Request(
                method="GET",
                url=httpcore.URL(
                    scheme=request.url.raw_scheme,
                    host=request.url.raw_host,
                    port=request.url.port,
                    target=request.url.raw_path,
                ),
                headers=request.headers.raw,
                content=b"",
                extensions=request.extensions,
            )
        )
        return httpx.Response(
            status_code=response.status,
            headers=response.headers,
            stream=_ResponseStream(response),
            extensions=response.extensions,
        )

    async def aclose(self) -> None:
        await self._pool.aclose()


class SafeWebFetcher:
    """Fetch public HTTP(S) with bounded time/body/redirects and no shared cookies."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 15,
        max_redirects: int = 5,
        resolver: Resolver = system_resolver,
        backend: httpcore.AsyncNetworkBackend | None = None,
    ) -> None:
        if not 0 < timeout_seconds <= 120 or not 0 <= max_redirects <= 10:
            raise ArtifactError("web_configuration_invalid")
        self._timeout = timeout_seconds
        self._max_redirects = max_redirects
        self._backend = PublicNetworkBackend(resolver=resolver, backend=backend)
        self._clients: set[httpx.AsyncClient] = set()
        self._closed = False

    async def fetch(self, url: str, *, max_bytes: int) -> WebResponse:
        if self._closed:
            raise ArtifactError("web_fetcher_closed")
        if not 0 < max_bytes <= _MAX_BODY_BYTES:
            raise ArtifactError("web_limit_invalid")
        current = validate_web_url(url)
        # A fresh client owns each fetch: response cookies and auth challenges
        # cannot become a cross-user state channel in a shared worker.
        client = httpx.AsyncClient(
            transport=_PublicTransport(self._backend),
            trust_env=False,
            follow_redirects=False,
            timeout=min(self._timeout, 10),
        )
        self._clients.add(client)
        try:
            async with asyncio.timeout(self._timeout), client:
                for hop in range(self._max_redirects + 1):
                    # Construct Request directly so redirect cookies never apply.
                    request = httpx.Request(
                        "GET",
                        current,
                        headers={
                            "Accept-Encoding": "identity",
                            "Accept": "*/*",
                            "User-Agent": "PersonalKnowledgeAssistant/0.100",
                        },
                        extensions={
                            "timeout": {
                                name: min(self._timeout, 10)
                                for name in ("connect", "read", "write", "pool")
                            }
                        },
                    )
                    response = await client.send(request, stream=True)
                    try:
                        if response.status_code in _REDIRECTS:
                            if hop == self._max_redirects:
                                raise ArtifactError("web_redirect_limit")
                            location = response.headers.get("location")
                            if not location:
                                raise ArtifactError("web_redirect_invalid")
                            # Validate even dangerous whitespace that URL.join
                            # would silently normalize out of a Location header.
                            if (
                                len(location) > 8192
                                or "\\" in location
                                or any(ord(char) <= 32 or ord(char) == 127 for char in location)
                            ):
                                raise ArtifactError("web_url_invalid")
                            current = validate_web_url(str(current.join(location)))
                            continue
                        if not 200 <= response.status_code < 300:
                            raise ArtifactError(
                                f"web_http_{response.status_code}",
                                retryable=response.status_code in {408, 425, 429}
                                or response.status_code >= 500,
                            )
                        encoding = response.headers.get("content-encoding", "identity")
                        if encoding.lower().strip() not in {"", "identity"}:
                            raise ArtifactError("web_encoding_unsupported")
                        declared = response.headers.get("content-length")
                        if declared is not None:
                            if not declared.isdecimal():
                                raise ArtifactError("web_response_invalid")
                            if int(declared) > max_bytes:
                                raise ArtifactError("web_body_too_large")
                        body = bytearray()
                        async for chunk in response.aiter_raw():
                            if len(body) + len(chunk) > max_bytes:
                                raise ArtifactError("web_body_too_large")
                            body.extend(chunk)
                        return WebResponse(
                            url=str(current),
                            status_code=response.status_code,
                            body=bytes(body),
                            content_type=response.headers.get(
                                "content-type", "application/octet-stream"
                            )[:256],
                        )
                    finally:
                        await response.aclose()
                raise ArtifactError("web_redirect_limit")
        except (httpx.TimeoutException, httpcore.TimeoutException, TimeoutError):
            raise ArtifactError("web_timeout", retryable=True) from None
        except (httpx.NetworkError, httpcore.NetworkError, OSError):
            raise ArtifactError("web_unavailable", retryable=True) from None
        except (httpx.ProtocolError, httpcore.ProtocolError):
            raise ArtifactError("web_response_invalid") from None
        except (httpx.InvalidURL, ValueError):
            raise ArtifactError("web_url_invalid") from None
        finally:
            self._clients.discard(client)

    async def aclose(self) -> None:
        self._closed = True
        for client in tuple(self._clients):
            await client.aclose()
