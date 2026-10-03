"""Exercise the real HTTPX/HTTPCore path with controlled DNS and socket streams."""

# ruff: noqa: ASYNC109 -- The test streams implement HTTPCore's public interface.

import asyncio
import socket
import ssl
from collections.abc import Iterable, Sequence
from typing import Any

import httpcore
import pytest

from app.adapters.web_http import (
    PublicNetworkBackend,
    SafeWebFetcher,
    SocketOption,
    system_resolver,
    validate_web_url,
)
from app.domain.artifacts import ArtifactError

PUBLIC_IP = "93.184.216.34"
PUBLIC_V6 = "2606:4700:4700::1111"


async def public_dns(host: str, port: int) -> Sequence[str]:
    return [PUBLIC_IP]


class FakeStream(httpcore.AsyncNetworkStream):
    def __init__(self, parts: list[bytes], *, wait: bool = False) -> None:
        self.parts = list(parts)
        self.sent = bytearray()
        self.closed = False
        self.sni: str | None = None
        self.tls_context: ssl.SSLContext | None = None
        self.wait = wait

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self.wait and not self.parts:
            await asyncio.sleep(60)
        return self.parts.pop(0) if self.parts else b""

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self.sent.extend(buffer)

    async def aclose(self) -> None:
        self.closed = True

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> httpcore.AsyncNetworkStream:
        self.tls_context = ssl_context
        self.sni = server_hostname
        return self

    def get_extra_info(self, info: str) -> Any:
        return None


class FakeBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, *streams: FakeStream) -> None:
        self.streams = list(streams)
        self.calls: list[tuple[str, int]] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[SocketOption] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        self.calls.append((host, port))
        return self.streams.pop(0)


def wire(
    body: bytes = b"hello",
    *,
    status: int = 200,
    headers: bytes = b"Content-Type: text/html\r\n",
    length: bool = True,
) -> FakeStream:
    raw = f"HTTP/1.1 {status} Response\r\n".encode() + headers
    if length:
        raw += f"Content-Length: {len(body)}\r\n".encode()
    return FakeStream([raw + b"\r\n", body])


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/",
        "http://api.localhost/",
        "http://metadata.google.internal/",
        "http://postgres/",
        "http://service.local/",
        "http://x.localdomain/",
        "http://router.home/",
        "http://host.lan/",
        "http://example.invalid/",
        "http://127.0.0.1/",
        "http://127.255.255.255/",
        "http://0.0.0.0/",
        "http://10.0.0.1/",
        "http://172.16.0.1/",
        "http://172.31.255.255/",
        "http://192.168.0.1/",
        "http://169.254.169.254/latest/",
        "http://100.64.0.1/",
        "http://192.0.2.1/",
        "http://198.18.0.1/",
        "http://224.0.0.1/",
        "http://255.255.255.255/",
        "http://[::1]/",
        "http://[::]/",
        "http://[fc00::1]/",
        "http://[fe80::1]/",
        "http://[fe80::1%25eth0]/",
        "http://[ff02::1]/",
        "http://[2001:db8::1]/",
        "http://[fec0::1]/",
        "http://[::ffff:127.0.0.1]/",
        "http://[::ffff:8.8.8.8]/",
        "http://[64:ff9b::a00:1]/",
        "http://[2002:0808:0808::1]/",
        "http://127.1/",
        "http://0177.0.0.1/",
        "http://0x7f.0.0.1/",
        "http://0x7f000001/",
        "http://2130706433/",
        "http://%31%32%37.0.0.1/",
        "ftp://example.com/a",
        "file:///etc/passwd",
        "data:text/html,test",
        "gopher://example.com/",
        "http://user:password@example.com/",
        "http://@example.com/",
        "http://example.com:6379/",
        "http://example.com:0/",
        "http://example.com:999999/",
        "http://example.com\\@127.0.0.1/",
        "http://example.com/\r\nsecret",
        " http://example.com/",
        "https:///example.com",
    ],
)
async def test_blocked_targets_never_dial(url: str) -> None:
    backend = FakeBackend()
    fetcher = SafeWebFetcher(resolver=public_dns, backend=backend)
    with pytest.raises(ArtifactError):
        await fetcher.fetch(url, max_bytes=1000)
    assert backend.calls == []
    await fetcher.aclose()


async def test_public_https_pins_socket_keeps_host_and_certificate_checks() -> None:
    stream = wire()
    backend = FakeBackend(stream)
    names: list[str] = []

    async def resolver(host: str, port: int) -> Sequence[str]:
        names.append(host)
        return [PUBLIC_IP]

    fetcher = SafeWebFetcher(resolver=resolver, backend=backend)
    result = await fetcher.fetch("https://Example.com/a?secret=value#fragment", max_bytes=1000)
    assert result.body == b"hello"
    assert result.url == "https://example.com/a?secret=value"
    assert result.status_code == 200
    assert result.content_type == "text/html"
    assert backend.calls == [(PUBLIC_IP, 443)]
    assert names == ["example.com"]
    assert stream.sni == "example.com"
    assert stream.tls_context is not None
    assert stream.tls_context.check_hostname is True
    assert stream.tls_context.verify_mode == ssl.CERT_REQUIRED
    assert b"Host: example.com\r\n" in stream.sent
    assert b"Accept-Encoding: identity\r\n" in stream.sent
    assert stream.closed
    await fetcher.aclose()


@pytest.mark.parametrize("address", [PUBLIC_IP, PUBLIC_V6])
async def test_public_literal_never_uses_dns(address: str) -> None:
    async def no_dns(host: str, port: int) -> Sequence[str]:
        pytest.fail("Literal IP must not be resolved again")

    stream = wire()
    backend = FakeBackend(stream)
    fetcher = SafeWebFetcher(resolver=no_dns, backend=backend)
    host = f"[{address}]" if ":" in address else address
    await fetcher.fetch(f"http://{host}/", max_bytes=1000)
    assert backend.calls == [(address, 80)]
    assert stream.sni is None
    await fetcher.aclose()


@pytest.mark.parametrize(
    "answers",
    [
        ["127.0.0.1"],
        [PUBLIC_IP, "10.0.0.1"],
        ["192.168.1.1", PUBLIC_IP],
        [PUBLIC_IP, "::1"],
        [PUBLIC_V6, "::ffff:192.168.0.1"],
        ["100.64.0.1"],
        ["not.an.ip"],
        [],
        [PUBLIC_IP] * 65,
    ],
)
async def test_dns_all_answers_must_be_public(answers: list[str]) -> None:
    async def resolver(host: str, port: int) -> Sequence[str]:
        return answers

    backend = FakeBackend()
    fetcher = SafeWebFetcher(resolver=resolver, backend=backend)
    with pytest.raises(ArtifactError):
        await fetcher.fetch("https://public.example.com/", max_bytes=100)
    assert backend.calls == []
    await fetcher.aclose()


async def test_rebinding_later_connection_is_rechecked() -> None:
    first = wire()
    backend = FakeBackend(first)
    answers = [[PUBLIC_IP], ["127.0.0.1"]]

    async def resolver(host: str, port: int) -> Sequence[str]:
        return answers.pop(0)

    fetcher = SafeWebFetcher(resolver=resolver, backend=backend)
    await fetcher.fetch("https://example.com/", max_bytes=100)
    with pytest.raises(ArtifactError, match="web_address_blocked"):
        await fetcher.fetch("https://example.com/", max_bytes=100)
    assert backend.calls == [(PUBLIC_IP, 443)]
    assert first.closed
    await fetcher.aclose()


@pytest.mark.parametrize(
    "location",
    [
        b"http://127.0.0.1/",
        b"http://[::1]/",
        b"//metadata.google.internal/",
        b"file:///etc/passwd",
        b"http://user:secret@example.com/",
        b"http://example.com:6379/",
    ],
)
async def test_redirect_revalidates_destination(location: bytes) -> None:
    first = wire(status=302, headers=b"Location: " + location + b"\r\n")
    backend = FakeBackend(first)
    fetcher = SafeWebFetcher(resolver=public_dns, backend=backend)
    with pytest.raises(ArtifactError):
        await fetcher.fetch("https://example.com/", max_bytes=100)
    assert backend.calls == [(PUBLIC_IP, 443)]
    assert first.closed
    await fetcher.aclose()


async def test_redirect_rechecks_dns_even_on_same_origin() -> None:
    first = wire(status=302, headers=b"Location: /second\r\n")
    backend = FakeBackend(first)
    answers = [[PUBLIC_IP], ["10.0.0.1"]]

    async def resolver(host: str, port: int) -> Sequence[str]:
        return answers.pop(0)

    fetcher = SafeWebFetcher(resolver=resolver, backend=backend)
    with pytest.raises(ArtifactError, match="web_address_blocked"):
        await fetcher.fetch("https://example.com/first", max_bytes=100)
    assert len(backend.calls) == 1
    assert first.closed
    await fetcher.aclose()


async def test_relative_redirect_and_cookies_do_not_propagate() -> None:
    first = wire(status=302, headers=b"Location: /second\r\nSet-Cookie: secret=token\r\n")
    second = wire(headers=b"Content-Type: application/javascript\r\n")
    third = wire()
    backend = FakeBackend(first, second, third)
    fetcher = SafeWebFetcher(resolver=public_dns, backend=backend)
    result = await fetcher.fetch("https://example.com/first", max_bytes=100)
    assert result.url == "https://example.com/second"
    assert result.content_type == "application/javascript"
    assert b"Cookie:" not in second.sent
    await fetcher.fetch("https://example.com/other-user", max_bytes=100)
    assert b"Cookie:" not in third.sent
    assert first.closed and second.closed and third.closed
    await fetcher.aclose()


async def test_redirect_limit_closes_every_response() -> None:
    streams = [wire(status=302, headers=b"Location: /next\r\n") for _ in range(3)]
    backend = FakeBackend(*streams)
    fetcher = SafeWebFetcher(resolver=public_dns, backend=backend, max_redirects=2)
    with pytest.raises(ArtifactError, match="web_redirect_limit"):
        await fetcher.fetch("https://example.com/", max_bytes=100)
    assert len(backend.calls) == 3
    assert all(stream.closed for stream in streams)
    await fetcher.aclose()


@pytest.mark.parametrize(
    "headers,body,length,code",
    [
        (b"", b"abcdef", True, "web_body_too_large"),
        (b"", b"abcdef", False, "web_body_too_large"),
        (b"Content-Encoding: gzip\r\n", b"z", True, "web_encoding_unsupported"),
        (b"Content-Encoding: br\r\n", b"z", True, "web_encoding_unsupported"),
        (b"Content-Encoding: gzip, identity\r\n", b"z", True, "web_encoding_unsupported"),
    ],
)
async def test_body_and_compression_limits_close_stream(
    headers: bytes,
    body: bytes,
    length: bool,
    code: str,
) -> None:
    stream = wire(body, headers=headers, length=length)
    fetcher = SafeWebFetcher(resolver=public_dns, backend=FakeBackend(stream))
    with pytest.raises(ArtifactError, match=code):
        await fetcher.fetch("https://example.com/", max_bytes=5)
    assert stream.closed
    await fetcher.aclose()


async def test_chunked_body_limit() -> None:
    stream = FakeStream(
        [
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"4\r\nabcd\r\n",
            b"4\r\nefgh\r\n",
            b"0\r\n\r\n",
        ]
    )
    fetcher = SafeWebFetcher(resolver=public_dns, backend=FakeBackend(stream))
    with pytest.raises(ArtifactError, match="web_body_too_large"):
        await fetcher.fetch("https://example.com/", max_bytes=5)
    assert stream.closed
    await fetcher.aclose()


@pytest.mark.parametrize(
    "status,retryable",
    [
        (401, False),
        (403, False),
        (404, False),
        (408, True),
        (429, True),
        (500, True),
        (503, True),
        (304, False),
    ],
)
async def test_http_error_classification_and_no_sensitive_exception(
    status: int,
    retryable: bool,
) -> None:
    stream = wire(status=status)
    fetcher = SafeWebFetcher(resolver=public_dns, backend=FakeBackend(stream))
    with pytest.raises(ArtifactError) as captured:
        await fetcher.fetch("https://example.com/?password=secret", max_bytes=100)
    assert captured.value.code == f"web_http_{status}"
    assert captured.value.retryable is retryable
    assert "secret" not in str(captured.value)
    assert stream.closed
    await fetcher.aclose()


async def test_total_timeout_includes_dns() -> None:
    async def delayed(host: str, port: int) -> Sequence[str]:
        await asyncio.sleep(1)
        return [PUBLIC_IP]

    backend = FakeBackend()
    fetcher = SafeWebFetcher(resolver=delayed, backend=backend, timeout_seconds=0.01)
    with pytest.raises(ArtifactError, match="web_timeout") as captured:
        await fetcher.fetch("https://example.com/?secret=x", max_bytes=100)
    assert captured.value.retryable
    assert backend.calls == []
    await fetcher.aclose()


async def test_body_timeout_and_cancellation_close_stream() -> None:
    stream = FakeStream([b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n"], wait=True)
    fetcher = SafeWebFetcher(
        resolver=public_dns,
        backend=FakeBackend(stream),
        timeout_seconds=0.01,
    )
    with pytest.raises(ArtifactError, match="web_timeout"):
        await fetcher.fetch("https://example.com/", max_bytes=100)
    assert stream.closed
    await fetcher.aclose()


async def test_caller_cancellation_is_preserved() -> None:
    stream = FakeStream([b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n"], wait=True)
    fetcher = SafeWebFetcher(resolver=public_dns, backend=FakeBackend(stream))
    task = asyncio.create_task(fetcher.fetch("https://example.com/", max_bytes=100))
    for _ in range(50):
        if stream.sent:
            break
        await asyncio.sleep(0.001)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stream.closed
    await fetcher.aclose()


async def test_dns_error_has_only_safe_code() -> None:
    async def unavailable(host: str, port: int) -> Sequence[str]:
        raise OSError("password=secret private-ip")

    fetcher = SafeWebFetcher(resolver=unavailable, backend=FakeBackend())
    with pytest.raises(ArtifactError) as captured:
        await fetcher.fetch("https://example.com/?token=secret", max_bytes=100)
    assert str(captured.value) == "web_dns_failed"
    assert captured.value.retryable
    await fetcher.aclose()


async def test_multiple_public_answers_can_fail_over_without_dns_repeat() -> None:
    class FailFirst(FakeBackend):
        async def connect_tcp(
            self,
            host: str,
            port: int,
            timeout: float | None = None,
            local_address: str | None = None,
            socket_options: Iterable[SocketOption] | None = None,
        ) -> httpcore.AsyncNetworkStream:
            if host == PUBLIC_IP:
                self.calls.append((host, port))
                raise httpcore.ConnectError("private failure text")
            return await super().connect_tcp(host, port, timeout, local_address, socket_options)

    async def resolver(host: str, port: int) -> Sequence[str]:
        return [PUBLIC_IP, PUBLIC_V6]

    backend = FailFirst(wire())
    fetcher = SafeWebFetcher(resolver=resolver, backend=backend)
    assert (await fetcher.fetch("https://example.com/", max_bytes=100)).body == b"hello"
    assert backend.calls == [(PUBLIC_IP, 443), (PUBLIC_V6, 443)]
    await fetcher.aclose()


async def test_environment_proxies_are_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:4444")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:4444")
    monkeypatch.setenv("ALL_PROXY", "socks5://127.0.0.1:4444")
    backend = FakeBackend(wire())
    fetcher = SafeWebFetcher(resolver=public_dns, backend=backend)
    await fetcher.fetch("https://example.com/", max_bytes=100)
    assert backend.calls == [(PUBLIC_IP, 443)]
    await fetcher.aclose()


async def test_system_resolver_disables_search_suffix(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, int]] = []

    async def addresses(host: str, port: int, **kwargs: Any) -> list[Any]:
        calls.append((host, port))
        assert kwargs["type"] == socket.SOCK_STREAM
        assert kwargs["proto"] == socket.IPPROTO_TCP
        return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (PUBLIC_IP, port))]

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", addresses)
    assert await system_resolver("example.com", 443) == [PUBLIC_IP]
    assert calls == [("example.com.", 443)]


async def test_close_and_invalid_configuration() -> None:
    fetcher = SafeWebFetcher(resolver=public_dns, backend=FakeBackend())
    await fetcher.aclose()
    with pytest.raises(ArtifactError, match="web_fetcher_closed"):
        await fetcher.fetch("https://example.com/", max_bytes=100)
    for kwargs in [{"timeout_seconds": 0}, {"max_redirects": -1}, {"max_redirects": 11}]:
        with pytest.raises(ArtifactError, match="web_configuration_invalid"):
            SafeWebFetcher(**kwargs)
    backend = PublicNetworkBackend(resolver=public_dns, backend=FakeBackend())
    with pytest.raises(ArtifactError, match="web_address_blocked"):
        await backend.connect_unix_socket("/var/run/docker.sock")
    with pytest.raises(ArtifactError, match="web_address_blocked"):
        await backend.connect_tcp("example.com", 22)


async def test_failed_connections_are_retryable_and_do_not_expose_host() -> None:
    class Failure(httpcore.AsyncNetworkBackend):
        async def connect_tcp(
            self,
            host: str,
            port: int,
            timeout: float | None = None,
            local_address: str | None = None,
            socket_options: Iterable[SocketOption] | None = None,
        ) -> httpcore.AsyncNetworkStream:
            raise httpcore.ConnectError("secret private connection data")

    fetcher = SafeWebFetcher(resolver=public_dns, backend=Failure())
    with pytest.raises(ArtifactError) as captured:
        await fetcher.fetch("https://example.com/?secret=token", max_bytes=100)
    assert str(captured.value) == "web_connect_failed"
    assert captured.value.retryable
    await fetcher.aclose()


async def test_truncated_body_fails_and_closes_stream() -> None:
    stream = FakeStream([b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort"])
    fetcher = SafeWebFetcher(resolver=public_dns, backend=FakeBackend(stream))
    with pytest.raises(ArtifactError, match="web_response_invalid"):
        await fetcher.fetch("https://example.com/", max_bytes=100)
    assert stream.closed
    await fetcher.aclose()


async def test_missing_redirect_location_fails_and_closes() -> None:
    stream = wire(status=302)
    fetcher = SafeWebFetcher(resolver=public_dns, backend=FakeBackend(stream))
    with pytest.raises(ArtifactError, match="web_redirect_invalid"):
        await fetcher.fetch("https://example.com/", max_bytes=100)
    assert stream.closed
    await fetcher.aclose()


@pytest.mark.parametrize("max_bytes", [0, -1, 20 * 1024 * 1024 + 1])
async def test_invalid_limits_never_connect(max_bytes: int) -> None:
    backend = FakeBackend()
    fetcher = SafeWebFetcher(resolver=public_dns, backend=backend)
    with pytest.raises(ArtifactError, match="web_limit_invalid"):
        await fetcher.fetch("https://example.com/", max_bytes=max_bytes)
    assert backend.calls == []
    await fetcher.aclose()


def test_international_and_fully_qualified_hostnames() -> None:
    assert validate_web_url("https://例子.中国/").raw_host.startswith(b"xn--")
    assert validate_web_url("https://example.com./").host == "example.com"
