"""Synthetic media responses; no credentials or network business calls."""

import asyncio
import hashlib
from collections.abc import AsyncIterator, Callable
from pathlib import Path

import httpx
import pytest

from app.connectors.wecom.contracts import APIError
from app.connectors.wecom.media import HttpWeComMedia
from app.domain.artifacts import ArtifactError


class Tokens:
    def __init__(self) -> None:
        self.invalidated: list[str] = []

    async def get_token(self) -> str:
        return "synthetic-new" if self.invalidated else "synthetic-old"

    async def invalidate(self, token: str) -> None:
        self.invalidated.append(token)


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes], *, delay: float = 0, fail: bool = False) -> None:
        self.chunks, self.delay, self.fail = chunks, delay, fail
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            if self.delay:
                await asyncio.sleep(self.delay)
            yield chunk
        if self.fail:
            raise httpx.ReadError("synthetic URL with a secret must not escape")

    async def aclose(self) -> None:
        self.closed = True


Handler = Callable[[httpx.Request], httpx.Response]


async def download(
    handler: Handler, path: Path, *, max_bytes: int = 1024, tokens: Tokens | None = None
) -> bytes:
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await HttpWeComMedia(http, tokens or Tokens(), range_bytes=16).download(
            "synthetic-media", path, max_bytes=max_bytes
        )
    data = await asyncio.to_thread(path.read_bytes)
    assert result.path == path
    assert result.sha256 == hashlib.sha256(data).hexdigest()
    assert result.size_bytes == len(data)
    return data


async def test_download_full_200_fixed_origin_and_safe_metadata(tmp_path: Path) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            content=b"synthetic bytes",
            headers={
                "Content-Type": "application/pdf; charset=binary",
                "Content-Disposition": 'attachment; filename="../../report.pdf"',
            },
        )

    path = tmp_path / "caller-selected.tmp"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await HttpWeComMedia(http, Tokens()).download(
            "synthetic-media", path, max_bytes=1024
        )
    assert result.filename == "report.pdf"
    assert result.content_type == "application/pdf"
    assert result.sha256 == hashlib.sha256(b"synthetic bytes").hexdigest()
    assert result.size_bytes == 15
    assert path.read_bytes() == b"synthetic bytes"
    assert len(seen) == 1
    assert seen[0].url.scheme == "https"
    assert seen[0].url.host == "qyapi.weixin.qq.com"
    assert seen[0].url.path == "/cgi-bin/media/get"
    assert seen[0].url.params["media_id"] == "synthetic-media"
    assert seen[0].headers["range"] == "bytes=0-1048575"
    assert seen[0].headers["accept-encoding"] == "identity"


async def test_contiguous_range_download_and_incremental_hash(tmp_path: Path) -> None:
    content = b"0123456789abcdefghijklmnopqrstuvxyz"
    ranges: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        value = request.headers["range"]
        ranges.append(value)
        start, end = (int(part) for part in value.removeprefix("bytes=").split("-"))
        end = min(end, len(content) - 1)
        return httpx.Response(
            206,
            content=content[start : end + 1],
            headers={"Content-Range": f"bytes {start}-{end}/{len(content)}"},
        )

    assert await download(handler, tmp_path / "range") == content
    assert ranges == ["bytes=0-15", "bytes=16-31", "bytes=32-47"]


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ('attachment; filename="C:\\fakepath\\report.txt"', "report.txt"),
        ("attachment; filename*=UTF-8''%E7%AC%94%E8%AE%B0.txt", "笔记.txt"),
        ('attachment; filename="../../.."', None),
        ('attachment; filename="a:b?.txt"', "a_b_.txt"),
        ('attachment; filename="\u202evil.exe"', "vil.exe"),
        ("a" * 4097, None),
        ("", None),
    ],
)
async def test_filename_is_bounded_display_metadata(
    tmp_path: Path, header: str, expected: str | None
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x", headers={"Content-Disposition": header})

    # HTTP header values are bytes on the wire; support synthetic Unicode controls.
    if "\u202e" in header:
        from app.connectors.wecom.media import _filename

        assert _filename(header) == expected
        return
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await HttpWeComMedia(http, Tokens()).download(
            "synthetic-media", tmp_path / "result", max_bytes=1024
        )
    assert result.filename == expected


async def test_long_unicode_filename_respects_byte_limit(tmp_path: Path) -> None:
    from app.connectors.wecom.media import _filename

    result = _filename("attachment; filename=" + "中" * 200)
    assert result is not None
    assert len(result.encode()) <= 255


@pytest.mark.parametrize("mime", ["application/json", "text/plain", "application/octet-stream"])
async def test_one_token_refresh_with_json_error(tmp_path: Path, mime: str) -> None:
    tokens = Tokens()
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        token = request.url.params["access_token"]
        seen.append(token)
        if token == "synthetic-old":
            return httpx.Response(
                200,
                content=b'{"errcode":40014,"errmsg":"SECRET"}',
                headers={"Content-Type": mime},
            )
        return httpx.Response(200, content=b"data")

    assert await download(handler, tmp_path / "data", tokens=tokens) == b"data"
    assert tokens.invalidated == ["synthetic-old"]
    assert seen == ["synthetic-old", "synthetic-new"]


async def test_token_refresh_mid_range_preserves_previous_bytes(tmp_path: Path) -> None:
    tokens = Tokens()
    content = b"0123456789abcdefGHIJKLM"

    def handler(request: httpx.Request) -> httpx.Response:
        start = int(request.headers["range"].split("=")[1].split("-")[0])
        if start == 16 and request.url.params["access_token"] == "synthetic-old":
            return httpx.Response(200, json={"errcode": 42001})
        end = min(start + 15, len(content) - 1)
        return httpx.Response(
            206,
            content=content[start : end + 1],
            headers={"Content-Range": f"bytes {start}-{end}/{len(content)}"},
        )

    assert await download(handler, tmp_path / "file", tokens=tokens) == content
    assert len(tokens.invalidated) == 1


async def test_refresh_is_finite_and_errors_are_sanitized(tmp_path: Path) -> None:
    seen: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(1)
        return httpx.Response(200, json={"errcode": 40014, "errmsg": "SECRET"})

    path = tmp_path / "missing"
    with pytest.raises(ArtifactError, match="^wecom_40014$"):
        await download(handler, path)
    assert len(seen) == 2
    assert not path.exists()


@pytest.mark.parametrize(
    ("payload", "code", "retryable"),
    [
        ({"errcode": 40007, "errmsg": "SECRET"}, "wecom_40007", False),
        ({"errcode": -1}, "wecom_-1", True),
        ({"errcode": 45009}, "wecom_45009", True),
        ({"errcode": 830002}, "wecom_830002", False),
        ({"errcode": 0}, "media_invalid_response", False),
        ({"errcode": True}, "media_invalid_response", False),
        ({"download_url": "http://127.0.0.1/private"}, "media_invalid_response", False),
    ],
)
async def test_api_errors_do_not_become_saved_files(
    tmp_path: Path, payload: dict[str, object], code: str, retryable: bool
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    path = tmp_path / "output"
    with pytest.raises(ArtifactError) as error:
        await download(handler, path)
    assert error.value.code == code
    assert error.value.retryable == retryable
    assert not path.exists()


async def test_genuine_json_attachment_is_preserved(tmp_path: Path) -> None:
    content = b'{"errcode":40007,"customer":"document"}'

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=content,
            headers={
                "Content-Type": "application/json",
                "Content-Disposition": 'attachment; filename="example.json"',
            },
        )

    assert await download(handler, tmp_path / "file") == content


@pytest.mark.parametrize("status", [301, 302, 307, 308])
async def test_redirect_never_followed_even_if_client_default_enabled(
    tmp_path: Path, status: int
) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(status, headers={"Location": "http://169.254.169.254/latest"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), follow_redirects=True
    ) as http:
        with pytest.raises(ArtifactError, match="^media_redirect_forbidden$"):
            await HttpWeComMedia(http, Tokens()).download(
                "synthetic-media", tmp_path / "file", max_bytes=1024
            )
    assert len(calls) == 1
    assert not (tmp_path / "file").exists()


@pytest.mark.parametrize(
    ("headers", "data", "expected"),
    [
        ({"Content-Length": "9999"}, b"x", "media_too_large"),
        ({"Content-Length": "abc"}, b"x", "media_invalid_length"),
        ({"Content-Length": "4"}, b"x", "media_truncated"),
        ({"Content-Length": "1"}, b"xx", "media_invalid_length"),
        ({"Content-Encoding": "gzip"}, b"x", "media_encoding_unsupported"),
        ({}, b"", "media_empty_response"),
    ],
)
async def test_invalid_response_headers_or_body_cleanup(
    tmp_path: Path, headers: dict[str, str], data: bytes, expected: str
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=Chunks([data]), headers=headers)

    path = tmp_path / "file"
    with pytest.raises(ArtifactError, match=f"^{expected}$"):
        await download(handler, path)
    assert not path.exists()


async def test_stream_without_length_cannot_exceed_limit(tmp_path: Path) -> None:
    stream = Chunks([b"a" * 65536, b"b" * 65536])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    path = tmp_path / "file"
    with pytest.raises(ArtifactError, match="^media_too_large$"):
        await download(handler, path, max_bytes=70000)
    assert not path.exists()
    assert stream.closed


@pytest.mark.parametrize(
    "content_range",
    ["", "bytes 1-15/32", "bytes 0-14/32", "bytes 0-16/32", "bytes 0-15/*", "bytes 0-15/2048"],
)
async def test_invalid_range_rejected(tmp_path: Path, content_range: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(206, content=b"0" * 16, headers={"Content-Range": content_range})

    with pytest.raises(ArtifactError):
        await download(handler, tmp_path / "file")
    assert not (tmp_path / "file").exists()


@pytest.mark.parametrize("second", ["range_ignored", "total_changed", "mime_changed"])
async def test_range_consistency_rejected_with_partial_cleanup(tmp_path: Path, second: str) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(
                206, content=b"0" * 16, headers={"Content-Range": "bytes 0-15/32"}
            )
        if second == "range_ignored":
            return httpx.Response(200, content=b"0" * 32)
        return httpx.Response(
            206,
            content=b"1" * 16,
            headers={
                "Content-Range": "bytes 16-31/48"
                if second == "total_changed"
                else "bytes 16-31/32",
                "Content-Type": "video/mp4"
                if second == "mime_changed"
                else "application/octet-stream",
            },
        )

    with pytest.raises(ArtifactError):
        await download(handler, tmp_path / "file")
    assert not (tmp_path / "file").exists()


async def test_io_failure_is_safe_and_partial_file_is_removed(tmp_path: Path) -> None:
    stream = Chunks([b"a" * 65536], fail=True)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    with pytest.raises(ArtifactError, match="^media_transport_failed$") as error:
        await download(handler, tmp_path / "file", max_bytes=1024 * 1024)
    assert error.value.retryable
    assert not (tmp_path / "file").exists()
    assert stream.closed


async def test_total_deadline_and_cleanup(tmp_path: Path) -> None:
    stream = Chunks([b"a" * 65536, b"b"], delay=0.02)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(ArtifactError, match="^media_transport_failed$"):
            await HttpWeComMedia(http, Tokens(), timeout_seconds=0.03).download(
                "synthetic-media", tmp_path / "file", max_bytes=1024 * 1024
            )
    assert not (tmp_path / "file").exists()
    assert stream.closed


async def test_cancellation_preserved_and_partial_cleaned(tmp_path: Path) -> None:
    stream = Chunks([b"a"], delay=10)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        task = asyncio.create_task(
            HttpWeComMedia(http, Tokens()).download(
                "synthetic-media", tmp_path / "file", max_bytes=100
            )
        )
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not (tmp_path / "file").exists()


async def test_existing_destination_is_never_removed_or_overwritten(tmp_path: Path) -> None:
    path = tmp_path / "existing"
    path.write_bytes(b"preserve me")

    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("existing destination must be rejected before network")

    with pytest.raises(ArtifactError, match="^media_destination_exists$"):
        await download(handler, path)
    assert path.read_bytes() == b"preserve me"


@pytest.mark.parametrize("media_id", ["", "\x00bad", "a" * 1025, "https://evil.test/\n"])
async def test_invalid_media_id_before_io(tmp_path: Path, media_id: str) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    ) as http:
        with pytest.raises(ArtifactError, match="^media_invalid_request$"):
            await HttpWeComMedia(http, Tokens()).download(
                media_id, tmp_path / "file", max_bytes=100
            )
    assert not (tmp_path / "file").exists()


async def test_token_provider_errors_use_artifact_boundary(tmp_path: Path) -> None:
    class BrokenTokens(Tokens):
        async def get_token(self) -> str:
            raise APIError("wecom_token_unavailable", retryable=True)

    with pytest.raises(ArtifactError, match="^wecom_token_unavailable$") as error:
        await download(
            lambda request: httpx.Response(500), tmp_path / "file", tokens=BrokenTokens()
        )
    assert error.value.retryable
    assert not (tmp_path / "file").exists()


@pytest.mark.parametrize(
    ("status", "retryable"), [(400, False), (403, False), (429, True), (503, True)]
)
async def test_http_errors_have_bounded_retry_classification(
    tmp_path: Path, status: int, retryable: bool
) -> None:
    with pytest.raises(ArtifactError, match="^media_http_error$") as error:
        await download(lambda request: httpx.Response(status), tmp_path / "file")
    assert error.value.retryable == retryable
    assert not (tmp_path / "file").exists()


async def test_error_json_size_is_bounded_separately_from_file_limit(tmp_path: Path) -> None:
    stream = Chunks([b'{"errmsg":"' + b"a" * 65536 + b'"}'])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream, headers={"Content-Type": "application/json"})

    with pytest.raises(ArtifactError, match="^media_invalid_response$"):
        await download(handler, tmp_path / "file", max_bytes=1024 * 1024)
    assert not (tmp_path / "file").exists()
    assert stream.closed


@pytest.mark.parametrize("limit", [0, -1, True, 1024**3 + 1])
async def test_invalid_download_budget_precedes_network(tmp_path: Path, limit: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid byte budget must be rejected before network")

    with pytest.raises(ArtifactError, match="^media_invalid_request$"):
        await download(handler, tmp_path / "file", max_bytes=limit)
    assert not (tmp_path / "file").exists()


@pytest.mark.parametrize(
    ("deadline", "range_bytes"), [(0, 16), (301, 16), (120, 15), (120, 17), (120, 21 * 1024 * 1024)]
)
async def test_invalid_adapter_configuration(deadline: float, range_bytes: int) -> None:
    async with httpx.AsyncClient() as http:
        with pytest.raises(ArtifactError, match="^media_invalid_configuration$"):
            HttpWeComMedia(http, Tokens(), timeout_seconds=deadline, range_bytes=range_bytes)


async def test_media_id_url_is_only_a_query_value(tmp_path: Path) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, content=b"synthetic")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        await HttpWeComMedia(http, Tokens()).download(
            "http://127.0.0.1/secret?x=1&media_id=x", tmp_path / "file", max_bytes=1024
        )
    assert len(calls) == 1
    assert calls[0].url.host == "qyapi.weixin.qq.com"
    assert calls[0].url.params["media_id"] == "http://127.0.0.1/secret?x=1&media_id=x"
    assert len(calls[0].url.params.multi_items()) == 2
