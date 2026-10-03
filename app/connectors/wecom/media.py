"""Bounded downloads from the official, fixed-origin temporary-media endpoint.

The API documents byte ranges, including 16-byte alignment for encrypted resources.
No redirect or JSON download URL is followed; file names are metadata only.
"""

import asyncio
import hashlib
import json
import re
import unicodedata
from collections.abc import AsyncIterator
from email.message import Message
from pathlib import Path
from typing import BinaryIO

import httpx

from app.connectors.wecom.contracts import TokenProvider, WeComError
from app.domain.artifacts import ArtifactError, DownloadedFile

_ENDPOINT = "https://qyapi.weixin.qq.com/cgi-bin/media/get"
_JSON_LIMIT = 64 * 1024
_INVALID_TOKEN = {40014, 42001}
_TRANSIENT = {-1, 45009}
_RANGE = re.compile(r"bytes ([0-9]{1,20})-([0-9]{1,20})/([0-9]{1,20})")
_MIME = re.compile(r"[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+")


def _filename(header: str) -> str | None:
    if not header or len(header) > 4096:
        return None
    message = Message()
    message["Content-Disposition"] = header
    try:
        value = message.get_filename()
    except (ValueError, LookupError, UnicodeError):
        return None
    if not value:
        return None
    value = value.replace("\\", "/").rsplit("/", 1)[-1]
    value = "".join(
        "_" if char in '<>:"|?*' else char
        for char in value
        if not unicodedata.category(char).startswith("C")
    ).strip(" .")
    value = value.encode("utf-8", errors="ignore")[:255].decode("utf-8", errors="ignore")
    return value or None


def _content_type(header: str) -> str:
    value = header.partition(";")[0].strip().lower()
    return value if len(value) <= 127 and _MIME.fullmatch(value) else "application/octet-stream"


def _content_length(response: httpx.Response) -> int | None:
    value = response.headers.get("content-length")
    if value is None:
        return None
    if not re.fullmatch(r"[0-9]{1,20}", value):
        raise ArtifactError("media_invalid_length")
    return int(value)


def _error_code(data: bytes) -> int:
    try:
        payload: object = json.loads(data)
    except (ValueError, UnicodeError):
        raise ArtifactError("media_invalid_response") from None
    if not isinstance(payload, dict) or type(payload.get("errcode")) is not int:
        raise ArtifactError("media_invalid_response")
    code = payload["errcode"]
    if code == 0 or not -(2**31) <= code < 2**31:
        raise ArtifactError("media_invalid_response")
    return int(code)


class _TokenRejected(Exception):
    def __init__(self, code: int) -> None:
        self.code = code


async def _with_initial(initial: bytes, chunks: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    if initial:
        yield initial
    async for chunk in chunks:
        yield chunk


class HttpWeComMedia:
    def __init__(
        self,
        http: httpx.AsyncClient,
        tokens: TokenProvider,
        *,
        timeout_seconds: float = 120,
        range_bytes: int = 1024 * 1024,
    ) -> None:
        if not 0 < timeout_seconds <= 300 or not 16 <= range_bytes <= 20 * 1024 * 1024:
            raise ArtifactError("media_invalid_configuration")
        if range_bytes % 16:
            raise ArtifactError("media_invalid_configuration")
        self._http, self._tokens = http, tokens
        self._timeout, self._range_bytes = timeout_seconds, range_bytes

    async def download(self, media_id: str, destination: Path, *, max_bytes: int) -> DownloadedFile:
        try:
            valid_id = (
                bool(media_id)
                and len(media_id.encode("utf-8")) <= 1024
                and not any(unicodedata.category(char).startswith("C") for char in media_id)
            )
        except UnicodeError:
            valid_id = False
        if not valid_id or type(max_bytes) is not int or not 0 < max_bytes <= 1024**3:
            raise ArtifactError("media_invalid_request")
        created = completed = False
        try:
            # The caller owns this temporary destination; exclusive creation never
            # overwrites existing files or follows an existing destination symlink.
            with destination.open("xb") as output:
                created = True
                async with asyncio.timeout(self._timeout):
                    result = await self._download(media_id, destination, output, max_bytes)
                output.flush()
            completed = True
            return result
        except FileExistsError:
            raise ArtifactError("media_destination_exists") from None
        except (httpx.HTTPError, TimeoutError):
            raise ArtifactError("media_transport_failed", retryable=True) from None
        except WeComError as error:
            # Only stable error codes cross the artifact boundary, never raw URLs.
            code = error.code if re.fullmatch(r"[a-z0-9_]{1,80}", error.code) else "token_failed"
            raise ArtifactError(code, retryable=error.retryable) from None
        except OSError:
            raise ArtifactError("media_local_io_failed") from None
        finally:
            if created and not completed:
                try:
                    await asyncio.to_thread(destination.unlink, missing_ok=True)
                except OSError:
                    raise ArtifactError("media_cleanup_failed") from None

    async def _download(
        self, media_id: str, destination: Path, output: BinaryIO, max_bytes: int
    ) -> DownloadedFile:
        offset, total = 0, None
        refreshed = False
        digest = hashlib.sha256()
        mime: str | None = None
        filename: str | None = None
        while True:
            token = await self._tokens.get_token()
            requested_end = offset + self._range_bytes - 1
            try:
                async with self._http.stream(
                    "GET",
                    _ENDPOINT,
                    params={"access_token": token, "media_id": media_id},
                    headers={
                        "Range": f"bytes={offset}-{requested_end}",
                        "Accept-Encoding": "identity",
                    },
                    follow_redirects=False,
                    timeout=httpx.Timeout(min(self._timeout, 30), connect=min(self._timeout, 5)),
                ) as response:
                    if 300 <= response.status_code < 400:
                        raise ArtifactError("media_redirect_forbidden")
                    if response.status_code not in (200, 206):
                        raise ArtifactError(
                            "media_http_error",
                            retryable=response.status_code == 429 or response.status_code >= 500,
                        )
                    encoding = response.headers.get("content-encoding", "identity").lower()
                    if encoding != "identity":
                        raise ArtifactError("media_encoding_unsupported")
                    content_type = _content_type(response.headers.get("content-type", ""))
                    disposition = response.headers.get("content-disposition", "")
                    is_attachment = disposition.strip().lower().startswith("attachment;")
                    length = _content_length(response)
                    # API errors arrive as JSON with HTTP 200, including expired tokens.
                    # Genuine JSON files carry Content-Disposition: attachment.
                    is_json = (
                        content_type in ("application/json", "text/json") and not is_attachment
                    )
                    chunks = response.aiter_bytes(chunk_size=64 * 1024)
                    if is_json:
                        await self._read_error(chunks)
                    initial = await anext(chunks, b"")
                    if not is_attachment and initial.lstrip().startswith(b"{"):
                        await self._read_error(chunks, initial=initial)
                    expected: int | None = None
                    response_total: int | None = None
                    if response.status_code == 206:
                        match = _RANGE.fullmatch(response.headers.get("content-range", ""))
                        if not match:
                            raise ArtifactError("media_invalid_range")
                        start, end, response_total = (int(value) for value in match.groups())
                        if (
                            start != offset
                            or not start <= end < response_total
                            or end != min(requested_end, response_total - 1)
                            or (total is not None and total != response_total)
                        ):
                            raise ArtifactError("media_invalid_range")
                        if response_total > max_bytes:
                            raise ArtifactError("media_too_large")
                        expected = end - start + 1
                        if length is not None and length != expected:
                            raise ArtifactError("media_invalid_length")
                    elif offset:
                        raise ArtifactError("media_range_ignored")
                    elif length is not None:
                        expected = length
                    if length is not None and length > max_bytes:
                        raise ArtifactError("media_too_large")
                    if mime is not None and mime != content_type:
                        raise ArtifactError("media_changed_during_download")
                    mime = content_type
                    filename = filename or _filename(disposition)
                    received = 0
                    async for chunk in _with_initial(initial, chunks):
                        received += len(chunk)
                        if offset + received > max_bytes:
                            raise ArtifactError("media_too_large")
                        if expected is not None and received > expected:
                            raise ArtifactError("media_invalid_length")
                        output.write(chunk)
                        digest.update(chunk)
                    if expected is not None and received != expected:
                        raise ArtifactError("media_truncated", retryable=True)
                    if received == 0:
                        raise ArtifactError("media_empty_response")
                    offset += received
                    total = response_total
            except _TokenRejected as error:
                if refreshed:
                    raise ArtifactError(f"wecom_{error.code}") from None
                await self._tokens.invalidate(token)
                refreshed = True
                continue
            if total is None or offset == total:
                return DownloadedFile(
                    path=destination,
                    sha256=digest.hexdigest(),
                    size_bytes=offset,
                    content_type=mime or "application/octet-stream",
                    filename=filename,
                )

    async def _read_error(self, chunks: AsyncIterator[bytes], *, initial: bytes = b"") -> None:
        data = bytearray(initial)
        if len(data) > _JSON_LIMIT:
            raise ArtifactError("media_invalid_response")
        async for chunk in chunks:
            data.extend(chunk)
            if len(data) > _JSON_LIMIT:
                raise ArtifactError("media_invalid_response")
        code = _error_code(bytes(data))
        if code in _INVALID_TOKEN:
            raise _TokenRejected(code)
        raise ArtifactError(f"wecom_{code}", retryable=code in _TRANSIENT)
