"""Bounded official OpenAI retrieval API calls; callers own retries and reconciliation."""

import asyncio
import hashlib
import json
import math
import re
from collections.abc import Callable
from typing import cast
from uuid import UUID

import httpx
from pydantic import JsonValue, SecretStr

from app.domain.artifacts import ArtifactError
from app.domain.knowledge import VectorFileStatus, VectorHit, VectorRequestRejected

_BASE_URL = "https://api.openai.com/v1"
_STORE_ID = re.compile(r"vs_[A-Za-z0-9_-]{1,200}\Z")
_FILE_ID = re.compile(r"file[-_][A-Za-z0-9_-]{1,200}\Z")
_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_FILENAME = re.compile(rf"pkb-({_UUID})-({_UUID})-([0-9a-f]{{64}})\.md\Z")
_RESPONSE_BYTES = 2 * 1024 * 1024
_MAX_CONTENT_BYTES = 8 * 1024 * 1024
_MAX_PAGES = 100
_PAGE_SIZE = 100
_MAX_QUERY_CHARS = 16_384
_MAX_TEXT_CHARS = 1_000_000
_INVALID = "openai_vector_invalid_response"
_INVALID_INPUT = "openai_vector_invalid_input"


def _identifier(value: object, pattern: re.Pattern[str], *, code: str = _INVALID) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ArtifactError(code)
    return value


def _tenant(user_id: UUID) -> str:
    if not isinstance(user_id, UUID):
        raise ArtifactError(_INVALID_INPUT)
    return str(user_id)


def _filename(value: str) -> re.Match[str]:
    if not isinstance(value, str) or (match := _FILENAME.fullmatch(value)) is None:
        raise ArtifactError(_INVALID_INPUT)
    return match


def _finite_number(value: object, *, code: str = _INVALID) -> float:
    if type(value) not in (int, float):
        raise ArtifactError(code)
    try:
        result = float(value)  # type: ignore[arg-type]
    except (ValueError, OverflowError):
        raise ArtifactError(code) from None
    if not math.isfinite(result):
        raise ArtifactError(code)
    return result


def _attributes(value: object, *, code: str = _INVALID) -> dict[str, JsonValue]:
    if not isinstance(value, dict) or len(value) > 16:
        raise ArtifactError(code)
    result: dict[str, JsonValue] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not 1 <= len(key) <= 64:
            raise ArtifactError(code)
        if isinstance(item, str):
            if len(item) > 512:
                raise ArtifactError(code)
        elif type(item) is not bool:
            _finite_number(item, code=code)
        result[key] = item
    return result


def _metadata(value: object) -> dict[str, str]:
    if value is None:
        return {}
    attributes = _attributes(value)
    if any(not isinstance(item, str) for item in attributes.values()):
        raise ArtifactError(_INVALID)
    return cast(dict[str, str], attributes)


def _object(value: object, kind: str) -> dict[str, object]:
    if not isinstance(value, dict) or value.get("object") != kind:
        raise ArtifactError(_INVALID)
    return value


def _json_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_key")
        result[key] = value
    return result


def _json_constant(value: str) -> None:
    raise ValueError("invalid_json_constant")


def _file_status(payload: dict[str, object], store_id: str, file_id: str) -> VectorFileStatus:
    item = _object(payload, "vector_store.file")
    if item.get("id") != file_id or item.get("vector_store_id") != store_id:
        raise ArtifactError(_INVALID)
    status = item.get("status")
    if status not in ("in_progress", "completed", "cancelled", "failed"):
        raise ArtifactError(_INVALID)
    return status


class OpenAIVectorAdapter:
    def __init__(
        self,
        http: httpx.AsyncClient,
        api_key: str,
        *,
        timeout_seconds: float = 30,
        max_content_bytes: int = _MAX_CONTENT_BYTES,
    ) -> None:
        code = "openai_vector_invalid_configuration"
        timeout = _finite_number(timeout_seconds, code=code)
        if (
            not isinstance(api_key, str)
            or not 1 <= len(api_key) <= 512
            or any(not 33 <= ord(char) <= 126 for char in api_key)
            or not 0 < timeout <= 120
            or type(max_content_bytes) is not int
            or not 1 <= max_content_bytes <= _MAX_CONTENT_BYTES
        ):
            raise ArtifactError(code)
        self._http = http
        self._api_key = SecretStr(api_key)
        self._timeout = timeout
        self._max_content_bytes = max_content_bytes

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        payload: dict[str, object] | None = None,
        filename: str | None = None,
        content: bytes | None = None,
    ) -> httpx.Request:
        # Standalone requests deliberately bypass client base URLs, auth, cookies,
        # default headers and query parameters. The caller retains client ownership.
        try:
            return httpx.Request(
                method,
                _BASE_URL + path,
                headers={
                    "Authorization": f"Bearer {self._api_key.get_secret_value()}",
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                },
                params=params,
                json=payload,
                data={"purpose": "assistants"} if filename is not None else None,
                files={"file": (filename, content, "text/markdown")}
                if filename is not None and content is not None
                else None,
                extensions={
                    "timeout": httpx.Timeout(
                        self._timeout, connect=min(self._timeout, 10)
                    ).as_dict()
                },
            )
        except (ValueError, TypeError, UnicodeError):
            raise VectorRequestRejected(_INVALID_INPUT) from None

    async def _send(
        self, request: httpx.Request, *, allow_missing: bool = False
    ) -> dict[str, object] | None:
        try:
            async with asyncio.timeout(self._timeout):
                response = await self._http.send(
                    request, stream=True, auth=None, follow_redirects=False
                )
                try:
                    if response.status_code == 404 and allow_missing:
                        return None
                    if 300 <= response.status_code < 400:
                        raise ArtifactError("openai_vector_redirect_forbidden")
                    if response.status_code in (401, 403):
                        raise VectorRequestRejected("openai_vector_auth_failed")
                    if response.status_code == 429:
                        raise VectorRequestRejected("openai_vector_rate_limited", retryable=True)
                    if 500 <= response.status_code < 600:
                        raise ArtifactError("openai_vector_service_failed", retryable=True)
                    if response.status_code == 408:
                        raise ArtifactError("openai_vector_http_error", retryable=True)
                    if 400 <= response.status_code < 500:
                        raise VectorRequestRejected("openai_vector_http_error")
                    if response.status_code != 200:
                        raise ArtifactError("openai_vector_http_error")
                    if response.headers.get("content-encoding", "identity").lower() != "identity":
                        raise ArtifactError(_INVALID)
                    if (
                        response.headers.get("content-type", "").partition(";")[0].strip().lower()
                        != "application/json"
                    ):
                        raise ArtifactError(_INVALID)
                    length = response.headers.get("content-length")
                    if length is not None:
                        if not re.fullmatch(r"[0-9]{1,20}", length):
                            raise ArtifactError(_INVALID)
                        if int(length) > _RESPONSE_BYTES:
                            raise ArtifactError("openai_vector_response_too_large")
                    body = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=64 * 1024):
                        if len(body) + len(chunk) > _RESPONSE_BYTES:
                            raise ArtifactError("openai_vector_response_too_large")
                        body.extend(chunk)
                    if length is not None and len(body) != int(length):
                        raise ArtifactError(_INVALID)
                finally:
                    await response.aclose()
        except (httpx.HTTPError, TimeoutError, OSError):
            raise ArtifactError("openai_vector_transport_failed", retryable=True) from None
        try:
            payload: object = json.loads(
                body, object_pairs_hook=_json_pairs, parse_constant=_json_constant
            )
        except (ValueError, UnicodeError, RecursionError):
            raise ArtifactError(_INVALID) from None
        if not isinstance(payload, dict):
            raise ArtifactError(_INVALID)
        if payload.get("error") is not None:
            raise ArtifactError("openai_vector_api_error")
        return payload

    async def _required(self, request: httpx.Request) -> dict[str, object]:
        payload = await self._send(request)
        assert payload is not None  # _send only returns None for explicitly allowed 404s.
        return payload

    async def _find(
        self,
        path: str,
        pattern: re.Pattern[str],
        matches: Callable[[dict[str, object]], bool],
        *,
        purpose: str | None = None,
    ) -> str | None:
        params = {"limit": str(_PAGE_SIZE), "order": "asc"}
        if purpose is not None:
            params["purpose"] = purpose
        seen: set[str] = set()
        found: str | None = None
        # A scan has one overall deadline as well as bounded individual HTTP calls.
        try:
            async with asyncio.timeout(self._timeout):
                for _ in range(_MAX_PAGES):
                    payload = _object(
                        await self._required(self._request("GET", path, params=params)), "list"
                    )
                    data, has_more = payload.get("data"), payload.get("has_more")
                    if (
                        not isinstance(data, list)
                        or len(data) > _PAGE_SIZE
                        or type(has_more) is not bool
                        or "first_id" not in payload
                        or "last_id" not in payload
                    ):
                        raise ArtifactError(_INVALID)
                    page_ids: list[str] = []
                    for item in data:
                        if not isinstance(item, dict):
                            raise ArtifactError(_INVALID)
                        item_id = _identifier(item.get("id"), pattern)
                        if item_id in seen:
                            raise ArtifactError("openai_vector_incomplete_scan")
                        seen.add(item_id)
                        page_ids.append(item_id)
                        if matches(item):
                            if found is not None:
                                raise ArtifactError("openai_vector_ambiguous_match")
                            found = item_id
                    if not page_ids:
                        if (
                            has_more
                            or payload.get("first_id") is not None
                            or payload.get("last_id") is not None
                        ):
                            raise ArtifactError("openai_vector_incomplete_scan")
                        return found
                    if (
                        payload.get("first_id") != page_ids[0]
                        or payload.get("last_id") != page_ids[-1]
                    ):
                        raise ArtifactError("openai_vector_incomplete_scan")
                    if not has_more:
                        return found
                    params["after"] = page_ids[-1]
        except TimeoutError:
            raise ArtifactError("openai_vector_transport_failed", retryable=True) from None
        raise ArtifactError("openai_vector_incomplete_scan")

    async def find_store(self, user_id: UUID) -> str | None:
        tenant = _tenant(user_id)

        def matches(item: dict[str, object]) -> bool:
            _object(item, "vector_store")
            if "metadata" not in item:
                raise ArtifactError(_INVALID)
            metadata = _metadata(item.get("metadata"))
            if item.get("status") not in ("in_progress", "completed", "expired"):
                raise ArtifactError(_INVALID)
            if metadata.get("pkb_user_id") != tenant:
                return False
            if item.get("status") == "expired" or item.get("expires_after") is not None:
                raise ArtifactError("openai_vector_store_expired")
            return True

        return await self._find("/vector_stores", _STORE_ID, matches)

    async def create_store(self, user_id: UUID) -> str:
        tenant = _tenant(user_id)
        payload = _object(
            await self._required(
                self._request(
                    "POST",
                    "/vector_stores",
                    payload={"name": "Personal knowledge", "metadata": {"pkb_user_id": tenant}},
                )
            ),
            "vector_store",
        )
        store_id = _identifier(payload.get("id"), _STORE_ID)
        if (
            _metadata(payload.get("metadata")).get("pkb_user_id") != tenant
            or payload.get("status") not in ("in_progress", "completed")
            or payload.get("expires_after") is not None
        ):
            raise ArtifactError(_INVALID)
        return store_id

    async def find_file(self, filename: str) -> str | None:
        _filename(filename)

        def matches(item: dict[str, object]) -> bool:
            _object(item, "file")
            name = item.get("filename")
            if (
                item.get("purpose") != "assistants"
                or not isinstance(name, str)
                or not 1 <= len(name) <= 1024
            ):
                raise ArtifactError(_INVALID)
            return name == filename

        return await self._find("/files", _FILE_ID, matches, purpose="assistants")

    async def upload(self, filename: str, content: bytes) -> str:
        match = _filename(filename)
        if not isinstance(content, bytes) or not content:
            raise ArtifactError(_INVALID_INPUT)
        if len(content) > self._max_content_bytes:
            raise ArtifactError("openai_vector_content_too_large")
        if hashlib.sha256(content).hexdigest() != match.group(3):
            raise ArtifactError(_INVALID_INPUT)
        payload = _object(
            await self._required(
                self._request("POST", "/files", filename=filename, content=content)
            ),
            "file",
        )
        file_id = _identifier(payload.get("id"), _FILE_ID)
        if (
            payload.get("filename") != filename
            or payload.get("purpose") != "assistants"
            or type(payload.get("bytes")) is not int
            or payload.get("bytes") != len(content)
        ):
            raise ArtifactError(_INVALID)
        return file_id

    async def file_status(self, store_id: str, file_id: str) -> VectorFileStatus | None:
        _identifier(store_id, _STORE_ID, code=_INVALID_INPUT)
        _identifier(file_id, _FILE_ID, code=_INVALID_INPUT)
        payload = await self._send(
            self._request("GET", f"/vector_stores/{store_id}/files/{file_id}"),
            allow_missing=True,
        )
        return _file_status(payload, store_id, file_id) if payload is not None else None

    async def attach(
        self, store_id: str, file_id: str, attributes: dict[str, JsonValue]
    ) -> VectorFileStatus:
        _identifier(store_id, _STORE_ID, code=_INVALID_INPUT)
        _identifier(file_id, _FILE_ID, code=_INVALID_INPUT)
        checked = _attributes(attributes, code=_INVALID_INPUT)
        tenant = checked.get("user_id")
        if not isinstance(tenant, str) or not re.fullmatch(_UUID, tenant):
            raise ArtifactError(_INVALID_INPUT)
        payload = await self._required(
            self._request(
                "POST",
                f"/vector_stores/{store_id}/files",
                payload={"file_id": file_id, "attributes": checked},
            )
        )
        status = _file_status(payload, store_id, file_id)
        if _attributes(payload.get("attributes")) != checked:
            raise ArtifactError(_INVALID)
        return status

    async def _delete(self, path: str, file_id: str, kind: str) -> None:
        payload = await self._send(self._request("DELETE", path), allow_missing=True)
        if payload is not None:
            item = _object(payload, kind)
            if item.get("id") != file_id or item.get("deleted") is not True:
                raise ArtifactError(_INVALID)

    async def detach(self, store_id: str, file_id: str) -> None:
        _identifier(store_id, _STORE_ID, code=_INVALID_INPUT)
        _identifier(file_id, _FILE_ID, code=_INVALID_INPUT)
        await self._delete(
            f"/vector_stores/{store_id}/files/{file_id}", file_id, "vector_store.file.deleted"
        )

    async def delete_file(self, file_id: str) -> None:
        _identifier(file_id, _FILE_ID, code=_INVALID_INPUT)
        await self._delete(f"/files/{file_id}", file_id, "file")

    async def search(
        self, store_id: str, query: str, *, user_id: UUID, limit: int
    ) -> tuple[VectorHit, ...]:
        _identifier(store_id, _STORE_ID, code=_INVALID_INPUT)
        tenant = _tenant(user_id)
        if (
            not isinstance(query, str)
            or not query.strip()
            or len(query) > _MAX_QUERY_CHARS
            or type(limit) is not int
            or not 1 <= limit <= 50
        ):
            raise ArtifactError(_INVALID_INPUT)
        payload = _object(
            await self._required(
                self._request(
                    "POST",
                    f"/vector_stores/{store_id}/search",
                    payload={
                        "query": query,
                        "max_num_results": limit,
                        "rewrite_query": False,
                        "filters": {"type": "eq", "key": "user_id", "value": tenant},
                    },
                )
            ),
            "vector_store.search_results.page",
        )
        data, has_more, next_page = (
            payload.get("data"),
            payload.get("has_more"),
            payload.get("next_page"),
        )
        search_query = payload.get("search_query")
        if (
            not isinstance(data, list)
            or len(data) > limit
            or type(has_more) is not bool
            or (has_more and (not isinstance(next_page, str) or not 1 <= len(next_page) <= 1024))
            or (not has_more and next_page is not None)
            or not isinstance(search_query, list)
            or len(search_query) != 1
            or search_query[0] != query
        ):
            raise ArtifactError(_INVALID)
        hits: list[VectorHit] = []
        for item in data:
            if not isinstance(item, dict):
                raise ArtifactError(_INVALID)
            file_id = _identifier(item.get("file_id"), _FILE_ID)
            score = _finite_number(item.get("score"))
            attributes = _attributes(item.get("attributes"))
            content = item.get("content")
            filename = item.get("filename")
            if (
                not 0 <= score <= 1
                or not isinstance(content, list)
                or not 1 <= len(content) <= 100
                or not isinstance(filename, str)
                or not 1 <= len(filename) <= 1024
            ):
                raise ArtifactError(_INVALID)
            if attributes.get("user_id") != tenant:
                raise ArtifactError("openai_vector_tenant_mismatch")
            parts: list[str] = []
            for part in content:
                if not isinstance(part, dict) or part.get("type") != "text":
                    raise ArtifactError(_INVALID)
                text = part.get("text")
                if not isinstance(text, str) or not text.strip():
                    raise ArtifactError(_INVALID)
                parts.append(text)
            text = "\n".join(parts)
            if len(text) > _MAX_TEXT_CHARS:
                raise ArtifactError("openai_vector_response_too_large")
            hits.append(VectorHit(file_id=file_id, score=score, text=text, attributes=attributes))
        return tuple(hits)
