import asyncio
import hashlib
import json
import traceback
from collections.abc import AsyncIterator, Callable
from email import policy
from email.parser import BytesParser
from typing import Any
from uuid import UUID

import httpx
import pytest

from app.adapters.openai_vector import OpenAIVectorAdapter
from app.domain.artifacts import ArtifactError
from app.domain.knowledge import VectorProvider, VectorRequestRejected

USER = UUID("11111111-1111-4111-8111-111111111111")
SOURCE = UUID("22222222-2222-4222-8222-222222222222")
OTHER = UUID("33333333-3333-4333-8333-333333333333")
CONTENT = "# 证据\n\n仅为测试文本。\n".encode()
NAME = f"pkb-{USER}-{SOURCE}-{hashlib.sha256(CONTENT).hexdigest()}.md"
ATTRS = {"user_id": str(USER), "source_id": str(SOURCE), "revision": 1, "active": True}
INVALID = "openai_vector_invalid_response"


def store(store_id: str = "vs_first", user: UUID = USER) -> dict[str, Any]:
    return {
        "object": "vector_store",
        "id": store_id,
        "status": "completed",
        "metadata": {"pkb_user_id": str(user)},
    }


def file(file_id: str = "file-first", filename: str = NAME) -> dict[str, Any]:
    return {
        "object": "file",
        "id": file_id,
        "filename": filename,
        "purpose": "assistants",
        "bytes": len(CONTENT),
    }


def page(items: list[dict[str, Any]], *, more: bool = False) -> dict[str, Any]:
    return {
        "object": "list",
        "data": items,
        "has_more": more,
        "first_id": items[0]["id"] if items else None,
        "last_id": items[-1]["id"] if items else None,
    }


def status(value: str = "completed", file_id: str = "file-first") -> dict[str, Any]:
    return {
        "object": "vector_store.file",
        "id": file_id,
        "vector_store_id": "vs_first",
        "status": value,
        "attributes": ATTRS.copy(),
    }


def search_result() -> dict[str, Any]:
    return {
        "object": "vector_store.search_results.page",
        "search_query": ["证据在哪里？"],
        "has_more": False,
        "next_page": None,
        "data": [
            {
                "file_id": "file-first",
                "filename": NAME,
                "score": 0.75,
                "attributes": ATTRS.copy(),
                "content": [{"type": "text", "text": "第一段"}, {"type": "text", "text": "第二段"}],
            }
        ],
    }


def api(client: httpx.AsyncClient, **kwargs: Any) -> OpenAIVectorAdapter:
    return OpenAIVectorAdapter(client, "test-private-key", **kwargs)


def client_with(payload: object, *, status_code: int = 200) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status_code, json=payload))
    )


async def test_create_is_official_fixed_request_without_client_credentials_or_expiry() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "POST"
        assert str(request.url) == "https://api.openai.com/v1/vector_stores"
        assert request.headers["Authorization"] == "Bearer test-private-key"
        assert request.headers["accept-encoding"] == "identity"
        assert "cookie" not in request.headers
        assert "x-private" not in request.headers
        assert "idempotency-key" not in request.headers
        assert request.extensions["timeout"] == {
            "connect": 10,
            "read": 30,
            "write": 30,
            "pool": 30,
        }
        body = json.loads(request.content)
        assert body == {"name": "Personal knowledge", "metadata": {"pkb_user_id": str(USER)}}
        return httpx.Response(200, json=store())

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle),
        base_url="https://untrusted.invalid/",
        params={"private": "query"},
        headers={"X-Private": "secret", "Authorization": "secret"},
        cookies={"private": "cookie"},
        auth=("private-user", "private-password"),
        follow_redirects=True,
    ) as client:
        adapter: VectorProvider = api(client)
        assert await adapter.create_store(USER) == "vs_first"
        assert not client.is_closed
        assert "test-private-key" not in repr(adapter)
        assert "test-private-key" not in repr(vars(adapter))
    assert len(requests) == 1


async def test_find_store_finishes_scan_after_matching_first_page() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        assert request.url.path == "/v1/vector_stores"
        assert request.url.params["limit"] == "100"
        assert request.url.params["order"] == "asc"
        if len(requests) == 1:
            assert "after" not in request.url.params
            return httpx.Response(200, json=page([store()], more=True))
        assert request.url.params["after"] == "vs_first"
        return httpx.Response(200, json=page([store("vs_other", OTHER)]))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        assert await api(client).find_store(USER) == "vs_first"
    assert len(requests) == 2


async def test_find_file_finishes_scan_and_sets_assistants_purpose() -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.method == "GET"
        assert request.url.path == "/v1/files"
        assert request.url.params["purpose"] == "assistants"
        if calls == 1:
            return httpx.Response(200, json=page([file("file-other", "other.md")], more=True))
        assert request.url.params["after"] == "file-other"
        return httpx.Response(200, json=page([file()]))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        assert await api(client).find_file(NAME) == "file-first"
    assert calls == 2


@pytest.mark.parametrize("operation", ["find_store", "find_file"])
async def test_completed_empty_scan_returns_none(operation: str) -> None:
    async with client_with(page([])) as client:
        adapter = api(client)
        assert (
            await getattr(adapter, operation)(USER if operation == "find_store" else NAME) is None
        )


async def test_find_store_matches_uuid_text_exactly() -> None:
    item = store(user=OTHER)
    item["metadata"] = {"pkb_user_id": f" {USER}"}
    async with client_with(page([item])) as client:
        assert await api(client).find_store(USER) is None


@pytest.mark.parametrize("operation", ["find_store", "find_file"])
async def test_multiple_matches_are_not_arbitrarily_selected(operation: str) -> None:
    items = (
        [store(), store("vs_second")]
        if operation == "find_store"
        else [file(), file("file-second")]
    )
    async with client_with(page(items)) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_ambiguous_match$"):
            await getattr(api(client), operation)(USER if operation == "find_store" else NAME)


@pytest.mark.parametrize(
    "mutate,code",
    [
        (lambda p: p.update(object="unknown"), INVALID),
        (lambda p: p.update(data=None), INVALID),
        (lambda p: p.update(data=[None]), INVALID),
        (lambda p: p.update(has_more="false"), INVALID),
        (lambda p: p.pop("has_more"), INVALID),
        (lambda p: p.pop("first_id"), INVALID),
        (lambda p: p.pop("last_id"), INVALID),
        (lambda p: p.update(first_id="vs_other"), "openai_vector_incomplete_scan"),
        (lambda p: p.update(last_id="../private"), "openai_vector_incomplete_scan"),
        (lambda p: p["data"][0].update(id="vs_/escape"), INVALID),
        (lambda p: p["data"][0].update(metadata=[]), INVALID),
        (lambda p: p["data"][0].pop("metadata"), INVALID),
        (lambda p: p["data"][0].update(metadata={"pkb_user_id": 1}), INVALID),
        (lambda p: p["data"][0].update(status="unknown"), INVALID),
        (lambda p: p["data"][0].update(status="expired"), "openai_vector_store_expired"),
        (
            lambda p: p["data"][0].update(expires_after={"anchor": "last_active_at", "days": 7}),
            "openai_vector_store_expired",
        ),
    ],
)
async def test_malformed_listing_fails_closed(
    mutate: Callable[[dict[str, Any]], object], code: str
) -> None:
    payload = page([store()])
    mutate(payload)
    async with client_with(payload) as client:
        with pytest.raises(ArtifactError, match=f"^{code}$"):
            await api(client).find_store(USER)


@pytest.mark.parametrize(
    "payload",
    [page([], more=True), {**page([]), "last_id": "vs_lost"}, page([store()] * 101)],
)
async def test_impossible_empty_or_oversized_listing_rejected(payload: dict[str, Any]) -> None:
    async with client_with(payload) as client:
        with pytest.raises(ArtifactError):
            await api(client).find_store(USER)


async def test_cursor_loop_is_not_absence_or_single_match() -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=page([store()], more=True))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_incomplete_scan$"):
            await api(client).find_store(USER)
    assert calls == 2


async def test_scan_limit_never_reports_found_item_or_absence_when_incomplete() -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=page([store(f"vs_{calls}", OTHER)], more=True))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_incomplete_scan$"):
            await api(client).find_store(USER)
    assert calls == 100


async def test_error_after_match_does_not_return_match_or_retry_in_adapter() -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, json=page([store()], more=True))
        return httpx.Response(503, text="private upstream response")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_service_failed$"):
            await api(client).find_store(USER)
    assert calls == 2


@pytest.mark.parametrize(
    "change",
    [
        {"id": "file-other"},
        {"metadata": {"pkb_user_id": str(OTHER)}},
        {"metadata": None},
        {"status": "expired"},
        {"expires_after": {"anchor": "last_active_at", "days": 1}},
    ],
)
async def test_created_store_requires_correct_identity_and_persistent_scope(
    change: dict[str, Any],
) -> None:
    async with client_with({**store(), **change}) as client:
        with pytest.raises(ArtifactError, match=f"^{INVALID}$"):
            await api(client).create_store(USER)


async def test_upload_uses_bounded_markdown_bytes_deterministic_name_and_assistants() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert str(request.url) == "https://api.openai.com/v1/files"
        message = BytesParser(policy=policy.default).parsebytes(
            b"Content-Type: "
            + request.headers["content-type"].encode()
            + b"\r\n\r\n"
            + request.content
        )
        parts = {
            part.get_param("name", header="content-disposition"): part
            for part in message.iter_parts()
        }
        assert set(parts) == {"purpose", "file"}
        assert parts["purpose"].get_payload(decode=True) == b"assistants"
        assert parts["file"].get_filename() == NAME
        assert parts["file"].get_content_type() == "text/markdown"
        assert parts["file"].get_payload(decode=True) == CONTENT
        return httpx.Response(200, json=file())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        assert (
            await api(client, max_content_bytes=len(CONTENT)).upload(NAME, CONTENT) == "file-first"
        )


@pytest.mark.parametrize(
    "change",
    [
        {"id": "file-../escape"},
        {"filename": "other.md"},
        {"purpose": "user_data"},
        {"bytes": 0},
        {"bytes": True},
        {"object": "vector_store.file"},
    ],
)
async def test_upload_response_must_match_uploaded_file(change: dict[str, Any]) -> None:
    async with client_with({**file(), **change}) as client:
        with pytest.raises(ArtifactError, match=f"^{INVALID}$"):
            await api(client).upload(NAME, CONTENT)


@pytest.mark.parametrize("value", ["in_progress", "completed", "cancelled", "failed"])
async def test_file_status_and_attach_follow_documented_status_values(value: str) -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            assert request.url.path == "/v1/vector_stores/vs_first/files/file-first"
        else:
            assert request.method == "POST"
            assert request.url.path == "/v1/vector_stores/vs_first/files"
            assert json.loads(request.content) == {"file_id": "file-first", "attributes": ATTRS}
        return httpx.Response(200, json=status(value))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        adapter = api(client)
        assert await adapter.file_status("vs_first", "file-first") == value
        assert await adapter.attach("vs_first", "file-first", ATTRS) == value
    assert len(requests) == 2


@pytest.mark.parametrize(
    "change",
    [
        {"id": "file-other"},
        {"vector_store_id": "vs_other"},
        {"status": "uploaded"},
        {"status": None},
        {"status": []},
        {"object": "file"},
    ],
)
async def test_status_response_identity_and_type_are_checked(change: dict[str, Any]) -> None:
    async with client_with({**status(), **change}) as client:
        with pytest.raises(ArtifactError, match=f"^{INVALID}$"):
            await api(client).file_status("vs_first", "file-first")


async def test_attach_requires_attributes_confirmed_by_provider() -> None:
    async with client_with({**status(), "attributes": {"user_id": str(OTHER)}}) as client:
        with pytest.raises(ArtifactError, match=f"^{INVALID}$"):
            await api(client).attach("vs_first", "file-first", ATTRS)


async def test_404_is_missing_status_and_idempotent_delete_only() -> None:
    async with client_with({"error": "private response"}, status_code=404) as client:
        adapter = api(client)
        assert await adapter.file_status("vs_first", "file-first") is None
        assert await adapter.detach("vs_first", "file-first") is None
        assert await adapter.delete_file("file-first") is None
        with pytest.raises(ArtifactError, match="^openai_vector_http_error$"):
            await adapter.attach("vs_first", "file-first", ATTRS)
        with pytest.raises(ArtifactError, match="^openai_vector_http_error$"):
            await adapter.find_store(USER)


async def test_delete_contracts_do_not_confuse_detachment_and_underlying_file() -> None:
    paths: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        assert request.method == "DELETE"
        return httpx.Response(
            200,
            json={
                "id": "file-first",
                "object": "vector_store.file.deleted" if len(paths) == 1 else "file",
                "deleted": True,
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        adapter = api(client)
        await adapter.detach("vs_first", "file-first")
        await adapter.delete_file("file-first")
    assert paths == ["/v1/vector_stores/vs_first/files/file-first", "/v1/files/file-first"]


@pytest.mark.parametrize("change", [{"deleted": False}, {"deleted": 1}, {"id": "file-other"}])
async def test_delete_does_not_assume_success(change: dict[str, Any]) -> None:
    async with client_with(
        {"id": "file-first", "object": "file", "deleted": True, **change}
    ) as client:
        with pytest.raises(ArtifactError, match=f"^{INVALID}$"):
            await api(client).delete_file("file-first")


async def test_search_has_trusted_filter_without_query_rewrite_and_returns_plain_dto() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert str(request.url) == "https://api.openai.com/v1/vector_stores/vs_first/search"
        assert json.loads(request.content) == {
            "query": "证据在哪里？",
            "max_num_results": 10,
            "rewrite_query": False,
            "filters": {"type": "eq", "key": "user_id", "value": str(USER)},
        }
        return httpx.Response(200, json=search_result())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        result = await api(client).search("vs_first", "证据在哪里？", user_id=USER, limit=10)
    assert isinstance(result, tuple)
    assert len(result) == 1
    assert result[0].file_id == "file-first"
    assert result[0].score == 0.75
    assert result[0].text == "第一段\n第二段"
    assert result[0].attributes == ATTRS
    assert "第一段" not in repr(result)


@pytest.mark.parametrize("prefix", ["file-first", "file_first"])
async def test_both_official_documented_file_id_formats_are_accepted(prefix: str) -> None:
    async with client_with(file(prefix)) as client:
        assert await api(client).upload(NAME, CONTENT) == prefix
    payload = search_result()
    payload["data"][0]["file_id"] = prefix
    async with client_with(payload) as client:
        assert (await api(client).search("vs_first", "证据在哪里？", user_id=USER, limit=1))[
            0
        ].file_id == prefix


@pytest.mark.parametrize(
    "mutate,code",
    [
        (lambda p: p.update(object="list"), INVALID),
        (lambda p: p.update(data=None), INVALID),
        (lambda p: p.update(data=[None]), INVALID),
        (lambda p: p.update(has_more=1), INVALID),
        (lambda p: p.update(has_more=True, next_page=None), INVALID),
        (lambda p: p.update(next_page="unexpected"), INVALID),
        (lambda p: p.update(search_query=["rewritten query"]), INVALID),
        (lambda p: p["data"][0].update(file_id="file-../escape"), INVALID),
        (lambda p: p["data"][0].update(filename=None), INVALID),
        (lambda p: p["data"][0].update(score=True), INVALID),
        (lambda p: p["data"][0].update(score="0.5"), INVALID),
        (lambda p: p["data"][0].update(score=-0.1), INVALID),
        (lambda p: p["data"][0].update(score=1.1), INVALID),
        (lambda p: p["data"][0].update(score=10**400), INVALID),
        (lambda p: p["data"][0].update(content=[]), INVALID),
        (lambda p: p["data"][0].update(content=[None]), INVALID),
        (lambda p: p["data"][0].update(content=[{"type": "image", "text": "private"}]), INVALID),
        (lambda p: p["data"][0].update(content=[{"type": "text", "text": None}]), INVALID),
        (lambda p: p["data"][0].update(content=[{"type": "text", "text": "  "}]), INVALID),
        (lambda p: p["data"][0].update(attributes=None), INVALID),
        (lambda p: p["data"][0].update(attributes={"user_id": [str(USER)]}), INVALID),
        (
            lambda p: p["data"][0].update(attributes={"user_id": str(OTHER)}),
            "openai_vector_tenant_mismatch",
        ),
        (lambda p: p["data"][0].update(attributes={}), "openai_vector_tenant_mismatch"),
    ],
)
async def test_malformed_search_or_foreign_tenant_response_rejected(
    mutate: Callable[[dict[str, Any]], object], code: str
) -> None:
    payload = search_result()
    mutate(payload)
    async with client_with(payload) as client:
        with pytest.raises(ArtifactError, match=f"^{code}$"):
            await api(client).search("vs_first", "证据在哪里？", user_id=USER, limit=1)


async def test_search_count_is_bounded_and_empty_result_is_valid() -> None:
    payload = search_result()
    payload["data"] *= 2
    async with client_with(payload) as client:
        with pytest.raises(ArtifactError, match=f"^{INVALID}$"):
            await api(client).search("vs_first", "证据在哪里？", user_id=USER, limit=1)
    payload["data"] = []
    async with client_with(payload) as client:
        assert await api(client).search("vs_first", "证据在哪里？", user_id=USER, limit=1) == ()


@pytest.mark.parametrize("value", [0, 1, 0.5])
async def test_search_score_boundaries(value: float) -> None:
    payload = search_result()
    payload["data"][0]["score"] = value
    async with client_with(payload) as client:
        assert (await api(client).search("vs_first", "证据在哪里？", user_id=USER, limit=1))[
            0
        ].score == value


@pytest.mark.parametrize(
    "operation,args,kwargs,code",
    [
        ("create_store", (str(USER),), {}, "openai_vector_invalid_input"),
        ("find_store", (str(USER),), {}, "openai_vector_invalid_input"),
        ("find_file", ("other.md",), {}, "openai_vector_invalid_input"),
        ("find_file", (NAME + "\n",), {}, "openai_vector_invalid_input"),
        ("upload", (NAME, b"different bytes"), {}, "openai_vector_invalid_input"),
        ("upload", (NAME, b""), {}, "openai_vector_invalid_input"),
        ("upload", (NAME, bytearray(CONTENT)), {}, "openai_vector_invalid_input"),
        ("upload", (NAME, b"x" * (8 * 1024 * 1024 + 1)), {}, "openai_vector_content_too_large"),
        ("file_status", ("vs_/escape", "file-first"), {}, "openai_vector_invalid_input"),
        ("file_status", ("vs_first", "file-first?secret"), {}, "openai_vector_invalid_input"),
        ("attach", ("vs_first", "file-first", {}), {}, "openai_vector_invalid_input"),
        (
            "attach",
            ("vs_first", "file-first", {"user_id": "../tenant"}),
            {},
            "openai_vector_invalid_input",
        ),
        ("detach", ("vs_first", "file-first\n"), {}, "openai_vector_invalid_input"),
        ("delete_file", ("https://untrusted.invalid/",), {}, "openai_vector_invalid_input"),
        ("search", ("vs_first", ""), {"user_id": USER, "limit": 1}, "openai_vector_invalid_input"),
        (
            "search",
            ("vs_first", "x" * 16_385),
            {"user_id": USER, "limit": 1},
            "openai_vector_invalid_input",
        ),
        (
            "search",
            ("vs_first", "test"),
            {"user_id": USER, "limit": True},
            "openai_vector_invalid_input",
        ),
        (
            "search",
            ("vs_first", "test"),
            {"user_id": USER, "limit": 0},
            "openai_vector_invalid_input",
        ),
        (
            "search",
            ("vs_first", "test"),
            {"user_id": USER, "limit": 51},
            "openai_vector_invalid_input",
        ),
        (
            "search",
            ("vs_first", "test"),
            {"user_id": str(USER), "limit": 1},
            "openai_vector_invalid_input",
        ),
    ],
)
async def test_invalid_inputs_never_make_requests(
    operation: str, args: tuple[Any, ...], kwargs: dict[str, Any], code: str
) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid input reached HTTP transport")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match=f"^{code}$"):
            await getattr(api(client), operation)(*args, **kwargs)


@pytest.mark.parametrize(
    "attributes",
    [
        {"user_id": str(USER), "nested": {"private": "text"}},
        {"user_id": str(USER), "empty": None},
        {"user_id": str(USER), "large": "x" * 513},
        {"user_id": str(USER), "x" * 65: True},
        {"user_id": str(USER), "": True},
        {"user_id": str(USER), "number": float("nan")},
        {"user_id": str(USER), "number": float("inf")},
        {"user_id": str(USER), "number": 10**400},
        {"user_id": str(USER), **{f"key{x}": True for x in range(16)}},
    ],
)
async def test_attach_rejects_unsupported_attribute_types_or_limits(
    attributes: dict[str, Any],
) -> None:
    async with client_with(status()) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_invalid_input$"):
            await api(client).attach("vs_first", "file-first", attributes)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timeout_seconds": 0},
        {"timeout_seconds": -1},
        {"timeout_seconds": 121},
        {"timeout_seconds": True},
        {"timeout_seconds": "30"},
        {"timeout_seconds": float("inf")},
        {"timeout_seconds": float("nan")},
        {"timeout_seconds": 10**400},
        {"max_content_bytes": 0},
        {"max_content_bytes": True},
        {"max_content_bytes": 8 * 1024 * 1024 + 1},
    ],
)
async def test_invalid_configuration_is_safely_rejected(kwargs: dict[str, Any]) -> None:
    async with client_with({}) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_invalid_configuration$"):
            api(client, **kwargs)


@pytest.mark.parametrize("key", ["", "a" * 513, "secret\nheader", "secret value", "中文", None])
async def test_invalid_api_key_is_not_echoed(key: Any) -> None:
    async with client_with({}) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_invalid_configuration$"):
            OpenAIVectorAdapter(client, key)


@pytest.mark.parametrize(
    "status_code,code,retryable",
    [
        (301, "openai_vector_redirect_forbidden", False),
        (302, "openai_vector_redirect_forbidden", False),
        (307, "openai_vector_redirect_forbidden", False),
        (308, "openai_vector_redirect_forbidden", False),
        (400, "openai_vector_http_error", False),
        (401, "openai_vector_auth_failed", False),
        (403, "openai_vector_auth_failed", False),
        (404, "openai_vector_http_error", False),
        (408, "openai_vector_http_error", True),
        (429, "openai_vector_rate_limited", True),
        (500, "openai_vector_service_failed", True),
        (503, "openai_vector_service_failed", True),
        (201, "openai_vector_http_error", False),
        (204, "openai_vector_http_error", False),
    ],
)
async def test_http_errors_are_sanitized_without_retry_or_following_redirects(
    status_code: int, code: str, retryable: bool, caplog: pytest.LogCaptureFixture
) -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            status_code,
            headers={"location": "https://untrusted.invalid/?private-query"},
            text="private upstream response user@example.invalid",
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle), follow_redirects=True
    ) as client:
        with pytest.raises(ArtifactError) as error:
            await api(client).create_store(USER)
    assert error.value.code == code
    assert error.value.retryable is retryable
    assert isinstance(error.value, VectorRequestRejected) is (
        400 <= status_code < 500 and status_code != 408
    )
    assert calls == 1
    assert "private" not in str(error.value)
    assert "private upstream" not in caplog.text
    assert "test-private-key" not in caplog.text


@pytest.mark.parametrize("exception", [httpx.ConnectError, httpx.ReadTimeout, OSError])
async def test_transport_error_has_no_raw_exception_chain(exception: type[Exception]) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise exception("private credential and URL query")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError) as error:
            await api(client).create_store(USER)
    assert error.value.code == "openai_vector_transport_failed"
    assert error.value.retryable is True
    assert "private credential" not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize(
    "body",
    [
        b"[]",
        b"null",
        b"{",
        b"\xff",
        b'{"object":"vector_store","object":"file"}',
        b'{"score":NaN}',
        b'{"score":Infinity}',
        b"[" * 1500 + b"]" * 1500,
    ],
)
async def test_malformed_json_is_rejected_with_safe_error(body: bytes) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"content-type": "application/json"}, content=body
            )
        )
    ) as client:
        with pytest.raises(ArtifactError, match=f"^{INVALID}$"):
            await api(client).create_store(USER)


async def test_success_status_with_error_payload_is_not_success() -> None:
    async with client_with({"error": {"message": "private error"}}) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_api_error$"):
            await api(client).create_store(USER)


class Stream(httpx.AsyncByteStream):
    def __init__(self, parts: list[bytes], *, wait: asyncio.Event | None = None) -> None:
        self.parts = parts
        self.wait = wait
        self.started = asyncio.Event()
        self.closed = False
        self.reads = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        self.started.set()
        if self.wait is not None:
            await self.wait.wait()
        for part in self.parts:
            self.reads += 1
            yield part

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.parametrize(
    "headers,parts,code",
    [
        ({"content-type": "text/html"}, [b"private"], INVALID),
        ({"content-type": "application/json", "content-encoding": "gzip"}, [], INVALID),
        ({"content-type": "application/json", "content-length": "-1"}, [], INVALID),
        ({"content-type": "application/json", "content-length": "1,1"}, [], INVALID),
        ({"content-type": "application/json", "content-length": "1"}, [b"{}"], INVALID),
        ({"content-type": "application/json", "content-length": "5"}, [b"{}"], INVALID),
        (
            {"content-type": "application/json", "content-length": "2097153"},
            [],
            "openai_vector_response_too_large",
        ),
        (
            {"content-type": "application/json"},
            [b" " * (64 * 1024)] * 33,
            "openai_vector_response_too_large",
        ),
    ],
)
async def test_header_and_stream_caps_close_response(
    headers: dict[str, str], parts: list[bytes], code: str
) -> None:
    stream = Stream(parts)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, headers=headers, stream=stream)
        )
    ) as client:
        with pytest.raises(ArtifactError, match=f"^{code}$"):
            await api(client).create_store(USER)
    assert stream.closed


async def test_error_response_is_closed_without_consuming_sensitive_body() -> None:
    stream = Stream([b"private upstream response"])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(503, stream=stream))
    ) as client:
        with pytest.raises(ArtifactError):
            await api(client).create_store(USER)
    assert stream.closed
    assert stream.reads == 0


async def test_deadline_bounds_waiting_response_and_closes_it() -> None:
    stream = Stream([], wait=asyncio.Event())
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"content-type": "application/json"}, stream=stream
            )
        )
    ) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_transport_failed$") as error:
            await api(client, timeout_seconds=0.02).create_store(USER)
    assert error.value.retryable is True
    assert stream.closed


async def test_cancellation_propagates_and_closes_response() -> None:
    stream = Stream([], wait=asyncio.Event())
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"content-type": "application/json"}, stream=stream
            )
        )
    ) as client:
        task = asyncio.create_task(api(client).create_store(USER))
        await asyncio.wait_for(stream.started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not client.is_closed
    assert stream.closed


async def test_scan_deadline_covers_all_pages_instead_of_restarting_each_page() -> None:
    stream = Stream([], wait=asyncio.Event())
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, json=page([store()], more=True))
        return httpx.Response(200, headers={"content-type": "application/json"}, stream=stream)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_transport_failed$") as error:
            await api(client, timeout_seconds=0.05).find_store(USER)
    assert error.value.retryable is True
    assert not isinstance(error.value, VectorRequestRejected)
    assert calls == 2
    assert stream.closed


async def test_invalid_unicode_request_is_rejected_before_transport_without_raw_error() -> None:
    private_query = "private-text\ud800"

    def handle(request: httpx.Request) -> httpx.Response:
        pytest.fail("malformed Unicode reached HTTP transport")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(VectorRequestRejected, match="^openai_vector_invalid_input$") as error:
            await api(client).search("vs_first", private_query, user_id=USER, limit=1)
    assert "private-text" not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize(
    "change",
    [{"purpose": "vision"}, {"filename": None}, {"filename": ""}, {"filename": "x" * 1025}],
)
async def test_malformed_file_listing_is_never_reported_as_absent(change: dict[str, Any]) -> None:
    async with client_with(page([{**file(), **change}])) as client:
        with pytest.raises(ArtifactError, match=f"^{INVALID}$"):
            await api(client).find_file(NAME)


async def test_duplicate_store_on_later_page_is_not_silently_ignored() -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=page([store(f"vs_{calls}")], more=calls == 1))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_ambiguous_match$"):
            await api(client).find_store(USER)
    assert calls == 2


async def test_search_text_limit_is_checked_after_joining_chunks() -> None:
    payload = search_result()
    payload["data"][0]["content"] = [
        {"type": "text", "text": "a" * 500_000},
        {"type": "text", "text": "b" * 500_000},
    ]
    async with client_with(payload) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_response_too_large$"):
            await api(client).search("vs_first", "证据在哪里？", user_id=USER, limit=1)


async def test_valid_search_page_can_have_more_top_k_hits_than_requested() -> None:
    payload = search_result()
    payload.update(has_more=True, next_page="opaque-next-token")
    async with client_with(payload) as client:
        results = await api(client).search("vs_first", "证据在哪里？", user_id=USER, limit=1)
    assert len(results) == 1


async def test_one_extra_upload_byte_is_rejected_before_transport() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        pytest.fail("over-limit upload reached HTTP transport")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match="^openai_vector_content_too_large$"):
            await api(client, max_content_bytes=len(CONTENT) - 1).upload(NAME, CONTENT)
