import asyncio
import copy
import json
import traceback
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest

from app.adapters.openai_responses import OpenAIResponsesAdapter
from app.domain.agent import ModelTurn, ResponsesProvider
from app.domain.artifacts import ArtifactError

INVALID = "openai_responses_invalid_response"
INPUT = "openai_responses_invalid_input"
KEY = "test-responses-private-key"
LIMIT = 256 * 1024


def schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"],
        "additionalProperties": False,
    }


def tool() -> dict[str, Any]:
    return {
        "type": "function",
        "name": "search_knowledge",
        "description": "Find authorized evidence.",
        "strict": True,
        "parameters": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": False,
        },
    }


def inputs() -> tuple[dict[str, Any], ...]:
    return ({"role": "user", "content": "Find the supplied evidence."},)


def call() -> dict[str, Any]:
    return {
        "type": "function_call",
        "id": "fc_first",
        "call_id": "call_first",
        "name": "search_knowledge",
        "arguments": '{"query":"evidence"}',
        "status": "completed",
    }


def reasoning() -> dict[str, Any]:
    return {
        "type": "reasoning",
        "id": "rs_first",
        "summary": [],
        "encrypted_content": "opaque-encrypted-reasoning==",
    }


def message(*, refused: bool = False) -> dict[str, Any]:
    return {
        "type": "message",
        "id": "msg_first",
        "status": "completed",
        "role": "assistant",
        "content": [{"type": "refusal", "refusal": "Cannot answer this request."}]
        if refused
        else [
            {
                "type": "output_text",
                "text": '{"answer":"evidence"}',
                "annotations": [],
                "logprobs": [],
            }
        ],
    }


def response(*items: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": "resp_first",
        "object": "response",
        "status": "completed",
        "error": None,
        "incomplete_details": None,
        "output": list(items) if items else [message()],
    }


def api(client: httpx.AsyncClient, **kwargs: Any) -> OpenAIResponsesAdapter:
    return OpenAIResponsesAdapter(client, KEY, model="configured-model", **kwargs)


async def ask(adapter: ResponsesProvider, **kwargs: Any) -> ModelTurn:
    arguments = {
        "instructions": "Use only the provided evidence.",
        "input_items": inputs(),
        "tools": (tool(),),
        "output_schema": schema(),
    }
    arguments.update(kwargs)
    return await adapter.respond(**arguments)


def client_with(payload: object, *, status_code: int = 200) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status_code, json=payload))
    )


async def test_request_is_explicit_stateless_strict_and_without_client_contamination() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "POST"
        assert str(request.url) == "https://api.openai.com/v1/responses"
        assert request.headers["Authorization"] == f"Bearer {KEY}"
        assert request.headers["Content-Type"] == "application/json"
        assert request.headers["Accept-Encoding"] == "identity"
        assert "cookie" not in request.headers
        assert "x-private" not in request.headers
        assert "idempotency-key" not in request.headers
        assert request.extensions["timeout"] == {"connect": 10, "read": 30, "write": 30, "pool": 30}
        assert json.loads(request.content) == {
            "model": "configured-model",
            "instructions": "Use only the provided evidence.",
            "input": list(inputs()),
            "tools": [tool()],
            "tool_choice": "auto",
            "store": False,
            "parallel_tool_calls": False,
            "include": ["reasoning.encrypted_content"],
            "max_output_tokens": 2000,
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "agent_answer",
                    "strict": True,
                    "schema": schema(),
                }
            },
        }
        return httpx.Response(200, json=response())

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle),
        base_url="https://untrusted.invalid/",
        headers={"X-Private": "secret", "Authorization": "bad"},
        cookies={"private": "cookie"},
        params={"private": "query"},
        auth=("default-user", "default-password"),
        follow_redirects=True,
    ) as client:
        adapter: ResponsesProvider = api(client)
        result = await ask(adapter)
        assert result.text == '{"answer":"evidence"}'
        assert result.calls == () and not result.refused
        assert result.items == (message(),)
        assert not client.is_closed
        assert KEY not in repr(adapter) + repr(vars(adapter))
        assert "evidence" not in repr(result)
    assert len(requests) == 1


async def test_reasoning_and_function_call_round_trip_without_remote_response_id() -> None:
    requests: list[dict[str, Any]] = []
    first = response(reasoning(), call())

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200, json=first if len(requests) == 1 else response())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        adapter = api(client)
        turn = await ask(adapter)
        assert turn.items == (reasoning(), call())
        assert turn.text is None and not turn.refused
        assert len(turn.calls) == 1
        assert turn.calls[0].call_id == "call_first"
        assert turn.calls[0].name == "search_knowledge"
        assert turn.calls[0].arguments == {"query": "evidence"}
        assert "evidence" not in repr(turn.calls[0])
        next_inputs = (
            *inputs(),
            *turn.items,
            {
                "type": "function_call_output",
                "call_id": "call_first",
                "output": '{"evidence":["verified"]}',
            },
        )
        await ask(adapter, input_items=next_inputs, tool_choice="none")
    assert requests[1]["input"] == list(next_inputs)
    assert requests[1]["tool_choice"] == "none"
    assert all(
        "previous_response_id" not in body and "conversation" not in body for body in requests
    )


async def test_refusal_is_not_treated_as_final_answer_text() -> None:
    async with client_with(response(reasoning(), message(refused=True))) as client:
        result = await ask(api(client))
        assert result.refused and result.text is None and result.calls == ()
        assert result.items == (reasoning(), message(refused=True))


async def test_reasoning_summary_and_content_are_preserved() -> None:
    item = reasoning()
    item.update(
        status="completed",
        summary=[{"type": "summary_text", "text": "Summary"}],
        content=[{"type": "reasoning_text", "text": "Reasoning content"}],
    )
    async with client_with(response(item, call())) as client:
        assert (await ask(api(client))).items[0] == item


async def test_explicit_commentary_is_replayed_without_becoming_a_final_answer() -> None:
    commentary = message()
    commentary.update(phase="commentary", id="msg_commentary")
    async with client_with(response(reasoning(), commentary, call())) as client:
        turn = await ask(api(client))
        assert turn.items == (reasoning(), commentary, call())
        assert turn.text is None and not turn.refused and len(turn.calls) == 1
    final = message()
    final["phase"] = "final_answer"
    async with client_with(response(commentary, final)) as client:
        turn = await ask(api(client))
        assert turn.items == (commentary, final)
        assert turn.text == '{"answer":"evidence"}'


@pytest.mark.parametrize(
    "kwargs",
    [
        {"api_key": ""},
        {"api_key": "bad\nkey"},
        {"api_key": "密钥"},
        {"api_key": "k" * 513},
        {"api_key": None},
        {"model": ""},
        {"model": "bad/model"},
        {"model": None},
        {"timeout_seconds": 0},
        {"timeout_seconds": 121},
        {"timeout_seconds": True},
        {"timeout_seconds": float("nan")},
        {"timeout_seconds": float("inf")},
        {"timeout_seconds": "30"},
        {"max_output_tokens": 0},
        {"max_output_tokens": 16_385},
        {"max_output_tokens": True},
        {"max_output_tokens": 2.0},
    ],
)
async def test_configuration_is_bounded_and_model_has_no_default(kwargs: dict[str, Any]) -> None:
    async with client_with(response()) as client:
        args: dict[str, Any] = {"api_key": KEY, "model": "configured-model"}
        args.update(kwargs)
        with pytest.raises(ArtifactError, match="^openai_responses_invalid_configuration$"):
            OpenAIResponsesAdapter(client, **args)
        with pytest.raises(TypeError):
            OpenAIResponsesAdapter(client, KEY)  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "mutate,code",
    [
        (lambda p: p.update(object="unknown"), INVALID),
        (lambda p: p.pop("id"), INVALID),
        (lambda p: p.update(id="../secret"), INVALID),
        (lambda p: p.update(status="incomplete"), "openai_responses_incomplete_response"),
        (
            lambda p: p.update(incomplete_details={"reason": "max_output_tokens"}),
            "openai_responses_incomplete_response",
        ),
        (lambda p: p.update(status="in_progress"), INVALID),
        (lambda p: p.update(error={"message": KEY}), "openai_responses_api_error"),
        (lambda p: p.update(output=[]), INVALID),
        (lambda p: p.update(output=[None]), INVALID),
        (lambda p: p.update(output=[reasoning()]), INVALID),
        (lambda p: p.update(output=[{**message(), "phase": "commentary"}]), INVALID),
        (lambda p: p.update(output=[message(), call()]), INVALID),
        (lambda p: p.update(output=[call(), message()]), INVALID),
        (lambda p: p.update(output=[message(), message()]), INVALID),
        (
            lambda p: p.update(
                output=[call(), {**call(), "id": "fc_second", "call_id": "call_second"}]
            ),
            INVALID,
        ),
        (lambda p: p.update(output=[{"type": "web_search_call"}]), INVALID),
        (lambda p: p.update(output=[{"type": "custom_tool_call"}]), INVALID),
        (lambda p: p["output"][0].update(role="user"), INVALID),
        (lambda p: p["output"][0].update(status="in_progress"), INVALID),
        (lambda p: p["output"][0].update(phase="unknown"), INVALID),
        (lambda p: p["output"][0].update(content=[]), INVALID),
        (
            lambda p: p["output"][0]["content"].append({"type": "refusal", "refusal": "refused"}),
            INVALID,
        ),
        (lambda p: p["output"][0]["content"][0].update(text=""), INVALID),
        (lambda p: p["output"][0]["content"][0].update(text="x" * 65_537), INVALID),
        (lambda p: p["output"][0]["content"][0].update(type="input_text"), INVALID),
        (
            lambda p: p["output"][0]["content"][0].update(annotations=[{"type": "url_citation"}]),
            INVALID,
        ),
    ],
)
async def test_malformed_or_ambiguous_turn_is_rejected(
    mutate: Callable[[dict[str, Any]], object],
    code: str,
) -> None:
    payload = response()
    mutate(payload)
    async with client_with(payload) as client:
        with pytest.raises(ArtifactError, match=f"^{code}$"):
            await ask(api(client))


@pytest.mark.parametrize(
    "changes",
    [
        {"call_id": ""},
        {"call_id": "call/escape"},
        {"name": "unknown_tool"},
        {"name": "bad name"},
        {"arguments": {}},
        {"arguments": "[]"},
        {"arguments": '{"query":1,"query":2}'},
        {"arguments": '{"query":NaN}'},
        {"arguments": '{"query":Infinity}'},
        {"arguments": '{"query":1e999}'},
        {"arguments": "not-json"},
        {"arguments": "{"},
        {"arguments": "{}" * 9000},
        {"arguments": '{"x":"\\ud800"}'},
        {"status": "incomplete"},
        {"type": "computer_call"},
        {"namespace": "external"},
    ],
)
async def test_function_call_contract_is_fail_closed(changes: dict[str, Any]) -> None:
    item = call()
    item.update(changes)
    async with client_with(response(item)) as client:
        with pytest.raises(ArtifactError, match=f"^{INVALID}$"):
            await ask(api(client))


async def test_none_tool_choice_and_reused_call_ids_cannot_invoke_tools() -> None:
    history = (
        *inputs(),
        call(),
        {"type": "function_call_output", "call_id": "call_first", "output": "{}"},
    )
    async with client_with(response(call())) as client:
        for kwargs in ({"tool_choice": "none"}, {"input_items": history}, {"tools": ()}):
            with pytest.raises(ArtifactError, match=f"^{INVALID}$"):
                await ask(api(client), **kwargs)


@pytest.mark.parametrize(
    "changes",
    [
        {"encrypted_content": None},
        {"encrypted_content": ""},
        {"encrypted_content": "bad\nvalue"},
        {"encrypted_content": "x" * (128 * 1024 + 1)},
        {"summary": None},
        {"summary": [{"type": "output_text", "text": "unexpected"}]},
        {"content": [{"type": "reasoning_text", "text": "text", "hidden": "unexpected"}]},
        {"status": "in_progress"},
        {"id": "../reasoning"},
    ],
)
async def test_reasoning_must_be_replayable_and_bounded(changes: dict[str, Any]) -> None:
    item = reasoning()
    item.update(changes)
    async with client_with(response(item, call())) as client:
        with pytest.raises(ArtifactError, match=f"^{INVALID}$"):
            await ask(api(client))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s.update(type="array"),
        lambda s: s.pop("additionalProperties"),
        lambda s: s.update(additionalProperties=True),
        lambda s: s.update(required=[]),
        lambda s: s.update(required=["answer", "answer"]),
        lambda s: s.update(required=["missing"]),
        lambda s: s.update(anyOf=[schema()]),
        lambda s: s.update(required=[None]),
        lambda s: s["properties"].update(answer={"type": "object", "properties": {}}),
        lambda s: s["properties"].update(answer={"type": "array", "items": {"type": "object"}}),
        lambda s: s["properties"].update(answer={"$ref": "https://untrusted.invalid/schema"}),
        lambda s: s["properties"].update(answer={"$ref": "#/$defs/missing"}),
        lambda s: s["properties"].update(answer={"type": []}),
        lambda s: s["properties"].update(answer={"type": ["string", "string"]}),
        lambda s: s["properties"].update(answer={"type": "string", "maxLength": float("nan")}),
        lambda s: s.update(unknown="private"),
    ],
)
async def test_bad_schema_is_rejected_before_http(
    mutate: Callable[[dict[str, Any]], object],
) -> None:
    outgoing = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal outgoing
        outgoing += 1
        return httpx.Response(200, json=response())

    bad = schema()
    mutate(bad)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match=f"^{INPUT}$"):
            await ask(api(client), output_schema=bad)
    assert outgoing == 0


async def test_strict_nested_nullable_and_local_ref_schemas_are_supported() -> None:
    nested = schema()
    nested["$defs"] = {"answer": schema()}
    nested["properties"]["answer"] = {
        "anyOf": [
            {"type": "null"},
            {"type": "array", "items": {"$ref": "#/$defs/answer"}, "maxItems": 5},
        ]
    }
    async with client_with(response()) as client:
        await ask(api(client), output_schema=nested)


@pytest.mark.parametrize(
    "changes",
    [
        {"strict": False},
        {"type": "web_search"},
        {"name": ""},
        {"name": "bad tool"},
        {"parameters": {"type": "object"}},
        {"parameters": None},
        {"description": ""},
        {"defer_loading": True},
    ],
)
async def test_only_explicit_strict_function_tools_are_allowed(changes: dict[str, Any]) -> None:
    item = tool()
    item.update(changes)
    async with client_with(response()) as client:
        with pytest.raises(ArtifactError, match=f"^{INPUT}$"):
            await ask(api(client), tools=(item,))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"instructions": ""},
        {"instructions": "x" * 65_537},
        {"instructions": "\ud800"},
        {"tool_choice": "required"},
        {"input_items": ()},
        {"input_items": []},
        {"tools": []},
        {"tools": (tool(), tool())},
        {"input_items": ({"role": "system", "content": "bad"},)},
        {"input_items": ({"role": "user", "content": [{"type": "input_image"}]},)},
        {"input_items": ({"role": "user", "content": "text", "user_id": "tenant"},)},
        {"input_items": (call(),)},
        {"input_items": ({"type": "function_call_output", "call_id": "unknown", "output": "{}"},)},
        {
            "input_items": (
                call(),
                {"type": "function_call_output", "call_id": "wrong", "output": "{}"},
            )
        },
    ],
)
async def test_invalid_input_is_rejected_before_http(kwargs: dict[str, Any]) -> None:
    sent = False

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal sent
        sent = True
        return httpx.Response(200, json=response())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match=f"^{INPUT}$"):
            await ask(api(client), **kwargs)
    assert not sent


async def test_request_byte_limit_is_measured_after_utf8_encoding() -> None:
    sent = False

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal sent
        sent = True
        return httpx.Response(200, json=response())

    large = ({"role": "user", "content": "证" * 50_000},) * 2
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match="^openai_responses_request_too_large$"):
            await ask(api(client), input_items=large)
    assert not sent


@pytest.mark.parametrize(
    "body",
    [
        b'{"status":"completed","status":"incomplete"}',
        b'{"nested":{"a":1,"a":2}}',
        b'{"value":NaN}',
        b'{"value":Infinity}',
        b'{"value":1e999}',
        b"[]",
        b"null",
        b"{",
        b"\xff",
        b'{"value":"\\ud800"}',
        b'{"value":' + b"[" * 40 + b"0" + b"]" * 40 + b"}",
    ],
)
async def test_non_strict_json_response_is_rejected(body: bytes) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, content=body, headers={"Content-Type": "application/json"}
            )
        )
    ) as client:
        with pytest.raises(ArtifactError, match=f"^{INVALID}$"):
            await ask(api(client))


@pytest.mark.parametrize(
    "status,code,retryable",
    [
        (301, "redirect_forbidden", False),
        (307, "redirect_forbidden", False),
        (401, "auth_failed", False),
        (403, "auth_failed", False),
        (429, "rate_limited", True),
        (500, "service_failed", True),
        (503, "service_failed", True),
        (408, "http_error", True),
        (400, "http_error", False),
        (404, "http_error", False),
        (201, "http_error", False),
        (204, "http_error", False),
    ],
)
async def test_http_failures_are_safe_and_never_retried(
    status: int,
    code: str,
    retryable: bool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            status, content=KEY, headers={"Location": "https://untrusted.invalid"}
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle), follow_redirects=True
    ) as client:
        with pytest.raises(ArtifactError, match=f"^openai_responses_{code}$") as caught:
            await ask(api(client))
    assert caught.value.retryable is retryable
    assert len(requests) == 1
    assert KEY not in "".join(traceback.format_exception(caught.value)) + caplog.text


class Body(httpx.AsyncByteStream):
    def __init__(self, parts: list[bytes], *, delay: float = 0) -> None:
        self.parts = parts
        self.delay = delay
        self.started = asyncio.Event()
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        self.started.set()
        for part in self.parts:
            await asyncio.sleep(self.delay)
            yield part

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.parametrize(
    "headers,parts,code",
    [
        ({"Content-Length": str(LIMIT + 1)}, [b"{}"], "openai_responses_response_too_large"),
        ({}, [b"x" * (LIMIT // 2)] * 3, "openai_responses_response_too_large"),
        ({"Content-Length": "999"}, [b"{}"], INVALID),
        ({"Content-Length": "-1"}, [b"{}"], INVALID),
        ({"Content-Length": "1.5"}, [b"{}"], INVALID),
        ({"Content-Type": "text/html"}, [b"{}"], INVALID),
        ({"Content-Encoding": "gzip"}, [], INVALID),
    ],
)
async def test_response_headers_and_stream_size_are_bounded_and_closed(
    headers: dict[str, str],
    parts: list[bytes],
    code: str,
) -> None:
    stream = Body(parts)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, stream=stream, headers={"Content-Type": "application/json", **headers}
            )
        )
    ) as client:
        with pytest.raises(ArtifactError, match=f"^{code}$"):
            await ask(api(client))
    assert stream.closed


@pytest.mark.parametrize("failure", [httpx.ConnectError, httpx.ReadTimeout, OSError])
async def test_transport_errors_are_retryable_without_leaking_raw_exception(
    failure: type[Exception],
    caplog: pytest.LogCaptureFixture,
) -> None:
    count = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal count
        count += 1
        raise failure(f"private raw failure {KEY}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match="^openai_responses_transport_failed$") as caught:
            await ask(api(client))
    assert caught.value.retryable and count == 1
    assert KEY not in "".join(traceback.format_exception(caught.value)) + caplog.text


async def test_total_budget_covers_send_and_all_stream_reads() -> None:
    stream = Body([b" "] * 10, delay=0.03)

    async def handle(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.03)
        return httpx.Response(200, stream=stream, headers={"Content-Type": "application/json"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match="^openai_responses_transport_failed$") as caught:
            await ask(api(client, timeout_seconds=0.2))
    assert caught.value.retryable and stream.started.is_set() and stream.closed


async def test_cancellation_propagates_and_closes_stream_without_retry() -> None:
    stream = Body([b"{}"], delay=60)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, stream=stream, headers={"Content-Type": "application/json"}
            )
        )
    ) as client:
        task = asyncio.create_task(ask(api(client)))
        await asyncio.wait_for(stream.started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed and not client.is_closed


async def test_slow_close_cannot_extend_http_budget() -> None:
    class SlowClose(Body):
        async def aclose(self) -> None:
            self.closed = True
            await asyncio.sleep(60)

    stream = SlowClose([json.dumps(response()).encode()])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, stream=stream, headers={"Content-Type": "application/json"}
            )
        )
    ) as client:
        with pytest.raises(ArtifactError, match="^openai_responses_transport_failed$"):
            await asyncio.wait_for(ask(api(client, timeout_seconds=0.2)), timeout=1)
    assert stream.closed


async def test_cancellation_is_preserved_even_when_close_fails() -> None:
    class BadClose(Body):
        async def aclose(self) -> None:
            self.closed = True
            raise OSError(KEY)

    stream = BadClose([b"{}"], delay=60)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, stream=stream, headers={"Content-Type": "application/json"}
            )
        )
    ) as client:
        task = asyncio.create_task(ask(api(client)))
        await asyncio.wait_for(stream.started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
    assert stream.closed


async def test_caller_input_is_not_mutated_and_custom_output_token_budget_is_sent() -> None:
    history = (
        {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "Evidence?"}],
        },
    )
    original = copy.deepcopy(history)
    requests: list[dict[str, Any]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200, json=response())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        await ask(api(client, max_output_tokens=800), input_items=history)
    assert history == original
    assert requests[0]["max_output_tokens"] == 800


async def test_cyclic_and_excessively_wide_inputs_fail_with_safe_error() -> None:
    cyclic: dict[str, Any] = schema()
    cyclic["properties"]["answer"] = cyclic
    wide = schema()
    wide["properties"] = {str(index): {"type": "string"} for index in range(1025)}
    async with client_with(response()) as client:
        for bad in (cyclic, wide):
            with pytest.raises(ArtifactError, match=f"^{INPUT}$"):
                await ask(api(client), output_schema=bad)
