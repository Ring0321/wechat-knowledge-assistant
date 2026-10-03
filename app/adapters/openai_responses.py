"""Bounded, stateless Responses transport; the caller owns tools, retries and evidence."""

import asyncio
import json
import math
import re
from typing import Literal, cast

import httpx
from pydantic import JsonValue, SecretStr

from app.domain.agent import ModelTurn, ToolCall
from app.domain.artifacts import ArtifactError

_ENDPOINT = "https://api.openai.com/v1/responses"
_BYTES = 256 * 1024
_INVALID = "openai_responses_invalid_response"
_INPUT = "openai_responses_invalid_input"
_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,200}\Z")
_NAME = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
_MODEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


def _json_tree(value: object, *, code: str, size_code: str | None = None) -> None:
    """Bound both serialization work and nesting, including caller-owned Python values."""
    pending: list[tuple[object, int]] = [(value, 0)]
    nodes = 0
    string_bytes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > 20_000 or depth > 32:
            raise ArtifactError(code)
        if type(item) is dict:
            if len(item) > 1024:
                raise ArtifactError(code)
            for key, child in item.items():
                if type(key) is not str or len(key) > 1024:
                    raise ArtifactError(code)
                pending.append((key, depth + 1))
                pending.append((child, depth + 1))
        elif type(item) is list:
            if len(item) > 4096:
                raise ArtifactError(code)
            pending.extend((child, depth + 1) for child in item)
        elif type(item) is str:
            if len(item) > _BYTES:
                raise ArtifactError(size_code or code)
            try:
                string_bytes += len(item.encode("utf-8"))
            except UnicodeError:
                raise ArtifactError(code) from None
            if string_bytes > _BYTES:
                raise ArtifactError(size_code or code)
        elif type(item) is float:
            if not math.isfinite(item):
                raise ArtifactError(code)
        elif type(item) is int:
            if item.bit_length() > 1024:
                raise ArtifactError(code)
        elif item is not None and type(item) is not bool:
            raise ArtifactError(code)


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_key")
        result[key] = value
    return result


def _constant(value: str) -> None:
    raise ValueError("invalid_constant")


def _json_object(value: str | bytes | bytearray, *, code: str) -> dict[str, JsonValue]:
    try:
        result: object = json.loads(value, object_pairs_hook=_pairs, parse_constant=_constant)
    except (ValueError, UnicodeError, RecursionError):
        raise ArtifactError(code) from None
    _json_tree(result, code=code)
    if not isinstance(result, dict):
        raise ArtifactError(code)
    return cast(dict[str, JsonValue], result)


def _identifier(value: object, *, code: str, name: bool = False) -> str:
    if not isinstance(value, str) or not (_NAME if name else _IDENTIFIER).fullmatch(value):
        raise ArtifactError(code)
    return value


def _text(value: object, *, code: str, limit: int = 65_536) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ArtifactError(code)
    return value


def _keys(item: dict[str, JsonValue], allowed: set[str], *, code: str) -> None:
    if item.keys() - allowed:
        raise ArtifactError(code)


def _schema(value: object, *, root: bool = True, definitions: set[str] | None = None) -> None:
    """Validate the strict JSON Schema subset used by this text/function boundary."""
    if not isinstance(value, dict):
        raise ArtifactError(_INPUT)
    if definitions is None:
        raw_definitions = value.get("$defs", {})
        if not isinstance(raw_definitions, dict):
            raise ArtifactError(_INPUT)
        definitions = set(raw_definitions)
    allowed = {
        "type",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "enum",
        "anyOf",
        "description",
        "title",
        "$defs",
        "$ref",
        "minLength",
        "maxLength",
        "pattern",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minItems",
        "maxItems",
        "format",
    }
    if value.keys() - allowed or (root and value.get("type") != "object"):
        raise ArtifactError(_INPUT)
    for key in ("description", "title", "pattern", "format"):
        if key in value and not isinstance(value[key], str):
            raise ArtifactError(_INPUT)
    if "$ref" in value:
        ref = value["$ref"]
        if (
            root
            or not isinstance(ref, str)
            or not ref.startswith("#/$defs/")
            or ref.removeprefix("#/$defs/") not in definitions
            or value.keys() - {"$ref", "description", "title"}
        ):
            raise ArtifactError(_INPUT)
        return
    kinds = value.get("type")
    kinds = kinds if isinstance(kinds, list) else [kinds]
    if kinds == [None] and "anyOf" in value:
        kinds = []
    if any(
        kind not in ("object", "array", "string", "number", "integer", "boolean", "null")
        for kind in kinds
    ) or len(kinds) != len(set(kinds)):
        raise ArtifactError(_INPUT)
    if not kinds and "anyOf" not in value:
        raise ArtifactError(_INPUT)
    if "object" in kinds:
        properties, required = value.get("properties"), value.get("required")
        if (
            not isinstance(properties, dict)
            or not isinstance(required, list)
            or any(not isinstance(key, str) for key in required)
            or len(required) != len(set(required))
            or set(required) != set(properties)
            or value.get("additionalProperties") is not False
        ):
            raise ArtifactError(_INPUT)
        for child in properties.values():
            _schema(child, root=False, definitions=definitions)
    elif any(key in value for key in ("properties", "required", "additionalProperties")):
        raise ArtifactError(_INPUT)
    if "array" in kinds:
        _schema(value.get("items"), root=False, definitions=definitions)
    elif "items" in value:
        raise ArtifactError(_INPUT)
    if "anyOf" in value:
        branches = value["anyOf"]
        if root or not isinstance(branches, list) or not 1 <= len(branches) <= 16:
            raise ArtifactError(_INPUT)
        for child in branches:
            _schema(child, root=False, definitions=definitions)
    if "enum" in value and (not isinstance(value["enum"], list) or not value["enum"]):
        raise ArtifactError(_INPUT)
    for key in ("minLength", "maxLength", "minItems", "maxItems"):
        if key in value and (type(value[key]) is not int or value[key] < 0):
            raise ArtifactError(_INPUT)
    for key in ("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"):
        if key in value and type(value[key]) not in (int, float):
            raise ArtifactError(_INPUT)
    if "$defs" in value:
        if not root or not isinstance(value["$defs"], dict):
            raise ArtifactError(_INPUT)
        for child in value["$defs"].values():
            _schema(child, root=False, definitions=definitions)


def _tools(value: tuple[dict[str, JsonValue], ...]) -> set[str]:
    if not isinstance(value, tuple) or len(value) > 32:
        raise ArtifactError(_INPUT)
    names: set[str] = set()
    for item in value:
        if not isinstance(item, dict):
            raise ArtifactError(_INPUT)
        _keys(item, {"type", "name", "description", "parameters", "strict"}, code=_INPUT)
        name = _identifier(item.get("name"), code=_INPUT, name=True)
        if item.get("type") != "function" or item.get("strict") is not True or name in names:
            raise ArtifactError(_INPUT)
        if "description" in item:
            _text(item["description"], code=_INPUT, limit=8192)
        _schema(item.get("parameters"))
        names.add(name)
    return names


def _call(item: dict[str, JsonValue], names: set[str], *, code: str) -> ToolCall:
    _keys(item, {"type", "id", "call_id", "name", "arguments", "status"}, code=code)
    if "id" in item:
        _identifier(item["id"], code=code)
    if item.get("status", "completed") != "completed":
        raise ArtifactError(code)
    call_id = _identifier(item.get("call_id"), code=code)
    name = _identifier(item.get("name"), code=code, name=True)
    if name not in names:
        raise ArtifactError(code)
    arguments = _json_object(_text(item.get("arguments"), code=code, limit=16_384), code=code)
    return ToolCall(call_id=call_id, name=name, arguments=arguments)


def _reasoning(item: dict[str, JsonValue], *, code: str) -> None:
    _keys(item, {"type", "id", "summary", "content", "encrypted_content", "status"}, code=code)
    _identifier(item.get("id"), code=code)
    encrypted = _text(item.get("encrypted_content"), code=code, limit=128 * 1024)
    if any(not 33 <= ord(char) <= 126 for char in encrypted):
        raise ArtifactError(code)
    if item.get("status", "completed") != "completed":
        raise ArtifactError(code)
    for key, kind in (("summary", "summary_text"), ("content", "reasoning_text")):
        parts = item.get(key, [] if key == "content" else None)
        if not isinstance(parts, list) or len(parts) > 16:
            raise ArtifactError(code)
        for part in parts:
            if not isinstance(part, dict) or set(part) != {"type", "text"}:
                raise ArtifactError(code)
            if part.get("type") != kind:
                raise ArtifactError(code)
            _text(part.get("text"), code=code)


def _message(item: dict[str, JsonValue], *, code: str) -> tuple[str | None, bool]:
    _keys(item, {"type", "id", "role", "status", "content", "phase"}, code=code)
    _identifier(item.get("id"), code=code)
    if item.get("role") != "assistant" or item.get("status") != "completed":
        raise ArtifactError(code)
    if "phase" in item and item["phase"] not in (None, "commentary", "final_answer"):
        raise ArtifactError(code)
    parts = item.get("content")
    if not isinstance(parts, list) or len(parts) != 1 or not isinstance(parts[0], dict):
        raise ArtifactError(code)
    part = parts[0]
    if part.get("type") == "refusal":
        _keys(part, {"type", "refusal"}, code=code)
        _text(part.get("refusal"), code=code)
        return None, True
    _keys(part, {"type", "text", "annotations", "logprobs"}, code=code)
    if part.get("type") != "output_text" or part.get("annotations", []) != []:
        raise ArtifactError(code)
    if part.get("logprobs", []) not in (None, []):
        raise ArtifactError(code)
    return _text(part.get("text"), code=code), False


def _inputs(items: tuple[dict[str, JsonValue], ...], names: set[str]) -> set[str]:
    if not isinstance(items, tuple) or not 1 <= len(items) <= 128:
        raise ArtifactError(_INPUT)
    seen: set[str] = set()
    pending: str | None = None
    for item in items:
        if not isinstance(item, dict):
            raise ArtifactError(_INPUT)
        kind = item.get("type", "message")
        if kind == "reasoning":
            _reasoning(item, code=_INPUT)
        elif kind == "function_call":
            call = _call(item, names, code=_INPUT)
            if pending is not None or call.call_id in seen:
                raise ArtifactError(_INPUT)
            seen.add(call.call_id)
            pending = call.call_id
        elif kind == "function_call_output":
            _keys(item, {"type", "call_id", "output"}, code=_INPUT)
            if pending is None or item.get("call_id") != pending:
                raise ArtifactError(_INPUT)
            _text(item.get("output"), code=_INPUT)
            pending = None
        elif kind == "message" and item.get("role") == "assistant":
            _message(item, code=_INPUT)
        elif kind == "message" and item.get("role") == "user":
            _keys(item, {"type", "role", "content"}, code=_INPUT)
            content = item.get("content")
            if isinstance(content, str):
                _text(content, code=_INPUT)
            elif isinstance(content, list) and 1 <= len(content) <= 16:
                for part in content:
                    if (
                        not isinstance(part, dict)
                        or set(part) != {"type", "text"}
                        or part.get("type") != "input_text"
                    ):
                        raise ArtifactError(_INPUT)
                    _text(part.get("text"), code=_INPUT)
            else:
                raise ArtifactError(_INPUT)
        else:
            raise ArtifactError(_INPUT)
    if pending is not None:
        raise ArtifactError(_INPUT)
    return seen


def _turn(payload: dict[str, JsonValue], names: set[str], seen: set[str]) -> ModelTurn:
    if payload.get("error") is not None:
        raise ArtifactError("openai_responses_api_error")
    if payload.get("status") == "incomplete" or payload.get("incomplete_details") is not None:
        raise ArtifactError("openai_responses_incomplete_response")
    if payload.get("object") != "response" or payload.get("status") != "completed":
        raise ArtifactError(_INVALID)
    _identifier(payload.get("id"), code=_INVALID)
    output = payload.get("output")
    if not isinstance(output, list) or not 1 <= len(output) <= 32:
        raise ArtifactError(_INVALID)
    items: list[dict[str, JsonValue]] = []
    calls: list[ToolCall] = []
    message_seen = False
    text: str | None = None
    refused = False
    ids: set[str] = set()
    for item in output:
        if not isinstance(item, dict):
            raise ArtifactError(_INVALID)
        item_id = item.get("id")
        if item_id is not None:
            identifier = _identifier(item_id, code=_INVALID)
            if identifier in ids:
                raise ArtifactError(_INVALID)
            ids.add(identifier)
        if item.get("type") == "reasoning":
            _reasoning(item, code=_INVALID)
        elif item.get("type") == "function_call":
            call = _call(item, names, code=_INVALID)
            if calls or message_seen or call.call_id in seen:
                raise ArtifactError(_INVALID)
            calls.append(call)
        elif item.get("type") == "message":
            item_text, item_refused = _message(item, code=_INVALID)
            if item.get("phase") == "commentary":
                if item_refused or message_seen:
                    raise ArtifactError(_INVALID)
            elif calls or message_seen:
                raise ArtifactError(_INVALID)
            else:
                text, refused = item_text, item_refused
                message_seen = True
        else:
            raise ArtifactError(_INVALID)
        items.append(item)
    if not calls and not message_seen:
        raise ArtifactError(_INVALID)
    return ModelTurn(items=tuple(items), calls=tuple(calls), text=text, refused=refused)


class OpenAIResponsesAdapter:
    def __init__(
        self,
        http: httpx.AsyncClient,
        api_key: str,
        *,
        model: str,
        timeout_seconds: float = 30,
        max_output_tokens: int = 2000,
    ) -> None:
        if (
            not isinstance(api_key, str)
            or not 1 <= len(api_key) <= 512
            or any(not 33 <= ord(char) <= 126 for char in api_key)
            or not isinstance(model, str)
            or not _MODEL.fullmatch(model)
            or type(timeout_seconds) not in (int, float)
            or not 0 < timeout_seconds <= 120
            or type(max_output_tokens) is not int
            or not 1 <= max_output_tokens <= 16_384
        ):
            raise ArtifactError("openai_responses_invalid_configuration")
        self._http = http
        self._api_key = SecretStr(api_key)
        self._model = model
        self._timeout = float(timeout_seconds)
        self._max_output_tokens = max_output_tokens

    async def respond(
        self,
        *,
        instructions: str,
        input_items: tuple[dict[str, JsonValue], ...],
        tools: tuple[dict[str, JsonValue], ...],
        output_schema: dict[str, JsonValue],
        tool_choice: Literal["auto", "none"] = "auto",
    ) -> ModelTurn:
        if tool_choice not in ("auto", "none"):
            raise ArtifactError(_INPUT)
        _text(instructions, code=_INPUT)
        if not isinstance(input_items, tuple) or not isinstance(tools, tuple):
            raise ArtifactError(_INPUT)
        payload: dict[str, JsonValue] = {
            "model": self._model,
            "instructions": instructions,
            "input": list(input_items),
            "tools": list(tools),
            "tool_choice": tool_choice,
            "store": False,
            "parallel_tool_calls": False,
            "include": ["reasoning.encrypted_content"],
            "max_output_tokens": self._max_output_tokens,
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "agent_answer",
                    "strict": True,
                    "schema": output_schema,
                }
            },
        }
        _json_tree(payload, code=_INPUT, size_code="openai_responses_request_too_large")
        names = _tools(tools)
        _schema(output_schema)
        seen = _inputs(input_items, names)
        try:
            body = json.dumps(
                payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            ).encode("utf-8")
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise ArtifactError(_INPUT) from None
        if len(body) > _BYTES:
            raise ArtifactError("openai_responses_request_too_large")
        # Standalone Request bypasses client auth, default headers, cookies, and query params.
        request = httpx.Request(
            "POST",
            _ENDPOINT,
            content=body,
            headers={
                "Authorization": f"Bearer {self._api_key.get_secret_value()}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Encoding": "identity",
            },
            extensions={
                "timeout": httpx.Timeout(self._timeout, connect=min(self._timeout, 10)).as_dict()
            },
        )
        result = await self._send(request)
        return _turn(result, names if tool_choice == "auto" else set(), seen)

    async def _send(self, request: httpx.Request) -> dict[str, JsonValue]:
        deadline = asyncio.get_running_loop().time() + self._timeout
        try:
            async with asyncio.timeout_at(deadline):
                response = await self._http.send(
                    request, stream=True, auth=None, follow_redirects=False
                )
                try:
                    status = response.status_code
                    if 300 <= status < 400:
                        raise ArtifactError("openai_responses_redirect_forbidden")
                    if status in (401, 403):
                        raise ArtifactError("openai_responses_auth_failed")
                    if status == 429:
                        raise ArtifactError("openai_responses_rate_limited", retryable=True)
                    if 500 <= status < 600:
                        raise ArtifactError("openai_responses_service_failed", retryable=True)
                    if status != 200:
                        raise ArtifactError("openai_responses_http_error", retryable=status == 408)
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
                        if int(length) > _BYTES:
                            raise ArtifactError("openai_responses_response_too_large")
                    body = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=16 * 1024):
                        if len(body) + len(chunk) > _BYTES:
                            raise ArtifactError("openai_responses_response_too_large")
                        body.extend(chunk)
                    if length is not None and len(body) != int(length):
                        raise ArtifactError(_INVALID)
                except BaseException:
                    # Closing cannot extend the request budget or replace cancellation.
                    try:
                        close_deadline = min(deadline, asyncio.get_running_loop().time() + 0.05)
                        async with asyncio.timeout_at(close_deadline):
                            await response.aclose()
                    except (httpx.HTTPError, TimeoutError, OSError):
                        pass
                    raise
                else:
                    await response.aclose()
        except (httpx.HTTPError, TimeoutError, OSError):
            raise ArtifactError("openai_responses_transport_failed", retryable=True) from None
        return _json_object(body, code=_INVALID)
