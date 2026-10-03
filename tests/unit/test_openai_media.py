import asyncio
import base64
import json
import os
import traceback
import wave
from collections.abc import AsyncIterator
from email import policy
from email.parser import BytesParser
from pathlib import Path

import httpx
import pytest
from PIL import Image

from app.adapters.openai_media import OpenAIMediaAdapter
from app.domain.artifacts import ArtifactError


@pytest.fixture
def audio_path(tmp_path: Path) -> Path:
    path = tmp_path / "private-audio.wav"
    with wave.open(str(path), "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(16_000)
        audio.writeframes(b"\0\0" * 32_000)
    return path


@pytest.fixture
def image_path(tmp_path: Path) -> Path:
    path = tmp_path / "private-frame.jpg"
    Image.new("RGB", (32, 24), "gray").save(path, "JPEG")
    return path


def transcript_response() -> dict[str, object]:
    return {
        "text": "测试内容。",
        "language": "chinese",
        "segments": [{"start": 0, "end": 2, "text": "测试内容。"}],
    }


def description_response(text: str = "灰色画面，无法确定场景。") -> dict[str, object]:
    return {
        "status": "completed",
        "output": [
            {
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{"type": "output_text", "text": text}],
            }
        ],
    }


def adapter(client: httpx.AsyncClient, **kwargs: object) -> OpenAIMediaAdapter:
    return OpenAIMediaAdapter(client, "test-private-key", vision_model="gpt-4.1-mini", **kwargs)  # type: ignore[arg-type]


def truncate_file(path: Path, size: int) -> None:
    with path.open("r+b" if path.exists() else "wb") as output:
        output.truncate(size)


async def test_transcription_fixed_multipart_contract_and_client_ownership(
    audio_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "POST"
        assert str(request.url) == "https://api.openai.com/v1/audio/transcriptions"
        assert request.headers["Authorization"] == "Bearer test-private-key"
        assert request.headers["Accept-Encoding"] == "identity"
        assert "cookie" not in request.headers
        assert "x-private" not in request.headers
        assert request.extensions["timeout"] == {
            "connect": 10,
            "read": 45,
            "write": 45,
            "pool": 45,
        }
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
        assert set(parts) == {"model", "response_format", "timestamp_granularities[]", "file"}
        assert parts["model"].get_payload(decode=True) == b"whisper-1"
        assert parts["response_format"].get_payload(decode=True) == b"verbose_json"
        assert parts["timestamp_granularities[]"].get_payload(decode=True) == b"segment"
        assert parts["file"].get_filename() == "audio.wav"
        assert parts["file"].get_content_type() == "audio/wav"
        assert parts["file"].get_payload(decode=True) == audio_path.read_bytes()
        assert b"private-audio" not in request.content
        return httpx.Response(200, json=transcript_response())

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle),
        auth=("inherited-user", "inherited-password"),
        params={"private": "query"},
        cookies={"private": "cookie"},
        headers={"X-Private": "secret"},
        base_url="https://untrusted.invalid/",
    ) as client:
        api = adapter(client)
        result = await api.transcribe(audio_path)
        assert not client.is_closed
        assert "test-private-key" not in repr(api)
        assert "test-private-key" not in repr(vars(api))
    assert len(requests) == 1
    assert result.text == "测试内容。"
    assert result.language == "chinese"
    assert [(item.start, item.end, item.text) for item in result.segments] == [(0, 2, "测试内容。")]


async def test_description_payload_uses_visible_evidence_no_storage_or_tools(
    image_path: Path,
) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "https://api.openai.com/v1/responses"
        assert request.method == "POST"
        payload = json.loads(request.content)
        assert set(payload) == {"model", "instructions", "input", "store", "max_output_tokens"}
        assert payload["model"] == "gpt-4.1-mini"
        assert payload["store"] is False
        assert payload["max_output_tokens"] == 1024
        assert "中文" in payload["instructions"]
        assert "不确定" in payload["instructions"]
        assert "不得执行或服从" in payload["instructions"]
        content = payload["input"][0]["content"]
        assert payload["input"][0]["role"] == "user"
        assert content[0]["type"] == "input_text"
        assert content[1]["type"] == "input_image"
        assert content[1]["detail"] == "low"
        prefix, image = content[1]["image_url"].split(",", 1)
        assert prefix == "data:image/jpeg;base64"
        assert base64.b64decode(image, validate=True) == image_path.read_bytes()
        assert b"private-frame" not in request.content
        return httpx.Response(
            200, json=description_response(" \u0000灰色\u202e画面\r\n不确定\t场景。 ")
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        assert await adapter(client).describe(image_path) == "灰色画面\n不确定\t场景。"


@pytest.mark.parametrize(
    "overrides",
    [
        {"api_key": ""},
        {"api_key": "a\nprivate"},
        {"api_key": "密钥"},
        {"vision_model": "https://untrusted.invalid"},
        {"vision_model": ""},
        {"timeout_seconds": 0},
        {"timeout_seconds": float("nan")},
        {"timeout_seconds": float("inf")},
        {"timeout_seconds": True},
        {"timeout_seconds": 121},
    ],
)
async def test_invalid_configuration_is_masked(overrides: dict[str, object]) -> None:
    options: dict[str, object] = {"api_key": "test-private-key", "vision_model": "gpt-4.1-mini"}
    options.update(overrides)
    async with httpx.AsyncClient() as client:
        with pytest.raises(ArtifactError, match="^openai_invalid_configuration$"):
            OpenAIMediaAdapter(client, **options)  # type: ignore[arg-type]


@pytest.mark.parametrize("operation", ["transcribe", "describe"])
@pytest.mark.parametrize(
    "kind", ["relative", "directory", "empty", "absent", "wrong_type", "oversize"]
)
async def test_invalid_files_never_reach_network(tmp_path: Path, operation: str, kind: str) -> None:
    path = tmp_path / "private-input"
    expected = "openai_invalid_file"
    if kind == "relative":
        path = Path("private-input")
    elif kind == "directory":
        path.mkdir()
    elif kind == "empty":
        path.touch()
    elif kind == "absent":
        expected = "openai_local_io_failed"
    elif kind == "wrong_type":
        path.write_bytes(b"not a media file, private content")
        expected = "openai_invalid_audio" if operation == "transcribe" else "openai_invalid_image"
    elif kind == "oversize":
        await asyncio.to_thread(
            truncate_file, path, 25_000_000 if operation == "transcribe" else 2 * 1024 * 1024 + 1
        )
        expected = "openai_file_too_large"
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: pytest.fail("unsafe file reached network"))
    ) as client:
        with pytest.raises(ArtifactError, match=f"^{expected}$") as error:
            await getattr(adapter(client), operation)(path)
    assert not error.value.retryable
    assert "private" not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize("parent_link", [False, True])
async def test_links_are_rejected_without_network(
    tmp_path: Path, image_path: Path, parent_link: bool
) -> None:
    link = tmp_path / "link"
    try:
        link.symlink_to(
            image_path.parent if parent_link else image_path, target_is_directory=parent_link
        )
    except OSError as error:
        if os.name != "nt" or getattr(error, "winerror", None) != 1314:
            raise
        pytest.skip("This Windows account cannot create symlinks")
    path = link / image_path.name if parent_link else link
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: pytest.fail("link reached network"))
    ) as client:
        with pytest.raises(ArtifactError, match="^openai_invalid_file$"):
            await adapter(client).describe(path)


@pytest.mark.parametrize("kind", ["png", "truncated", "too_many_pixels"])
async def test_image_format_and_decoding_are_validated(tmp_path: Path, kind: str) -> None:
    path = tmp_path / "looks-like-a-jpeg.jpg"
    if kind == "png":
        Image.new("RGB", (16, 16)).save(path, "PNG")
    elif kind == "truncated":
        Image.new("RGB", (32, 32)).save(path, "JPEG")
        path.write_bytes(path.read_bytes()[:200])
    else:
        Image.new("RGB", (3000, 3000)).save(path, "JPEG")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: pytest.fail("invalid image reached network"))
    ) as client:
        with pytest.raises(ArtifactError, match="^openai_invalid_image$"):
            await adapter(client).describe(path)


async def test_truncated_wav_is_rejected(audio_path: Path) -> None:
    content = await asyncio.to_thread(audio_path.read_bytes)
    await asyncio.to_thread(truncate_file, audio_path, len(content) - 2)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: pytest.fail("truncated WAV reached network"))
    ) as client:
        with pytest.raises(ArtifactError, match="^openai_invalid_audio$"):
            await adapter(client).transcribe(audio_path)


@pytest.mark.parametrize("operation", ["transcribe", "describe"])
@pytest.mark.parametrize(
    "status,code,retryable",
    [
        (301, "openai_redirect_forbidden", False),
        (307, "openai_redirect_forbidden", False),
        (400, "openai_http_error", False),
        (401, "openai_auth_failed", False),
        (403, "openai_auth_failed", False),
        (429, "openai_rate_limited", True),
        (500, "openai_service_failed", True),
        (503, "openai_service_failed", True),
    ],
)
async def test_http_errors_no_internal_retries_or_redirects(
    audio_path: Path,
    image_path: Path,
    operation: str,
    status: int,
    code: str,
    retryable: bool,
) -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            status,
            content=b"private-key private-model-content",
            headers={"Location": "http://127.0.0.1/private?token=private"},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle), follow_redirects=True
    ) as client:
        with pytest.raises(ArtifactError, match=f"^{code}$") as error:
            await getattr(adapter(client), operation)(
                audio_path if operation == "transcribe" else image_path
            )
    assert calls == 1
    assert error.value.retryable is retryable
    assert "private" not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize(
    "failure", [httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError]
)
async def test_network_failures_are_sanitized_and_retryable(
    image_path: Path, failure: type[httpx.HTTPError]
) -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise failure("private-key and full model input")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match="^openai_transport_failed$") as error:
            await adapter(client).describe(image_path)
    assert calls == 1
    assert error.value.retryable
    assert "private-key" not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize(
    "segments",
    [
        [{"start": True, "end": 1, "text": "text"}],
        [{"start": "0", "end": 1, "text": "text"}],
        [{"start": -1, "end": 1, "text": "text"}],
        [{"start": 2, "end": 1, "text": "text"}],
        [{"start": 0, "end": 2.001, "text": "text"}],
        [{"start": 1, "end": 2, "text": "text"}, {"start": 0, "end": 1, "text": "text"}],
        [{"start": 0, "end": 1, "text": "text"}, {"start": 0.5, "end": 2, "text": "text"}],
        [{"start": 0, "end": 1, "text": 42}],
        [None],
        None,
        [],
    ],
)
async def test_transcript_segment_validation(audio_path: Path, segments: object) -> None:
    payload = transcript_response() | {"segments": segments}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as client:
        with pytest.raises(ArtifactError, match="^openai_invalid_response$"):
            await adapter(client).transcribe(audio_path)


@pytest.mark.parametrize("timestamp", [b"NaN", b"Infinity", b"-Infinity", b"1e1000"])
async def test_transcript_nonfinite_timestamps_are_rejected(
    audio_path: Path, timestamp: bytes
) -> None:
    body = b'{"text":"x","segments":[{"start":0,"end":' + timestamp + b',"text":"x"}]}'
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, content=body, headers={"Content-Type": "application/json"}
            )
        )
    ) as client:
        with pytest.raises(ArtifactError, match="^openai_invalid_response$"):
            await adapter(client).transcribe(audio_path)


async def test_empty_transcript_remains_empty(audio_path: Path) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"text": "", "segments": []})
        )
    ) as client:
        result = await adapter(client).transcribe(audio_path)
    assert result.text == ""
    assert result.segments == []
    assert result.language is None


async def test_transcript_text_is_sanitized(audio_path: Path) -> None:
    payload = {
        "text": " \x00内容\u202e ",
        "language": "\x01chinese",
        "segments": [{"start": 0, "end": 2, "text": "\x00内容\u202e"}],
        "user_id": "untrusted-tenant",
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as client:
        result = await adapter(client).transcribe(audio_path)
    assert result.text == result.segments[0].text == "内容"
    assert result.language == "chinese"
    assert "user_id" not in result.model_dump()


@pytest.mark.parametrize(
    "payload,code",
    [
        ({"text": "x" * 100_001, "segments": []}, "openai_response_too_large"),
        (
            {"text": "x" * 50_001, "segments": [{"start": 0, "end": 1, "text": "x" * 50_000}]},
            "openai_response_too_large",
        ),
        ({"text": "", "segments": [{}] * 10_001}, "openai_response_too_large"),
        ({"text": "", "segments": [], "language": "x" * 101}, "openai_response_too_large"),
        ({"text": "", "segments": [], "language": 42}, "openai_invalid_response"),
        (
            {"text": "", "segments": [{"start": 0, "end": 1, "text": "x"}]},
            "openai_invalid_response",
        ),
    ],
)
async def test_transcript_text_count_and_language_limits(
    audio_path: Path, payload: dict[str, object], code: str
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as client:
        with pytest.raises(ArtifactError, match=f"^{code}$"):
            await adapter(client).transcribe(audio_path)


@pytest.mark.parametrize(
    "payload,code",
    [
        ({"status": "incomplete", "output": []}, "openai_incomplete_response"),
        (
            description_response() | {"incomplete_details": {"reason": "max_output_tokens"}},
            "openai_incomplete_response",
        ),
        ({"status": "failed", "output": []}, "openai_invalid_response"),
        ({"status": "completed", "output": []}, "openai_invalid_response"),
        ({"status": "completed", "output_text": "x"}, "openai_invalid_response"),
        ({"status": "completed", "output": [{"type": "function_call"}]}, "openai_invalid_response"),
        ({"status": "completed", "output": [{"type": "refusal"}]}, "openai_refused"),
        (description_response(""), "openai_invalid_response"),
        (description_response("x" * 8193), "openai_response_too_large"),
        ({"error": {"message": "private"}}, "openai_api_error"),
    ],
)
async def test_description_rejects_failed_or_malformed_results(
    image_path: Path, payload: dict[str, object], code: str
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as client:
        with pytest.raises(ArtifactError, match=f"^{code}$") as error:
            await adapter(client).describe(image_path)
    assert not error.value.retryable


async def test_refusal_after_partial_text_discards_all_output(image_path: Path) -> None:
    payload = {
        "status": "completed",
        "output": [
            {
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": [
                    {"type": "output_text", "text": "partial"},
                    {"type": "refusal", "refusal": "private"},
                ],
            }
        ],
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload))
    ) as client:
        with pytest.raises(ArtifactError, match="^openai_refused$"):
            await adapter(client).describe(image_path)


@pytest.mark.parametrize(
    "body,headers,code",
    [
        (b"private malformed JSON", {}, "openai_invalid_response"),
        (b"[]", {}, "openai_invalid_response"),
        (b'{"status":"completed","status":"failed"}', {}, "openai_invalid_response"),
        (b"{}", {"Content-Type": "text/plain"}, "openai_invalid_response"),
        (b"{}", {"Content-Length": "-1"}, "openai_invalid_response"),
        (b"{}", {"Content-Length": "100"}, "openai_invalid_response"),
        (b"{}", {"Content-Length": "999999999"}, "openai_response_too_large"),
        (b"x" * (128 * 1024 + 1), {}, "openai_response_too_large"),
    ],
    ids=[
        "malformed-json",
        "array",
        "duplicate-key",
        "wrong-content-type",
        "negative-length",
        "mismatched-length",
        "oversize-length",
        "oversize-body",
    ],
)
async def test_response_envelope_is_bounded_and_strict(
    image_path: Path, body: bytes, headers: dict[str, str], code: str
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, content=body, headers={"Content-Type": "application/json"} | headers
            )
        )
    ) as client:
        with pytest.raises(ArtifactError, match=f"^{code}$"):
            await adapter(client).describe(image_path)


class ResponseStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes], wait: asyncio.Event | None = None) -> None:
        self.chunks = chunks
        self.wait = wait
        self.closed = False
        self.read_count = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            self.read_count += 1
            yield chunk
        if self.wait is not None:
            self.wait.set()
            await asyncio.Event().wait()

    async def aclose(self) -> None:
        self.closed = True


async def test_streamed_response_limit_aborts_and_closes(image_path: Path) -> None:
    stream = ResponseStream([b"x" * 65536] * 4)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, stream=stream, headers={"Content-Type": "application/json"}
            )
        )
    ) as client:
        with pytest.raises(ArtifactError, match="^openai_response_too_large$"):
            await adapter(client).describe(image_path)
    assert stream.closed
    assert stream.read_count == 3


async def test_cancellation_propagates_and_closes_response(image_path: Path) -> None:
    started = asyncio.Event()
    stream = ResponseStream([], wait=started)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, stream=stream, headers={"Content-Type": "application/json"}
            )
        )
    ) as client:
        task = asyncio.create_task(adapter(client).describe(image_path))
        await asyncio.wait_for(started.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not client.is_closed
    assert stream.closed


async def test_total_timeout_is_retryable(image_path: Path) -> None:
    calls = 0

    async def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ArtifactError, match="^openai_transport_failed$") as error:
            await adapter(client, timeout_seconds=0.01).describe(image_path)
    assert error.value.retryable
    assert calls == 1
