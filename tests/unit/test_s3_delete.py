import asyncio
import threading
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, call, patch
from uuid import UUID

import pytest
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    ParamValidationError,
    ReadTimeoutError,
)
from botocore.stub import Stubber

from app.adapters.s3 import S3ObjectStore
from app.domain.artifacts import ArtifactError

USER = UUID("1a000000-0000-4000-8000-000000000001")
SOURCE = UUID("2b000000-0000-4000-8000-000000000001")
OTHER_USER = UUID("3c000000-0000-4000-8000-000000000001")
OTHER_SOURCE = UUID("4d000000-0000-4000-8000-000000000001")
DIGEST = "abcdef0123456789" * 4
KEY = f"users/{USER}/sources/{SOURCE}/original/{DIGEST}"


@pytest.fixture
def sdk() -> Iterator[MagicMock]:
    with patch("app.adapters.s3.boto3.session.Session") as factory:
        client = factory.return_value.client.return_value
        client.delete_object.return_value = {}
        yield client


@pytest.fixture
def store(sdk: MagicMock) -> S3ObjectStore:
    return S3ObjectStore("http://minio:9000", "test-access", "test-secret", "test-artifacts")


@pytest.mark.parametrize(
    "kind", ["original", "canonical", "markdown", "audio", "keyframe", "transcript"]
)
async def test_delete_only_the_exact_single_object_off_event_loop(
    store: S3ObjectStore, sdk: MagicMock, kind: str
) -> None:
    key = f"users/{USER}/sources/{SOURCE}/{kind}/{DIGEST}"
    main_thread = threading.get_ident()

    def delete_object(**kwargs: Any) -> dict[str, object]:
        assert threading.get_ident() != main_thread
        assert kwargs == {"Bucket": "test-artifacts", "Key": key}
        return {}

    sdk.delete_object.side_effect = delete_object
    assert await store.delete(user_id=USER, source_id=SOURCE, key=key) is None
    assert sdk.mock_calls == [call.delete_object(Bucket="test-artifacts", Key=key)]


@pytest.mark.parametrize(
    "changes",
    [
        {"user_id": OTHER_USER},
        {"source_id": OTHER_SOURCE},
        {"user_id": str(USER)},
        {"source_id": str(SOURCE)},
        {"key": None},
        {"key": KEY.encode()},
        {"key": ""},
        {"key": KEY.replace(str(USER), str(USER).upper())},
        {"key": KEY.replace(str(SOURCE), str(SOURCE).upper())},
        {"key": KEY.replace("users/", "user/", 1)},
        {"key": KEY.replace("/sources/", "/source/")},
        {"key": KEY.replace("/original/", "/Original/")},
        {"key": KEY.replace("/original/", "/unknown/")},
        {"key": KEY.replace(DIGEST, DIGEST.upper())},
        {"key": KEY.replace(DIGEST, "a" * 63)},
        {"key": KEY.replace(DIGEST, "g" * 64)},
        {"key": KEY.replace(DIGEST, "*")},
        {"key": KEY.replace("/original/", "/../")},
        {"key": KEY.replace("/original/", "/original/../original/")},
        {"key": KEY.replace("/original/", "/%2e%2e/")},
        {"key": KEY.replace("/original/", "/original%2f/")},
        {"key": KEY.replace("/", "\\")},
        {"key": "/" + KEY},
        {"key": "../" + KEY},
        {"key": KEY + "/"},
        {"key": KEY + "/another-object"},
        {"key": KEY + ".txt"},
        {"key": KEY + "?versionId=other"},
        {"key": KEY + "#fragment"},
        {"key": KEY + "\n"},
        {"key": KEY + "\x00"},
        {"key": "s3://test-artifacts/" + KEY},
    ],
)
async def test_reject_invalid_identity_or_key_before_any_sdk_call(
    store: S3ObjectStore, sdk: MagicMock, changes: dict[str, Any]
) -> None:
    arguments: dict[str, Any] = {"user_id": USER, "source_id": SOURCE, "key": KEY}
    arguments.update(changes)
    with pytest.raises(ArtifactError, match="^s3_object_key_invalid$") as error:
        await store.delete(**arguments)
    assert not error.value.retryable
    assert sdk.mock_calls == []


@pytest.mark.parametrize(
    ("status", "code"), [(404, "NoSuchKey"), (404, "NotFound"), (400, "NoSuchKey")]
)
async def test_missing_object_is_idempotent(
    store: S3ObjectStore, sdk: MagicMock, status: int, code: str
) -> None:
    sdk.delete_object.side_effect = ClientError(
        {
            "Error": {"Code": code, "Message": "sensitive-details"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "DeleteObject",
    )
    await store.delete(user_id=USER, source_id=SOURCE, key=KEY)
    await store.delete(user_id=USER, source_id=SOURCE, key=KEY)
    assert sdk.mock_calls == [call.delete_object(Bucket="test-artifacts", Key=KEY)] * 2


@pytest.mark.parametrize(
    ("status", "code", "retryable"),
    [
        (503, "ServiceUnavailable", True),
        (500, "UnknownError", True),
        (429, "Throttling", True),
        (400, "RequestTimeout", True),
        (400, "SlowDown", True),
        (400, "InternalError", True),
        (403, "AccessDenied", False),
        (400, "InvalidRequest", False),
    ],
)
async def test_delete_errors_are_classified_and_redacted(
    store: S3ObjectStore,
    sdk: MagicMock,
    caplog: pytest.LogCaptureFixture,
    status: int,
    code: str,
    retryable: bool,
) -> None:
    sdk.delete_object.side_effect = ClientError(
        {
            "Error": {"Code": code, "Message": f"test-secret {KEY}"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "DeleteObject",
    )
    with pytest.raises(ArtifactError, match="^s3_delete_failed$") as error:
        await store.delete(user_id=USER, source_id=SOURCE, key=KEY)
    assert error.value.retryable is retryable
    assert error.value.__suppress_context__
    assert not caplog.records


@pytest.mark.parametrize("error_type", [EndpointConnectionError, ReadTimeoutError])
async def test_delete_transport_error_is_retryable_and_safe(
    store: S3ObjectStore,
    sdk: MagicMock,
    error_type: type[EndpointConnectionError] | type[ReadTimeoutError],
) -> None:
    sdk.delete_object.side_effect = error_type(endpoint_url="https://sensitive.example")
    with pytest.raises(ArtifactError, match="^s3_unavailable$") as error:
        await store.delete(user_id=USER, source_id=SOURCE, key=KEY)
    assert error.value.retryable
    assert error.value.__suppress_context__


async def test_delete_sdk_validation_error_is_safe(store: S3ObjectStore, sdk: MagicMock) -> None:
    sdk.delete_object.side_effect = ParamValidationError(report=f"test-secret {KEY}")
    with pytest.raises(ArtifactError, match="^s3_request_invalid$") as error:
        await store.delete(user_id=USER, source_id=SOURCE, key=KEY)
    assert not error.value.retryable
    assert error.value.__suppress_context__


@pytest.mark.parametrize("fails", [False, True])
async def test_delete_repeated_cancellation_waits_for_sdk_thread(
    store: S3ObjectStore, sdk: MagicMock, fails: bool
) -> None:
    started = asyncio.Event()
    release = threading.Event()
    finished = threading.Event()
    loop = asyncio.get_running_loop()

    def delayed(**kwargs: Any) -> dict[str, object]:
        loop.call_soon_threadsafe(started.set)
        try:
            assert release.wait(timeout=5)
            if fails:
                raise EndpointConnectionError(endpoint_url="https://sensitive.example")
            return {}
        finally:
            finished.set()

    sdk.delete_object.side_effect = delayed
    task = asyncio.create_task(store.delete(user_id=USER, source_id=SOURCE, key=KEY))
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        for _ in range(3):
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            assert not finished.is_set()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=5)
    assert finished.is_set()
    assert sdk.mock_calls == [call.delete_object(Bucket="test-artifacts", Key=KEY)]


async def test_real_sdk_model_accepts_only_single_object_delete() -> None:
    store = S3ObjectStore("https://s3.example.com", "test-access", "test-secret", "test-artifacts")
    try:
        with Stubber(store._client) as stubber:
            stubber.add_response("delete_object", {}, {"Bucket": "test-artifacts", "Key": KEY})
            await store.delete(user_id=USER, source_id=SOURCE, key=KEY)
            stubber.assert_no_pending_responses()
    finally:
        await store.aclose()
