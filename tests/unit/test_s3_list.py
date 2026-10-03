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
PREFIX = f"users/{USER}/sources/{SOURCE}/"
DIGEST = "abcdef0123456789" * 4
KEY = f"{PREFIX}original/{DIGEST}"
OPTIONS = {"Bucket": "test-artifacts", "Prefix": PREFIX, "MaxKeys": 1000}


def page(*keys: str, more: bool = False, token: str = "next") -> dict[str, Any]:
    result: dict[str, Any] = {
        "Prefix": PREFIX,
        "IsTruncated": more,
        "KeyCount": len(keys),
        "Contents": [{"Key": key} for key in keys],
    }
    if more:
        result["NextContinuationToken"] = token
    return result


@pytest.fixture
def sdk() -> Iterator[MagicMock]:
    with patch("app.adapters.s3.boto3.session.Session") as factory:
        client = factory.return_value.client.return_value
        client.list_objects_v2.return_value = page()
        yield client


@pytest.fixture
def store(sdk: MagicMock) -> S3ObjectStore:
    return S3ObjectStore("http://minio:9000", "test-access", "test-secret", "test-artifacts")


async def test_scan_exact_source_prefix_off_event_loop(
    store: S3ObjectStore, sdk: MagicMock
) -> None:
    main_thread = threading.get_ident()
    keys = tuple(
        f"{PREFIX}{kind}/{DIGEST}"
        for kind in ("original", "canonical", "markdown", "audio", "keyframe", "transcript")
    )

    def list_objects(**kwargs: Any) -> dict[str, Any]:
        assert threading.get_ident() != main_thread
        assert kwargs == OPTIONS
        return page(*keys)

    sdk.list_objects_v2.side_effect = list_objects
    assert await store.list_keys(user_id=USER, source_id=SOURCE) == keys
    assert sdk.mock_calls == [call.list_objects_v2(**OPTIONS)]


async def test_empty_prefix_without_contents_is_complete(
    store: S3ObjectStore, sdk: MagicMock
) -> None:
    sdk.list_objects_v2.return_value = {"Prefix": PREFIX, "IsTruncated": False, "KeyCount": 0}
    assert await store.list_keys(user_id=USER, source_id=SOURCE) == ()
    assert sdk.mock_calls == [call.list_objects_v2(**OPTIONS)]


async def test_pagination_uses_only_returned_opaque_token(
    store: S3ObjectStore, sdk: MagicMock
) -> None:
    second = f"{PREFIX}canonical/{DIGEST}"
    token = "opaque+not/a/key=="
    sdk.list_objects_v2.side_effect = [
        page(KEY, more=True, token=token),
        {**page(second), "ContinuationToken": token},
    ]
    assert await store.list_keys(user_id=USER, source_id=SOURCE) == (KEY, second)
    assert sdk.mock_calls == [
        call.list_objects_v2(**OPTIONS),
        call.list_objects_v2(**OPTIONS, ContinuationToken=token),
    ]


@pytest.mark.parametrize(
    "changes",
    [{"user_id": str(USER)}, {"source_id": str(SOURCE)}, {"user_id": None}],
    ids=["string-user", "string-source", "missing-user"],
)
async def test_invalid_identity_never_calls_sdk(
    store: S3ObjectStore, sdk: MagicMock, changes: dict[str, Any]
) -> None:
    arguments: dict[str, Any] = {"user_id": USER, "source_id": SOURCE}
    arguments.update(changes)
    with pytest.raises(ArtifactError, match="^s3_object_key_invalid$"):
        await store.list_keys(**arguments)
    assert sdk.mock_calls == []


@pytest.mark.parametrize(
    "key",
    [
        None,
        1,
        "",
        PREFIX,
        KEY.replace(str(USER), str(OTHER_USER)),
        KEY.replace(str(SOURCE), str(OTHER_SOURCE)),
        KEY.replace(str(USER), str(USER).upper()),
        KEY.replace(str(SOURCE), str(SOURCE).upper()),
        KEY.replace("/original/", "/unknown/"),
        KEY.replace("/original/", "/../"),
        KEY.replace("/original/", "/original/../original/"),
        KEY.replace(DIGEST, DIGEST.upper()),
        KEY.replace(DIGEST, "a" * 63),
        KEY.replace(DIGEST, "g" * 64),
        KEY.replace("/", "\\"),
        KEY + "/child",
        KEY + "?versionId=other",
        KEY + "\n",
        KEY + "\x00",
    ],
    ids=[
        "missing",
        "number",
        "empty",
        "prefix-only",
        "other-user",
        "other-source",
        "user-case",
        "source-case",
        "kind",
        "dotdot",
        "traversal",
        "hash-case",
        "short-hash",
        "invalid-hash",
        "backslash",
        "child",
        "query",
        "newline",
        "nul",
    ],
)
async def test_invalid_or_foreign_key_discards_entire_scan(
    store: S3ObjectStore, sdk: MagicMock, key: object
) -> None:
    sdk.list_objects_v2.side_effect = [
        page(KEY, more=True),
        {**page(), "KeyCount": 1, "Contents": [{"Key": key}]},
    ]
    with pytest.raises(ArtifactError, match="^s3_object_key_invalid$") as error:
        await store.list_keys(user_id=USER, source_id=SOURCE)
    assert not error.value.retryable
    assert sdk.list_objects_v2.call_count == 2
    sdk.delete_object.assert_not_called()


@pytest.mark.parametrize(
    "changes",
    [
        {"Prefix": PREFIX.replace(str(USER), str(OTHER_USER))},
        {"Prefix": None},
        {"Name": "other-bucket"},
        {"IsTruncated": None},
        {"IsTruncated": "false"},
        {"IsTruncated": 0},
        {"Contents": None},
        {"Contents": {}},
        {"KeyCount": 1},
        {"KeyCount": "0"},
        {"KeyCount": False},
        {"Delimiter": "/"},
        {"StartAfter": KEY},
        {"CommonPrefixes": [{"Prefix": PREFIX + "original/"}]},
        {"CommonPrefixes": None},
        {"ContinuationToken": "not-requested"},
        {"NextContinuationToken": "more-but-marked-complete"},
        {"IsTruncated": True, "NextContinuationToken": None},
        {"IsTruncated": True, "NextContinuationToken": ""},
        {"IsTruncated": True, "NextContinuationToken": 1},
    ],
    ids=[
        "foreign-prefix",
        "missing-prefix",
        "other-bucket",
        "unknown-truncation",
        "string-truncation",
        "int-truncation",
        "null-contents",
        "dict-contents",
        "count-mismatch",
        "string-count",
        "bool-count",
        "delimiter",
        "start-after",
        "hidden-prefix",
        "null-prefixes",
        "wrong-cursor",
        "unexpected-next",
        "null-next",
        "empty-next",
        "nonstring-next",
    ],
)
async def test_malformed_or_incomplete_page_fails_closed(
    store: S3ObjectStore, sdk: MagicMock, changes: dict[str, Any]
) -> None:
    sdk.list_objects_v2.return_value = {**page(), **changes}
    with pytest.raises(ArtifactError, match="^s3_list_incomplete$") as error:
        await store.list_keys(user_id=USER, source_id=SOURCE)
    assert not error.value.retryable
    assert sdk.mock_calls == [call.list_objects_v2(**OPTIONS)]


@pytest.mark.parametrize("response", [None, [], {}, {"IsTruncated": False}])
async def test_missing_page_contract_fails_closed(
    store: S3ObjectStore, sdk: MagicMock, response: object
) -> None:
    sdk.list_objects_v2.return_value = response
    with pytest.raises(ArtifactError, match="^s3_list_incomplete$"):
        await store.list_keys(user_id=USER, source_id=SOURCE)


@pytest.mark.parametrize("item", [None, "key", {}, {"Size": 0}])
async def test_missing_object_key_fails_closed(
    store: S3ObjectStore, sdk: MagicMock, item: object
) -> None:
    sdk.list_objects_v2.return_value = {**page(), "KeyCount": 1, "Contents": [item]}
    with pytest.raises(ArtifactError, match="^s3_object_key_invalid$"):
        await store.list_keys(user_id=USER, source_id=SOURCE)


async def test_cursor_cycle_fails_before_repeating_request(
    store: S3ObjectStore, sdk: MagicMock
) -> None:
    sdk.list_objects_v2.side_effect = [
        page(more=True, token="one"),
        page(more=True, token="two"),
        page(more=True, token="one"),
    ]
    with pytest.raises(ArtifactError, match="^s3_list_incomplete$"):
        await store.list_keys(user_id=USER, source_id=SOURCE)
    assert sdk.mock_calls == [
        call.list_objects_v2(**OPTIONS),
        call.list_objects_v2(**OPTIONS, ContinuationToken="one"),
        call.list_objects_v2(**OPTIONS, ContinuationToken="two"),
    ]


async def test_duplicate_objects_fail_before_certifying_completion(
    store: S3ObjectStore, sdk: MagicMock
) -> None:
    sdk.list_objects_v2.side_effect = [page(KEY, more=True), page(KEY)]
    with pytest.raises(ArtifactError, match="^s3_list_incomplete$"):
        await store.list_keys(user_id=USER, source_id=SOURCE)


@pytest.mark.parametrize("complete", [False, True])
async def test_scan_stops_at_one_hundred_pages(
    store: S3ObjectStore, sdk: MagicMock, complete: bool
) -> None:
    keys = tuple(f"{PREFIX}original/{index:064x}" for index in range(100))
    sdk.list_objects_v2.side_effect = [
        page(key, more=not complete or index < 99, token=f"token-{index}")
        for index, key in enumerate(keys)
    ]
    if complete:
        assert await store.list_keys(user_id=USER, source_id=SOURCE) == keys
    else:
        with pytest.raises(ArtifactError, match="^s3_list_incomplete$"):
            await store.list_keys(user_id=USER, source_id=SOURCE)
    assert sdk.list_objects_v2.call_count == 100


@pytest.mark.parametrize("count", [1000, 1001])
async def test_page_object_count_is_bounded(
    store: S3ObjectStore, sdk: MagicMock, count: int
) -> None:
    keys = tuple(f"{PREFIX}original/{index:064x}" for index in range(count))
    sdk.list_objects_v2.return_value = page(*keys)
    if count == 1000:
        assert await store.list_keys(user_id=USER, source_id=SOURCE) == keys
    else:
        with pytest.raises(ArtifactError, match="^s3_list_incomplete$"):
            await store.list_keys(user_id=USER, source_id=SOURCE)


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
        (404, "NoSuchBucket", False),
        (400, "InvalidRequest", False),
    ],
)
async def test_scan_failure_discards_partial_results_and_redacts_error(
    store: S3ObjectStore,
    sdk: MagicMock,
    caplog: pytest.LogCaptureFixture,
    status: int,
    code: str,
    retryable: bool,
) -> None:
    sdk.list_objects_v2.side_effect = [
        page(KEY, more=True),
        ClientError(
            {
                "Error": {"Code": code, "Message": f"test-secret {KEY}"},
                "ResponseMetadata": {"HTTPStatusCode": status},
            },
            "ListObjectsV2",
        ),
    ]
    with pytest.raises(ArtifactError, match="^s3_list_failed$") as error:
        await store.list_keys(user_id=USER, source_id=SOURCE)
    assert error.value.retryable is retryable
    assert error.value.__suppress_context__
    assert not caplog.records
    assert sdk.list_objects_v2.call_count == 2


@pytest.mark.parametrize("error_type", [EndpointConnectionError, ReadTimeoutError])
async def test_list_transport_error_is_retryable(
    store: S3ObjectStore,
    sdk: MagicMock,
    error_type: type[EndpointConnectionError] | type[ReadTimeoutError],
) -> None:
    sdk.list_objects_v2.side_effect = error_type(endpoint_url="https://sensitive.example")
    with pytest.raises(ArtifactError, match="^s3_list_failed$") as error:
        await store.list_keys(user_id=USER, source_id=SOURCE)
    assert error.value.retryable
    assert error.value.__suppress_context__


async def test_list_sdk_validation_error_is_safe(store: S3ObjectStore, sdk: MagicMock) -> None:
    sdk.list_objects_v2.side_effect = ParamValidationError(report=f"test-secret {KEY}")
    with pytest.raises(ArtifactError, match="^s3_list_failed$") as error:
        await store.list_keys(user_id=USER, source_id=SOURCE)
    assert not error.value.retryable
    assert error.value.__suppress_context__


@pytest.mark.parametrize("fails", [False, True])
async def test_repeated_cancellation_drains_the_sdk_scan(
    store: S3ObjectStore, sdk: MagicMock, fails: bool
) -> None:
    started = asyncio.Event()
    release = threading.Event()
    finished = threading.Event()
    loop = asyncio.get_running_loop()

    def delayed(**kwargs: Any) -> dict[str, Any]:
        loop.call_soon_threadsafe(started.set)
        try:
            assert release.wait(timeout=5)
            if fails:
                raise EndpointConnectionError(endpoint_url="https://sensitive.example")
            return page(KEY)
        finally:
            finished.set()

    sdk.list_objects_v2.side_effect = delayed
    task = asyncio.create_task(store.list_keys(user_id=USER, source_id=SOURCE))
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
    assert sdk.mock_calls == [call.list_objects_v2(**OPTIONS)]


async def test_real_sdk_model_accepts_prefix_scoped_pagination() -> None:
    store = S3ObjectStore("https://s3.example.com", "test-access", "test-secret", "test-artifacts")
    try:
        with Stubber(store._client) as stubber:
            stubber.add_response("list_objects_v2", page(KEY, more=True), OPTIONS)
            stubber.add_response(
                "list_objects_v2", page(), {**OPTIONS, "ContinuationToken": "next"}
            )
            assert await store.list_keys(user_id=USER, source_id=SOURCE) == (KEY,)
            stubber.assert_no_pending_responses()
    finally:
        await store.aclose()
