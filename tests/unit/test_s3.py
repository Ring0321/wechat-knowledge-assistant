import asyncio
import base64
import hashlib
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError, ParamValidationError
from botocore.stub import ANY, Stubber

from app.adapters.s3 import S3ObjectStore
from app.domain.artifacts import ArtifactError

USER = UUID("10000000-0000-4000-8000-000000000001")
SOURCE = UUID("20000000-0000-4000-8000-000000000001")
DATA = "测试资料\n".encode()
DIGEST = hashlib.sha256(DATA).hexdigest()


@pytest.fixture
def sdk() -> Iterator[tuple[MagicMock, MagicMock]]:
    with patch("app.adapters.s3.boto3.session.Session") as factory:
        client = factory.return_value.client.return_value
        client.put_object.return_value = {}
        yield factory, client


@pytest.fixture
def store(sdk: tuple[MagicMock, MagicMock]) -> S3ObjectStore:
    return S3ObjectStore("http://minio:9000", "test-access", "test-secret", "test-artifacts")


@pytest.fixture
def artifact(tmp_path: Path) -> Path:
    path = tmp_path / "caller-owned-file"
    path.write_bytes(DATA)
    return path


async def upload(store: S3ObjectStore, path: Path, **changes: Any) -> Any:
    arguments = {
        "user_id": USER,
        "source_id": SOURCE,
        "kind": "original",
        "path": path,
        "sha256": DIGEST,
        "content_type": "text/plain; charset=utf-8",
    }
    arguments.update(changes)
    return await store.put_file(**arguments)


def test_explicit_sdk_configuration(sdk: tuple[MagicMock, MagicMock]) -> None:
    factory, _ = sdk
    S3ObjectStore("https://s3.example.com", "explicit-access", "explicit-secret", "pkb-artifacts")
    factory.assert_called_once_with(
        aws_access_key_id="explicit-access",
        aws_secret_access_key="explicit-secret",
        aws_session_token="",
        region_name="us-east-1",
    )
    options = factory.return_value.client.call_args.kwargs
    assert options["endpoint_url"] == "https://s3.example.com"
    assert options["region_name"] == "us-east-1"
    config = options["config"]
    assert config.connect_timeout == 3
    assert config.read_timeout == 20
    assert config.retries == {"mode": "standard", "total_max_attempts": 2}
    assert config.proxies == {}
    assert config.s3["addressing_style"] == "path"


@pytest.mark.parametrize(
    "changes",
    [
        {"endpoint_url": ""},
        {"endpoint_url": "file:///tmp/data"},
        {"endpoint_url": "https://user:password@s3.example.com"},
        {"endpoint_url": "https://s3.example.com?token=secret"},
        {"endpoint_url": "https://s3.example.com/#secret"},
        {"endpoint_url": "https://s3.example.com/other"},
        {"endpoint_url": "https://s3.example.com:bad"},
        {"endpoint_url": "https://s3.example.com\n"},
        {"access_key": ""},
        {"secret_key": "  "},
        {"region": " "},
        {"bucket": "UPPER"},
        {"bucket": "../secrets"},
        {"bucket": "two..dots"},
        {"bucket": "127.0.0.1"},
        {"bucket": "ab"},
    ],
)
def test_reject_invalid_config_without_sdk(
    sdk: tuple[MagicMock, MagicMock], changes: dict[str, str]
) -> None:
    arguments = {
        "endpoint_url": "http://minio:9000",
        "access_key": "test-access",
        "secret_key": "test-secret",
        "bucket": "test-artifacts",
        "region": "us-east-1",
    }
    arguments.update(changes)
    with pytest.raises(ArtifactError, match="^s3_configuration_invalid$"):
        S3ObjectStore(**arguments)
    sdk[0].assert_not_called()


async def test_file_stream_and_sha256_header(
    store: S3ObjectStore, sdk: tuple[MagicMock, MagicMock], artifact: Path
) -> None:
    main_thread = threading.get_ident()

    def put_object(**kwargs: Any) -> dict[str, str]:
        assert threading.get_ident() != main_thread
        assert kwargs["Body"].read() == DATA
        assert kwargs["ContentLength"] == len(DATA)
        assert kwargs["ChecksumSHA256"] == base64.b64encode(bytes.fromhex(DIGEST)).decode()
        assert kwargs["Metadata"] == {"sha256": DIGEST}
        assert "ACL" not in kwargs
        return {"ChecksumSHA256": kwargs["ChecksumSHA256"]}

    sdk[1].put_object.side_effect = put_object
    result = await upload(store, artifact)
    assert result.key == f"users/{USER}/sources/{SOURCE}/original/{DIGEST}"
    assert result.sha256 == DIGEST
    assert result.size_bytes == len(DATA)
    assert result.content_type == "text/plain; charset=utf-8"
    assert await asyncio.to_thread(artifact.read_bytes) == DATA
    assert sdk[1].put_object.call_args.kwargs["Body"].closed


async def test_deterministic_keys_and_tenant_source_kind_isolation(
    store: S3ObjectStore, artifact: Path
) -> None:
    first = await upload(store, artifact)
    assert first == await upload(store, artifact)
    others = [
        await upload(store, artifact, user_id=uuid4()),
        await upload(store, artifact, source_id=uuid4()),
        await upload(store, artifact, kind="canonical"),
        await upload(store, artifact, kind="markdown"),
    ]
    assert len({first.key, *(item.key for item in others)}) == 5
    assert artifact.name not in first.key


@pytest.mark.parametrize(
    "changes",
    [
        {"user_id": "../user"},
        {"source_id": "../source"},
        {"kind": "../../private"},
        {"kind": "Original"},
        {"sha256": "a" * 63},
        {"sha256": "G" * 64},
        {"sha256": DIGEST.upper()},
        {"content_type": "text/plain\r\nX-Credential: secret"},
        {"content_type": "text"},
        {"content_type": "a/b;" + "x" * 256},
    ],
)
async def test_reject_invalid_artifact_before_upload(
    store: S3ObjectStore,
    sdk: tuple[MagicMock, MagicMock],
    artifact: Path,
    changes: dict[str, object],
) -> None:
    with pytest.raises(ArtifactError, match="^s3_artifact_invalid$"):
        await upload(store, artifact, **changes)
    sdk[1].put_object.assert_not_called()


async def test_reject_wrong_hash_without_upload(
    store: S3ObjectStore, sdk: tuple[MagicMock, MagicMock], artifact: Path
) -> None:
    with pytest.raises(ArtifactError, match="^s3_file_checksum_mismatch$"):
        await upload(store, artifact, sha256="0" * 64)
    sdk[1].put_object.assert_not_called()


@pytest.mark.parametrize("path", [Path("relative"), Path("/a/../b")])
async def test_reject_unsafe_path(store: S3ObjectStore, path: Path) -> None:
    with pytest.raises(ArtifactError, match="^s3_file_invalid$"):
        await upload(store, path)


async def test_missing_file_error_is_safe(store: S3ObjectStore, tmp_path: Path) -> None:
    with pytest.raises(ArtifactError, match="^s3_file_unavailable$") as error:
        await upload(store, tmp_path / "sensitive-customer-name")
    assert "sensitive" not in str(error.value)


async def test_reject_symlink_component(
    store: S3ObjectStore, artifact: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(Path, "is_symlink", lambda self: self == artifact.parent)
    with pytest.raises(ArtifactError, match="^s3_file_invalid$"):
        await upload(store, artifact)


async def test_reject_file_above_defensive_cap(
    store: S3ObjectStore,
    artifact: Path,
    sdk: tuple[MagicMock, MagicMock],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.adapters.s3._MAX_BYTES", 16)
    large = tmp_path / "large-sparse-file"
    with large.open("wb") as file:
        file.truncate(17)
    with pytest.raises(ArtifactError, match="^s3_file_invalid$"):
        await upload(store, large)
    sdk[1].put_object.assert_not_called()


async def test_empty_file_is_preserved(store: S3ObjectStore, tmp_path: Path) -> None:
    path = tmp_path / "empty"
    path.write_bytes(b"")
    result = await upload(store, path, sha256=hashlib.sha256(b"").hexdigest())
    assert result.size_bytes == 0


@pytest.mark.parametrize(
    ("status", "code", "retryable"),
    [
        (503, "ServiceUnavailable", True),
        (429, "Throttling", True),
        (400, "RequestTimeout", True),
        (403, "AccessDenied", False),
        (404, "NoSuchBucket", False),
        (400, "BadDigest", False),
    ],
)
async def test_sdk_error_classification_and_redaction(
    store: S3ObjectStore,
    sdk: tuple[MagicMock, MagicMock],
    artifact: Path,
    status: int,
    code: str,
    retryable: bool,
) -> None:
    sdk[1].put_object.side_effect = ClientError(
        {
            "Error": {"Code": code, "Message": "secret-customer-details"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "PutObject",
    )
    with pytest.raises(ArtifactError, match="^s3_upload_failed$") as error:
        await upload(store, artifact)
    assert error.value.retryable is retryable
    assert error.value.__suppress_context__


async def test_connection_failure_is_retryable(
    store: S3ObjectStore, sdk: tuple[MagicMock, MagicMock], artifact: Path
) -> None:
    sdk[1].put_object.side_effect = EndpointConnectionError(endpoint_url="https://secret.example")
    with pytest.raises(ArtifactError, match="^s3_unavailable$") as error:
        await upload(store, artifact)
    assert error.value.retryable


async def test_sdk_validation_failure_is_safe(
    store: S3ObjectStore, sdk: tuple[MagicMock, MagicMock], artifact: Path
) -> None:
    sdk[1].put_object.side_effect = ParamValidationError(report="secret-customer-details")
    with pytest.raises(ArtifactError, match="^s3_request_invalid$"):
        await upload(store, artifact)


async def test_server_checksum_mismatch_is_not_success(
    store: S3ObjectStore, sdk: tuple[MagicMock, MagicMock], artifact: Path
) -> None:
    sdk[1].put_object.return_value = {"ChecksumSHA256": "wrong"}
    with pytest.raises(ArtifactError, match="^s3_remote_checksum_mismatch$"):
        await upload(store, artifact)


async def test_file_mutated_during_upload_is_not_success(
    store: S3ObjectStore, sdk: tuple[MagicMock, MagicMock], artifact: Path
) -> None:
    def mutate(**kwargs: Any) -> dict[str, str]:
        with artifact.open("ab") as file:
            file.write(b"tampered")
        return {}

    sdk[1].put_object.side_effect = mutate
    with pytest.raises(ArtifactError, match="^s3_file_changed$"):
        await upload(store, artifact)


async def test_cancellation_waits_for_thread_to_release_file(
    store: S3ObjectStore, sdk: tuple[MagicMock, MagicMock], artifact: Path
) -> None:
    started = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()

    def delayed(**kwargs: Any) -> dict[str, str]:
        loop.call_soon_threadsafe(started.set)
        assert release.wait(timeout=5)
        return {}

    sdk[1].put_object.side_effect = delayed
    task = asyncio.create_task(upload(store, artifact))
    await asyncio.wait_for(started.wait(), timeout=5)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sdk[1].put_object.call_args.kwargs["Body"].closed


async def test_close_releases_sdk_pool(
    store: S3ObjectStore, sdk: tuple[MagicMock, MagicMock]
) -> None:
    await store.aclose()
    sdk[1].close.assert_called_once()


async def test_real_sdk_model_accepts_file_and_sha256(artifact: Path) -> None:
    store = S3ObjectStore("https://s3.example.com", "test-access", "test-secret", "test-artifacts")
    checksum = base64.b64encode(bytes.fromhex(DIGEST)).decode()
    try:
        with Stubber(store._client) as stubber:
            stubber.add_response(
                "put_object",
                {"ChecksumSHA256": checksum},
                {
                    "Bucket": "test-artifacts",
                    "Key": f"users/{USER}/sources/{SOURCE}/original/{DIGEST}",
                    "Body": ANY,
                    "ContentLength": len(DATA),
                    "ContentType": "text/plain; charset=utf-8",
                    "ChecksumSHA256": checksum,
                    "Metadata": {"sha256": DIGEST},
                },
            )
            result = await upload(store, artifact)
            assert result.sha256 == DIGEST
            stubber.assert_no_pending_responses()
    finally:
        await store.aclose()
