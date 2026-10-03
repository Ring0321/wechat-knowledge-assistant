"""Private S3-compatible artifact storage with deterministic tenant-scoped keys."""

import asyncio
import base64
import hashlib
import ipaddress
import os
import re
import stat
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO
from urllib.parse import urlsplit
from uuid import UUID

import boto3.session
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, ConnectionError, HTTPClientError

from app.domain.artifacts import ArtifactError, StoredObject

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_KINDS = frozenset({"original", "canonical", "markdown", "audio", "keyframe", "transcript"})
_SHA256 = re.compile(r"[0-9a-f]{64}")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
_CONTENT_TYPE = re.compile(r"[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+(?:;[ -~]+)?")
_MAX_BYTES = 256 * 1024 * 1024
_CHUNK_BYTES = 1024 * 1024
_LIST_PAGE_SIZE = 1000
_MAX_LIST_PAGES = 100
_RETRYABLE_CODES = frozenset({"SlowDown", "RequestTimeout", "InternalError", "ServiceUnavailable"})


class S3ObjectStore:
    """Callers own private temporary files and enforce their smaller download limits.

    The bucket must be provisioned privately outside this adapter. No ACL, public URL,
    bucket creation, or credential discovery is performed here. HTTP is supported for
    explicitly configured private MinIO networks; production endpoints should use TLS.
    """

    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        region: str = "us-east-1",
    ) -> None:
        self._validate_configuration(endpoint_url, access_key, secret_key, bucket, region)
        self._bucket = bucket
        try:
            session = boto3.session.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token="",
                region_name=region,
            )
            self._client: S3Client = session.client(
                "s3",
                endpoint_url=endpoint_url,
                region_name=region,
                config=Config(
                    signature_version="s3v4",
                    connect_timeout=3,
                    read_timeout=20,
                    retries={"mode": "standard", "total_max_attempts": 2},
                    s3={"addressing_style": "path", "payload_signing_enabled": True},
                    proxies={},
                    request_checksum_calculation="when_required",
                ),
            )
        except (BotoCoreError, ValueError):
            raise ArtifactError("s3_configuration_invalid") from None

    @staticmethod
    def _validate_configuration(
        endpoint_url: str, access_key: str, secret_key: str, bucket: str, region: str
    ) -> None:
        try:
            parsed = urlsplit(endpoint_url)
            _ = parsed.port
            valid_endpoint = (
                parsed.scheme in {"http", "https"}
                and bool(parsed.hostname)
                and parsed.username is None
                and parsed.password is None
                and not parsed.query
                and not parsed.fragment
                and parsed.path in {"", "/"}
                and not any(character.isspace() for character in endpoint_url)
            )
        except ValueError:
            valid_endpoint = False
        try:
            ipaddress.ip_address(bucket)
            bucket_is_ip = True
        except ValueError:
            bucket_is_ip = False
        if (
            not valid_endpoint
            or not access_key.strip()
            or not secret_key.strip()
            or not region.strip()
            or not _BUCKET.fullmatch(bucket)
            or ".." in bucket
            or bucket_is_ip
        ):
            raise ArtifactError("s3_configuration_invalid")

    async def put_file(
        self,
        *,
        user_id: UUID,
        source_id: UUID,
        kind: str,
        path: Path,
        sha256: str,
        content_type: str,
    ) -> StoredObject:
        if (
            not isinstance(user_id, UUID)
            or not isinstance(source_id, UUID)
            or kind not in _KINDS
            or not _SHA256.fullmatch(sha256)
            or len(content_type) > 256
            or not _CONTENT_TYPE.fullmatch(content_type)
        ):
            raise ArtifactError("s3_artifact_invalid")
        key = f"users/{user_id}/sources/{source_id}/{kind}/{sha256}"
        # Hashing, filesystem IO and the synchronous SDK all stay off the event loop.
        upload = asyncio.create_task(
            asyncio.to_thread(self._put_file, key, path, sha256, content_type)
        )
        try:
            return await asyncio.shield(upload)
        except asyncio.CancelledError:
            # Do not let a caller remove its temporary file while a thread owns it.
            # SDK timeouts/retry caps bound the additional shutdown wait.
            try:
                await upload
            except ArtifactError:
                pass
            raise

    async def aclose(self) -> None:
        await asyncio.to_thread(self._client.close)

    async def list_keys(self, *, user_id: UUID, source_id: UUID) -> tuple[str, ...]:
        if not isinstance(user_id, UUID) or not isinstance(source_id, UUID):
            raise ArtifactError("s3_object_key_invalid")
        listing = asyncio.create_task(asyncio.to_thread(self._list_keys, user_id, source_id))
        try:
            return await asyncio.shield(listing)
        except asyncio.CancelledError:
            # Keep the caller's cleanup lease until the bounded SDK scan has ended.
            while True:
                try:
                    await asyncio.shield(listing)
                except asyncio.CancelledError:
                    continue
                except ArtifactError:
                    pass
                break
            raise

    def _list_keys(self, user_id: UUID, source_id: UUID) -> tuple[str, ...]:
        prefix = f"users/{user_id}/sources/{source_id}/"
        keys: list[str] = []
        seen_keys: set[str] = set()
        seen_tokens: set[str] = set()
        token: str | None = None
        try:
            for _ in range(_MAX_LIST_PAGES):
                if token is None:
                    response = self._client.list_objects_v2(
                        Bucket=self._bucket, Prefix=prefix, MaxKeys=_LIST_PAGE_SIZE
                    )
                else:
                    response = self._client.list_objects_v2(
                        Bucket=self._bucket,
                        Prefix=prefix,
                        MaxKeys=_LIST_PAGE_SIZE,
                        ContinuationToken=token,
                    )
                if (
                    not isinstance(response, dict)
                    or response.get("Prefix") != prefix
                    or response.get("Name", self._bucket) != self._bucket
                    or response.get("Delimiter", "") != ""
                    or response.get("StartAfter", "") != ""
                    or response.get("CommonPrefixes", []) != []
                    or response.get("ContinuationToken", token) != token
                ):
                    raise ArtifactError("s3_list_incomplete")
                contents = response.get("Contents", [])
                truncated = response.get("IsTruncated")
                count = response.get(
                    "KeyCount", len(contents) if isinstance(contents, list) else -1
                )
                if (
                    not isinstance(contents, list)
                    or len(contents) > _LIST_PAGE_SIZE
                    or type(count) is not int
                    or count != len(contents)
                    or type(truncated) is not bool
                ):
                    raise ArtifactError("s3_list_incomplete")
                for item in contents:
                    if not isinstance(item, dict):
                        raise ArtifactError("s3_object_key_invalid")
                    key = self._validate_object_key(user_id, source_id, item.get("Key"))
                    if key in seen_keys:
                        raise ArtifactError("s3_list_incomplete")
                    seen_keys.add(key)
                    keys.append(key)
                if not truncated:
                    if "NextContinuationToken" in response:
                        raise ArtifactError("s3_list_incomplete")
                    return tuple(keys)
                token = response.get("NextContinuationToken")
                if not isinstance(token, str) or not token or token in seen_tokens:
                    raise ArtifactError("s3_list_incomplete")
                seen_tokens.add(token)
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            code = error.response.get("Error", {}).get("Code", "")
            retryable = status == 429 or status >= 500 or code in _RETRYABLE_CODES
            raise ArtifactError("s3_list_failed", retryable=retryable) from None
        except (ConnectionError, HTTPClientError):
            raise ArtifactError("s3_list_failed", retryable=True) from None
        except BotoCoreError:
            raise ArtifactError("s3_list_failed") from None
        # Returning a partial list would incorrectly certify deletion completeness.
        raise ArtifactError("s3_list_incomplete")

    @staticmethod
    def _validate_object_key(user_id: UUID, source_id: UUID, key: object) -> str:
        if (
            not isinstance(user_id, UUID)
            or not isinstance(source_id, UUID)
            or not isinstance(key, str)
        ):
            raise ArtifactError("s3_object_key_invalid")
        parts = key.split("/", maxsplit=6)
        if (
            len(parts) != 6
            or parts[:4] != ["users", str(user_id), "sources", str(source_id)]
            or parts[4] not in _KINDS
            or not _SHA256.fullmatch(parts[5])
        ):
            raise ArtifactError("s3_object_key_invalid")
        return key

    async def delete(self, *, user_id: UUID, source_id: UUID, key: str) -> None:
        self._validate_object_key(user_id, source_id, key)
        deletion = asyncio.create_task(asyncio.to_thread(self._delete, key))
        try:
            await asyncio.shield(deletion)
        except asyncio.CancelledError:
            # A retry must not race an abandoned SDK call. Keep draining even if
            # shutdown cancels again; the configured SDK timeouts bound this wait.
            while True:
                try:
                    await asyncio.shield(deletion)
                except asyncio.CancelledError:
                    continue
                except ArtifactError:
                    pass
                break
            raise

    def _delete(self, key: str) -> None:
        try:
            self._client.delete_object(Bucket=self._bucket, Key=key)
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            code = error.response.get("Error", {}).get("Code", "")
            if status == 404 or code == "NoSuchKey":
                return
            retryable = status == 429 or status >= 500 or code in _RETRYABLE_CODES
            raise ArtifactError("s3_delete_failed", retryable=retryable) from None
        except (ConnectionError, HTTPClientError):
            raise ArtifactError("s3_unavailable", retryable=True) from None
        except BotoCoreError:
            raise ArtifactError("s3_request_invalid") from None

    def _put_file(self, key: str, path: Path, sha256: str, content_type: str) -> StoredObject:
        try:
            self._validate_path(path)
            flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
            flags |= getattr(os, "O_NONBLOCK", 0)
            with os.fdopen(os.open(path, flags), "rb") as body:
                initial = os.fstat(body.fileno())
                if not stat.S_ISREG(initial.st_mode) or not 0 <= initial.st_size <= _MAX_BYTES:
                    raise ArtifactError("s3_file_invalid")
                actual_sha, size = self._hash_file(body)
                if size != initial.st_size or actual_sha != sha256:
                    raise ArtifactError("s3_file_checksum_mismatch")
                self._check_unchanged(initial, os.fstat(body.fileno()))
                body.seek(0)
                checksum = base64.b64encode(bytes.fromhex(sha256)).decode("ascii")
                response = self._client.put_object(
                    Bucket=self._bucket,
                    Key=key,
                    Body=body,
                    ContentLength=size,
                    ContentType=content_type,
                    ChecksumSHA256=checksum,
                    Metadata={"sha256": sha256},
                )
                self._check_unchanged(initial, os.fstat(body.fileno()))
                returned_checksum = response.get("ChecksumSHA256")
                if returned_checksum is not None and returned_checksum != checksum:
                    raise ArtifactError("s3_remote_checksum_mismatch")
                return StoredObject(
                    key=key, sha256=sha256, size_bytes=size, content_type=content_type
                )
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            code = error.response.get("Error", {}).get("Code", "")
            retryable = status == 429 or status >= 500 or code in _RETRYABLE_CODES
            raise ArtifactError("s3_upload_failed", retryable=retryable) from None
        except (ConnectionError, HTTPClientError):
            raise ArtifactError("s3_unavailable", retryable=True) from None
        except BotoCoreError:
            raise ArtifactError("s3_request_invalid") from None
        except (OSError, ValueError):
            raise ArtifactError("s3_file_unavailable") from None

    @staticmethod
    def _validate_path(path: Path) -> None:
        if not isinstance(path, Path) or not path.is_absolute() or ".." in path.parts:
            raise ArtifactError("s3_file_invalid")
        # Internal paths must not redirect through symlinks or Windows junctions.
        for component in (path, *path.parents):
            if component.is_symlink() or component.is_junction():
                raise ArtifactError("s3_file_invalid")

    @staticmethod
    def _hash_file(body: BinaryIO) -> tuple[str, int]:
        digest = hashlib.sha256()
        size = 0
        while chunk := body.read(_CHUNK_BYTES):
            size += len(chunk)
            if size > _MAX_BYTES:
                raise ArtifactError("s3_file_invalid")
            digest.update(chunk)
        return digest.hexdigest(), size

    @staticmethod
    def _check_unchanged(before: os.stat_result, after: os.stat_result) -> None:
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise ArtifactError("s3_file_changed")
