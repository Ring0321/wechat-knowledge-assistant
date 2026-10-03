"""Transport-neutral file contracts. Paths belong to a caller-owned temporary directory."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol
from uuid import UUID


class ArtifactError(Exception):
    def __init__(self, code: str, *, retryable: bool = False) -> None:
        super().__init__(code)
        self.code, self.retryable = code, retryable


@dataclass(frozen=True)
class DownloadedFile:
    path: Path = field(repr=False)
    sha256: str
    size_bytes: int
    content_type: str
    filename: str | None = None


@dataclass(frozen=True)
class StoredObject:
    key: str
    sha256: str
    size_bytes: int
    content_type: str


class MediaDownloader(Protocol):
    async def download(
        self, media_id: str, destination: Path, *, max_bytes: int
    ) -> DownloadedFile: ...


class ObjectStore(Protocol):
    async def put_file(
        self,
        *,
        user_id: UUID,
        source_id: UUID,
        kind: str,
        path: Path,
        sha256: str,
        content_type: str,
    ) -> StoredObject: ...
