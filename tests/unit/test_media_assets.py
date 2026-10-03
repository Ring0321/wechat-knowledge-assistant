"""Media completion and local derivative path boundaries, without database mocks."""

import hashlib
from pathlib import Path

import pytest

from app.db.models import Source
from app.domain.artifacts import ArtifactError
from app.domain.enums import SourceStatus
from app.ingestion.pipeline import _artifact_hash, _reusable_media


@pytest.mark.parametrize(
    "status,version,outcome,expected",
    [
        (SourceStatus.STORED, 4, "normalized", True),
        (SourceStatus.READY, 5, "normalized", True),
        (SourceStatus.METADATA_ONLY, 5, "no_speech_detected", True),
        (SourceStatus.METADATA_ONLY, 4, "no_speech_detected", False),
        (SourceStatus.METADATA_ONLY, 5, "unreadable", False),
        (SourceStatus.DELETED, 5, "no_speech_detected", False),
    ],
)
def test_reuse_only_successfully_parsed_media(
    status: SourceStatus, version: int, outcome: str, expected: bool
) -> None:
    source = Source(status=status, metadata_={"parser_version": version, "parse_status": outcome})
    assert _reusable_media(source) == expected


def test_asset_hash_cannot_read_parent_traversal(tmp_path: Path) -> None:
    directory = tmp_path / "job"
    directory.mkdir()
    outside = tmp_path / "other-user"
    outside.write_bytes(b"not part of this job")
    with pytest.raises(ArtifactError, match="media_artifact_invalid"):
        _artifact_hash(directory / ".." / outside.name, directory)


def test_asset_hash_accepts_job_file(tmp_path: Path) -> None:
    path = tmp_path / "transcript.json"
    path.write_bytes(b"synthetic")
    assert _artifact_hash(path, tmp_path) == hashlib.sha256(b"synthetic").hexdigest()
