"""Private diagnostics must not become another credential or content output path."""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pydantic import SecretStr

from app.core.config import Settings
from app.operations import status


def settings() -> Settings:
    return Settings(
        database_url=SecretStr("postgresql+asyncpg://app:private-password@localhost/test"),
        connector_database_url=SecretStr("postgresql+asyncpg://coord:private-pass@localhost/test"),
        redis_url=SecretStr("redis://localhost/0"),
        wecom_corp_id="private-corp",
    )


@pytest.mark.parametrize(
    "value",
    [
        "wecom_95007",
        "wecom_-1",
        "delivery_failure_1",
        "s3_upload_failed",
        "job_timeout",
        "knowledge_upload_outcome_unknown",
        "agent_timeout",
        None,
    ],
)
def test_known_safe_error_is_preserved(value: str | None) -> None:
    assert status.safe_error(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "postgresql://user:password@db/private",
        "api-key-private",
        "private_lowercase_secret",
        "wecom_95007\nprivate",
        "wecom_1234567",
        "delivery_failure_1234",
        "sk-secret-value",
        "https://host/?access_token=private",
        "\x00",
        "",
        "wecom_+1",
        "wecom_１２３",
    ],
)
def test_unknown_errors_are_not_disclosed(value: str) -> None:
    assert status.safe_error(value) == "unrecognized_error"


def test_age_and_utc_timestamp_are_bounded_and_nullable() -> None:
    now = datetime(2026, 10, 2, tzinfo=UTC)
    assert status.age_seconds(None, now) is None
    assert status.timestamp(None) is None
    assert status.age_seconds(now + timedelta(seconds=3), now) == 0
    assert status.age_seconds(now - timedelta(seconds=5.8), now) == 5
    assert status.timestamp(now) == "2026-10-02T00:00:00+00:00"


@pytest.mark.parametrize(
    "arguments",
    [
        ["--dispatch", "sk-sensitive-value"],
        ["--unknown-sensitive-argument"],
        ["--limit", "postgresql://sensitive-value"],
        ["--limit", "0"],
        ["--limit", "101"],
        ["--dispatch"],
        ["sensitive-positional"],
    ],
)
def test_cli_invalid_arguments_emit_only_safe_json(
    arguments: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert status.main(arguments) == 2
    captured = capsys.readouterr()
    assert not captured.out
    assert json.loads(captured.err) == {"error": "invalid_arguments"}


@pytest.mark.parametrize(
    "error",
    [
        ValueError("secret-config-value"),
        OSError("postgresql://secret-db-value"),
        status.StatusError("unknown-secret-value"),
        TimeoutError("secret-timeout"),
    ],
)
def test_cli_runtime_failures_hide_raw_exception(
    error: Exception,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(status, "get_settings", lambda: settings())
    monkeypatch.setattr(status, "run", AsyncMock(side_effect=error))
    assert status.main([]) == 1
    captured = capsys.readouterr()
    assert not captured.out
    assert json.loads(captured.err) == {"error": "status_unavailable"}


def test_cli_success_does_not_claim_business_health(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monitor = AsyncMock(return_value={"business_health": "not_assessed", "sync": {}})
    monkeypatch.setattr(status, "get_settings", lambda: settings())
    monkeypatch.setattr(status, "run", monitor)
    dispatch = uuid4()
    assert status.main(["--dispatch", str(dispatch), "--limit", "3"]) == 0
    captured = capsys.readouterr()
    assert not captured.err
    assert json.loads(captured.out)["business_health"] == "not_assessed"
    assert monitor.call_args.kwargs == {"dispatch_id": dispatch, "limit": 3}


async def test_configuration_rejected_before_database_access() -> None:
    with pytest.raises(status.StatusError, match="invalid_configuration"):
        await status.run(settings().model_copy(update={"connector_database_url": None}))


@pytest.mark.parametrize("limit", [-1, 0, 101, True])
async def test_snapshot_invalid_limits_fail_before_database_access(limit: int) -> None:
    with pytest.raises(status.StatusError, match="invalid_configuration"):
        await status.snapshot(None, None, settings(), limit=limit)


async def test_total_timeout_includes_role_validation_and_disposes_connector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def hanging_database() -> None:
        await asyncio.Event().wait()

    @asynccontextmanager
    async def resources(config: Settings):
        yield SimpleNamespace(database=hanging_database)

    connector = SimpleNamespace(dispose=AsyncMock())
    monkeypatch.setattr(status, "infrastructure", resources)
    monkeypatch.setattr(status, "create_async_engine", lambda *args, **kwargs: connector)
    monkeypatch.setattr(status, "TOTAL_TIMEOUT_SECONDS", 0.01)
    with pytest.raises(TimeoutError):
        await status.run(settings())
    connector.dispose.assert_awaited_once()
