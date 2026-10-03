import json
import logging
from uuid import uuid4

import pytest

from app.core.logging import SafeJsonFormatter


def test_log_drops_sensitive_extras_raw_messages_and_exceptions() -> None:
    record = logging.LogRecord("test", logging.ERROR, "file", 1, "secret %s", ("token",), None)
    record.password = "secret"  # type: ignore[attr-defined]
    record.url = "https://example.com?token=secret"  # type: ignore[attr-defined]
    record.exc_text = "Traceback containing secret"
    output = SafeJsonFormatter().format(record)
    assert "secret" not in output
    assert "token" not in output
    assert json.loads(output)["event"] == "library_log"


def test_known_event_preserves_operational_fields() -> None:
    record = logging.LogRecord("test", logging.INFO, "file", 1, "http_request", (), None)
    record.status_code = 503  # type: ignore[attr-defined]
    request_id = str(uuid4())
    record.request_id = request_id  # type: ignore[attr-defined]
    output = json.loads(SafeJsonFormatter().format(record))
    assert output["event"] == "http_request"
    assert output["status_code"] == 503
    assert output["request_id"] == request_id


@pytest.mark.parametrize(
    "event",
    [
        "agent_invalid_dispatch",
        "agent_completed",
        "agent_publish_retry",
        "agent_worker_retry",
        "agent_worker_failed",
        "knowledge_backfill_queued",
    ],
)
def test_worker_events_keep_actionable_names_without_payloads(event: str) -> None:
    record = logging.LogRecord("test", logging.WARNING, "file", 1, event, (), None)
    record.content = "private-body"  # type: ignore[attr-defined]
    record.count = 3  # type: ignore[attr-defined]
    output = SafeJsonFormatter().format(record)
    assert "private-body" not in output
    assert json.loads(output)["event"] == event
    assert json.loads(output)["count"] == 3


@pytest.mark.parametrize(
    "field", ["request_id", "status_code", "duration_ms", "dependency", "method", "count"]
)
@pytest.mark.parametrize("value", ["private-secret", True, [], {}, -1, float("nan"), float("inf")])
def test_allowlisted_field_names_cannot_smuggle_private_values(field: str, value: object) -> None:
    record = logging.LogRecord("test", logging.ERROR, "file", 1, "http_request", (), None)
    setattr(record, field, value)
    output = json.loads(SafeJsonFormatter().format(record))
    assert field not in output
