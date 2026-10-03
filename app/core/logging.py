"""Allowlisted JSON logs: arbitrary messages, tracebacks and extras are never serialized."""

import json
import logging
import math
from datetime import UTC, datetime
from uuid import UUID

EVENTS = frozenset(
    {
        "http_request",
        "dependency_unavailable",
        "application_started",
        "application_stopped",
        "wecom_sync_failed",
        "wecom_worker_failed",
        "ingestion_invalid_dispatch",
        "ingestion_completed",
        "ingestion_worker_retry",
        "ingestion_publish_retry",
        "ingestion_worker_failed",
        "agent_invalid_dispatch",
        "agent_completed",
        "agent_publish_retry",
        "agent_worker_retry",
        "agent_worker_failed",
        "knowledge_backfill_queued",
    }
)
FIELDS = ("request_id", "status_code", "duration_ms", "dependency", "method", "count")


def _safe_field(key: str, value: object) -> bool:
    if key == "request_id" and isinstance(value, str):
        try:
            return str(UUID(value)) == value
        except ValueError:
            return False
    if key == "status_code":
        return type(value) is int and 100 <= value <= 599
    if key == "count":
        return type(value) is int and 0 <= value <= 1_000_000
    if key == "duration_ms":
        return (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and 0 <= value <= 86_400_000
            and math.isfinite(value)
        )
    if key == "dependency":
        return isinstance(value, str) and value in {"database", "redis"}
    if key == "method":
        return isinstance(value, str) and value in {"GET", "POST", "HEAD", "OTHER"}
    return False


class SafeJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event = (
            record.msg if isinstance(record.msg, str) and record.msg in EVENTS else "library_log"
        )
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "event": event,
        }
        for key in FIELDS:
            value = getattr(record, key, None)
            if _safe_field(key, value):
                payload[key] = value
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(SafeJsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    # Uvicorn's access line can contain user-supplied URLs and query strings.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers = []
        logger.propagate = True
    logging.getLogger("uvicorn.access").disabled = True
